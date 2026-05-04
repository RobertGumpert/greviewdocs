package balancers

import (
	"context"
	"errors"
	"math"
	"math/rand/v2"
	"sync"
	"time"
)

type RetryBaseOptions[Input any, Output any] struct {
	Limit      int64
	Backoff    time.Duration
	Jitter     int64
	MaxBackoff time.Duration
}

var ErrRetryLimitReached = errors.New("retries limit reaches")

type Retry[Input any, Output any] struct {
	outputPool    sync.Pool
	resetOutputFn func(*Output)
	opts          RetryBaseOptions[Input, Output]
	retryFn       func(*Input, *Output) (domainErr, retryErr error)
}

func NewRetry[Input any, Output any](
	newOutputFn func() any,
	resetOutputFn func(*Output),
	retryFn func(*Input, *Output) (domainErr, retryErr error),
	opts RetryBaseOptions[Input, Output],
) (*Retry[Input, Output], error) {
	if opts.Jitter <= 0 {
		return nil, errors.New("invalid jitter value")
	}

	rt := Retry[Input, Output]{
		outputPool: sync.Pool{
			New: newOutputFn,
		},
		resetOutputFn: resetOutputFn,
		opts:          opts,
		retryFn:       retryFn,
	}

	return &rt, nil
}

func (r *Retry[Input, Output]) WithPreAllocs(ctx context.Context, input *Input, output *Output) error {
	var attempt int64
	var timer = time.NewTimer(r.opts.MaxBackoff)
	defer func() {
		timer.Stop()
	}()

	for {
		if err := requiredPreAttemptChecks(ctx, attempt, r.opts); err != nil {
			return err
		}

		domainErr, retryErr := r.retryFn(input, output)
		if retryErr == nil {
			return domainErr
		}
		r.resetOutputFn(output)

		attempt++
		delay := calculateNextDelay(attempt, r.opts)
		timer.Reset(delay)

		select {
		case <-timer.C:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// WithLazyAllocs - обязательно потребитель должен после использования
// либо скопировать output либо, убедиться что никуда не убежит и вызвать releaseOutput!
func (r *Retry[Input, Output]) WithLazyAllocs(ctx context.Context, input *Input) (output *Output, releaseOutput func(*Output), err error) {
	var attempt int64
	var timer = time.NewTimer(r.opts.MaxBackoff)
	defer func() {
		timer.Stop()
	}()

	for {
		if err = requiredPreAttemptChecks(ctx, attempt, r.opts); err != nil {
			return nil, nil, err
		}

		output = r.outputPool.Get().(*Output)
		domainErr, retryErr := r.retryFn(input, output)
		if retryErr == nil {
			return output, r.releaseOutput, domainErr
		}
		r.releaseOutput(output)

		attempt++
		delay := calculateNextDelay(attempt, r.opts)
		timer.Reset(delay)

		select {
		case <-timer.C:
		case <-ctx.Done():
			r.releaseOutput(output)
			return nil, nil, ctx.Err()
		}
	}
}

func (r *Retry[Input, Output]) releaseOutput(output *Output) {
	r.resetOutputFn(output)
	r.outputPool.Put(output)
}

func requiredPreAttemptChecks[Input any, Output any](ctx context.Context, attempt int64, opts RetryBaseOptions[Input, Output]) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	// если < 0 то значит до упора пытаемся выполнить
	if opts.Limit > 0 && attempt > opts.Limit {
		return ErrRetryLimitReached
	}

	return nil
}

func calculateNextDelay[Input any, Output any](attempt int64, opts RetryBaseOptions[Input, Output]) time.Duration {
	// экспонентно растем в задержке - Backoff * 2^attempt
	delay := time.Duration(float64(opts.Backoff) * math.Pow(2, float64(attempt)))
	if delay > opts.MaxBackoff {
		delay = opts.MaxBackoff
	}

	// берем рандомную часть от delay - это и есть jitter
	jitter := rand.Int64N(int64(delay) / opts.Jitter)
	delay += time.Duration(jitter)

	return delay
}
