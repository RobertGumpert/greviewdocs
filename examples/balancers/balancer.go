package balancers

import (
	"bytes"
	"context"
	"errors"
	"io"
	"math"
	"math/rand"
	"net"
	"os"
	"sync/atomic"
	"time"
)

// Оcтается как есть
type Connection interface {
	Send(ctx context.Context, req io.Reader, resp io.Writer) error
	Close() error
}

/*
Мы не знаем как именно создается соединение, мы лишь знаем что
на выходе должны получить новый объект реализующий:

	type Connection interface {
		Send(ctx context.Context, req io.Reader, resp io.Writer) error
		Close() error
	}

Но мы знаем хост до которого надо создать Connection, пускай
при создании ConnetionPool, пользователь скроет детали, а мы уже
будем использовать готовый объект Connection'а
*/
type ConnectCallback func(ctx context.Context, ip net.IP) (Connection, error)

/*
Если ConnetionPool гарантирует, что он выполнит ретрай,
ему нужно как-то начать чтение данных вновь.
*/
type ReadCloserFabric func() (io.ReadCloser, error)

/*
Если пользователь ConnetionPool пытается пропихнуть обычный JSON
то просто пересоздаем bytes.Reader, backing array
JSON остается в куче, никакого глубокого копирования
просто создается еще один объект bytes.Reader

ВАЖНО: bytes.Reader на Read не делает "реслайсинга",
в отличии от bytes.Buffer, который на Read уменьшает
размер слайса, но так как мы пересоздаем ридеров заливая
им исходный слайс data, плевать что именно использовать :)
Оданко bytes.Buffer - это поток, труба если хочешь,
то есть данные залетили с одного конца и вылетили с другого
поэтому bytes.Buffer использовать как основу для ReadCloserFabric
выглядит несколько нелогчино :)
*/
func BytesReaderFabric(data []byte) ReadCloserFabric {
	return func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(data)), nil
	}
}

/*
Чтение файла и ретрай запроса, несколько несвоместимые
друг с другом вещи, так как ретрай будет с использованием
Jitter'а а значит дексриптор будет открыт пока мы сидим и спим :)
*/
func FileReaderFabric(path string) ReadCloserFabric {
	return func() (io.ReadCloser, error) {
		f, err := os.Open(path)
		if err != nil {
			return nil, err
		}
		return f, nil
	}
}

// Этот конфиг будет у каждого ConnetionPool
type ConnetionPoolConfig struct {
	Requests struct {
		// Сколько раз разрешаем выполнить ретрай запроса
		Retry struct {
			AttemptsMax int
		}
	}

	Responses struct {
		Buffer struct {
			InitSize int
			MaxSize  int
		}
	}

	Connections struct {
		Timeout time.Duration

		// Если ConnetionPool гарантирует, что он выполнит ретрай,
		// а автор решил что ретрай выполняется только через реконнект.
		// Настройки экспонециального Jitter'а
		// sleep = JitterDurBase * (attempt ^ 2)
		// sleep = rand(0, sleep)
		Jitter struct {
			DurBase     time.Duration
			DurMax      time.Duration
			AttemptsMax int
		}

		Active struct {
			Number int
		}
		Idle struct {
			Number int
		}
	}

	// Любой ConnetionPool это своего рода Арена
	// в которой горутины дерутся за ресурся друг с другом
	// в данном слечае за право овладеть Connection'ом
	// а значит должны быть правило на этой самой арене
	// которым нужно следовать всем :)
	Arena struct {
		// Сколько попыток допускаем для захвата Connection'а
		AttemptsMax int
		// Сколько времени заставляем ждать до следующей попытки
		WaitDur time.Duration
	}
}

// собственно за этим будем скрывать реализацию
type ConnetionPool interface {
	// IdempotentRequest
	// То есть мы можем позволить себе выпонить ретрай, при обрывах
	// Теперь представь что будет если user code выполняет
	// запрос на списание денежных средств, бэкенд зафиксировал,
	// а клиентский Connection сломался:
	// 		то читай про At-Least-Once delivery
	// Здесь мы должны не разгребать последствия бага в Connection,
	// а должны гарантировать retry запроса :)
	//
	// Но если user code намерено использован этот метод на запрос
	// которой может вызвать изменение состояния - это уже его проблема :)
	// Однако такая же отвественность лежит и на принимающей стороне
	// если в Connection "по приколу" после чтения 1 байта ответа
	// отваливает ошибку, а этот метод transport'а ретраит запрос - это проблема
	// принимающей стороны что она не реализовала никаких механизмов защиты
	IdempotentRequest(ctx context.Context, reqContentFabric ReadCloserFabric) (resp io.ReadCloser, err error)
	NonIdempotentRequest(ctx context.Context, reqContent io.ReadCloser) (resp io.ReadCloser, err error)
}

var errRetryAttemptsLimitReached = errors.New("limit of retry attempts was reached")
var errConnectionWasClosed = errors.New("connection was closed")

/*
Каждый ConnetionPool будет оперировать этими объектами
обертками вокруг обеъкта Connection чтобы скрыть логику
рекннокта.
*/
type transport struct {
	host       net.IP
	connectFn  ConnectCallback
	conn       Connection
	closed     atomic.Bool
	randomizer *rand.Rand
}

func configureTransport(
	t *transport,
	host net.IP,
	connectFn ConnectCallback,
) error {
	if connectFn == nil || len(host) == 0 {
		return errors.New("invalid transport settings")
	}

	t.host = host
	t.connectFn = connectFn
	t.closed.Store(true)
	t.randomizer = rand.New(rand.NewSource(time.Now().UnixNano()))

	return nil
}

func (t *transport) connectWithJitterRetry(
	ctx context.Context,
	jitterMaxAttempts int,
	jitterBase, jitterMax time.Duration,
) error {
	if !t.closed.Load() {
		return nil
	}

	var attempts int
	var dnsErr *net.DNSError
	var addrErr *net.AddrError
	var netUnknown *net.UnknownNetworkError

	timer := time.NewTimer(0)
	defer func() {
		_ = timer.Stop()
	}()

	for {

		if jitterMaxAttempts > 0 {
			if attempts == jitterMaxAttempts {
				return errRetryAttemptsLimitReached
			}
		}

		err := t.connect(ctx)
		if err == nil {
			return nil
		}

		// адрес просто не тот, нет смысла долбить reconnect'ами :)
		if errors.As(err, &addrErr) {
			return err
		}

		if errors.As(err, &dnsErr) {
			// это фатальная ошибка, DNS записи просто нет
			// нет смысла долбить reconnect'ами :)
			if dnsErr.IsNotFound {
				return err
			}
		}

		if errors.As(err, &netUnknown) {
			return err
		}

		attempts++

		sleep := t.jitter(attempts, jitterBase, jitterMax)
		timer.Reset(sleep)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func (t *transport) connect(ctx context.Context) error {
	if !t.closed.Load() {
		return nil
	}

	conn, err := t.connectFn(ctx, t.host)
	if err != nil {
		return err
	}

	t.conn = conn
	t.closed.Store(false)

	return nil
}

func (t *transport) close() error {
	if t.closed.Load() {
		return errConnectionWasClosed
	}

	_ = t.conn.Close()
	t.closed.Store(true)

	return nil
}

func (t *transport) jitter(attempt int, base, max time.Duration) time.Duration {
	delay := time.Duration(float64(base) * math.Pow(float64(attempt), 2))
	if delay > max {
		return max
	}

	return time.Duration(t.randomizer.Int63n(int64(delay)))
}
