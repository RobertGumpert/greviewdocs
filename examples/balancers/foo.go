package balancers

import (
	"errors"
	"sync"
	"time"
)

type circuitBreakerStatus int64

const (
	// Если ты хотя бы раз в жизни делал ремонт (вот автор делал, поэтому
	// небольшой ликбез), то должен знать что существуют автоматы стоящие
	// на входе в квартиру. Они бывают разными A,B,C,D. Каждый из них делает
	// одно и тоже - ловит короткое замыкание. Быстрый ликбез:
	// 1) есть фаза - провод, по нему ток заходит в квартиру с силой N ампер
	// 2) есть ноль - провод, по нему ток уходит из квартиры с силой N ампер
	// Если вдруг на фазе ток стал N*2 и сразу же на нуле сила тока
	// стала равна N*2, то бьем тревогу - короткое замыкание. При увелечении
	// силы тока, провода начинают греться и если буквально их не разорвать, сила тока
	// будет расти по экспоненте и нагрев пркатически мгновенно приведет к возгаранию
	// изоляции. То есть в норме у тебя всегда и только всегда сила тока в фазе
	// равна силе тока в нуле. Так вот:
	// 1) тип A с номиналом пускай 63 ампера, реагирует на увелечение силы тока
	//	  на 30%, то есть реагирует на 81.9 ампер
	// 2) тип B с номиналом пускай 63 ампера, реагирует на увелечение силы тока
	//	  на 200%, то есть реагирует на 189 ампер
	// то есть каждый из типов делает главное: размыкает оба провода в квартире
	// от провода, который введен в дом как основной, для всех.
	// Бывают еще УЗО автоматы, они реагируют на ситуацию когда,
	// разница силы тока между фазой и нулем становится критически маленькой -
	// это когда ты дотронулся до фазы, ток начал убегать в тебя, а по нулю
	// он перестал убегать из квартиры, так как ты не дотрунулся до нуля, и как
	// следствие сила тока в нуле начала падать при неизменной силе тока в фазе.
	//
	// Так вот человек который когда-то очень давно придумал CircuitBreaker явно
	// делал ремонт, потому что Circuit Breaker - во всем мире (англоязычном)
	// и есть то, что мы называем "Пробками" или "Автоматами". Работает он примерно
	// также как и автомат: мы хотим защитить сервис стоящий перед CircuitBreaker'ом
	// от "перегрева" :). В реальной жизни при срабатывании автомата ты пойдешь и
	// дернешь рычаг, но CircuitBreaker опускает и поднимает рычаг сам, автоматически.
	// Про принципу своей работы он напоминает УЗО автомат:
	// "ты допускаешь, что сервис может сыпать не больше 30 ошибок за последнюю секунду".
	// Если хотя бы 16% запросов (то есть ~5) вернули ошибку, то "дергаем" рубильник :)
	//
	// circuitBreakerStatusOpen - cостояние автомата когда "цепь" разомкнута, то есть запросы
	// в сервис мы отклоняем, так как 16% запросов за последнюю секунду завершились ошибками.
	circuitBreakerStatusOpen = iota
	// circuitBreakerStatusHalfOpen - cостояние автомата когда "цепь" как бы "немного замкнута".
	// Сервис только что был в состоянии Open. То есть это своего рода "испытальный срок" для сервиса:
	// если начнет греться, снова уводим в состоянии Open. Задача CircuitBreaker пропускать только
	// определенный процент запросов. Здесь математика немного должна отличаться, так как мы
	// пропускаем только часть запросов и если хотя бы один вернет ошибку - снова отправляем в Open.
	// При этом для тех запросов, которые не попали в пробное тестиование так и отдается CircuitBreakerStatusOpen
	circuitBreakerStatusHalfOpen
	// circuitBreakerStatusClosed - cостояние автомата когда "цепь" замкнута и сервис вообще
	// практически не сыпит ошибками, то есть порог в 16% не превышен.
	circuitBreakerStatusClosed
)

var (
	ErrCircuitBreakerOpen                   = errors.New("(cb) circuit breaker: was opened")
	ErrCircuitBreakerInFligthLimitIsReached = errors.New("(cb) circuit breaker: limit of in fligth actions was reached")
)

type circuitBreakerActionFn[Input any, Output any] func(*Input, *Output) error

type CircuitBreakerConfig struct {
	Metrics struct {
		MaxActionsInFligth       int
		MaxFailureActionsPercent int
	}
	OnOpened struct {
		Deadline time.Duration
	}
	OnHalfOpened struct {
		Deadline         time.Duration
		MaxProbeFailures int
		ProbesInterval   time.Duration
	}
}

type CircuitBreaker[Input any, Output any] struct {
	mu sync.Mutex

	status  circuitBreakerStatus
	window  slidingWindow
	metrics circuitBreakerMetrics

	openDeadline     int64
	halfOpenDeadline int64

	config CircuitBreakerConfig
	action circuitBreakerActionFn[Input, Output]
}

func (b *CircuitBreaker[Input, Output]) DoActionWithPreAllocs(input *Input, output *Output) error {
	startedAt := time.Now().UnixNano()
	b.checkAllowed(startedAt)

	return nil
}

func (b *CircuitBreaker[Input, Output]) checkAllowed(now int64) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	switch b.status {
	case circuitBreakerStatusOpen:
		// проверяем закончился ли таймаут
		// на ожидание сервиса
		err := b.checkAllowedOnOpened(now)
		if err != nil {
			// увы еще нет, так что выходим и отдаем ошибку
			return err
		}

		return nil
	case circuitBreakerStatusHalfOpen:
		return nil

	case circuitBreakerStatusClosed:
		// проверяем достигли ли лимитов в скользящем окне:
		// 1) макс. процента ошибок
		// 2) макс. одновременно выполняющихся запросов
		err := b.checkAllowedOnClosed()
		if err != nil {
			// все ок, инкрементируем метрики в скользящем окне:
			// 1) кол-во одновременно выполняющихся запросов
			// 2) кол-во запущенных запросов
			b.preparedOnClosed(now)
			return nil
		}

		// увы, лимиты достигли, так что дергаем рубильник :)
		b.openBreaker(now)
		// отдаем одну из ошибок которую поймали при проверках
		return err
	}

	return nil
}

func (b *CircuitBreaker[Input, Output]) checkAllowedOnClosed() error {
	if b.inFligthLimitIsReached() {
		return ErrCircuitBreakerInFligthLimitIsReached
	}
	if b.failuresLimitIsReached() {
		return ErrCircuitBreakerOpen
	}

	return nil
}

func (b *CircuitBreaker[Input, Output]) preparedOnClosed(now int64) {
	b.metrics.addActionInFligth(1)
	b.window.addActionStarted(now)
}

func (b *CircuitBreaker[Input, Output]) inFligthLimitIsReached() bool {
	var inFligth = b.metrics.loadInFligth()
	var maxInFligth = b.config.Metrics.MaxActionsInFligth

	if inFligth >= maxInFligth {
		return true
	}

	return false
}

func (b *CircuitBreaker[Input, Output]) failuresLimitIsReached() bool {
	var failures = b.window.metrics.loadFailures()
	var successes = b.window.metrics.loadSuccesses()
	var percent = int((float32(failures) / float32(failures+successes)) * 100)
	var maxPercent = b.config.Metrics.MaxFailureActionsPercent

	if percent >= maxPercent {
		return true
	}

	return false
}

func (b *CircuitBreaker[Input, Output]) checkAllowedOnOpened(now int64) error {
	if now < b.openDeadline {
		return ErrCircuitBreakerOpen
	}

	return nil
}

func (b *CircuitBreaker[Input, Output]) openBreaker(now int64) {
	b.openDeadline = now + int64(b.config.OnOpened.Deadline)
	b.status = circuitBreakerStatusOpen
	b.metrics = circuitBreakerMetrics{}
}

func (b *CircuitBreaker[Input, Output]) halfOpenBreaker(now int64) {
	// сбрасываем таймаут для открытого breaker
	b.openDeadline = 0

	b.halfOpenDeadline = now + int64(b.config.OnHalfOpened.Deadline)
	b.status = circuitBreakerStatusHalfOpen
}

// const defaultNumberBuckets = 3

// type circuitBreakerBucket struct {
// 	numberStarted   int
// 	numberFinished  int
// 	numberFailures  int
// 	numberSuccesses int
// }

// type circuitBreakerSlidingWindow struct {
// 	startTime, endTime int64 // time.UnixNano
// 	bucketSize         int64 // (endTime - startTime) / numberBuckets
// 	head, tail         int
// 	buckets            []circuitBreakerBucket // numberBuckets + 1
// }

// func (sw *circuitBreakerSlidingWindow) resetWithNewSettings(
// 	startTime, windowSize int64,
// 	numberBuckets int,
// 	limitFailures int,
// ) {
// 	sw.head = 0
// 	sw.tail = numberBuckets - 2
// 	sw.startTime = startTime
// 	sw.endTime = startTime + windowSize
// 	sw.buckets = sw.buckets[:0]

// 	for idx := 0; idx < numberBuckets+1; idx++ {
// 		sw.buckets = append(sw.buckets, circuitBreakerBucket{
// 			numberStarted:   0,
// 			numberFinished:  0,
// 			numberFailures:  0,
// 			numberSuccesses: 0,
// 		})
// 	}
// }

// type CircuitBreakerConfig struct {
// 	Open struct {
// 		DeadlineInterval time.Duration
// 	}
// 	HalfOpen struct {
// 		SlidingWindowSize    time.Duration
// 		SlidingWindowBuckets int
// 		NumberFailureProbes  int
// 	}
// 	Close struct {
// 		SlidingWindowSize    time.Duration
// 		SlidingWindowBuckets int
// 		PercentFailures      int // 0-100
// 		PercentSlowCalls     int // 0-100
// 	}
// }

// type CircuitBreaker struct {
// 	mu sync.Mutex

// 	status           circuitBreakerStatus
// 	setHalfOpenAfter int64 // time.UnixNano()
// 	window           circuitBreakerSlidingWindow

// 	config CircuitBreakerConfig
// }

// func (sb *CircuitBreaker) Allow() error {
// 	sb.mu.Lock()
// 	defer sb.mu.Unlock()

// 	now := time.Now().UnixNano()

// 	switch sb.status {
// 	case circuitBreakerStatusOpen:

// 		if now <= sb.setHalfOpenAfter {
// 			return ErrCircuitBreakerOpen
// 		}

// 		sb.status = circuitBreakerStatusHalfOpen

// 		sb.window.resetWithNewSettings(
// 			now,
// 			int64(sb.config.HalfOpen.SlidingWindowSize),
// 			sb.config.HalfOpen.SlidingWindowBuckets,
// 			sb.config.HalfOpen.NumberFailureProbes,
// 		)

// 		fallthrough

// 	case circuitBreakerStatusHalfOpen:

// 		if sb.window.bucketsIsExpired(now) {

// 		}

// 		sb.status = circuitBreakerStatusClosed

// 		sb.window.resetWithNewSettings(
// 			now,
// 			int64(sb.config.HalfOpen.SlidingWindowSize),
// 			sb.config.HalfOpen.SlidingWindowBuckets,
// 			sb.config.HalfOpen.NumberFailureProbes,
// 		)

// 		fallthrough

// 	case circuitBreakerStatusClosed:

// 	}

// 	return nil
// }

// func (sb *CircuitBreaker) aggregateWindow(stat *circuitBreakerBucket) {

// 	for idx := range sb.window.buckets {
// 		stat.numberStarted += sb.window.buckets[idx].numberStarted
// 		stat.numberFinished += sb.window.buckets[idx].numberFinished
// 		stat.numberFailures += sb.window.buckets[idx].numberFailures
// 		stat.numberSuccesses += sb.window.buckets[idx].numberSuccesses
// 	}

// }
