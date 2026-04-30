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
)

// circuitBreakerActionFn 
// Пользователь в callback сам определяет что для  
// него является критичной ошибкой, тут полный контроль за ним,
// то есть архитектурно, совершенно не защищает 
// от ложнополжительных результатов  - факт. 
// Тут вопрос в разделении ответсвенности, на мой взгляд CircuitBreaker
// не должен решать что именно является критичным, 
// зато он обязан обеспечить защиту ресурса. 
type circuitBreakerActionFn[Input any, Output any] func(*Input, *Output) (domainErr, breakerErr error)

type CircuitBreakerConfig struct {
	SlidingWindow struct {
		BucketSize time.Duration
		WindowSize time.Duration
	}
	Metrics struct {
		MaxActionsInFligth       int
		MaxFailureActionsPercent int
		MaxProbeFailures         int
		MaxNumberProbes          int
	}
	OnClose struct {
		WindowSize time.Duration
		BucketSize time.Duration
	}
	OnOpened struct {
		Deadline time.Duration
	}
	OnHalfOpened struct {
		ProbesInterval time.Duration
	}
}

type CircuitBreaker[Input any, Output any] struct {
	mu sync.Mutex

	status       circuitBreakerStatus
	epochCounter uint64
	window       nonThreadSafeSlidingWindow
	circuitBreakerMetrics

	openDeadline int64 // time.UnixNano
	nextProbeTs  int64 // time.UnixNano

	config CircuitBreakerConfig
	action circuitBreakerActionFn[Input, Output]
}

// DoActionWithGreedyAllocs
// Жадная реализация, так как ожидает input *Input, output *Output, которые могли
// быть пользователем уже аллоцированы снаружи, еще до проверки доступности, а следовательно
// аллокации (если они есть снаружи) выполнены вхолустую, если CircuitBreaker не проспустит
// запрос
func (b *CircuitBreaker[Input, Output]) DoActionWithGreedyAllocs(input *Input, output *Output) (domainErr, breakerErr error) {
	startedAt := time.Now().UnixNano()
	savedStatus, savedEpochCounter, err := b.allowedAction(startedAt)
	if err != nil {
		return nil, err
	}

	domainErr, breakerErr = b.action(input, output)
	finishedAt := time.Now().UnixNano()

	var isFailure bool
	if breakerErr != nil {
		isFailure = true
	}
	b.afterActionReport(startedAt, finishedAt, isFailure, savedStatus, savedEpochCounter)

	return domainErr, breakerErr
}

func (b *CircuitBreaker[Input, Output]) afterActionReport(
	startedAt, finishedAt int64,
	isFailure bool,
	savedStatus circuitBreakerStatus,
	savedEpochCounter uint64,
) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// всегда уменьшаем InFligth
	b.inFlight--

	// Такое возможно только если:
	// был Close    -> стал Open или HalfOpen
	// был HalfOpen -> стал Open или снова стал HalfOpen
	if b.epochCounter-savedEpochCounter > 0 {
		// если был Close наивно надеимся на то что
		// окно не убежало вперед и пытаемся зафиксировать результат
		if savedStatus == circuitBreakerStatusClosed {

			isSlow := (finishedAt - startedAt) > b.window.windowSizeDur
			if isSlow {
				// честно говоря, автор хз что именно с такими запросами делать,
				// потому что ловить их сделать и учитывать их в статистике для
				// принятие решение "Открывать CircuitBreaker" - это выстрел
				// себе в ногу, так представь у тебя вот такое окно:
				//
				// 		       [head]               [tail]
				// 		[25,27][15,17][17,19][19,21][23,25]
				//		   |
				//		overflow
				//
				// потом оно провернулось:
				//
				// 		       [head]               [tail]
				// 		[35,37][37,39][29,31][31,33][33,35]
				//		   |
				//		overflow
				//
				// и тут херакс прилетает вот это чудо - isSlow на 35 ts, а теперь представь
				// если все запросы такие, потому что сервису плохо? Получится так
				// что мы продолжим сервис бомбить запросами и все они также будут зависать,
				// что в таком случае делать?
				// Вот поэтому есть метрика CircuitBreaker.inFligthCalls -
				// метрика кол-ва запросов выполняющихся одновременно, которая
				// не зависит от окна и его метрик, и полностью подконтрольна
				// только самому CircuitBreaker'у. Ну и cbSlidingWindow понятия не имеет
				// об этой метрики, да и о CircuitBreaker'е, так что для окна вот такие isSlow
				// запросы - это выбросы, которые на уровне окна корректно разрешить просто
				// невозможно. Именно поэтому Return так как  "> w.windowSize" :)
				// Окно уже точно провернулось и как следствие, ничего не знает о прошедшей
				// эпохе и изменять текущую, ничего не зная о прошлой - нельзя, ж**а будет
				// статистике :)
				return
			}

			// если был Close и action не является медленным,
			// то пытаемся зафиксировать результат
			b.window.AddFinishedAndMoveRingIfNeeds(startedAt, finishedAt, isFailure)
		}
		// если был HalfOpen, то ловить нам уже нечеге
		// метрики самого CircuitBreaker уже точно были сброшены :)
		return
	}

	// если за время выполнения action'а
	// статус breaker'а не изменился,
	// то действуем по общему плану

	switch b.status {
	case circuitBreakerStatusClosed:
		b.window.AddFinishedAndMoveRingIfNeeds(startedAt, finishedAt, isFailure)

	case circuitBreakerStatusHalfOpen:
		b.finishedProbes++
		if isFailure {
			b.failureProbes++
		}
	}
}

func (b *CircuitBreaker[Input, Output]) allowedAction(startedAt int64) (circuitBreakerStatus, uint64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	switch b.status {
	case circuitBreakerStatusOpen:
		// проверяем лимиты и
		// временные границы таймаута
		if startedAt < b.openDeadline || b.limitInFlightActionsReached() {
			return b.status, b.epochCounter, ErrCircuitBreakerOpen
		}

		// частично открываем breaker'а
		b.breakerToHalfOpen(startedAt)

		fallthrough // прыгаем в HalfOpen

	case circuitBreakerStatusHalfOpen:
		// всегда в первую очередь проверяем,
		// исчерпан ли лимит ошибок в пробах или
		// лимит на InFlight
		if b.limitFailuresProbesReached() || b.limitInFlightActionsReached() {
			// увы, лимиты достигнуты, так что снова
			// открывать рубильник :)
			b.breakerToOpen(startedAt)
			return b.status, b.epochCounter, ErrCircuitBreakerOpen
		}

		allProbesFinished := (b.finishedProbes == b.startedProbes) && b.startedProbes > 0
		// лимит ошибок в пробах еще не исчерпан,
		// и не все пробы еще завершились, так что
		// пробуем запустить еще одну пробу, если
		// не исчерпали лимит на кол-во проб
		if !allProbesFinished {
			// если лимит на кол-во проб исчерпан, то
			// увы отдаем ошибку, а CircuitBreaker "уходит
			// ждать запущенные пробы".
			if b.startedProbes >= b.config.Metrics.MaxNumberProbes {
				return b.status, b.epochCounter, ErrCircuitBreakerOpen
			}

			// если следующая проба должна была
			// запуститься уже после текущего времени
			// даем шанс быть участником проб
			if startedAt >= b.nextProbeTs {
				b.inFlight++
				b.nextProbeTs = startedAt + int64(b.config.OnHalfOpened.ProbesInterval)
				b.startedProbes++
				return b.status, b.epochCounter, nil
			}

			// иначе отдаем ошибку
			return b.status, b.epochCounter, ErrCircuitBreakerOpen
		}

		// иначе время "испытательного срока" закончилось
		// пора закрывать рубильник :) как видишь алгоритм
		// жесткие для потребителя - он не прыгает в Closed
		// только потому, что закончился "срок реальный", он дожидается
		// момента когда все пробы завершатся, так как повисшие пробы 
		// не являются фактором "отличного здоровья" ресурса, который
		// мы защищаем с помощью CircuitBreaker. Если пользователь не
		// позаботился о прерывании медленных action'ов, а CircuitBreaker
		// будет "забивать" на повисшие пробы, по истечению N-ого времени
		// и закрывать рубильник (Closed), то архитектурно такое решение 
		// будет гигантской черной дырой выпитывающей все больше 
		// зависающих action'ов окончтельно добивая ресурс. Так мы не только
		// усложним логику, но и не выполним главного требования - защищать
		// ресурс любой ценой
		b.breakerToClosed(startedAt)

		fallthrough // прыгаем в Close

	case circuitBreakerStatusClosed:
		// проверяем достигли ли лимитов в скользящем окне:
		// 1) макс. процента ошибок
		// 2) макс. одновременно выполняющихся запросов
		if b.limitFailuresActionsReached() || b.limitInFlightActionsReached() {
			// увы, лимиты достигли, так что дергаем рубильник :)
			b.breakerToOpen(startedAt)
			return b.status, b.epochCounter, ErrCircuitBreakerOpen
		}

		// все ок, инкрементируем метрики в скользящем окне:
		// 1) кол-во одновременно выполняющихся запросов
		// 2) кол-во запущенных запросов
		b.inFlight++
		b.window.AddStartedAndMoveRingIfNeeds(startedAt)
	}

	return b.status, b.epochCounter, nil
}

func (b *CircuitBreaker[Input, Output]) breakerToOpen(nowTs int64) {
	b.openDeadline = nowTs + int64(b.config.OnOpened.Deadline)
	b.nextProbeTs = 0
	b.circuitBreakerMetrics = circuitBreakerMetrics{
		inFlight:       b.inFlight,
		failureProbes:  0,
		startedProbes:  0,
		finishedProbes: 0,
	}
	b.status = circuitBreakerStatusOpen
	b.epochCounter++

	b.window.syncBucketsRing(nowTs)
}

func (b *CircuitBreaker[Input, Output]) breakerToHalfOpen(nowTs int64) {
	b.openDeadline = 0
	b.nextProbeTs = nowTs + int64(b.config.OnHalfOpened.ProbesInterval)
	b.circuitBreakerMetrics = circuitBreakerMetrics{
		inFlight:       b.inFlight,
		failureProbes:  0,
		startedProbes:  0,
		finishedProbes: 0,
	}
	b.status = circuitBreakerStatusHalfOpen
	b.epochCounter++

	b.window.syncBucketsRing(nowTs)
}

func (b *CircuitBreaker[Input, Output]) breakerToClosed(nowTs int64) {
	b.openDeadline = 0
	b.nextProbeTs = 0
	b.circuitBreakerMetrics = circuitBreakerMetrics{
		inFlight:       b.inFlight,
		failureProbes:  0,
		startedProbes:  0,
		finishedProbes: 0,
	}
	b.status = circuitBreakerStatusClosed
	b.epochCounter++

	b.window.syncBucketsRing(nowTs)
}

func (b *CircuitBreaker[Input, Output]) limitInFlightActionsReached() bool {
	if b.inFlight >= b.config.Metrics.MaxActionsInFligth{
		return true
	}

	return false
}

func (b *CircuitBreaker[Input, Output]) limitFailuresActionsReached() bool {
	var finished = b.window.loadFinished()
	if finished == 0 {
		return false
	}
	
	var failures = b.window.loadFailures()
	var percent = int((float32(failures) / float32(finished)) * 100)
	var maxPercent = b.config.Metrics.MaxFailureActionsPercent

	if percent >= maxPercent {
		return true
	}

	return false
}

func (b *CircuitBreaker[Input, Output]) limitFailuresProbesReached() bool {
	if b.failureProbes >= b.config.Metrics.MaxProbeFailures {
		return true
	}

	return false
}

type circuitBreakerMetrics struct {
	inFlight       int
	failureProbes  int
	startedProbes  int
	finishedProbes int
}

type windowMetrics struct {
	started   int
	finished  int
	failures  int
	successes int
}

func (m *windowMetrics) addActionStarted(n int) {
	m.started += n
}

func (m *windowMetrics) addActionFinished(n int) {
	m.finished += n
}

func (m *windowMetrics) addActionFailures(n int) {
	m.failures += n
}

func (m *windowMetrics) addActionSuccesses(n int) {
	m.successes += n
}

func (m *windowMetrics) loadStarted() int {
	return m.started
}

func (m *windowMetrics) loadFinished() int {
	return m.finished
}

func (m *windowMetrics) loadFailures() int {
	return m.failures
}

func (m *windowMetrics) loadSuccesses() int {
	return m.successes
}

type nonThreadSafeBucket struct {
	startTs, endTs int64 // time.UnixNano
	windowMetrics
}

type nonThreadSafeSlidingWindow struct {
	startTs, endTs int64 // time.UnixNano
	windowSizeDur  int64 // time.Duration
	bucketSizeDur  int64 // time.Duration
	head, tail     int
	buckets        []nonThreadSafeBucket
	// предобработанные метрики
	windowMetrics
}

func newNonThreadSafeSlidingWindow(
	windowSizeDur time.Duration,
	bucketSizeDur time.Duration,
) nonThreadSafeSlidingWindow {
	now := time.Now()

	startTime := now.UnixNano()
	endTime := now.Add(windowSizeDur).UnixNano()
	// +1 на overflowbucket
	countBuckets := (int64(windowSizeDur) / int64(bucketSizeDur)) + 1
	buckets := make([]nonThreadSafeBucket, 0, countBuckets)

	var bucketStartTime int64 = startTime
	for idx := int64(0); idx < countBuckets; idx++ {
		buckets = append(buckets, nonThreadSafeBucket{
			startTs:       bucketStartTime,
			endTs:         bucketStartTime + int64(bucketSizeDur),
			windowMetrics: windowMetrics{},
		})
		bucketStartTime += int64(bucketSizeDur)
	}

	return nonThreadSafeSlidingWindow{
		startTs:       startTime,
		endTs:         endTime,
		windowSizeDur: int64(windowSizeDur),
		bucketSizeDur: int64(bucketSizeDur),
		head:          0,
		tail:          int(countBuckets - 2),
		buckets:       buckets,
		windowMetrics: windowMetrics{},
	}
}

func (w *nonThreadSafeSlidingWindow) getBucketIdIfExist(now int64) (int64, bool) {
	if w.startTs > now {
		return -1, false
	}

	idx := (now - w.startTs) / w.bucketSizeDur
	idx = (int64(w.head) + idx) % int64(len(w.buckets))
	return idx, true
}

func (w *nonThreadSafeSlidingWindow) syncBucketsRing(now int64) {
	// здесь уже знакомая нам схема с overflowbucket :)
	//
	// 		[head]               [tail]
	// 		[13,15][15,17][17,19][19,21][23,25]
	//									   |
	//								    overflow
	// см. подробно в SlidingWindow[Storage any, Metrics any, Aggregate any]
	//
	if now < w.endTs {
		return
	}

	numResetBuckets := (now - w.endTs) / w.bucketSizeDur
	if numResetBuckets >= int64(len(w.buckets)) {

		startTs := now
		for idx := 0; idx < len(w.buckets); idx++ {
			w.resetWindowBucket(
				&w.buckets[w.head],
				startTs,
				startTs+w.bucketSizeDur,
			)

			startTs += w.bucketSizeDur
		}

		w.windowMetrics = windowMetrics{}
		w.startTs = now
		w.endTs = now + w.windowSizeDur
		w.head = 0
		w.tail = len(w.buckets) - 2

		return
	}

	startTime := now - (w.bucketSizeDur * numResetBuckets)
	for idx := int64(0); idx < numResetBuckets; idx++ {
		// раз бакет прокис, значит его метрики вычитаем из
		// предобработанных метрик - preprocessed :)
		w.addActionStarted(-1 * w.buckets[w.head].loadStarted())
		w.addActionFinished(-1 * w.buckets[w.head].loadFinished())
		w.addActionFailures(-1 * w.buckets[w.head].loadFailures())
		w.addActionSuccesses(-1 * w.buckets[w.head].loadSuccesses())

		w.resetWindowBucket(
			&w.buckets[w.head],
			startTime,
			startTime+w.bucketSizeDur,
		)

		w.head = (w.head + 1) % len(w.buckets)
		w.tail = (w.tail + 1) % len(w.buckets)
		startTime += w.bucketSizeDur
	}

	w.endTs = now
	w.startTs = now - (w.bucketSizeDur * int64(len(w.buckets)-2))
}

func (w *nonThreadSafeSlidingWindow) resetWindowBucket(bucket *nonThreadSafeBucket, startTs, endTs int64) {
	bucket.windowMetrics = windowMetrics{}
	bucket.startTs = startTs
	bucket.endTs = endTs
}

func (w *nonThreadSafeSlidingWindow) addMetricsOnActionStarted(metrics *windowMetrics, started int) {
	metrics.addActionStarted(started)
}

func (w *nonThreadSafeSlidingWindow) addMetricsOnActionFinished(metrics *windowMetrics, failures, successes, finished int) {
	metrics.addActionFailures(failures)
	metrics.addActionSuccesses(successes)
	metrics.addActionFinished(finished)
}

func (w *nonThreadSafeSlidingWindow) AddStartedAndMoveRingIfNeeds(startedAt int64) {
	// принудительно синхронизуем кольцо с реальным временем
	// так как startedAt это мгновенное время взятое в CircuitBreaker
	// то либо кольцо полностью "прокисло", либо только несколько
	// бакетов "в начале" кольца. Главное что здесь мы всегда пишем
	// метрику в актуальное состояние кольца, поэтому бакет всегда найдется :)
	w.syncBucketsRing(startedAt)
	bucketID, _ := w.getBucketIdIfExist(startedAt)

	// предобработка :)
	w.addMetricsOnActionStarted(
		&w.windowMetrics,
		1,
	)
	// записываем в бакет
	w.addMetricsOnActionStarted(
		&w.buckets[bucketID].windowMetrics,
		1,
	)
}

func (w *nonThreadSafeSlidingWindow) AddFinishedAndMoveRingIfNeeds(startedAt, finishedAt int64, isFailure bool) {
	// принудительно синхронизуем кольцо с реальным временем
	// так как finishedAt это мгновенное время взятое в CircuitBreaker
	// то есть либо кольцо полностью "прокисло", либо только несколько
	// бакетов "в начале" кольца. 
	w.syncBucketsRing(finishedAt)
	// пытаемся найти бакет, если кольцо провернулось целиком
	// или же сдвинулось на несколько бакетов, то увы, писать нам некуда
	// так что выходим.
	bucketID, exist := w.getBucketIdIfExist(startedAt)
	if !exist {
		return
	}

	var failures, successes int
	if isFailure {
		failures++
	} else {
		successes++
	}

	// предобработка :)
	w.addMetricsOnActionFinished(
		&w.windowMetrics,
		failures,
		successes,
		1,
	)
	// записываем в бакет
	w.addMetricsOnActionFinished(
		&w.buckets[bucketID].windowMetrics,
		failures,
		successes,
		1,
	)
}
