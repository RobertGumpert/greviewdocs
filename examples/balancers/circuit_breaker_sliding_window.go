package balancers

type circuitBreakerMetrics struct {
	inFligth      int
	failureProbes int
}

func (m *circuitBreakerMetrics) addActionInFligth(n int) {
	m.inFligth += n
}

func (m *circuitBreakerMetrics) loadInFligth() int {
	return m.inFligth
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

type bucket struct {
	startTs, endTs int64 // time.UnixNano
	metrics        windowMetrics
}

type slidingWindow struct {
	startTs, endTs int64 // time.UnixNano
	windowSizeDur  int64 // time.Duration
	bucketSizeDur  int64 // time.Duration
	head, tail     int
	buckets        []bucket
	// предобработанные метрики
	metrics windowMetrics
}

func (w *slidingWindow) getBucketID(now int64) int64 {
	idx := (now - w.startTs) / w.bucketSizeDur
	idx = (int64(w.head) + idx) % int64(len(w.buckets))
	return idx
}

func (w *slidingWindow) syncBucketsRing(now int64) {
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

		w.metrics = windowMetrics{}
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
		w.metrics.addActionFailures(-1 * w.buckets[w.head].metrics.loadFailures())
		w.metrics.addActionSuccesses(-1 * w.buckets[w.head].metrics.loadSuccesses())

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

func (w *slidingWindow) resetWindowBucket(bucket *bucket, startTs, endTs int64) {
	bucket.metrics = windowMetrics{}
	bucket.startTs = startTs
	bucket.endTs = endTs
}

func (w *slidingWindow) addActionStarted(startedAt int64) {
	// принудительно синхронизуем кольцо с реальным временем
	w.syncBucketsRing(startedAt)

	// предобработка :)
	w.addMetricsOnActionStarted(
		&w.metrics,
		1,
	)

	// записываем в бакет
	bucketID := w.getBucketID(startedAt)
	w.addMetricsOnActionStarted(
		&w.buckets[bucketID].metrics,
		1,
	)
}

func (w *slidingWindow) addMetricsOnActionStarted(metrics *windowMetrics, started int) {
	metrics.addActionStarted(started)
}

func (w *slidingWindow) addActionFinished(startedAt, finishedAt int64, isFailure bool) {
	// принудительно синхронизуем кольцо с реальным временем
	w.syncBucketsRing(finishedAt)

	isSlow := (finishedAt - startedAt) > w.windowSizeDur
	if isSlow || finishedAt < w.startTs {
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

	var failures, successes int
	if isFailure {
		failures++
	} else {
		successes++
	}

	// предобработка :)
	w.addMetricsOnActionFinished(
		&w.metrics,
		failures,
		successes,
		1,
	)

	// записываем в бакет
	bucketID := w.getBucketID(finishedAt)
	w.addMetricsOnActionFinished(
		&w.buckets[bucketID].metrics,
		failures,
		successes,
		1,
	)
}

func (w *slidingWindow) addMetricsOnActionFinished(metrics *windowMetrics, failures, successes, finished int) {
	metrics.addActionFailures(failures)
	metrics.addActionSuccesses(successes)
	metrics.addActionFinished(finished)
}
