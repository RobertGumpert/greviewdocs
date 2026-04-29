package balancers

import (
	"testing"
	"time"
)

func Test_SlidingWindow(t *testing.T) {
	sw := NewSlidingWindow(
		200*time.Millisecond,
		1*time.Second,
		metricsUpdater,
		metricsAggregator,
	)

	sw.syncSetMetrics(time.Now().UnixNano(), &Metrics{requestFailed: false, responseTime: 100})
	time.Sleep(200 * time.Millisecond)
	sw.syncSetMetrics(time.Now().UnixNano(), &Metrics{requestFailed: false, responseTime: 200})
	time.Sleep(200 * time.Millisecond)
	sw.syncSetMetrics(time.Now().UnixNano(), &Metrics{requestFailed: false, responseTime: 300})
	time.Sleep(200 * time.Millisecond)
	sw.syncSetMetrics(time.Now().UnixNano(), &Metrics{requestFailed: false, responseTime: 400})
	time.Sleep(200 * time.Millisecond)
	sw.syncSetMetrics(time.Now().UnixNano(), &Metrics{requestFailed: false, responseTime: 500})
	time.Sleep(200 * time.Millisecond)
	sw.syncSetMetrics(time.Now().UnixNano(), &Metrics{requestFailed: false, responseTime: 600})
	time.Sleep(200 * time.Millisecond)
	sw.syncSetMetrics(time.Now().UnixNano(), &Metrics{requestFailed: false, responseTime: 700})

	// вот слепок из дебагера :)
	// head = 1
	// tail = 5
	// struct { balancers.numberMoveRing int; balancers.numberFullRebuildRing int } {numberMoveRing: 1, numberFullRebuildRing: 0}
	// [0] -> balancers.windowBucket[balancers.Storage] {startTime: 1777369043987305000, endTime: 1777369044187305000, storage: balancers.Storage {errorsCount: 0, countRequests: 1, sumResponseTime: 700}}
	// [1] -> balancers.windowBucket[balancers.Storage] {startTime: 1777369042981590000, endTime: 1777369043181590000, storage: balancers.Storage {errorsCount: 0, countRequests: 1, sumResponseTime: 200}}
	// [2] -> balancers.windowBucket[balancers.Storage] {startTime: 1777369043181590000, endTime: 1777369043381590000, storage: balancers.Storage {errorsCount: 0, countRequests: 1, sumResponseTime: 300}}
	// [3] -> balancers.windowBucket[balancers.Storage] {startTime: 1777369043381590000, endTime: 1777369043581590000, storage: balancers.Storage {errorsCount: 0, countRequests: 1, sumResponseTime: 400}}
	// [4] -> balancers.windowBucket[balancers.Storage] {startTime: 1777369043581590000, endTime: 1777369043781590000, storage: balancers.Storage {errorsCount: 0, countRequests: 1, sumResponseTime: 500}}
	// [5] -> balancers.windowBucket[balancers.Storage] {startTime: 1777369043781590000, endTime: 1777369043981590000, storage: balancers.Storage {errorsCount: 0, countRequests: 1, sumResponseTime: 600}}
	// Рефликсируем:
	// 1) [0] наш этот самый overflowbucket :)
	// 2) head как и ожидалось смотрит на 1
	// 3) tail как и ожидалось смотрит на 5
	// 4) кольцо сдвинулось один раз

	time.Sleep(400 * time.Millisecond)
	now := time.Now().UnixNano()
	sw.syncSetMetrics(now, &Metrics{requestFailed: false, responseTime: 800})

	// head = 3
	// tail = 1
	// struct { balancers.numberMoveRing int; balancers.numberFullRebuildRing int } {numberMoveRing: 2, numberFullRebuildRing: 0}
	// [0] -> balancers.windowBucket[balancers.Storage] {startTime: 1777369470149825000, endTime: 1777369470349825000, storage: balancers.Storage {errorsCount: 0, countRequests: 1, sumResponseTime: 700}}
	// [1] -> balancers.windowBucket[balancers.Storage] {startTime: 1777369470150953000, endTime: 1777369470350953000, storage: balancers.Storage {errorsCount: 0, countRequests: 0, sumResponseTime: 0}}
	// [2] -> balancers.windowBucket[balancers.Storage] {startTime: 1777369470550953000, endTime: 1777369470750953000, storage: balancers.Storage {errorsCount: 0, countRequests: 1, sumResponseTime: 800}}
	// [3] -> balancers.windowBucket[balancers.Storage] {startTime: 1777369469543320000, endTime: 1777369469743320000, storage: balancers.Storage {errorsCount: 0, countRequests: 1, sumResponseTime: 400}}
	// [4] -> balancers.windowBucket[balancers.Storage] {startTime: 1777369469743320000, endTime: 1777369469943320000, storage: balancers.Storage {errorsCount: 0, countRequests: 1, sumResponseTime: 500}}
	// [5] -> balancers.windowBucket[balancers.Storage] {startTime: 1777369469943320000, endTime: 1777369470143320000, storage: balancers.Storage {errorsCount: 0, countRequests: 1, sumResponseTime: 600}}
	// Рефликсируем:
	// 1) [1] наш этот самый overflowbucket :)
	// 2) head как и ожидалось смотрит на 3
	// 3) tail как и ожидалось смотрит на 1
	// 4) кольцо сдвинулось два раза

	storage := Storage{}
	_ = sw.LazyAggregate(&storage)
	// ну собсна6 все сходится
	// balancers.Storage {errorsCount: 0, countRequests: 5, sumResponseTime: 3000}

	time.Sleep(1500 * time.Millisecond)
	storage = Storage{}
	_ = sw.SyncAggregate(&storage)
	// balancers.Storage {errorsCount: 0, countRequests: 0, sumResponseTime: 0}
	// head = 0
	// tail = 4
	// struct { balancers.numberMoveRing int; balancers.numberFullRebuildRing int } {numberMoveRing: 2, numberFullRebuildRing: 1}
	// [0] -> balancers.windowBucket[balancers.Storage] {startTime: 1777370102453564000, endTime: 1777370102653564000, storage: balancers.Storage {errorsCount: 0, countRequests: 0, sumResponseTime: 0}}
	// [1] -> balancers.windowBucket[balancers.Storage] {startTime: 1777370102653564000, endTime: 1777370102853564000, storage: balancers.Storage {errorsCount: 0, countRequests: 0, sumResponseTime: 0}}
	// [2] -> balancers.windowBucket[balancers.Storage] {startTime: 1777370102853564000, endTime: 1777370103053564000, storage: balancers.Storage {errorsCount: 0, countRequests: 0, sumResponseTime: 0}}
	// [3] -> balancers.windowBucket[balancers.Storage] {startTime: 1777370103053564000, endTime: 1777370103253564000, storage: balancers.Storage {errorsCount: 0, countRequests: 0, sumResponseTime: 0}}
	// [4] -> balancers.windowBucket[balancers.Storage] {startTime: 1777370103253564000, endTime: 1777370103453564000, storage: balancers.Storage {errorsCount: 0, countRequests: 0, sumResponseTime: 0}}
	// [5] -> balancers.windowBucket[balancers.Storage] {startTime: 1777370103453564000, endTime: 1777370103653564000, storage: balancers.Storage {errorsCount: 0, countRequests: 0, sumResponseTime: 0}}
	// Рефликсируем:
	// Зашибись, оно работает :) Для более глубокого тестирования у автора нет времени (лень)
}

