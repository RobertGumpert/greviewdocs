package balancers

import (
	"sync"
	"time"
)

// -------------------------------------------------------------------------------------------------- реализация

type windowBucket[Storage any] struct {
	startTime int64
	endTime   int64
	storage   Storage
}

// Скользящему окну плевать какие именно метрики используются
// для принятии решения. Пускай пользователь сам определяет
// набор метрик и то как их обноалять с помощью - MetricsUpdaterFn.
// То есть funcval на пользовательскую функцию, которая метрики
// обновляет.
type MetricsUpdaterFn[Storage any, Metrics any] func(*Storage, *Metrics)

// Скользящему окну плевать как именно принимается решение, это задача
// пользовательсклй функции хранимой ввиде funcval - AggregateFn
type AggregateFn[Aggregate any, Storage any] func(*Aggregate, *Storage) bool

type SlidingWindow[Storage any, Metrics any, Aggregate any] struct {
	mu sync.RWMutex
	// Храним слайс значений чтобы максимально
	// локализовать данные - backing array хранит
	// сами бакеты друг за другом, а не указатели на кучу
	// объектов в куче.
	ringBuffer  []windowBucket[Storage]
	bucketCount int
	head, tail  int
	// Использование funcval автоматом отменяет возможность
	// инлайнинга, но и переход на интерфейсы тоже нам не поможет :)
	// так что вызовы этих функций точно будут порождать новые стек-фреймы
	// однако если же функции в своей сути достаточно просты, то компилятор
	// может сократить assembler удалив пролог функции,
	// по сути будет лищь одно обращешие в RAM - в инструкции callq
	// Кажется это некоторый баланс между сложностью и эффективностью
	updaterCallback   MetricsUpdaterFn[Storage, Metrics]
	aggregateCallback AggregateFn[Aggregate, Storage]
	// границы скользящего окна
	bucketSize int64 // -> time.Duration - размер каждого бакета
	startTime  int64 // -> time.Time.UnixNano()
	endTime    int64 // -> time.Time.UnixNano()

	debugStat struct {
		numberMoveRing        int
		numberFullRebuildRing int
	}
}

func NewSlidingWindow[Storage any, Metrics any, Aggregate any](
	bucketSize time.Duration,
	windowSize time.Duration,
	updaterCallback MetricsUpdaterFn[Storage, Metrics],
	aggregateCallback AggregateFn[Aggregate, Storage],
) *SlidingWindow[Storage, Metrics, Aggregate] {

	now := time.Now()

	startTime := now.UnixNano()
	endTime := now.Add(windowSize).UnixNano()
	// +1 на overflowbucket
	countBuckets := (int64(windowSize) / int64(bucketSize)) + 1
	ringBuffer := make([]windowBucket[Storage], 0, countBuckets)

	var bucketStartTime int64 = startTime
	for idx := int64(0); idx < countBuckets; idx++ {
		var zeroVal Storage
		ringBuffer = append(ringBuffer, windowBucket[Storage]{
			startTime: bucketStartTime,
			endTime:   bucketStartTime + int64(bucketSize),
			storage:   zeroVal,
		})
		bucketStartTime += int64(bucketSize)
	}

	sw := SlidingWindow[Storage, Metrics, Aggregate]{
		mu:                sync.RWMutex{},
		ringBuffer:        ringBuffer,
		bucketCount:       int(countBuckets - 1),
		head:              0,
		tail:              int(countBuckets - 2),
		updaterCallback:   updaterCallback,
		aggregateCallback: aggregateCallback,
		bucketSize:        int64(bucketSize),
		startTime:         startTime,
		endTime:           endTime,
		debugStat: struct {
			numberMoveRing        int
			numberFullRebuildRing int
		}{
			numberMoveRing:        0,
			numberFullRebuildRing: 0,
		},
	}

	return &sw
}

// SyncSetMetrics
// Метод ожидает от пользователя что Metrics является указателем
// так как заранее SlidingWindow не может знать какого размера
// будет структура и если пользователь уже ее разместил в куче,
// то этого его дело. Но если структура большая (но не больше 64 kib!)
// пользователь может возмользоваться возможностями компилятора GO -
// metrics *Metrics будет действительно указателем, но не на объект в
// куче, а переменную на стеке пользовательской функции и так мы избежим
// давления на GC (если конечно пользователь сам об этом не позаботился :)
func (w *SlidingWindow[Storage, Metrics, Aggregate]) SyncSetMetrics(metrics *Metrics) {
	w.mu.Lock()
	defer w.mu.Unlock()

	now := time.Now().UnixNano()
	w.moveRingIfNeeded(now)

	// "логический" индекс, по сути какой по порядку
	// бакет от 0 до len(w.ringBuffer) :)
	idx := (now - w.startTime) / w.bucketSize
	// переносим "логический" на "реальную, ситуацию на местах",
	// помним, что у нас кольцевой буффер!
	// head может оказаться не 0, а вообще N-1, где N = len(w.ringBuffer) :)
	idx = (int64(w.head) + idx) % int64(len(w.ringBuffer))

	storage := &w.ringBuffer[idx].storage
	// так как backing array у w.ringBuffer уже в куче,
	// то просто передаем указатель на место в куче с которого
	// начинается Storage, i-ого бакета
	w.updaterCallback(storage, metrics)
}

// SyncAggregate, LazyAggregate
// Собственное вычисляем агрегатное состояние со всех бакетов.
// Скользящему окну абсолютно плевать как именно примнимается решение:
// 1) максмальное значение среди всех метрик
// 2) наличие флага
// 3) etc...
// Пользователь передает указатель со стека функции, вызывающей SyncAggregate (если
// конечно сам пользователь на разместил объект в куче) для аггрегации результатов
// а задача SyncAggregate передать в пользавательский же funcval - SyncAggregate

// SyncAggregate
// Медленная аггрегация требующая синхронизации "реального времени"
// и текущего состояния RingBuffer'а. Медленная, но повыщающая точность
func (w *SlidingWindow[Storage, Metrics, Aggregate]) SyncAggregate(aggregate *Aggregate) bool {
	w.mu.Lock()
	defer w.mu.Unlock()

	now := time.Now().UnixNano()
	head, tail := w.moveRingIfNeeded(now)

	return w.aggregate(aggregate, head, tail, now)
}

// LazyAggregate
// Ленивая аггрегация, без жестого лока Lock :) Точность страдает, но позволяет
// не копить очередь горутины которые вносят изменение в состояние SlidingWindow
func (w *SlidingWindow[Storage, Metrics, Aggregate]) LazyAggregate(aggregate *Aggregate) bool {
	w.mu.RLock()
	defer w.mu.RUnlock()

	now := time.Now().UnixNano()
	head, tail := w.getRingPointers(now)

	return w.aggregate(aggregate, head, tail, now)
}

func (w *SlidingWindow[Storage, Metrics, Aggregate]) aggregate(aggregate *Aggregate, head, tail int, now int64) bool {
	//
	// 		[head]               [tail]
	// 		[13,15][15,17][17,19][19,21][23,25]
	//									   |
	//								    overflow
	//      windowSize = 25 - 13 -> 12
	//      если now - bucket.endTime > windowSize, просто
	//      отбрасываем этот бакет :) 
	//      
	//      по сути у нас тут родилась
	//      зависимость aggregate от LazyAggregate, ноооо...
	//      вцелом это допустимо :)
	//
	//      если now -> 30
	//      [13,15] -> 30 - 15 = 15, 15 > 12 - выкидываем бакет из агрегации
	//      ... etc
	//      [17,19] -> 30 - 19 = 11, !(11 > 12) - берем бакет в агрегацию
	//
	windowSize := w.endTime - w.startTime
	for {
		if now-w.ringBuffer[head].endTime > windowSize {
			head = (head + 1) % len(w.ringBuffer)
			continue
		}

		if !w.aggregateCallback(aggregate, &w.ringBuffer[head].storage) {
			return false
		}

		// если head дошел до tail
		// значит мы прошли весь круг
		// выходим :)
		if head == tail {
			break
		}

		head = (head + 1) % len(w.ringBuffer)
	}

	return true
}

func (w *SlidingWindow[Storage, Metrics, Aggregate]) getRingPointers(now int64) (head, tail int) {
	var numResetBuckets int64 = (now - w.endTime) / w.bucketSize
	if numResetBuckets == 0 {
		// вычисляем позицию overflowbucket, которая
		// всегда находится на +1 от tail, но так чтобы
		// не вывалиться за границы слайса
		return w.head, (w.tail + 1) % len(w.ringBuffer)
	}

	return w.head, w.tail
}

func (w *SlidingWindow[Storage, Metrics, Aggregate]) moveRingIfNeeded(now int64) (head, tail int) {
	// Допустим, исходное состояние выглядит так:
	// 		[head]         [tail]
	// 		[1,3][3,5][5,7][8,10]
	//
	// Шаг 1: now -> 21
	// numResetBuckets = (21 - 10) / 2 -> 5.5 -> 5
	// нам нужно reset'нуть все бакеты, так как у нас
	// всего-то 4 бакета.
	// ----> прыгаем в `numResetBuckets > w.bufferLen`
	// newEndTime = 21
	// newStartTime = 21 - (2 * 4) = 13
	// получим новое состояние, так как время убежало вперед
	// и все бакеты попросту прокисли:
	// 		[head]               [tail]
	// 		[13,15][15,17][17,19][19,21]
	//
	// Шаг 2: now -> 22
	// numResetBuckets = (22 - 21) / 2 -> 0.5 -> 0
	// В таком случае, говорим вызывающей стороне, что
	// нужно использовать так называемый overflowbucket
	// то есть бакет, который убегает вперед от самого окна
	// 		[head]               [tail]
	// 		[13,15][15,17][17,19][19,21]
	// ---> используй overflowbucket [22,23]
	//
	// Шаг 3: now -> 23
	// numResetBuckets = (23 - 21) / 2 -> 1
	// 		[head]               [tail]
	// 		[13,15][15,17][17,19][19,21]
	//	    + overflowbucket [22,23]
	// то
	// 		       [tail] [head]
	//      [19,21][21,23][15,17][17,19]
	//      + очищаем overflowbucket -> [23,25]
	//
	// ну и физика :)
	//
	// 		[head]               [tail]
	// 		[13,15][15,17][17,19][19,21][23,25]
	//									   |
	//								    overflow
	//
	//
	// 		       [head]               [tail]
	// 		[25,27][15,17][17,19][19,21][23,25]
	//		   |
	//		overflow
	//
	//
	// 		[tail]        [head]
	// 		[25,27][27,29][17,19][19,21][23,25]
	//		          |
	//		       overflow

	if now <= w.endTime {
		return w.head, w.tail
	}

	var numResetBuckets int64 = (now - w.endTime) / w.bucketSize
	if numResetBuckets == 0 {
		// вычисляем позицию overflowbucket, которая
		// всегда находится на +1 от tail, но так чтобы
		// не вывалиться за границы слайса
		return w.head, (w.tail + 1) % len(w.ringBuffer)
	}

	var endTime int64 = now
	var startTime int64 = now - (w.bucketSize * int64(w.bucketCount))

	// все бакеты прокисли :)
	if numResetBuckets >= int64(len(w.ringBuffer)) {
		// чистим каждый бакет, начиная с 0
		// и прям до конца, то есть если
		// overflowbucket = len(w.ringBuffer) - 1
		// то его тоже очищаем
		var bucketStartTime int64 = startTime
		for idx := range len(w.ringBuffer) {
			w.resetBucket(
				bucketStartTime,
				bucketStartTime+w.bucketSize,
				idx,
			)

			bucketStartTime += w.bucketSize
		}
		// обнолвяем данные всего окна
		w.head = 0
		w.tail = w.bucketCount - 1
		w.endTime = endTime
		w.startTime = startTime
		w.debugStat.numberFullRebuildRing++
		// было
		//  head           tail
		//   |              |
		// [1,3][3,5][5,7][8,10][10,12]
		//                         |
		//                      overflow
		//
		// стало
		//   head                 tail
		//    |                    |
		// [13,15][15,17][17,19][19,21][21,13]
		//                                |
		//                             overflow
		return w.head, w.tail
	}

	var bucketStartTime int64 = endTime - (w.bucketSize * numResetBuckets)
	// очищаем все бакеты, которые прокисли
	for idx := int64(0); idx < numResetBuckets; idx++ {
		w.resetBucket(
			bucketStartTime,
			bucketStartTime+w.bucketSize,
			w.head,
		)
		// двигаем head, tail вперед, но так чтобы
		// не вывалиться за грницы слайса
		w.head = (w.head + 1) % len(w.ringBuffer)
		w.tail = (w.tail + 1) % len(w.ringBuffer)
		bucketStartTime += w.bucketSize
	}

	// подготовим overflowbucket, который
	// всегда идет за tail + 1
	w.resetBucket(
		bucketStartTime,
		bucketStartTime+w.bucketSize,
		(w.tail+1)%len(w.ringBuffer),
	)

	w.startTime = startTime
	w.endTime = endTime
	w.debugStat.numberMoveRing++
	// было:
	//  head                 tail
	//   |                     |
	// [13,15][15,17][17,19][19,21][23,25]
	//                                |
	//                             overflow
	//
	// стало:
	//         head                 tail
	//           |                    |
	// [25,27][15,17][17,19][19,21][23,25]
	//    |
	// overflow
	return w.head, w.tail
}

func (w *SlidingWindow[Storage, Metrics, Aggregate]) resetBucket(startTime, endTime int64, bucketID int) {
	var zeroVal Storage
	w.ringBuffer[bucketID].storage = zeroVal
	w.ringBuffer[bucketID].startTime = startTime
	w.ringBuffer[bucketID].endTime = endTime
}

// -------------------------------------------------------------------------------------------------- пользовательская сторона

type Storage struct {
	errorsCount     int64
	countRequests   int64
	sumResponseTime int64
}

type Metrics struct {
	requestFailed bool
	responseTime  int64
}

func foo() {
	sw := NewSlidingWindow(
		200*time.Millisecond,
		1*time.Second,
		metricsUpdater,
		metricsAggregator,
	)

	metrics := Metrics{
		requestFailed: true,
		responseTime:  1000,
	}
	sw.SyncSetMetrics(&metrics)

	storage := Storage{}
	_ = sw.LazyAggregate(&storage)
	if storage.countRequests > 5 {
		panic("oookak (1)")
	}
	avgRT := float64(storage.sumResponseTime) / float64(storage.countRequests)
	if avgRT > 100 {
		panic("oookak (2)")
	}
}

func metricsUpdater(storage *Storage, metrics *Metrics) {
	storage.countRequests++
	if metrics.requestFailed {
		storage.errorsCount++
	}
	storage.sumResponseTime = storage.sumResponseTime + metrics.responseTime
	// если ты здесь вставишь запрос к бд - это твои проблемы братишка :)
	// SlidingWindow необязан "разруливать такие приколы"
}

func metricsAggregator(aggregate, storage *Storage) bool {
	aggregate.countRequests += storage.countRequests
	aggregate.errorsCount += storage.errorsCount
	aggregate.sumResponseTime += storage.sumResponseTime
	// если ты здесь вставишь запрос к бд - это твои проблемы братишка :)
	// SlidingWindow необязан "разруливать такие приколы"
	return true
}
