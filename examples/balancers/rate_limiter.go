package main

import (
	"sync/atomic"
	"time"
	"unsafe"
)

type RateLimiter struct {
	// интевал выичслений нагрузки
	interval int64
	// v - virtual пики (точенные, прости у автор юморное утро)
	// собственно, пики это сколько раз вызывался NeedThrottle
	// в пределах одной вирутальной эпохи :)
	// Помнишь мы разбирали rune и SnowflakeID?
	// вот здесь будет похожая механика:
	// 1) первые 32 бита это монотонный счетчик
	//    который растет при смене реальной эпохи
	// 2) следующие 32 бита это кол-во пиков сделанных
	//    в пределах текущей эпохи
	vpicks1 uint64 // один их этих snapshot'ов является активным
	vpicks2 uint64 // один их этих snapshot'ов является активным
	// логика простая, если есть горутины, которые успели захватить
	// одно из состояний, но на следующем такте CPU другая горутина
	// обнаружила что эпоха закончилась, то первый G пускай доделывают
	// свои дела, в рамках свой уже старой эпохи, но но все горутины
	// обнаружевшие что эпоха сменилась будут работать с новой
	vpicks unsafe.Pointer // указатель на авктивный snapshot :)
	// И так мы уже знакомы со SnowflakeID, почему нам применить
	// эти знания в RateLimiter? :)
	// так вот пускай от этого значения мы
	// и будем вычислять наши virtual пики
	epoch int64

	// сколько раз за эпоху можно напикать NeedThrottle :)
	threshold int64
}

func NewRateLimiter(epoch, interval, threshold int64) *RateLimiter {
	rt := RateLimiter{
		interval: interval,
		threshold: threshold,
		epoch: epoch,
	}

	vepoch := rt.getNowVEpoch()
	rt.vpicks1 = rt.encodeVpick(vepoch, 0)
	rt.vpicks2 = rt.encodeVpick(vepoch-1, 0)
	rt.vpicks = unsafe.Pointer(&rt.vpicks1)

	return &rt
}

func (r *RateLimiter) NeedThrottle() bool {

	var newEpoch uint64
	var prevEpoch uint64

	for {
		newEpoch = r.getNowVEpoch()

		// берем указатель на активный vpicks snapshot
		// чтобы сравнить его с адресами обоих snapshot'ов
		// все просто как два пальца обос.... :)
		vpicks := (*uint64)(atomic.LoadPointer(&r.vpicks))
		prevEpoch, _ = r.decodeVpick(atomic.LoadUint64(vpicks))

		// так как новая эпоха не может быть вепереди
		// то пробуем заново
		if newEpoch < prevEpoch {
			continue
		}

		break
	}

	// началась след эпоха, начинаем
	// выборы за право быть лидером в "установке"
	// новой виртуальной эпохи в качестве активной
	if newEpoch > prevEpoch {
		r.electionsForUpdateEpoch(newEpoch)
	}

	vpicks := (*uint64)(atomic.LoadPointer(&r.vpicks))
	epochArena, _ := r.decodeVpick(atomic.LoadUint64(vpicks))
	// сразу фиксируем эпоху, потому что если несколько
	// G закиснут в spin loop'е очень и очень надолго,
	// да так, что актвный snapshot успеет смениться аж дважды, то есть
	// "мир" выполнит 5 шагов:
	// 1) vpicks1 - сейчас активный
	// 2) смена эпохи
	// 3) vpicks2 - стал активным
	// 4) смена эпохи
	// 5) vpicks1 - снова стал активным
	// и вот если G херачатся друг с другом так долго
	// что "мир" аж дошел до шага 5, то увы это уже проблемы
	// тех G которые зависили на арене :)
	// Вероятность наступления такого события, ну...
	// Ну если ты сделаешь interval = 1 ns, а threshold = 1
	// тогда может быть и будет, но, камон, серьезно ?:)
	return r.arena(vpicks, epochArena)
}


func (r *RateLimiter) getNowVEpoch() uint64 {
	now := time.Now().UnixNano()
	// получаем кол-во прешедших интервалов с момента старта
	// эпохи, ну тут же знание о том в какой вирутальной
	// эпохи мы сейчас находимся
	return uint64((now - r.epoch) / r.interval)
}

func (r *RateLimiter) decodeVpick(vpick uint64) (epoch, pick uint64) {
	// 41 бит это как мы уже знаем из SnowflakeID -
	// 69 лет если твой интревал 1 миллисекнду
	// если этого мало, то автор бессилен... :)
	epoch = vpick >> 41
	// 23 бита -> 2^23 = 8_388_608 пиков,
	// если этого мало, то автор бессилен... :)
	pick = (vpick & 0x7FFFFF)
	return
}

func (r *RateLimiter) encodeVpick(epoch, pick uint64) (vpick uint64) {
	return epoch<<41 | pick
}

func (r *RateLimiter) arena(vpicks *uint64, epochArena uint64) bool {
	for {
		vpick := atomic.LoadUint64(vpicks)

		epoch, pick := r.decodeVpick(vpick)
		// то о чем уже говорили в NeedTrhrottle
		// если херачитесь так долго друг с другом что аж
		// мир прошал 5 шагов, нууу... выши проблемы
		if epoch != epochArena {
			return false
		}
		pick++
		if pick > uint64(r.threshold) {
			return false
		}

		if atomic.CompareAndSwapUint64(vpicks, vpick, r.encodeVpick(epoch, pick)) {
			return true
		}
	}
}

func (r *RateLimiter) electionsForUpdateEpoch(newEpoch uint64) bool {
	// резервируем указательно на простаивающий
	// newVpicks snapshot чтобы он стал активным
	// для новой эпохи
	var newVpicks *uint64
	var prevVpicks = (*uint64)(atomic.LoadPointer(&r.vpicks))
	// берем указатель на активный vpicks snapshot
	// чтобы сравнить его с адресами обоих snapshot'ов
	// все просто как два пальца обос.... :)
	if prevVpicks == &r.vpicks1 {
		newVpicks = &r.vpicks2
	} else {
		newVpicks = &r.vpicks1
	}
	// заморазиваем состояние newVpicks
	// на время выборов
	newVpicksFreezState := atomic.LoadUint64(newVpicks)
	// Начинаются выборы за право установить
	// новую эпоху, каждый кондидат как бы
	// голосует сам за себя через CompareAndSwap :)
	// Выборы состоят из двух гонок:
	// 1) кто первым инициализирует новое состояние для newVpicks
	// 2) кто первым установить новую эпоху в &r.vpicks
	for {
		// повторяем в цикле выбор
		// активного snapshot'а
		active := (*uint64)(atomic.LoadPointer(&r.vpicks))
		// кандидант профукал обе
		// гонки на выборах, так что
		// просто уходит на арену :)
		if active == newVpicks {
			return true
		}
		// бновляем замороженное состояние на каждой итерации
    	newVpicksFreezState = atomic.LoadUint64(newVpicks)
		// из актвиного snapshot'а,
		// берем виртуальную эпоху
		prevEpoch, _ := r.decodeVpick(atomic.LoadUint64(active))
		// все кандидаты слишком долго
		// херачились друг с другом,
		// уже начилась новая эпоха
		if prevEpoch > newEpoch {
			return false
		}
		// первая гонка: кандидат пытается первым
		// обновить состояние newVpicks, если
		// не получается, то переходит ко второй гонке
		_ = atomic.CompareAndSwapUint64(
			newVpicks,
			newVpicksFreezState,
			r.encodeVpick(newEpoch, 0),
		)
		// вторая гонка: кандидат пытается первым
		// установить новую эпоху в &r.vpicks
		if atomic.CompareAndSwapPointer(
			&r.vpicks,
			unsafe.Pointer(prevVpicks),
			unsafe.Pointer(newVpicks),
		) {
			// победитель в финальной гонке!
			// (призов не будет :)
			return true
		}
		// если кандидат работал, а не спал
		// при G/OS context switch, то он очевидно
		// успеет изменить &r.vpicks, и сюда просто не попадет.
	}
}

