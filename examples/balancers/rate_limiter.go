package balancers

import (
	"sync/atomic"
	"time"
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
	vpicks uint64 // указатель на авктивный snapshot :)
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
	rt.vpicks = rt.encodeVpick(vepoch, 0)

	return &rt
}

func (r *RateLimiter) NeedThrottle() bool {
	
	newEpoch := r.getNowVEpoch()

	// весь цикл замечателен тем что рано или поздно
	// все G пожрут picks в рамках эпохи поэтому
	// выжигать CPU серверной стойки мы не будем :) 
    for {
		// берем текущее актальное состояние
		vpicks := atomic.LoadUint64(&r.vpicks)
        epoch, picks := r.decodeVpick(vpicks)
		// если пики (точенные, прости) "закончились" 
		// выходим и отказываем в обслуживании :)
		if picks >= uint64(r.threshold) && newEpoch == epoch {
            return false 
        } 
		// если началась новая эпоха
		// то пытаемся ее захватить самыми первыми
		// но вот если текущая G уснет на G/OS context switch
		// то когда проснется, уже не сможет ее начать
		// так состояние vpicks скорее всего уже убежало далкео вперед
		// так что начинаем все заново :)
        if newEpoch > epoch {
            newVpicks := r.encodeVpick(newEpoch, 1)
            if atomic.CompareAndSwapUint64(&r.vpicks, vpicks, newVpicks) {
                return true
            }
            continue
        }
		// защизаемся от так называемого time drifta
		// когда время на разных ядрах разъехалось
        if newEpoch < epoch {
            newEpoch = epoch
        }
    
        newVpicks := r.encodeVpick(newEpoch, picks+1)
        if atomic.CompareAndSwapUint64(&r.vpicks, vpicks, newVpicks) {
            return true
        }
		// какая-то G оказалась шустрей или текущая G
		// уснула из G/OS context switch, так что не придумываем
		// себе геммороя на ж*** и просто начинаем цикл заново

    }
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