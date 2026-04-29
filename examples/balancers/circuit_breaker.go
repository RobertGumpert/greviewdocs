package balancers


// type circuitBreakerActionFn[Input any, Output any] func(*Input, *Output) error

// const mutexLock int64 = 1

// type CircuitBreaker[Input any, Output any] struct {
// 	mu       sync.RWMutex
// 	status   int64
// 	actionFn circuitBreakerActionFn[Input, Output]

// 	// определяем интревал времени
// 	// который необходимо выдержать
// 	// от перехода из статуса Open в Half-Open 
// 	openInterval int64 // time.Duration
// 	openDeadline int64 // time.UnixNano()

// 	// определяем интревал времени между
// 	// попытками сделать пробу и конец
// 	// эпохи "пробирования", а также кол-во проб
// 	halfOpenNumberProbes  int64
// 	halfOpenProbeInterval int64 // time.Duration
// 	halfOpenDeadline      int64 // time.UnixNano()
// 	// пороговое кол-во ошибок, после которого
// 	// потребуется снова уходить в статус Open
// 	halfOpenErrorsThreshold int64
// 	halfOpenNumberErrors    int64
// 	// дата следующей доступной пробы
// 	halfOpenNextProbeTs int64
// }

// // func (cb *CircuitBreaker[Input, Output]) setStatusOpen() circuitBreakerStatus {
// // 	temp := atomic.LoadInt64(&cb.openDeadline)
// // 	deadline := time.Now().UnixNano()


// // }


// func (cb *CircuitBreaker[Input, Output]) getStatus() circuitBreakerStatus {
// 	return circuitBreakerStatus(atomic.LoadInt64(&cb.status))
// }

// --------------------------------------------------------------------------------------------------------

// type CircuitBreaker[Input any, Output any] struct {
// 	mu       sync.RWMutex
// 	status   int64
// 	actionFn circuitBreakerActionFn[Input, Output]

// 	// определяем интревал времени
// 	// который необходимо выдержать
// 	// от перехода из статуса Open в Half-Open 
// 	openInterval int64 // time.Duration
// 	openDeadline int64 // time.UnixNano()

// 	// определяем интревал времени между
// 	// попытками сделать пробу и конец
// 	// эпохи "пробирования", а также кол-во проб
// 	halfOpenNumberProbes  int64
// 	halfOpenProbeInterval int64 // time.Duration
// 	halfOpenDeadline      int64 // time.UnixNano()
// 	// пороговое кол-во ошибок, после которого
// 	// потребуется снова уходить в статус Open
// 	halfOpenErrorsThreshold int64
// 	halfOpenNumberErrors    int64
// 	// дата следующей доступной пробы
// 	haldOpenNextProbeTs int64
// }

// func (cb *CircuitBreaker[Input, Output]) PreActionProbe() bool {
// 	_, doAction := cb.preActionProbe()
// 	return doAction
// }

// func (cb *CircuitBreaker[Input, Output]) DoAction(input *Input, output *Output) error {
// 	status, doAction := cb.preActionProbe()
// 	if !doAction {
// 		return ErrCircuitBreakerOpen
// 	}

// 	if status == circuitBreakerStatusHalfOpen {
// 		return cb.doHalfOpenAction(input, output)
// 	}

// 	return nil
// }

// func (cb *CircuitBreaker[Input, Output]) doHalfOpenAction(input *Input, output *Output) error {
// 	numberErrs := atomic.LoadInt64(&cb.halfOpenNumberErrors)
// 	if numberErrs > cb.halfOpenErrorsThreshold {

// 	}

// 	err := cb.actionFn(input, output)
// 	if err != nil {
// 		return nil
// 	}

// 	numberErrs := atomic.LoadInt64(&cb.halfOpenNumberErrors)

// }

// func (cb *CircuitBreaker[Input, Output]) preActionProbe() (status circuitBreakerStatus, doAction bool) {
// 	// если Open или Closed сразу же выходим
// 	if status = circuitBreakerStatus(atomic.LoadInt64(&cb.status)); status != circuitBreakerStatusHalfOpen {
// 		return status, cb.preActionProbe2(status)
// 	}

// 	probeTs := time.Now().UnixNano()
// 	nextProbeTs := atomic.LoadInt64(&cb.haldOpenNextProbeTs)
// 	// если с момент последней пробы не прошло указанное
// 	// кол-во времени между пробами, то отвечаем отказом
// 	if probeTs < nextProbeTs {
// 		return circuitBreakerStatusHalfOpen, false
// 	}

// 	// формируем дату для следующей попытки
// 	newNextProbeTs := probeTs + cb.halfOpenProbeInterval
// 	// если нашелся кто-то более шустрый, то он будет
// 	// участвовать в "пробировании", а эта G уже нет :)
// 	return circuitBreakerStatusHalfOpen,
// 		atomic.CompareAndSwapInt64(&cb.halfOpenNumberProbes, nextProbeTs, newNextProbeTs)
// }

// func (cb *CircuitBreaker[Input, Output]) setHalfOpenIfMaybe() circuitBreakerStatus {
// 	now := time.Now().UnixNano()
// 	deadlineTs := atomic.LoadInt64(&cb.halfOpenDeadline)
// 	if deadlineTs > now {
// 		return circuitBreakerStatusOpen
// 	}

// 	if atomic.CompareAndSwapInt64(
// 		&cb.status,
// 		int64(circuitBreakerStatusOpen),
// 		int64(circuitBreakerStatusHalfOpen),
// 	) {
// 		return circuitBreakerStatusHalfOpen
// 	}

// 	return circuitBreakerStatus(atomic.LoadInt64(&cb.status))
// }

// // Найдемся но то, что компилятор заинлайнит :)
// func (cb *CircuitBreaker[Input, Output]) preActionProbe2(status circuitBreakerStatus) (doAction bool) {
// 	switch status {
// 	case circuitBreakerStatusOpen:
// 		return false
// 	case circuitBreakerStatusClosed:
// 		return true
// 	}

// 	return false
// }

// func (cb *CircuitBreaker[Input, Output]) statusArena(wantStatus circuitBreakerStatus) bool {
// 	for {
// 		currStatus := atomic.LoadInt64(&cb.status)
// 		// так менять статусы мы просто не можем
// 		if currStatus == int64(wantStatus) ||
// 			currStatus == circuitBreakerStatusOpen && wantStatus == circuitBreakerStatusClosed {
// 			return false
// 		}

// 		if currStatus == circuitBreakerStatusHalfOpen && wantStatus == circuitBreakerStatusClosed {
// 			return atomic.CompareAndSwapInt64(&cb.status, currStatus, int64(wantStatus))
// 		}
// 	}
// }
