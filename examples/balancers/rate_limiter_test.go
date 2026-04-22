package main

import (
	"sync/atomic"
	"testing"
	"time"

)

/*
нууу оно работает, и если у автора нет проблем с мат то все сходится 
1219 / 10 + 100 (time.Now().Add(-1 * time.Second)) ~ 221 :)

goos: darwin
goarch: arm64
pkg: balancer
cpu: Apple M1
=== RUN   Benchmark_RateLimiter
Benchmark_RateLimiter
    /Users/vlad/Desktop/golang_lessons/asm_test_project/balancer/rate_limiter_test.go:57: ------------------------------------------------------
    /Users/vlad/Desktop/golang_lessons/asm_test_project/balancer/rate_limiter_test.go:59: 0 :  100 ,  1
    /Users/vlad/Desktop/golang_lessons/asm_test_project/balancer/rate_limiter_test.go:61: 0 :  99 ,  0
    /Users/vlad/Desktop/golang_lessons/asm_test_project/balancer/rate_limiter_test.go:57: ------------------------------------------------------
    /Users/vlad/Desktop/golang_lessons/asm_test_project/balancer/rate_limiter_test.go:59: 0 :  100 ,  100
    /Users/vlad/Desktop/golang_lessons/asm_test_project/balancer/rate_limiter_test.go:61: 0 :  99 ,  0
    /Users/vlad/Desktop/golang_lessons/asm_test_project/balancer/rate_limiter_test.go:57: ------------------------------------------------------
    /Users/vlad/Desktop/golang_lessons/asm_test_project/balancer/rate_limiter_test.go:59: 0 :  100 ,  100
    /Users/vlad/Desktop/golang_lessons/asm_test_project/balancer/rate_limiter_test.go:61: 0 :  99 ,  0
    /Users/vlad/Desktop/golang_lessons/asm_test_project/balancer/rate_limiter_test.go:57: ------------------------------------------------------
    /Users/vlad/Desktop/golang_lessons/asm_test_project/balancer/rate_limiter_test.go:59: 16 :  100 ,  100
    /Users/vlad/Desktop/golang_lessons/asm_test_project/balancer/rate_limiter_test.go:61: 16 :  101 ,  100
    /Users/vlad/Desktop/golang_lessons/asm_test_project/balancer/rate_limiter_test.go:57: ------------------------------------------------------
    /Users/vlad/Desktop/golang_lessons/asm_test_project/balancer/rate_limiter_test.go:59: 683 :  168 ,  100
    /Users/vlad/Desktop/golang_lessons/asm_test_project/balancer/rate_limiter_test.go:61: 683 :  168 ,  3
    /Users/vlad/Desktop/golang_lessons/asm_test_project/balancer/rate_limiter_test.go:57: ------------------------------------------------------
    /Users/vlad/Desktop/golang_lessons/asm_test_project/balancer/rate_limiter_test.go:59: 1219 :  221 ,  100
    /Users/vlad/Desktop/golang_lessons/asm_test_project/balancer/rate_limiter_test.go:61: 1219 :  221 ,  9
Benchmark_RateLimiter-8         130119478                9.370 ns/op           0 B/op          0 allocs/op
PASS
ok      balancer        2.326s
*/
func Benchmark_RateLimiter(b *testing.B) {

	rt := NewRateLimiter(
		time.Now().Add(-1 * time.Second).UnixNano(),
		int64(10 * time.Millisecond),
		100,
	)

	now := time.Now()

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = rt.NeedThrottle()
		}
	})

	b.StopTimer()

	ms := time.Since(now).Milliseconds()
	b.Log("------------------------------------------------------")
	epoch, picks := rt.decodeVpick(atomic.LoadUint64(&rt.vpicks1))
	b.Log(ms, ": ", epoch, ", ", picks)
	epoch, picks = rt.decodeVpick(atomic.LoadUint64(&rt.vpicks2))
	b.Log(ms, ": ", epoch, ", ", picks)
}