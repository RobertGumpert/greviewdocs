package main

import (
	"sync/atomic"
	"testing"
	"time"
)

/*
нууу оно работает, и если у автора нет проблем с мат. то все сходится

goos: darwin
goarch: arm64
pkg: balancer
cpu: Apple M1
=== RUN   Benchmark_RateLimiter
Benchmark_RateLimiter
    /Users/vlad/Desktop/golang_lessons/asm_test_project/balancer/rate_limiter_test.go:56: ------------------------------------------------------
    /Users/vlad/Desktop/golang_lessons/asm_test_project/balancer/rate_limiter_test.go:58: 0 :  100 ,  1
    /Users/vlad/Desktop/golang_lessons/asm_test_project/balancer/rate_limiter_test.go:56: ------------------------------------------------------
    /Users/vlad/Desktop/golang_lessons/asm_test_project/balancer/rate_limiter_test.go:58: 0 :  100 ,  100
    /Users/vlad/Desktop/golang_lessons/asm_test_project/balancer/rate_limiter_test.go:56: ------------------------------------------------------
    /Users/vlad/Desktop/golang_lessons/asm_test_project/balancer/rate_limiter_test.go:58: 0 :  100 ,  100
    /Users/vlad/Desktop/golang_lessons/asm_test_project/balancer/rate_limiter_test.go:56: ------------------------------------------------------
    /Users/vlad/Desktop/golang_lessons/asm_test_project/balancer/rate_limiter_test.go:58: 14 :  101 ,  100
    /Users/vlad/Desktop/golang_lessons/asm_test_project/balancer/rate_limiter_test.go:56: ------------------------------------------------------
    /Users/vlad/Desktop/golang_lessons/asm_test_project/balancer/rate_limiter_test.go:58: 701 :  170 ,  101
    /Users/vlad/Desktop/golang_lessons/asm_test_project/balancer/rate_limiter_test.go:56: ------------------------------------------------------
    /Users/vlad/Desktop/golang_lessons/asm_test_project/balancer/rate_limiter_test.go:58: 1323 :  232 ,  100
Benchmark_RateLimiter-8         139119766                9.514 ns/op           0 B/op          0 allocs/op
PASS
ok      balancer        2.643s
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
	epoch, picks := rt.decodeVpick(atomic.LoadUint64(&rt.vpicks))
	b.Log(ms, ": ", epoch, ", ", picks)
}