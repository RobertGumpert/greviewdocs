package main

import (
	"bytes"
	"context"
	"io"
	_ "math/rand"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var cfg = ConnetionPoolConfig{
	Requests: struct{ Retry struct{ AttemptsMax int } }{
		Retry: struct{ AttemptsMax int }{
			AttemptsMax: 3,
		},
	},
	Responses: struct {
		Buffer struct {
			InitSize int
			MaxSize  int
		}
	}{
		Buffer: struct {
			InitSize int
			MaxSize  int
		}{
			InitSize: 4096,  // 4 kib
			MaxSize:  65536, // 6 kib
		},
	},
	Connections: struct {
		Timeout time.Duration
		Jitter  struct {
			DurBase     time.Duration
			DurMax      time.Duration
			AttemptsMax int
		}
		Active struct{ Number int }
		Idle   struct{ Number int }
	}{
		Timeout: 5 * time.Second,
		Jitter: struct {
			DurBase     time.Duration
			DurMax      time.Duration
			AttemptsMax int
		}{
			DurBase:     10 * time.Millisecond,
			DurMax:      500 * time.Millisecond,
			AttemptsMax: 5,
		},
		Active: struct{ Number int }{
			Number: 100,
		},
		Idle: struct{ Number int }{
			Number: 100,
		},
	},
	Arena: struct {
		AttemptsMax int
		WaitDur     time.Duration
	}{
		AttemptsMax: 5,
		WaitDur:     5 * time.Millisecond,
	},
}

type dummy struct{}

func (d *dummy) Send(_ context.Context, req io.Reader, resp io.Writer) error {
	_, _ = io.ReadAll(req)
	resp.Write([]byte("hello, yopta"))
	time.Sleep(time.Duration(1500 * time.Millisecond))
	return nil
}

func (d *dummy) Close() error {
	return nil
}

/*
ну собственное как говорится... нельзя налить 10 литров в 2-х литровое ведро
-> time.Sleep(time.Duration(1500 * time.Millisecond))

=== RUN   Benchmark_RoundRobin
Benchmark_RoundRobin
Benchmark_RoundRobin-8                 1        1501286000 ns/op           12200 B/op         46 allocs/op
PASS
ok      balancer        2.090s
*/
func Benchmark_RoundRobin(b *testing.B) {

	// Настраиваем конфиг под жесткий стресс
	rb, err := NewRoundRobin(context.Background(), cfg, []net.IP{
		net.ParseIP("127.0.0.1"),
		net.ParseIP("127.0.0.2"),
		net.ParseIP("127.0.0.3"),
	}, func(_ context.Context, _ net.IP) (Connection, error) {
		return &dummy{}, nil
	})
	require.NoError(b, err)

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			req := io.NopCloser(bytes.NewBuffer([]byte("ping")))
			resp, err := rb.NonIdempotentRequest(context.Background(), req)

			if err != nil {
				panic(err)
			}

			if resp != nil {
				_, _ = io.ReadAll(resp)
				_ = resp.Close()
			}
		}
	})
}