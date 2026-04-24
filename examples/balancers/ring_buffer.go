package balancers

import (
	"errors"
	"sync"
)

type RingBuffer[T any] struct {
	mu         sync.Mutex
	data       []T
	size       int
	capacity   int
	head, tail int
}

func NewRingBuffer[T any](size int) (*RingBuffer[T], error) {
	if size < 2 {
		return nil, errors.New("irrational buffer size")
	}

	rb := RingBuffer[T]{
		mu:       sync.Mutex{},
		data:     make([]T, size),
		size:     0,
		capacity: size,
		head:     0,
		tail:     0,
	}

	return &rb, nil
}

func (r *RingBuffer[T]) Put(val T, rewrite bool) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.size+1 > r.capacity {
		if !rewrite {
			return false
		}

		next := r.head + 1
		if next == r.capacity {
			next = 0
		}
		r.head = next
		r.size--
	}

	r.size++
	r.data[r.tail] = val
	next := r.tail + 1

	if next == r.capacity {
		next = 0
	}

	r.tail = next
	return true
}

func (r *RingBuffer[T]) Pull() (T, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	var zeroVal T
	if r.size == 0 {
		return zeroVal, false
	}

	next := r.head + 1
	val := r.data[r.head]
	r.data[r.head] = zeroVal
	r.size--

	if next == r.capacity {
		next = 0
	}

	r.head = next

	return val, true
}
