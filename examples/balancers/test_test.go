package balancers

import (
	"testing"
	"time"
)

func Test_Test1(t *testing.T) {
	s := time.Now().UnixNano()
	t.Log(s)

	time.Sleep(1 * time.Nanosecond)

	s = time.Now().UnixNano()
	t.Log(s)
}
