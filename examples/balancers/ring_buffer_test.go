package balancers

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRingBuffer(t *testing.T) {
	rb, err := NewRingBuffer[int](5)
	require.NoError(t, err)

	rb.Put(1, false)
	rb.Put(2, false)
	rb.Put(3, false)
	rb.Put(4, false)
	rb.Put(5, false)
	rb.Put(6, true)

	v, _ := rb.Pull()
	require.Equal(t, 2, v)
	v, _ = rb.Pull()
	require.Equal(t, 3, v)
	v, _ = rb.Pull()
	require.Equal(t, 4, v)
	v, _ = rb.Pull()
	require.Equal(t, 5, v)
	v, _ = rb.Pull()
	require.Equal(t, 6, v)
	_, ok := rb.Pull()
	require.False(t, ok)
}
