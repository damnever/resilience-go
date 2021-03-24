package metric

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestWindowedMinimum(t *testing.T) {
	m := NewWindowedMinimum(100*time.Millisecond, 10*time.Millisecond)
	span := m.span

	for i := 60; i > 30; i-- {
		v := m.Observe(time.Duration(i))
		require.Equal(t, time.Duration(i), v)
		require.Equal(t, time.Duration(i), m.Value())
		if i > 31 {
			time.Sleep(span)
		}
	}
	for i := 1; i < 10; i++ {
		time.Sleep(span)
		v := m.Observe(time.Duration(i))
		require.Equal(t, time.Duration(1), v)
		require.Equal(t, time.Duration(1), m.Value())
	}

	time.Sleep(2 * span)
	require.Equal(t, time.Duration(2), m.Value())

	time.Sleep(8 * span)
	require.Equal(t, time.Duration(-1), m.Value())
}
