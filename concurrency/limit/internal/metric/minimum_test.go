package metric

import (
	"math"
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

	now := m.firsttime.Add(time.Duration(m.size-1) * m.span)
	for i := 1; i <= 10; i++ {
		now = now.Add(span)
		m.advance(now, true, time.Duration(i))
		require.Equal(t, time.Duration(1), m.Value())
	}

	m.advance(now, true, time.Duration(9))
	require.Equal(t, time.Duration(1), m.min)

	for i := 2; i < 10; i++ {
		now = now.Add(span)
		m.advance(now, false, -1)
		require.Equal(t, time.Duration(i), m.min)
	}

	now = now.Add(span)
	m.advance(now, false, -1)
	require.Equal(t, time.Duration(9), m.min)

	now = now.Add(span)
	m.advance(now, false, -1)
	require.Equal(t, time.Duration(math.MaxInt64), m.min)
}
