package circuitbreaker

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSlidingWindowedMetrics(t *testing.T) {
	now := time.Now()
	m := newWindowedMetrics(30*time.Second, now)
	require.Equal(t, time.Second, m.span)
	require.Equal(t, 30, m.size)

	for i := 0; i < 30; i++ {
		_now := now.Add(time.Duration(i) * time.Second)
		total := m.record(_now, true, false)
		require.Equal(t, i+1, total.totalcalls)
		require.Equal(t, i+1, total.failedcalls)
		require.Equal(t, 0, total.slowcalls)
	}
	for i := 0; i < 30; i++ {
		_now := now.Add(time.Duration(i) * time.Second)
		total := m.record(_now, false, true)
		require.Equal(t, 31+i, total.totalcalls)
		require.Equal(t, 30, total.failedcalls)
		require.Equal(t, 1+i, total.slowcalls)
	}
	for i := -5; i < 0; i++ {
		_now := now.Add(time.Duration(i) * time.Second)
		total := m.record(_now, false, false)
		require.Equal(t, 60, total.totalcalls)
		require.Equal(t, 30, total.failedcalls)
		require.Equal(t, 30, total.slowcalls)
	}
	for i := 30; i < 60; i++ {
		_now := now.Add(time.Duration(i) * time.Second)
		total := m.record(_now, true, true)
		require.Equal(t, 90-i-1, total.totalcalls)
		require.Equal(t, 30, total.failedcalls)
		require.Equal(t, 30, total.slowcalls)
	}
	for i := 60; i < 90; i++ {
		_now := now.Add(time.Duration(i) * time.Second)
		total := m.record(_now, false, false)
		require.Equal(t, 30, total.totalcalls)
		require.Equal(t, 89-i, total.failedcalls)
		require.Equal(t, 89-i, total.slowcalls)
	}

	now = time.Now().Add(5 * time.Minute)
	m.reset(now)
	total := m.count(now)
	require.Equal(t, 0, total.totalcalls)
	require.Equal(t, 0, total.failedcalls)
	require.Equal(t, 0, total.slowcalls)
}

func BenchmarkSlidingWindowedMetrics(b *testing.B) {
	m := newWindowedMetrics(20*time.Second, time.Now())

	for i := 0; i < b.N; i++ {
		now := time.Now()
		m.record(now, true, i%2 == 0)
		// m.count(now)
	}
}
