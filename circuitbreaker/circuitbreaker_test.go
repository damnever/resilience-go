package circuitbreaker

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var _testconf = Config{
	MetricsWindow:         time.Second,
	SleepWindow:           500 * time.Millisecond,
	TriggerThreshold:      10,
	ErrorRateThreshold:    0.5,
	SlowCallRateThreshold: 0.5,
	SlowCallDuration:      10 * time.Second,
	CoverPanic:            true,
}

func TestCircuitBreakerStateTransition(t *testing.T) {
	cb := New(_testconf)
	require.Equal(t, Close, cb.currentState())

	{ // CLOSE -> OPEN
		for i := 0; i < _testconf.TriggerThreshold-1; i++ {
			cb.trace(Close, time.Now(), false)
		}
		require.Equal(t, Close, cb.currentState())
		cb.trace(Close, time.Now(), true)
		require.Equal(t, Open, cb.currentState())
	}
	{ // OPEN -> HALF-OPEN
		require.Equal(t, Open, cb.currentState())
		cb.setState(Open, time.Now().Add(-_testconf.SleepWindow+10*time.Millisecond))
		time.Sleep(10 * time.Millisecond)
		require.Equal(t, HalfOpen, cb.currentState())
	}
	{ // HALF-OPEN -> CLOSE
		_, state, _ := cb.getState()
		require.Equal(t, HalfOpen, state)
		require.Equal(t, Open, cb.currentState())
		cb.trace(HalfOpen, time.Now(), true)
		require.Equal(t, Close, cb.currentState())
	}
	{ // CLOSE -> OPEN -> HALF-OPEN
		require.Equal(t, Close, cb.currentState())
		time.Sleep(time.Second / 2)
		for i := 0; i < _testconf.TriggerThreshold; i++ {
			cb.trace(Close, time.Now(), false)
		}
		require.Equal(t, Open, cb.currentState())
		cb.setState(Open, time.Now().Add(-_testconf.SleepWindow+10*time.Millisecond))
		time.Sleep(10 * time.Millisecond)
		require.Equal(t, HalfOpen, cb.currentState())
	}
	{ // HALF-OPEN -> OPEN
		_, state, _ := cb.getState()
		require.Equal(t, HalfOpen, state)
		require.Equal(t, Open, cb.currentState())
		cb.trace(HalfOpen, time.Now(), false)
		require.Equal(t, Open, cb.currentState())
	}
	{ // OPEN -> CLOSE
		require.Equal(t, Open, cb.currentState())
		for i := 0; i < _testconf.TriggerThreshold*2; i++ {
			cb.trace(Close, time.Now(), true)
		}
		require.Equal(t, Close, cb.currentState())
	}
}

func TestCircuitBreakerConcurrently(t *testing.T) {
	t.Parallel()

	_testconf.TriggerThreshold = 20
	_testconf.SleepWindow = 100 * time.Millisecond
	cb := New(_testconf)

	// OPEN
	wg := sync.WaitGroup{}
	for i := 0; i < _testconf.TriggerThreshold; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			cb.Run(func() bool {
				time.Sleep((_testconf.MetricsWindow / time.Duration(10)) * time.Duration(i%4))
				return i%2 != 0
			})
		}(i)
	}
	wg.Wait()
	require.True(t, cb.Circuit().IsInterrupted())

	// SLEEP: no request get through
	for i := 0; i < _testconf.TriggerThreshold; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			require.True(t, cb.Circuit().IsInterrupted())
		}()
	}
	wg.Wait()
	time.Sleep(_testconf.SleepWindow)

	// HALF-OPEN: only one request get through
	var passed int32
	for i := 1; i < _testconf.TriggerThreshold; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c := cb.Circuit()
			if c.IsInterrupted() {
				return
			}
			c.Observe(time.Now(), false)
			atomic.AddInt32(&passed, 1)
		}()
	}
	wg.Wait()
	require.Equal(t, int32(1), passed)

	{ // OPEN
		c := cb.Circuit()
		require.True(t, c.IsInterrupted())
	}

	{
		time.Sleep(_testconf.SleepWindow)
		c := cb.Circuit()
		require.False(t, c.IsInterrupted()) // HALF-OPEN
		c.Observe(time.Now(), true)
		require.False(t, c.IsInterrupted()) // CLOSE
	}
	// CLOSE
	for i := 0; i < _testconf.TriggerThreshold; i++ {
		c := cb.Circuit()
		require.False(t, c.IsInterrupted())
		c.Observe(time.Now(), true)
	}

	time.Sleep(_testconf.MetricsWindow)
	require.False(t, cb.Circuit().IsInterrupted())
}
