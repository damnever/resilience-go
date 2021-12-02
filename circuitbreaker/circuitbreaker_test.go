package circuitbreaker

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCircuitBreakerStateTransition(t *testing.T) {
	testconf := Config{
		MetricsSlidingWindow:      time.Second,
		CooldownDuration:          500 * time.Millisecond,
		AllowedProbeCalls:         2,
		ObservationsThreshold:     10,
		ErrorRateThreshold:        0.5,
		SlowCallRateThreshold:     0.5,
		SlowCallDurationThreshold: 10 * time.Second,
	}
	cb := New(testconf)
	require.Equal(t, Closed, cb.State())

	record := func(startAt time.Time, statusOk bool) {
		state := cb.beforeCall()
		if state.allowCall() {
			cb.afterCall(state, startAt, statusOk)
		}
	}

	for i, _tt := range []struct {
		startFunc     func() // setState to start if startFunc is nil.
		start         State
		end           State
		exec          func()
		isInterrupted bool // isInterrupted after exec.
	}{
		{
			start: Closed, end: Disabled,
			exec: func() {
				cb.Enable(false)
				for i := 0; i < testconf.ObservationsThreshold; i++ {
					record(time.Now(), false)
				}
			},
			isInterrupted: false,
		},
		{
			start: Disabled, end: Closed,
			exec: func() {
				cb.Enable(true)
			},
			isInterrupted: false,
		},
		{
			start: Closed, end: ForceOpen,
			exec: func() {
				cb.ForceOpen()
				for i := 0; i < testconf.ObservationsThreshold*2; i++ {
					record(time.Now(), true)
				}
			},
			isInterrupted: true,
		},
		{
			start: ForceOpen, end: Closed,
			exec: func() {
				cb.Enable(true)
			},
			isInterrupted: false,
		},
		{
			start: Closed, end: Closed,
			exec: func() {
				for i := 0; i < testconf.ObservationsThreshold-1; i++ {
					record(time.Now(), false)
				}
			},
			isInterrupted: false,
		},
		{ // ErrorRateThreshold
			start: Closed, end: Open,
			exec: func() {
				for i := 0; i < testconf.ObservationsThreshold-1; i++ {
					record(time.Now(), false)
				}
				record(time.Now(), true)
			},
			isInterrupted: true,
		},
		{ // SlowCallRateThreshold
			start: Closed, end: Open,
			exec: func() {
				for i := 0; i < testconf.ObservationsThreshold-1; i++ {
					record(time.Now().Add(-testconf.SlowCallDurationThreshold), true)
				}
				record(time.Now(), true)
			},
			isInterrupted: true,
		},
		{
			start: Open, end: Open,
			startFunc: func() {
				cb.setState(Open, time.Now().Add(-testconf.CooldownDuration+10*time.Millisecond))
			},
			exec: func() {
				time.Sleep(5 * time.Millisecond)
			},
			isInterrupted: true,
		},
		{
			start: Open, end: HalfOpen,
			startFunc: func() {
				cb.setState(Open, time.Now().Add(-testconf.CooldownDuration+10*time.Millisecond))
			},
			exec: func() {
				time.Sleep(11 * time.Millisecond)
			},
			isInterrupted: false,
		},
		{
			start: Closed, end: HalfOpen,
			exec: func() {
				for i := 0; i < testconf.ObservationsThreshold-1; i++ {
					record(time.Now(), false)
				}
				record(time.Now(), true)
				cb.setState(Open, time.Now().Add(-testconf.CooldownDuration+10*time.Millisecond))
				time.Sleep(11 * time.Millisecond)
			},
			isInterrupted: false,
		},
		{
			start: HalfOpen, end: HalfOpen,
			exec: func() {
				for i := 0; i < testconf.AllowedProbeCalls; i++ {
					cb.beforeCall().allowCall()
				}
			},
			isInterrupted: true,
		},
		{
			start: HalfOpen, end: Open,
			exec: func() {
				for i := 0; i < testconf.AllowedProbeCalls; i++ {
					record(time.Now(), false)
				}
			},
			isInterrupted: true,
		},
		{
			start: HalfOpen, end: Closed,
			exec: func() {
				for i := 0; i < testconf.AllowedProbeCalls; i++ {
					record(time.Now(), true)
				}
			},
			isInterrupted: false,
		},
		{
			start: Open, end: Closed,
			startFunc: func() {
				cb.setState(Open, time.Now().Add(-testconf.CooldownDuration+10*time.Millisecond))
			},
			exec: func() {
				time.Sleep(11 * time.Millisecond)
				for i := 0; i < testconf.AllowedProbeCalls; i++ {
					record(time.Now(), true)
				}
			},
			isInterrupted: false,
		},
	} {
		tt := _tt
		t.Run(fmt.Sprintf("#%02d#%s=>%s", i, tt.start, tt.end), func(t *testing.T) {
			if tt.startFunc != nil {
				tt.startFunc()
				require.Equal(t, tt.start, cb.State())
			} else {
				cb.setState(tt.start, time.Now())
			}
			tt.exec()
			state := cb.beforeCall()
			require.Equal(t, tt.end, state.state)
			require.Equal(t, tt.isInterrupted, !state.allowCall())
		})
	}
}

func TestCircuitBreakerConcurrently(t *testing.T) {
	t.Parallel()

	testconf := Config{
		MetricsSlidingWindow:      time.Second,
		CooldownDuration:          100 * time.Millisecond,
		AllowedProbeCalls:         2,
		ObservationsThreshold:     20,
		ErrorRateThreshold:        0.5,
		SlowCallRateThreshold:     0.5,
		SlowCallDurationThreshold: 10 * time.Second,
	}
	cb := New(testconf)

	// OPEN
	wg := sync.WaitGroup{}
	for i := 0; i < testconf.ObservationsThreshold; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			cb.Run(func() bool {
				time.Sleep((testconf.MetricsSlidingWindow / time.Duration(10)) * time.Duration(i%4))
				return i%2 != 0
			}, nil)
		}(i)
	}
	wg.Wait()
	require.Equal(t, Open, cb.State())

	// SLEEP: no request get through
	for i := 0; i < testconf.ObservationsThreshold; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			require.False(t, cb.beforeCall().allowCall())
		}()
	}
	wg.Wait()
	time.Sleep(testconf.CooldownDuration)

	// HALF-OPEN: only AllowedProbeCalls request get through
	var passed int32
	for i := 1; i < testconf.ObservationsThreshold; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			guard := cb.Guard()
			if !guard.Allow() {
				return
			}
			guard.Observe(time.Now(), false)
			atomic.AddInt32(&passed, 1)
		}()
	}
	wg.Wait()
	require.Equal(t, int32(testconf.AllowedProbeCalls), passed)

	{ // OPEN
		require.Equal(t, Open, cb.State())
	}

	{
		time.Sleep(testconf.CooldownDuration)
		for i := 0; i < testconf.AllowedProbeCalls; i++ {
			guard := cb.Guard()
			require.True(t, guard.Allow()) // HALF-OPEN
			require.Equal(t, HalfOpen, cb.State(), i)
			guard.Observe(time.Now(), true)
		}
		guard := cb.Guard()
		require.True(t, guard.Allow()) // CLOSED
		guard.Observe(time.Now(), true)
	}
	// CLOSED
	for i := 0; i < testconf.ObservationsThreshold; i++ {
		guard := cb.Guard()
		require.True(t, guard.Allow())
		guard.Observe(time.Now(), true)
	}

	time.Sleep(testconf.MetricsSlidingWindow)
	require.True(t, cb.Guard().Allow())
}

func BenchmarkCircuitBreaker(b *testing.B) {
	cb := New(Config{
		MetricsSlidingWindow:      500 * time.Millisecond,
		CooldownDuration:          50 * time.Millisecond,
		AllowedProbeCalls:         2,
		ObservationsThreshold:     20,
		ErrorRateThreshold:        0.1,
		SlowCallRateThreshold:     0.2,
		SlowCallDurationThreshold: 10 * time.Millisecond,
	})
	require.Equal(b, Closed, cb.State())

	for i := 0; i < b.N; i++ {
		cb.Run(func() bool {
			return true
		}, func() {})
	}
}
