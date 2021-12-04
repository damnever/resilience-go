package retry

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRetry(t *testing.T) {
	t.Parallel()

	errFatal := fmt.Errorf("retry-test: fatal error")
	backoff := 20 * time.Millisecond
	for i, _tt := range []struct {
		backoffs         Backoffs
		tryFunc          func() func() error
		expectError      error
		expectExecutions int
		expectTime       func(time.Duration) bool
	}{
		{
			backoffs: ConstantBackoffs(5, backoff),
			tryFunc: func() func() error {
				return func() error { return ErrContinue }
			},
			expectError:      ErrContinue,
			expectExecutions: 6,
			expectTime: func(elapsed time.Duration) bool {
				return elapsed > backoff*5 && elapsed < backoff*6
			},
		},
		{
			backoffs: ExponentialBackoffs(5, backoff, 0),
			tryFunc: func() func() error {
				return func() error { return nil }
			},
			expectError:      nil,
			expectExecutions: 1,
			expectTime: func(elapsed time.Duration) bool {
				return elapsed < backoff
			},
		},
		{
			backoffs: ZeroBackoffs(5),
			tryFunc: func() func() error {
				cnt := 0
				return func() error {
					cnt++
					if cnt == 2 {
						return nil
					}
					return ErrContinue
				}
			},
			expectError:      nil,
			expectExecutions: 2,
			expectTime: func(elapsed time.Duration) bool {
				return elapsed < backoff
			},
		},
		{
			backoffs: BackoffsFrom([]time.Duration{
				20*time.Millisecond + backoff, 10*time.Millisecond + backoff, backoff, backoff, backoff,
			}),
			tryFunc: func() func() error {
				cnt := 0
				return func() error {
					cnt++
					if cnt == 3 {
						return Unrecoverable(errFatal)
					}
					return ErrContinue
				}
			},
			expectError:      errFatal,
			expectExecutions: 3,
			expectTime: func(elapsed time.Duration) bool {
				min := 30*time.Millisecond + 2*backoff
				return elapsed > min && elapsed < min+backoff/2
			},
		},
	} {
		tt := _tt

		t.Run(fmt.Sprintf("#%02d#Retry", i), func(t *testing.T) {
			t.Parallel()

			count := 0
			startAt := time.Now()
			try := tt.tryFunc()
			err := Run(tt.backoffs, func() error {
				count++
				return try()
			})
			require.Equal(t, tt.expectError, err)
			require.Equal(t, tt.expectExecutions, count)
			require.True(t, tt.expectTime(time.Since(startAt)))
		})

		t.Run(fmt.Sprintf("#%02d#Iterator", i), func(t *testing.T) {
			t.Parallel()

			count := 0
			startAt := time.Now()
			try := tt.tryFunc()

			iter := New(tt.backoffs).Iterator()
			var err error
			for iter.Next(context.Background()) {
				count++
				if err = try(); err == nil {
					break
				}
				if errors.Is(err, errFatal) {
					err = errors.Unwrap(err)
					break
				}
			}

			require.Equal(t, tt.expectError, err)
			require.Equal(t, tt.expectExecutions, count)
			require.True(t, tt.expectTime(time.Since(startAt)))
		})
	}

	{
		err := Run(WithJitterFactor(BackoffsFrom([]time.Duration{time.Millisecond}), 0.35),
			func() error { return ErrContinue })
		require.Equal(t, ErrContinue, err)
	}
	{
		err := Run(WithJitterFactor(ConstantBackoffs(10, time.Millisecond), 0.35),
			func() error { return ErrContinue })
		require.Equal(t, ErrContinue, err)
	}
}

func TestIterator(t *testing.T) {
	t.Parallel()

	for _, _tt := range []struct {
		typ      string
		backoffs Backoffs
	}{
		{typ: "ZeroBackoffs", backoffs: ZeroBackoffs(5)},
		{typ: "ConstantBackoffs", backoffs: ConstantBackoffs(5, 10*time.Millisecond)},
	} {
		tt := _tt
		r := New(tt.backoffs)

		t.Run(tt.typ+"Exhausted", func(t *testing.T) {
			t.Parallel()

			iter := r.Iterator()
			for iter.Next(context.Background()) {
			}
			for i := 0; i < 100; i++ {
				require.False(t, iter.Next(context.Background()))
			}
		})

		t.Run(tt.typ+"ContextCanceled", func(t *testing.T) {
			t.Parallel()

			iter := r.Iterator()
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			count := 0
			for iter.Next(ctx) {
				count++
			}
			require.Equal(t, 1, count)
			for i := 0; i < 100; i++ {
				require.False(t, iter.Next(context.Background()))
			}
		})
	}
}

func TestBackoffFactory(t *testing.T) {
	t.Parallel()
	{
		backoffs := ZeroBackoffs(3)
		require.Equal(t, 3, len(backoffs.immutable))
		for _, v := range backoffs.immutable {
			require.Equal(t, time.Duration(0), v)
		}
	}
	{
		backoff := 100 * time.Millisecond
		backoffs := ConstantBackoffs(5, backoff)
		require.Equal(t, 5, len(backoffs.immutable))
		for _, v := range backoffs.immutable {
			require.Equal(t, backoff, v)
		}
	}
	{
		backoff := 10 * time.Millisecond
		backoffs := ExponentialBackoffs(10, backoff, 0)
		require.Equal(t, 10, len(backoffs.immutable))
		for i, v := range backoffs.immutable {
			require.Equal(t, backoff*(1<<uint(i)), v)
		}
	}
}
