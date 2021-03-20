package concurrency

import (
	"context"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/damnever/resilience-go/concurrency/limit"
)

func TestConcurrently(t *testing.T) {
	t.Parallel()

	testf := func(l limit.Limit) {
		limiter := New(l)
		wg := sync.WaitGroup{}
		for i := 0; i < 100; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if limiter.Allow(context.TODO()) != nil {
					return
				}
				start := time.Now()
				r := rand.New(rand.NewSource(start.UnixNano()))
				time.Sleep(time.Duration(float64(8*time.Millisecond) * r.Float64()))
				limiter.Observe(start, r.Intn(10) < 5)
			}()
		}

		donec := make(chan struct{})
		go func() {
			wg.Wait()
			close(donec)
		}()
		select {
		case <-donec:
		case <-time.After(10 * time.Second):
			t.Fatalf("timed out: %s", l.Name())
		}
	}

	t.Run("Fixed", func(t *testing.T) {
		l, err := limit.NewFixedLimit(18)
		require.Nil(t, err)
		t.Parallel()
		testf(l)
	})
	t.Run("Settable", func(t *testing.T) {
		l, err := limit.NewSettableLimit(18)
		require.Nil(t, err)
		t.Parallel()
		go func() {
			time.Sleep(3 * time.Millisecond)
			l.Set(20)
		}()
		testf(l)
	})
	t.Run("AIMD", func(t *testing.T) {
		l, err := limit.NewAIMDLimit(limit.AIMDOptions{
			MinLimit: 5,
			Timeout:  5 * time.Millisecond,
		})
		require.Nil(t, err)
		t.Parallel()
		testf(l)
	})
	t.Run("Gradient", func(t *testing.T) {
		l, err := limit.NewGradientLimit(limit.GradientOptions{
			MinLimit: 5,
		})
		require.Nil(t, err)
		t.Parallel()
		testf(l)
	})
}
