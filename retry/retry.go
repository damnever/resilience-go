// Package retry provides util functions to retry fail actions.
package retry

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

// ErrNeedRetry is a placholder helper, in case you have no error to return, such as bool status, etc.
var ErrNeedRetry = errors.New("retry: need retry")

// State controls whether the fail action should continue retrying.
type State uint8

const (
	// Continue continues retrying the fail action.
	Continue State = iota
	// StopWithErr stops retrying the fail action,
	// returns the error which the RetryFunc returns.
	StopWithErr
	// StopWithNil stops retrying the fail action, returns nil.
	StopWithNil
)

// Retrier retrys fail actions with backoff.
type Retrier struct {
	backoffs Backoffs

	randl sync.Mutex
	rand  *rand.Rand
}

// New creates a new Retrier with backoffs, the backoffs is the wait
// time before each retrying.
// The count of retrying will be len(backoffs), the first call
// is not counted in retrying.
func New(backoffs Backoffs) *Retrier {
	return &Retrier{
		backoffs: extend(backoffs),
		rand:     rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// Run keeps calling the RetryFunc if it returns (Continue, non-nil-err),
// otherwise it will stop retrying. It is goroutine safe.
func (r *Retrier) Run(ctx context.Context, try func() (State, error)) (err error) {
	var state State
	cancelc := ctx.Done()
	// Retry should be rare, so no need to pooling timers?
	var timer *time.Timer
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()

	for i := range r.backoffs.immutable {
		state, err = try()
		switch state {
		case StopWithErr:
			return err
		case StopWithNil:
			return nil
		default: // Continue
		}
		if err == nil {
			return nil
		}

		backoff := r.applyJitterFactor(i)
		if backoff > 0 {
			if timer == nil {
				timer = time.NewTimer(backoff)
			} else {
				// It is safe to reset it since the channel explicitly drained.
				timer.Reset(backoff)
			}
			select {
			case <-cancelc:
				return ctx.Err()
			case <-timer.C:
			}
		} else {
			select {
			case <-cancelc:
				return ctx.Err()
			default:
			}
		}
	}
	return err
}

func (r *Retrier) applyJitterFactor(i int) time.Duration {
	d := r.backoffs.immutable[i]
	jitter := r.backoffs.jitter
	if jitter == 1 {
		return d
	}

	f := float64(d)
	delta := f * jitter
	min := f - delta
	max := f + delta
	return time.Duration(min + (max-min)*r.rand01())
}

func (r *Retrier) rand01() float64 {
	r.randl.Lock()
	f := r.rand.Float64()
	r.randl.Unlock()
	return f
}

// Retry is a shortcut for Retrier.Run with context.Background().
func Retry(backoffs Backoffs, try func() (State, error)) error {
	return New(backoffs).Run(context.Background(), try)
}

// Backoffs holds a list of immutable backoffs with a optional jitter factor.
type Backoffs struct {
	immutable []time.Duration
	jitter    float64
}

func extend(backoffs Backoffs) Backoffs {
	capability := cap(backoffs.immutable)
	if capability != len(backoffs.immutable)+1 {
		panic(fmt.Errorf("unexpected backoffs capability: %d", cap(backoffs.immutable)))
	}
	return Backoffs{
		immutable: backoffs.immutable[0:capability],
		jitter:    backoffs.jitter,
	}
}

// BackoffsFromSlice creates Backoffs from a slice of time.Duration.
func BackoffsFromSlice(backoffs []time.Duration) Backoffs {
	n := len(backoffs)
	b := Backoffs{immutable: make([]time.Duration, n, n+1)}
	copy(b.immutable, backoffs)
	return b
}

// ConstantBackoffs creates a list of backoffs with constant values.
func ConstantBackoffs(n int, backoff time.Duration) Backoffs {
	backoffs := make([]time.Duration, n, n+1)
	if backoff > 0 {
		for i := 0; i < n; i++ {
			backoffs[i] = backoff
		}
	}
	return Backoffs{immutable: backoffs, jitter: 1}
}

// ZeroBackoffs creates a list of backoffs with zero values.
func ZeroBackoffs(n int) Backoffs {
	return ConstantBackoffs(n, 0)
}

// ExponentialBackoffs creates a list of backoffs with values are calculated by backoff*2^[0 1 2 .. n).
func ExponentialBackoffs(n int, backoff time.Duration) Backoffs {
	backoffs := make([]time.Duration, n, n+1)
	if backoff > 0 {
		for i := 0; i < n; i++ {
			backoffs[i] = backoff * (1 << uint(i))
		}
	}
	return Backoffs{immutable: backoffs, jitter: 1}
}

// WithJitterFactor applies a jitter factor on backoffs, the original one is unchanged,
// jitter must less than 1 and greater than 0, otherwise a random value will be chosen.
func WithJitterFactor(backoffs Backoffs, jitter float64) Backoffs {
	var r *rand.Rand
	for {
		if jitter > 0 && jitter < 1 {
			break
		}
		if r == nil {
			r = rand.New(rand.NewSource(time.Now().UnixNano()))
		}
		jitter = r.Float64()
	}
	return Backoffs{immutable: backoffs.immutable, jitter: jitter}
}
