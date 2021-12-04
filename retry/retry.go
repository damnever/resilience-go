// Package retry provides functions to retry failed actions elegantly.
package retry

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"time"
)

// ErrContinue is a placholder helper, in case you have no error to return.
var ErrContinue = errors.New("retry: continue")

type unrecoverableError struct {
	cause error
}

func (e unrecoverableError) Unwrap() error {
	return e.cause
}

func (e unrecoverableError) Error() string {
	return e.cause.Error()
}

// Unrecoverable marks the error as unrecoverable, unrecoverable error makes Retry stop.
func Unrecoverable(err error) error {
	if err == nil {
		return nil
	}
	return unrecoverableError{cause: err}
}

// Run is a shortcut for Retry.Run with context.Background().
func Run(backoffs Backoffs, tryFunc func() error) error {
	return New(backoffs).Run(context.Background(), tryFunc)
}

// Retry retries failed actions with backoffs.
type Retry struct {
	backoffs Backoffs
}

// New a Retry with backoffs.
func New(backoffs Backoffs) Retry {
	return Retry{backoffs: backoffs}
}

// Run keeps calling the tryFunc until it returns a nil error or Unrecoverable error.
// The maximum number of retry is the number of backoffs, the first call isn't counted as a retry.
// It is thread-safe.
func (r Retry) Run(ctx context.Context, tryFunc func() error) (err error) {
	cancelc := ctx.Done()
	// Retry should be rare, so no need to reuse those timers?
	var timer *time.Timer
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()

	for i, n := 0, r.backoffs.count()+1; i < n; i++ {
		err = tryFunc()
		if err == nil {
			return
		}
		unrecoverable := unrecoverableError{}
		if errors.As(err, &unrecoverable) {
			err = unrecoverable.cause
			return
		}

		backoff := r.backoffs.get(i)
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

// Iterator creates a new Iterator. The returned Iterator is not thread-safe.
func (r Retry) Iterator() *Iterator {
	return &Iterator{
		next:     0,
		backoffs: r.backoffs,
		timer:    nil,
	}
}

// Iterator wraps backoffs for for-loop style flat workflows.
// It is not thread-safe.
type Iterator struct {
	next     int
	backoffs Backoffs
	timer    *time.Timer
}

// Next returns false if the ctx is done or the Iterator is exhausted,
// otherwise it waits corresponding backoff time then returns true.
// There is no waiting in the first call, since the first call isn't
// counted as a retry.
func (i *Iterator) Next(ctx context.Context) bool {
	next := i.next
	i.next++
	if next > i.backoffs.count() {
		if i.timer != nil {
			i.timer.Stop()
		}
		return false
	}
	if next == 0 {
		return true
	}

	backoff := i.backoffs.get(next - 1)
	if backoff == 0 {
		select {
		case <-ctx.Done():
			i.next = i.backoffs.count() + 1 // Mark the Iterator invalid.
			return false
		default:
		}
		return true
	}

	if i.timer == nil {
		i.timer = time.NewTimer(backoff)
	} else {
		i.timer.Reset(backoff)
	}
	select {
	case <-ctx.Done():
		i.next = i.backoffs.count() + 1 // Mark the Iterator invalid.
		return false
	case <-i.timer.C:
		return true
	}
}

// Backoffs represents a list of duration to wait before each retrying.
type Backoffs struct {
	immutable []time.Duration
	jitter    float64
	rand      *lockedRand
}

func (b *Backoffs) count() int {
	return len(b.immutable)
}

func (b *Backoffs) get(index int) time.Duration {
	if index >= len(b.immutable) {
		return 0
	}

	d := b.immutable[index]
	jitter := b.jitter
	if jitter == 0 || jitter >= 1 {
		return d
	}

	f := float64(d)
	delta := f * jitter
	min := f - delta
	max := f + delta
	return time.Duration(min + (max-min)*b.rand.rand01())
}

// BackoffsFrom creates Backoffs from a list of time.Duration.
func BackoffsFrom(backoffs []time.Duration) Backoffs {
	n := len(backoffs)
	b := Backoffs{immutable: make([]time.Duration, n, n), rand: newLockedRand()}
	copy(b.immutable, backoffs)
	return b
}

// ConstantBackoffs creates a list of backoffs with constant values.
func ConstantBackoffs(n int, backoff time.Duration) Backoffs {
	backoffs := make([]time.Duration, n, n)
	if backoff > 0 {
		for i := 0; i < n; i++ {
			backoffs[i] = backoff
		}
	}
	return Backoffs{immutable: backoffs, rand: newLockedRand()}
}

// ZeroBackoffs creates a list of backoffs with zero values.
func ZeroBackoffs(n int) Backoffs {
	return ConstantBackoffs(n, 0)
}

// ExponentialBackoffs creates a list of backoffs with values are calculated by min*2^[0 1 2 .. n).
// All backoff value is equal or less than max, zero max means no such limitation.
func ExponentialBackoffs(n int, min, max time.Duration) Backoffs {
	backoffs := make([]time.Duration, n, n)
	if min > 0 {
		for i := 0; i < n; i++ {
			v := min * (1 << uint(i))
			if max > 0 && v > max {
				v = max
			}
			backoffs[i] = v
		}
	}
	return Backoffs{immutable: backoffs, rand: newLockedRand()}
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
			r = rand.New(rand.NewSource(time.Now().UnixNano())) //nolint:gosec
		}
		jitter = r.Float64()
	}
	return Backoffs{immutable: backoffs.immutable, jitter: jitter, rand: backoffs.rand}
}

type lockedRand struct {
	lock sync.Mutex
	rand *rand.Rand
}

func newLockedRand() *lockedRand {
	return &lockedRand{
		lock: sync.Mutex{},
		rand: rand.New(rand.NewSource(time.Now().UnixNano())), //nolint:gosec
	}
}

func (r *lockedRand) rand01() float64 {
	r.lock.Lock()
	f := r.rand.Float64()
	r.lock.Unlock()
	return f
}
