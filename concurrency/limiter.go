package concurrency

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"time"

	"github.com/damnever/resilience-go/concurrency/limit"
)

// ErrLimitExceeded is returned if the in-flight requests greater than the current limit.
var ErrLimitExceeded = errors.New("concurrency: the limit exceeded")

// A Limiter limits the concurrency.
//
// A global limiter for the whole serivce/instance is not recommended,
// isolation is a great idea.
type Limiter interface {
	// Allow checks if the request is allowed to pass,
	// blocking limiter will wait until context cancel or timeout.
	// Caller must returns immediately and ignore the Observe call if an error returned,
	// otherwise it may panic.
	Allow(ctx context.Context) error
	// Observe observes state and calculate stats, caller must call it if Allow returns nil,
	// otherwise it may panic.
	Observe(startAt time.Time, dropped bool)
	// Close closes the limiter, the following Allow will always returns ErrLimitExceeded.
	Close() error
}

// IsDropped checks if the error is context.Canceled/DeadlineExceeded, or net.Error.Timeout().
// It is caller's responsibility to retrieve the original error, such as github.com/pkg/errors.Cause.
func IsDropped(err error) bool {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}

	var neterr net.Error
	return errors.As(err, &neterr) && neterr.Timeout()
}

type simpleLimiter struct {
	limit *wrappedLimit
}

// New creates a simple limiter based on the limit algorithm.
func New(l limit.Limit) Limiter {
	return &simpleLimiter{limit: wrap(l)}
}

func (l *simpleLimiter) Allow(ctx context.Context) error {
	if l.limit.Allow() {
		return nil
	}
	return ErrLimitExceeded
}

func (l *simpleLimiter) Observe(startAt time.Time, dropped bool) {
	l.limit.Observe(startAt, time.Since(startAt), dropped)
}

func (l *simpleLimiter) Close() error {
	l.limit.Deactivate()
	return nil
}

type wrappedLimit struct {
	inflight int32
	limit    limit.Limit
}

func wrap(l limit.Limit) *wrappedLimit {
	return &wrappedLimit{limit: l}
}

func (l *wrappedLimit) Allow() bool {
	n := l.limit.Get()

	if uint32(atomic.AddInt32(&l.inflight, 1)) <= n {
		return true
	}
	if atomic.AddInt32(&l.inflight, -1) < 0 {
		panic("concurrency: negative inflight")
	}
	return false
}

func (l *wrappedLimit) Observe(startAt time.Time, rtt time.Duration, dropped bool) uint32 {
	inflight := atomic.AddInt32(&l.inflight, -1)
	if inflight < 0 {
		panic("concurrency: negative inflight")
	}
	return l.limit.Observe(startAt, rtt, uint32(inflight), dropped)
}

func (l *wrappedLimit) Deactivate() {
	l.limit.Deactivate()
}

func (l *wrappedLimit) Deactivated() bool {
	return l.limit.Deactivated()
}
