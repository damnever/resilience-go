package concurrency

import (
	"container/list"
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/damnever/resilience-go/concurrency/limit"
)

// ErrBlockingTimedout is the error returned by the blocking limiter when the MaxBlockingTimeout passes.
var ErrBlockingTimedout = errors.New("concurrency: blocking timed out")

// BlockingLimiterOptions is the options to configurates the blocking limiter.
type BlockingLimiterOptions struct {
	maxBacklogSize     int
	maxBlockingTimeout time.Duration
}

func defaultBlockingLimiterOptions() BlockingLimiterOptions {
	return BlockingLimiterOptions{
		maxBacklogSize:     1000,
		maxBlockingTimeout: 10 * time.Second,
	}
}

// WithBlockingLimiterOption is the type of option.
type WithBlockingLimiterOption func(opts *BlockingLimiterOptions)

// MaxBacklogSize is the option to set the max backlog size for blocking limiter.
// It will panic if the valueless than 1.
func MaxBacklogSize(n int) WithBlockingLimiterOption {
	return func(opts *BlockingLimiterOptions) {
		if n < 1 {
			panic("concurrency: MaxBacklogSize must greater than 0")
		}
		opts.maxBacklogSize = n
	}
}

// MaxBlockingTimeout is the option to set the max blocking timeout for blocking limiter.
// It will panic if the valueless than 1.
func MaxBlockingTimeout(d time.Duration) WithBlockingLimiterOption {
	return func(opts *BlockingLimiterOptions) {
		if d < 1 {
			panic("concurrency: MaxBlockingTimeout must greater than 0")
		}
		opts.maxBlockingTimeout = d
	}
}

type fifoBlockingLimiter struct {
	// Channels act as first-in-first-out queues, refer to:
	// https://golang.org/ref/spec#ChannelType:~:text=Channels%20act%20as%20first%2Din%2Dfirst%2Dout%20queues
	waitc              chan struct{}
	backlogSize        int32
	maxBacklogSize     int32
	maxBlockingTimeout time.Duration

	limit *wrappedLimit
}

// NewFIFOBlockingLimiter creates a new Limiter which blocks when the limit has been reached,
// it will wait until context cancel, or timeouts, or MaxBlockingTimeout passes,
// or another goroutines completed.
// The blocked goroutines exceeds the MaxBacklogSize, it will return immediately without waiting.
// The goroutines will wake in first-in-first-out order.
//
// The default value of MaxBacklogSize is 1000.
// The default value of MaxBlockingTimeout is 10 seconds.
func NewFIFOBlockingLimiter(l limit.Limit, optFuncs ...WithBlockingLimiterOption) Limiter {
	opts := defaultBlockingLimiterOptions()
	for _, optFunc := range optFuncs {
		optFunc(&opts)
	}
	return &fifoBlockingLimiter{
		waitc:              make(chan struct{}),
		backlogSize:        0,
		maxBacklogSize:     int32(opts.maxBacklogSize),
		maxBlockingTimeout: opts.maxBlockingTimeout,

		limit: wrap(l),
	}
}

func (l *fifoBlockingLimiter) Allow(ctx context.Context) error {
	if l.limit.Allow() {
		return nil
	}

	if atomic.AddInt32(&l.backlogSize, 1) > l.maxBacklogSize {
		atomic.AddInt32(&l.backlogSize, -1)
		return ErrLimitExceeded
	}
	var timeoutc <-chan time.Time
	maxDeadline := time.Now().Add(l.maxBlockingTimeout + time.Millisecond)
	if deadline, ok := ctx.Deadline(); ok && deadline.After(maxDeadline) {
		timeoutc = time.After(l.maxBlockingTimeout)
	}
	select {
	case <-ctx.Done():
		atomic.AddInt32(&l.backlogSize, -1)
		return ctx.Err()
	case <-timeoutc:
		atomic.AddInt32(&l.backlogSize, -1)
		return ErrBlockingTimedout
	case l.waitc <- struct{}{}:
		atomic.AddInt32(&l.backlogSize, -1)
	}

	if l.limit.Allow() {
		return nil
	}
	return ErrLimitExceeded
}

func (l *fifoBlockingLimiter) Observe(startAt time.Time, dropped bool) {
	l.limit.Observe(startAt, time.Since(startAt), dropped)

	select {
	case <-l.waitc:
	default:
	}
}

type lifoBlockingLimiter struct {
	lock               sync.Mutex
	backlog            *list.List
	maxBacklogSize     int
	maxBlockingTimeout time.Duration

	limit *wrappedLimit
}

// NewLIFOBlockingLimiter creates a new Limiter which blocks when the limit has been reached,
// it will wait until context cancel, or timeouts, or MaxBlockingTimeout passes,
// or another goroutines completed.
// The blocked goroutines exceeds the MaxBacklogSize, it will return immediately without waiting.
// The goroutines will wake in last-in-first-out order. This strategy ensures the resource is
// properly protected but favors availability over latency by not fast failing requests when
// the limit has been reached.
//
// The default value of MaxBacklogSize is 1000.
// The default value of MaxBlockingTimeout is 10 seconds.
func NewLIFOBlockingLimiter(l limit.Limit, optFuncs ...WithBlockingLimiterOption) Limiter {
	opts := defaultBlockingLimiterOptions()
	for _, optFunc := range optFuncs {
		optFunc(&opts)
	}
	return &lifoBlockingLimiter{
		backlog:            list.New(),
		maxBacklogSize:     opts.maxBacklogSize,
		maxBlockingTimeout: opts.maxBlockingTimeout,

		limit: wrap(l),
	}
}

func (l *lifoBlockingLimiter) Allow(ctx context.Context) error {
	if l.limit.Allow() {
		return nil
	}

	l.lock.Lock()
	if l.backlog.Len() >= l.maxBacklogSize {
		l.lock.Unlock()
		return ErrLimitExceeded
	}
	notifyc := make(chan struct{})
	elem := &list.Element{Value: notifyc}
	l.backlog.PushFront(elem)
	l.lock.Unlock()

	removeElem := func() {
		l.lock.Lock()
		l.backlog.Remove(elem)
		l.lock.Unlock()
	}

	var timeoutc <-chan time.Time
	maxDeadline := time.Now().Add(l.maxBlockingTimeout + time.Millisecond)
	if deadline, ok := ctx.Deadline(); ok && deadline.After(maxDeadline) {
		timeoutc = time.After(l.maxBlockingTimeout)
	}
	select {
	case <-ctx.Done():
		removeElem()
		return ctx.Err()
	case <-timeoutc:
		removeElem()
		return ErrBlockingTimedout
	case <-notifyc:
		removeElem()
	}

	if l.limit.Allow() {
		return nil
	}
	return ErrLimitExceeded
}

func (l *lifoBlockingLimiter) Observe(startAt time.Time, dropped bool) {
	l.limit.Observe(startAt, time.Since(startAt), dropped)

	l.lock.Lock()
	elem := l.backlog.Front()
	l.backlog.Remove(elem)
	l.lock.Unlock()
	if elem != nil {
		notifyc := elem.Value.(chan struct{})
		close(notifyc)
	}
}
