package limit

import (
	"sync/atomic"
	"time"
)

type fixedLimit struct {
	limit uint32

	*deactivated
}

// NewFixedLimit creates a fixed limit.
func NewFixedLimit(limit uint32) (Limit, error) {
	return &fixedLimit{
		limit:       limit,
		deactivated: &deactivated{},
	}, nil
}

func (l *fixedLimit) Name() string {
	return "fixed"
}

func (l *fixedLimit) Get() uint32 {
	return atomic.LoadUint32(&l.limit)
}

func (l *fixedLimit) Observe(startAt time.Time, rtt time.Duration, inflight uint32, dropped bool) uint32 {
	return atomic.LoadUint32(&l.limit)
}

func (l *fixedLimit) Deactivate() {
	atomic.StoreUint32(&l.limit, 0)
	l.deactivated.deactivate()
}
