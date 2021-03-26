package limit

import (
	"sync/atomic"
	"time"
)

// Settable allows the caller to set a new limit value manually.
type Settable interface {
	Limit

	Set(limit uint32)
}

type settableLimit struct {
	limit uint32

	*deactivated
}

// NewSettableLimit creates a settable limit, it acts as fixed limit after set.
func NewSettableLimit(limit uint32) (Settable, error) {
	return &settableLimit{
		limit:       limit,
		deactivated: &deactivated{},
	}, nil
}

func (l *settableLimit) Name() string {
	return "settable"
}

func (l *settableLimit) Set(limit uint32) {
	if l.deactivated.Deactivated() {
		return
	}
	atomic.StoreUint32(&l.limit, limit)
}

func (l *settableLimit) Get() uint32 {
	if l.deactivated.Deactivated() {
		return 0
	}
	return atomic.LoadUint32(&l.limit)
}

func (l *settableLimit) Observe(startAt time.Time, rtt time.Duration, inflight uint32, dropped bool) uint32 {
	if l.deactivated.Deactivated() {
		return 0
	}
	return atomic.LoadUint32(&l.limit)
}

func (l *settableLimit) Deactivate() {
	l.deactivated.deactivate()
}
