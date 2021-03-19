package limit

import (
	"time"
)

type fixedLimit struct {
	limit uint32
}

// NewFixedLimit creates a fixed limit.
func NewFixedLimit(limit uint32) (Limit, error) {
	return &fixedLimit{limit: limit}, nil
}

func (l *fixedLimit) Name() string {
	return "fixed"
}

func (l *fixedLimit) Get() uint32 {
	return l.limit
}

func (l *fixedLimit) Observe(startAt time.Time, rtt time.Duration, inflight uint32, dropped bool) uint32 {
	return l.limit
}
