package limit

import (
	"sync/atomic"
	"time"
)

// A Limit represents a kind of algorithm to calculate the limit.
type Limit interface {
	// Name returns the name of limit.
	Name() string
	// Get returns the current limit.
	Get() uint32
	// Observe calculates the limit and returns the value.
	Observe(startAt time.Time, rtt time.Duration, inflight uint32, dropped bool) uint32

	// Deactivate deactivates the limit and Get/Observe will always return 0.
	Deactivate()
	// Deactivated returns true if the limit deactivated.
	Deactivated() bool
}

type deactivated struct {
	flag uint32
}

func (d *deactivated) deactivate() bool {
	return atomic.CompareAndSwapUint32(&d.flag, 0, 1)
}

func (d *deactivated) Deactivated() bool {
	return atomic.LoadUint32(&d.flag) == 1
}
