package limit

import (
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
}
