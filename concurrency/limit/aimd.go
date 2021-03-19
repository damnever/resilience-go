package limit

import (
	"math"
	"sync"
	"sync/atomic"
	"time"
)

// AIMDOptions is the options for AIMD limit.
type AIMDOptions struct {
	// IncreaseNumber is the number to increase if the inflight greater than
	// one third of the current limit.
	// The default value is 1.
	IncreaseNumber uint32
	// DecreaseFactor is the decreasing factor if a dropped or timed out request occurs.
	// The default value is 0.5.
	DecreaseFactor float64
	// Timeout is the maximal tolerable latency/rtt value, otherwise we will treat it as dropped.
	// The default value is 5 seconds.
	Timeout time.Duration
	// MinLimit is the initial limit and the minimal limit.
	// The default value is 20.
	MinLimit uint32
	// MaxLimit is the allowed maximal limit.
	// The default value is 300.
	MaxLimit uint32
}

func (opts *AIMDOptions) withDefaults() {
	if opts.MinLimit == 0 {
		opts.MinLimit = 20
	}
	if opts.MaxLimit == 0 {
		opts.MaxLimit = 300
	}
	if opts.IncreaseNumber < 0 {
		opts.IncreaseNumber = 1
	}
	if opts.DecreaseFactor == 0 {
		opts.DecreaseFactor = 0.5
	}
	if opts.Timeout == 0 {
		opts.Timeout = 5 * time.Second
	}
}

func (opts AIMDOptions) validate() error {
	return nil
}

type aimdLimit struct {
	lock  sync.Mutex
	limit uint32

	minLimit       uint32
	maxLimit       uint32
	increaseNumber uint32
	decreaseFactor float64
	timeout        time.Duration
}

// NewAIMDLimit creates an additive increase and multiplicative decrease limit,
// best use case is on client side.
// The invalid options will be normalized to default values.
func NewAIMDLimit(opts AIMDOptions) (Limit, error) {
	(&opts).withDefaults()
	if err := opts.validate(); err != nil {
		return nil, err
	}
	return &aimdLimit{
		limit:          opts.MinLimit,
		minLimit:       opts.MinLimit,
		maxLimit:       opts.MaxLimit,
		increaseNumber: opts.IncreaseNumber,
		decreaseFactor: opts.DecreaseFactor,
		timeout:        opts.Timeout,
	}, nil
}

func (l *aimdLimit) Name() string {
	return "aimd"
}

func (l *aimdLimit) Get() uint32 {
	return atomic.LoadUint32(&l.limit)
}

func (l *aimdLimit) Observe(startAt time.Time, rtt time.Duration, inflight uint32, dropped bool) uint32 {
	// TODO: benchmarks
	// l.lock.Lock()
	// defer l.lock.Unlock()

	for {
		prev := atomic.LoadUint32(&l.limit)
		limit := float64(prev)
		if dropped || rtt > l.timeout {
			limit = float64(limit) * l.decreaseFactor
		} else if inflight*3 >= prev {
			limit += float64(l.increaseNumber)
		}

		newLimit := uint32(math.Min(float64(l.maxLimit), math.Max(float64(l.minLimit), limit)))
		if atomic.CompareAndSwapUint32(&l.limit, prev, newLimit) {
			return newLimit
		}
	}
}
