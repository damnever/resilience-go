package limit

import (
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/damnever/resilience-go/concurrency/limit/internal/metric"
)

// GradientOptions is the options for gradient limit.
type GradientOptions struct {
	// MinLimit is the allowed minimal limit.
	// No need to change it in normal circumstances.
	// The default value is 1.
	MinLimit uint32
	// MaxLimit is the allowed maximal limit.
	// The default value is 10000.
	MaxLimit uint32
	// RttToleranceFactor is the tolerance which will be applied to the measured minimal latency/rtt,
	// a proper value maintains stability and maximizes the resource utilization.
	// It must equal or greater than 1.
	// The default value is 1.25.
	RttToleranceFactor float64

	// FIXME(damnever): expose those configurations.
	// SampleWindow     time.Duration
	// SamplePercentile float64
	// MinRttWindow     time.Duration
}

func (opts *GradientOptions) withDefaults() {
	if opts.MinLimit == 0 {
		opts.MinLimit = 1
	}
	if opts.MaxLimit <= 0 {
		opts.MaxLimit = 10000
	}
	if opts.RttToleranceFactor == 0 {
		opts.RttToleranceFactor = 1.25
	}
}

//nolint:goerr113
func (opts GradientOptions) validate() error {
	if opts.RttToleranceFactor < 1 {
		return fmt.Errorf("concurrency/limit: RttToleranceFactor should equal or greater than 1: %f",
			opts.RttToleranceFactor)
	}
	return nil
}

type gradientLimit struct {
	lock           sync.RWMutex
	estimatedLimit uint32
	minLimit       uint32
	maxLimit       uint32
	rttTolerance   float64

	countDown    uint32
	maxCountDown float64

	summary *metric.Summary
	min     *metric.WindowedMinimum
}

// NewGradientLimit creates a gradient algorithm based on the adaptive concurrency filter from EnvoyProxy
// and the Gradient2 algorithm from Netflix.
// The empty options will be normalized to default values.
//
// Ref:
//   - https://github.com/Netflix/concurrency-limits#gradient2
//   - https://www.envoyproxy.io/docs/envoy/latest/configuration/http/http_filters/adaptive_concurrency_filter
//   - https://github.com/envoyproxy/envoy/issues/7789
//   - https://www.youtube.com/watch?v=CQvmSXlnyeQ
func NewGradientLimit(opts GradientOptions) (Limit, error) {
	(&opts).withDefaults()
	if err := opts.validate(); err != nil {
		return nil, err
	}
	return &gradientLimit{
		estimatedLimit: uint32(math.Ceil(float64(opts.MinLimit) * 1.1)), // Add a little buffer.
		minLimit:       opts.MinLimit,
		maxLimit:       opts.MaxLimit,
		rttTolerance:   opts.RttToleranceFactor,
		maxCountDown:   float64(125), // FIXME(damnever): magic number.
		countDown:      2,

		summary: metric.NewSummary(0.95, 200*time.Millisecond, 40*time.Millisecond), // FIXME(damnever): magic numbers.
		min:     metric.NewWindowedMinimum(90*time.Second, 2*time.Second),           // FIXME(damnever): magic numbers.
	}, nil
}

func (l *gradientLimit) Name() string {
	return "gradient"
}

func (l *gradientLimit) Get() uint32 {
	return atomic.LoadUint32(&l.estimatedLimit)
}

func (l *gradientLimit) Observe(startAt time.Time, rtt time.Duration, inflight uint32, dropped bool) uint32 {
	l.summary.Observe(rtt)
	l.min.Observe(rtt)

	l.lock.Lock()
	defer l.lock.Unlock()
	if l.countDown--; l.countDown > 0 {
		return l.estimatedLimit
	}
	l.countDown = 1 // In case of there is no samples.

	sampleRtt := l.summary.Value()
	if sampleRtt < 0 {
		return l.estimatedLimit
	}
	minRtt := l.min.Value()
	if minRtt < 0 {
		return l.estimatedLimit
	}

	gradient := math.Max(0.5, math.Min(2, l.rttTolerance*float64(minRtt)/float64(sampleRtt)))
	estimatedLimit := float64(l.estimatedLimit)
	newLimit := estimatedLimit * gradient
	if newLimit > estimatedLimit {
		newLimit += math.Sqrt(newLimit) // Burst room.
	}
	// newLimit = estimatedLimit*(1-l.smoothing) + newLimit*l.smoothing?
	newLimit = math.Max(float64(l.minLimit), math.Min(float64(l.maxLimit), newLimit))

	// Apply jitter?
	l.countDown = uint32(l.calcCountDown(newLimit, inflight, l.maxCountDown))

	atomic.StoreUint32(&l.estimatedLimit, uint32(newLimit))
	return l.estimatedLimit
}

func (l *gradientLimit) calcCountDown(limit float64, inflight uint32, max float64) float64 {
	v := math.Min(float64(inflight)*0.8, math.Round(limit*0.6)) // FIXME(damnever): magic numbers.
	return math.Round(math.Max(2, math.Min(max, v)))
}
