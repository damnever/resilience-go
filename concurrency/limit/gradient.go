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
	// MinLimit is the initial limit and the minimal limit.
	// The default value is 20.
	MinLimit uint32
	// MaxLimit is the allowed maximal limit.
	// The default value is 300.
	MaxLimit uint32
	// RttToleranceFactor is the tolerance for the recorded minimal latency/rtt, it must equal or greater than 1.
	// The default value is 1.5.
	RttToleranceFactor float64
	// Smoothing makes the limit increasing/decreasing smoother, the bigger the steeper.
	// The default value is 0.3.
	Smoothing float64

	// SampleWindow     time.Duration
	// SamplePercentile float64
	// MinRttWindow     time.Duration
}

func (opts *GradientOptions) withDefaults() {
	if opts.MinLimit <= 0 {
		opts.MinLimit = 20
	}
	if opts.MaxLimit <= 0 {
		opts.MaxLimit = 300
	}
	if opts.RttToleranceFactor == 0 {
		opts.RttToleranceFactor = 1.5
	}
	if opts.Smoothing == 0 {
		opts.Smoothing = 0.3
	}
}

//nolint:goerr113
func (opts GradientOptions) validate() error {
	if opts.RttToleranceFactor < 1 {
		return fmt.Errorf("concurrency/limit: RttToleranceFactor should equal or greater than 1: %f",
			opts.RttToleranceFactor)
	}
	if opts.Smoothing < 0 || opts.Smoothing > 1 {
		return fmt.Errorf("concurrency/limit: Smoothing should in range[0,1]: %f",
			opts.Smoothing)
	}
	return nil
}

type gradientLimit struct {
	lock           sync.RWMutex
	estimatedLimit uint32
	minLimit       uint32
	maxLimit       uint32
	rttTolerance   float64
	smoothing      float64
	countDown      uint32
	maxCountDown   uint32

	summary *metric.Summary
	min     *metric.WindowedMinimum
}

// NewGradientLimit creates a gradient algorithm based on the Gradient2 algorithm from Netflix
// and the adaptive concurrency filter from Envoyproxy.
// The empty options will be normalized to default values.
//
// The benchmark test shows that it is much more stable than the Gradient2 algorithm from Netflix.
// Ref:
//   - https://github.com/Netflix/concurrency-limits#gradient2
//   - https://www.envoyproxy.io/docs/envoy/latest/configuration/http/http_filters/adaptive_concurrency_filter
//   - https://github.com/envoyproxy/envoy/issues/7789
func NewGradientLimit(opts GradientOptions) (Limit, error) {
	(&opts).withDefaults()
	if err := opts.validate(); err != nil {
		return nil, err
	}
	maxCountDown := uint32(64) // FIXME(damnever): magic number.
	return &gradientLimit{
		estimatedLimit: uint32(math.Ceil(float64(opts.MinLimit) * 1.1)), // Add a little buffer.
		minLimit:       opts.MinLimit,
		maxLimit:       opts.MaxLimit,
		rttTolerance:   opts.RttToleranceFactor,
		smoothing:      opts.Smoothing,
		maxCountDown:   maxCountDown,
		countDown:      uint32(calcCountDown(float64(opts.MinLimit), maxCountDown)),

		summary: metric.NewSummary(0.95, 200*time.Millisecond, 40*time.Millisecond), // FIXME(damnever): magic numbers.
		min:     metric.NewWindowedMinimum(60*time.Second, 1*time.Second),           // FIXME(damnever): magic numbers.
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
	newLimit += math.Sqrt(newLimit) // Burst room.
	newLimit = estimatedLimit*(1-l.smoothing) + newLimit*l.smoothing
	newLimit = math.Max(float64(l.minLimit), math.Min(float64(l.maxLimit), newLimit))

	// Apply jitter?
	l.countDown = uint32(calcCountDown(newLimit, l.maxCountDown))

	atomic.StoreUint32(&l.estimatedLimit, uint32(newLimit))
	return l.estimatedLimit
}

func calcCountDown(limit float64, max uint32) float64 {
	return math.Max(2, math.Min(float64(max), math.Round(limit/10))) // FIXME(damnever): magic numbers.
}
