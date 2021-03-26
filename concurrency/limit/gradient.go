package limit

import (
	"fmt"
	"math"
	"math/rand"
	"sync/atomic"
	"time"

	"github.com/damnever/resilience-go/concurrency/limit/internal/metric"
)

// GradientOptions is the options for gradient limit.
type GradientOptions struct {
	// MinLimit is the allowed minimal limit.
	// No need to change it under normal circumstances.
	// The default value is 1.
	MinLimit uint32
	// MaxLimit is the allowed maximal limit.
	// A big value may slow down the limit decreasing process.
	// The default value is 8000.
	MaxLimit uint32
	// RttToleranceFactor is the tolerance which will be applied to the measured minimal latency/rtt,
	// a proper value maintains stability and maximizes the resource utilization.
	// It must equal or greater than 1.
	// The default value is 1.25.
	RttToleranceFactor float64
	// SampleWindow is the time window to keep the relevant latency/rtt samples for summary.
	// A big window may slow down the recovering process, a small window may impact the stability.
	// The default value is 1500ms.
	SampleWindow time.Duration
	// SampleQuantile is the quantile to summarizes the collected samples.
	// A proper value maintains the stability by excluding the impact of acceptable outliers.
	// The default value is 0.95.
	SampleQuantile float64
	// MinRttSampleWindow is the time window to keep the minimal latency/rtt samples.
	// The sample will be used as a reference value to calculate the limit.
	// The default value is 90s.
	MinRttSampleWindow time.Duration
	// CalculationInterval is the interval to calculate the limit.
	// The default value is 99ms.
	CalculationInterval time.Duration
	// CalculationIntervalJitter is the jitter which will be applied on CalculationInterval.
	// The default value is 0.18.
	CalculationIntervalJitter float64
}

func (opts *GradientOptions) withDefaults() {
	if opts.MinLimit == 0 {
		opts.MinLimit = 1
	}
	if opts.MaxLimit == 0 {
		opts.MaxLimit = 8000
	}
	if opts.RttToleranceFactor == 0 {
		opts.RttToleranceFactor = 1.25
	}

	if opts.SampleWindow == 0 {
		opts.SampleWindow = 1500 * time.Millisecond
	}
	if opts.SampleQuantile == 0 {
		opts.SampleQuantile = 0.95
	}
	if opts.MinRttSampleWindow == 0 {
		opts.MinRttSampleWindow = 90 * time.Second
	}
	if opts.CalculationInterval == 0 {
		opts.CalculationInterval = 99 * time.Millisecond
	}
	if opts.CalculationIntervalJitter == 0 {
		opts.CalculationIntervalJitter = 0.18
	}
}

//nolint:goerr113
func (opts GradientOptions) validate() error {
	if opts.RttToleranceFactor < 1 {
		return fmt.Errorf("concurrency/limit: RttToleranceFactor should equal or greater than 1: %f",
			opts.RttToleranceFactor)
	}
	if opts.SampleWindow <= 500*time.Millisecond {
		return fmt.Errorf("concurrency/limit: SampleWindow should greater than 500ms: %v",
			opts.SampleWindow)
	}
	if opts.SampleQuantile < 0.5 {
		return fmt.Errorf("concurrency/limit: SamplePercentile should equal or greater than 0.5: %f",
			opts.SampleQuantile)
	}
	if opts.MinRttSampleWindow <= 1*time.Second {
		return fmt.Errorf("concurrency/limit: MinRttSampleWindow should greater than 1s: %v",
			opts.MinRttSampleWindow)
	}
	if opts.CalculationInterval <= 50*time.Millisecond {
		return fmt.Errorf("concurrency/limit: CalculationInterval should greater than 50ms: %v",
			opts.CalculationInterval)
	}
	if opts.CalculationIntervalJitter <= 0 || opts.CalculationIntervalJitter >= 1 {
		return fmt.Errorf("concurrency/limit: CalculationIntervalJitter should greater than 0 and less than 1: %f",
			opts.CalculationIntervalJitter)
	}
	return nil
}

type gradientLimit struct {
	estimatedLimit uint32
	minLimit       uint32
	maxLimit       uint32
	rttTolerance   float64

	summary *metric.Summary
	min     *metric.WindowedMinimum

	stopc chan struct{}
	donec chan struct{}
	*deactivated
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

	// FIXME(damnever): magic numbers.
	span := 2 * time.Second
	if opts.MinRttSampleWindow/span < 10 {
		span = opts.MinRttSampleWindow / 10
	}
	if opts.MinRttSampleWindow/span > 60 {
		span = opts.MinRttSampleWindow / 60
	}

	l := &gradientLimit{
		estimatedLimit: uint32(math.Ceil(float64(opts.MinLimit) * 1.1)), // Add a little buffer.
		minLimit:       opts.MinLimit,
		maxLimit:       opts.MaxLimit,
		rttTolerance:   opts.RttToleranceFactor,

		summary: metric.NewSummary(opts.SampleQuantile, opts.SampleWindow, 5),
		min:     metric.NewWindowedMinimum(opts.MinRttSampleWindow, span),

		stopc:       make(chan struct{}),
		donec:       make(chan struct{}),
		deactivated: &deactivated{},
	}
	go l.calculationLoop(opts.CalculationInterval, opts.CalculationIntervalJitter)
	return l, nil
}

func (l *gradientLimit) Name() string {
	return "gradient"
}

func (l *gradientLimit) Get() uint32 {
	return atomic.LoadUint32(&l.estimatedLimit)
}

func (l *gradientLimit) Observe(startAt time.Time, rtt time.Duration, inflight uint32, dropped bool) uint32 {
	if l.deactivated.Deactivated() {
		return 0
	}

	// XXX(damnever): ignore stale samples??
	l.summary.Observe(rtt)
	l.min.Observe(rtt)
	return atomic.LoadUint32(&l.estimatedLimit)
}

func (l *gradientLimit) Deactivate() {
	if l.deactivated.deactivate() {
		close(l.stopc)
		<-l.donec
		atomic.StoreUint32(&l.estimatedLimit, 0)
	}
}

func (l *gradientLimit) calculationLoop(interval time.Duration, jitter float64) {
	rng := rand.New(rand.NewSource(time.Now().UnixNano())) //nolint:gosec
	jittered := func(interval time.Duration) time.Duration {
		f := float64(interval)
		delta := f * jitter
		min := f - delta
		max := f + delta
		return time.Duration(min + (max-min)*rng.Float64())
	}

	jitteredInterval := jittered(interval)
	ticker := time.NewTicker(jitteredInterval)
	defer func() {
		ticker.Stop()
		close(l.donec)
	}()
	for {
		ticker.Reset(jitteredInterval)
		select {
		case <-l.stopc:
			return
		case <-ticker.C:
		}
		jitteredInterval = jittered(interval)

		sampleRtt := l.summary.Value()
		if sampleRtt < 0 {
			continue
		}
		minRtt := l.min.Value()
		if minRtt < 0 {
			continue
		}
		// XXX(damnever): checks if the collected sample count is large enough?

		gradient := math.Max(0.5, math.Min(2, l.rttTolerance*float64(minRtt)/float64(sampleRtt)))
		if gradient < 0.75 { // Make the calculation more aggressive to handle bursty traffic.
			jitteredInterval = time.Duration(float64(jitteredInterval) * (gradient + 0.2))
		}
		estimatedLimit := float64(l.estimatedLimit)
		newLimit := estimatedLimit * gradient
		if newLimit > estimatedLimit {
			newLimit += math.Sqrt(newLimit) // Burst room.
		}
		// newLimit = estimatedLimit*(1-l.smoothing) + newLimit*l.smoothing?
		newLimit = math.Max(float64(l.minLimit), math.Min(float64(l.maxLimit), newLimit))
		atomic.StoreUint32(&l.estimatedLimit, uint32(newLimit))
	}
}
