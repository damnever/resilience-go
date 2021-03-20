package metric

import (
	"math"
	"sync/atomic"
	"time"
)

// ExponentialMovingAverage has a problem that the decreasing speed may slow.
type ExponentialMovingAverage struct {
	avg uint64

	smoothing float64
	window    int
}

// NewExponentialMovingAverage creates a new ExponentialMovingAverage.
// If the smoothing factor is increased, more recent observations have more influence on the EMA.
func NewExponentialMovingAverage(smoothing float64, window int) *ExponentialMovingAverage {
	if smoothing <= 0 {
		smoothing = 2
	}
	return &ExponentialMovingAverage{
		smoothing: smoothing,
		window:    window,
	}
}

func (ema *ExponentialMovingAverage) Value() time.Duration {
	return time.Duration(math.Float64frombits(atomic.LoadUint64(&ema.avg)))
}

func (ema *ExponentialMovingAverage) Observe(d time.Duration) time.Duration {
	v := float64(d)
	factor := ema.smoothing / float64(1+ema.window)

	for {
		prev := atomic.LoadUint64(&ema.avg)
		avg := v*factor + math.Float64frombits(prev)*(1-factor)
		if atomic.CompareAndSwapUint64(&ema.avg, prev, math.Float64bits(avg)) {
			return time.Duration(avg)
		}
	}
}
