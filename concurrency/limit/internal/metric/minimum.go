package metric

import (
	"math"
	"sync"
	"sync/atomic"
	"time"
)

type Minimum struct {
	value int64
}

func (m *Minimum) Value() time.Duration {
	return time.Duration(atomic.LoadInt64(&m.value))
}

func (m *Minimum) Observe(v time.Duration) time.Duration {
	for {
		prev := atomic.LoadInt64(&m.value)
		if int64(v) >= prev {
			return time.Duration(prev)
		}

		if atomic.CompareAndSwapInt64(&m.value, prev, int64(v)) {
			return v
		}
	}
}

type WindowedMinimum struct {
	lock sync.RWMutex

	min time.Duration
	idx int

	values   []time.Duration
	size     int
	first    int
	lasttime time.Time
	span     time.Duration
}

func NewWindowedMinimum(window, span time.Duration) *WindowedMinimum {
	size := int(window / span)
	values := make([]time.Duration, size, size)
	for i := range values {
		values[i] = -1
	}
	return &WindowedMinimum{
		min: math.MaxInt64,
		idx: -1,

		values:   values,
		size:     size,
		first:    0,
		lasttime: time.Now(),
		span:     span,
	}
}

func (m *WindowedMinimum) Value() time.Duration {
	m.lock.Lock()
	now := time.Now()
	m.advance(now, false, -1)
	min := m.min
	m.lock.Unlock()

	if min == math.MaxInt64 {
		return -1
	}
	return min
}

func (m *WindowedMinimum) Observe(v time.Duration) time.Duration {
	m.lock.Lock()
	now := time.Now()
	m.advance(now, true, v)
	min := m.min
	m.lock.Unlock()

	if min == math.MaxInt64 {
		return -1
	}
	return min
}

func (m *WindowedMinimum) advance(now time.Time, evaluated bool, v time.Duration) { //nolint:gocognit
	elapsed := now.Sub(m.lasttime)
	idx := m.size - 1 + int(elapsed/m.span)
	if idx < 0 { // out of date, not possible if the window large enough..
		return
	}

	if elapsed >= 0 {
		m.lasttime = now
	}
	if more := idx - m.size + 1; more > 0 { //nolint:nestif
		// advance
		changed := m.idx == -1
		for i := m.first; i < more+m.first; i++ {
			m.values[i%m.size] = -1
			changed = changed || i%m.size == m.idx
		}
		m.first = (m.first + more) % m.size
		idx = (m.first + m.size - 1) % m.size

		if changed {
			changed = false
			m.min = math.MaxInt64
			for i := m.first; i < m.size+m.first-more; i++ {
				if vv := m.values[i%m.size]; vv != -1 && vv < m.min {
					m.min = vv
					m.idx = i % m.size
					changed = true
				}
			}

			if !changed {
				m.min = math.MaxInt64
				m.idx = -1
			}
		}
	}

	if evaluated {
		if vv := m.values[idx]; vv == -1 || v < vv {
			m.values[idx] = v
		}
		if v < m.min {
			m.min = v
			m.idx = idx
		}
	}
}
