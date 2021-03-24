package circuitbreaker

import (
	"time"
)

type counter struct {
	totalcalls  int
	failedcalls int
	slowcalls   int
}

func (c *counter) reset() {
	c.totalcalls = 0
	c.failedcalls = 0
	c.slowcalls = 0
}

type windowedMetrics struct {
	total *counter

	counters []*counter
	size     int
	first    int
	lasttime time.Time
	span     time.Duration
}

func newWindowedMetrics(window time.Duration, now time.Time) *windowedMetrics {
	interval := time.Second
	size := int(window / interval)
	if size < 5 {
		size = 5
		interval = window / time.Duration(size)
	}
	counters := make([]*counter, size, size)
	for i := 0; i < size; i++ {
		counters[i] = &counter{}
	}
	return &windowedMetrics{
		total:    &counter{},
		counters: counters,
		size:     size,
		first:    0,
		lasttime: now,
		span:     interval,
	}
}

func (m *windowedMetrics) count(now time.Time) counter {
	m.advance(now, false, false, false)
	return *m.total
}

func (m *windowedMetrics) record(now time.Time, failed, isslow bool) counter {
	m.advance(now, true, failed, isslow)
	return *m.total
}

func (m *windowedMetrics) reset(now time.Time) {
	if m.lasttime.After(now) {
		return
	}
	m.lasttime = now
	if m.total.totalcalls == 0 {
		return
	}

	for i := 0; i < m.size; i++ {
		m.counters[i].reset()
	}
	m.total.reset()
}

func (m *windowedMetrics) advance(now time.Time, called, failed, isslow bool) {
	elapsed := now.Sub(m.lasttime)
	idx := m.size - 1 + int(elapsed/m.span)
	if idx < 0 { // out of date.
		return
	}

	if elapsed >= 0 {
		m.lasttime = now
	}
	if more := idx - m.size + 1; more > 0 { // advance
		for i := m.first; i < more+m.first; i++ {
			counter := m.counters[i%m.size]

			m.total.totalcalls -= counter.totalcalls
			m.total.failedcalls -= counter.failedcalls
			m.total.slowcalls -= counter.slowcalls
			counter.reset()
		}
		m.first = (m.first + more) % m.size
		idx = (m.first + m.size - 1) % m.size
	}

	if called {
		counter := m.counters[idx]
		m.total.totalcalls++
		counter.totalcalls++
		if failed {
			m.total.failedcalls++
			counter.failedcalls++
		}
		if isslow {
			m.total.slowcalls++
			counter.slowcalls++
		}
	}
}
