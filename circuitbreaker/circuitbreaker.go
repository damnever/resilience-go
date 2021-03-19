// Package circuitbreaker implements the Circuit-Breaker pattern.
//
// Reference: https://github.com/Netflix/Hystrix/wiki/How-it-Works#CircuitBreaker
// The transformation of circuitbreaker's state is as follows:
//  1. Assuming the number of requests reachs the threshold(Config.TriggerThreshold), initial state is CLOSE.
//  2. After 1, if the error rate or the rate of high latency exceeds the threshold(Config.ErrorRate/HighLatencyRate),
//     the circuitbreaker is OPEN.
//  3. Otherwise for 2, the circuitbreaker will be CLOSE.
//  4. After 2, the circuitbreaker remains OPEN until some amount of time(Config.SleepWindow) pass,
//     then it's the HALF-OPEN which let a single request through, if the request fails, it returns to
//     the OPEN. If the request succeeds, the circuitbreaker is CLOSE, and 1 takes over again.
//
// NOTE(damnever): we are not using consistent state since it is slow,
// though we use a big lock in metrics, it is fast enough for most circumstances.
package circuitbreaker

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

// ErrIsOpen means circuitbreaker is open, the system or API is not healthy.
var ErrIsOpen = errors.New("circuitbreaker: is open, not healthy")

// State represents the current state.
type State uint8

const (
	// Close state allows all requests to pass.
	Close State = iota
	// HalfOpen state allows a single one request to pass.
	HalfOpen
	// HalfOpen state disallows any request to pass.
	Open
)

// Config configures the CircuitBreaker.
type Config struct {
	// MetricsWindow is the time window to keep metrics, any metrics before the window will be droped.
	MetricsWindow time.Duration
	// SleepWindow is the wait time before trying to recover from OPEN state.
	SleepWindow time.Duration
	// TriggerThreshold is the threshold of total requests to trigger the circuitbreaker.
	// Zero value will disable the circuitbreaker.
	TriggerThreshold int
	// AbnormalRateThreshold is the error rate to trigger the circuitbreaker.
	// Abnormal could be errors, slow request(high latency), etc.
	ErrorRateThreshold float64
	// TODO
	// SlowCallRateThreshold is the slow call rate to trigger the circuitbreaker.
	SlowCallRateThreshold float64
	// SlowCallDuration is the maximal tolerable duration.
	SlowCallDuration time.Duration
	// CoverPanic will recover the panic, only used in Run.
	CoverPanic bool
}

// DefaultConfig returns a default config.
func DefaultConfig() Config {
	return Config{
		MetricsWindow:         20 * time.Second,
		SleepWindow:           3 * time.Second,
		TriggerThreshold:      50,
		ErrorRateThreshold:    0.1,
		SlowCallRateThreshold: 0.15,
		SlowCallDuration:      300 * time.Millisecond,
		CoverPanic:            false,
	}
}

// CircuitBreaker traces the failures then protects the service.
//
// A global circuitbreaker for the whole serivce/instance is not recommended,
// isolation is a great idea.
type CircuitBreaker struct {
	cfg      Config
	disabled uint32

	state   uint64 // This stores current state and open time.
	metrics *lockedMetrics
}

// New creates a new CircuitBreaker. It doesn't check the Config for the caller.
func New(cfg Config) *CircuitBreaker {
	disabled := uint32(0)
	if cfg.TriggerThreshold <= 0 {
		disabled = 1
	}
	now := time.Now()
	cb := &CircuitBreaker{
		cfg:      cfg,
		disabled: disabled,
		state:    0,
		metrics:  &lockedMetrics{metrics: newWindowedMetrics(cfg.MetricsWindow, now)},
	}
	cb.setState(Close, now)
	return cb
}

// Enable enables the circuitbreaker.
func (cb *CircuitBreaker) Enable() {
	if atomic.CompareAndSwapUint32(&cb.disabled, 0, 1) {
		now := time.Now()
		cb.metrics.reset(now)
		cb.setState(Close, now)
	}
}

// Disable disables the circuitbreaker.
func (cb *CircuitBreaker) Disable() {
	if atomic.CompareAndSwapUint32(&cb.disabled, 1, 0) {
		now := time.Now()
		cb.metrics.reset(now)
		cb.setState(Close, now)
	}
}

// Run is a shortcut for the workflow.
// Returns false from fn means the status is abnormal.
func (cb *CircuitBreaker) Run(fn func() bool) {
	if atomic.LoadUint32(&cb.disabled) == 1 {
		fn()
		return
	}

	state := cb.currentState()
	if state == Open {
		return
	}

	var statusOk bool
	defer func(startAt time.Time) {
		if cb.cfg.CoverPanic {
			if perr := recover(); perr != nil {
				statusOk = false
			}
		}
		cb.trace(state, startAt, statusOk)
	}(time.Now())

	statusOk = fn()
	return
}

// Circuit creates a Circuit, each request(API call) requires exactly one Circuit, DO NOT reuse it or ignore it.
// You can use Run for "convenience".
func (cb *CircuitBreaker) Circuit() Circuit {
	c := Circuit{
		state:   cb.currentState(),
		breaker: cb,
	}
	return c
}

func (cb *CircuitBreaker) setState(cbState State, now time.Time) {
	packedState := uint64(now.UnixNano()<<8) | uint64(cbState)
	atomic.StoreUint64(&cb.state, packedState)
}

func (cb *CircuitBreaker) casState(prevRawState uint64, cbState State, now time.Time) bool {
	packedState := uint64(now.UnixNano()<<8) | uint64(cbState)
	return atomic.CompareAndSwapUint64(&cb.state, prevRawState, packedState)
}

func (cb *CircuitBreaker) getState() (uint64, State, int64) {
	v := atomic.LoadUint64(&cb.state)
	return v, State(v & 0xFF), int64(v >> 8)
}

func (cb *CircuitBreaker) nowAfter(now time.Time, unixNano int64) bool {
	nowUnixNano := (now.UnixNano() << 8) >> 8
	return nowUnixNano > unixNano
}

func (cb *CircuitBreaker) currentState() State {
	if atomic.LoadUint32(&cb.disabled) == 1 {
		return Close
	}

	now := time.Now()
	if cb.metrics.isHealthy(now, cb.cfg.TriggerThreshold, cb.cfg.ErrorRateThreshold, cb.cfg.SlowCallRateThreshold) {
		cb.setState(Close, now) // XXX: approximatly..
		return Close
	}

	prev, state, unixNano := cb.getState()
	if state == Close {
		if cb.nowAfter(now, unixNano) {
			cb.setState(Open, now) // XXX: approximatly..
			return Open
		}
		return state
	}
	if !cb.nowAfter(now, unixNano+cb.cfg.SleepWindow.Nanoseconds()) {
		// Fast path, assume we are in OPEN state, we only returns HALF-OPEN if we CAS succeeded.
		return Open
	}

	// SleepWindow passed or passed again, try converting into HALF-OPEN stage.
	if cb.casState(prev, HalfOpen, now) {
		return HalfOpen
	}
	// In most cases, we should in HALF-OPEN state, but as the time pass,
	// the metrics may become healthy, and we cloud in CLOSE state as well.
	// also the previous HALF-OPEN may just arrive and we cloud in OPEN state..
	_, state, _ = cb.getState()
	if state == HalfOpen {
		// Only one request allowed, others should be blocked.
		return Open
	}
	return state
}

func (cb *CircuitBreaker) trace(prevState State, startAt time.Time, statusOk bool) {
	if atomic.LoadUint32(&cb.disabled) == 1 {
		return
	}
	failed := !statusOk
	isSlow := time.Since(startAt) >= cb.cfg.SlowCallDuration

	now := time.Now()
	cb.metrics.observe(now, failed, isSlow)

	if prevState != HalfOpen {
		return
	}
	prev, prevState, _ := cb.getState()
	if prevState != HalfOpen {
		return
	}
	// It's not possible that the current result come from newly switched HALF-OPEN state.
	if isSlow || failed { // OPEN again.
		cb.casState(prev, Open, now)
	} else if cb.casState(prev, Close, now) { // CLOSE.
		cb.metrics.reset(now)
	}
}

// Circuit is used for every call.
type Circuit struct {
	state   State
	breaker *CircuitBreaker
}

func (c Circuit) State() State {
	return c.state
}

// IsInterrupted returns true if the circuit is interrupted,
// in other words, the circuitbreaker is OPEN, the API or system is not healty.
// The caller should return immediately(fail fast).
func (c Circuit) IsInterrupted() bool {
	return c.state == Open
}

// Observe records the caller's state.
// If IsInterrupted returns false, the caller must(use defer) Trace the result,
// otherwise, the functionality cloud be broken.
func (c Circuit) Observe(startAt time.Time, statusOk bool) {
	if c.state == Open {
		return
	}
	c.breaker.trace(c.state, startAt, statusOk)
}

// Anyway this is fast enough for most circumstances..
type lockedMetrics struct {
	lock    sync.Mutex
	metrics *windowedMetrics
}

func (m *lockedMetrics) observe(now time.Time, failed, isSlow bool) {
	m.lock.Lock()
	m.metrics.record(now, failed, isSlow)
	m.lock.Unlock()
}

func (m *lockedMetrics) isHealthy(now time.Time, totalThreshold int,
	errorRateThreshold, slowCallRateThreshold float64) bool {
	m.lock.Lock()
	total := m.metrics.count(now)
	m.lock.Unlock()

	if total.totalcalls < totalThreshold {
		return true
	}
	totalcallsf := float64(total.totalcalls)
	if float64(total.failedcalls)/totalcallsf >= errorRateThreshold {
		return false
	}
	return float64(total.slowcalls)/totalcallsf < slowCallRateThreshold
}

func (m *lockedMetrics) reset(now time.Time) {
	m.lock.Lock()
	m.metrics.reset(now)
	m.lock.Unlock()
}
