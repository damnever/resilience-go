// Package circuitbreaker implements the Circuit-Breaker pattern.
//
// The transformation of circuit breaker's state is as follows:
//  1. Assuming the number of events(requests) reachs the threshold(Config.ObservationsThreshold)
//     and the corresponding observations is done (the initial state is CLOSED).
//  2. After 1, if the error rate or the rate of slow calls exceeds the
//     threshold(Config.ErrorRate/SlowCallRate), the circuit breaker is OPEN.
//  3. Otherwise for 2, the circuit breaker stays CLOSED.
//  4. After 2, the circuit breaker remains OPEN until some amount of
//     time(Config.CooldownDuration) pass, then in HALF_OPEN state which let
//     a configurable number(Config.AllowedProbeCalls) of probe events(requests)
//     through, if those events(requests) fail the threshold check, it returns
//     to the OPEN, otherwise the circuit breaker is CLOSED, and 1 takes over again.
//
// Reference:
//  - https://en.wikipedia.org/wiki/Circuit_breaker_design_pattern
//  - https://github.com/Netflix/Hystrix/wiki/How-it-Works#CircuitBreaker
//  - https://resilience4j.readme.io/docs/circuitbreaker
package circuitbreaker

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

// ErrIsOpen means the circuit breaker is open, all the events(requests) denied at the time,
// the system or API is not healthy.
var ErrIsOpen = errors.New("circuitbreaker: is open, not healthy")

// State represents the current state.
type State uint8

const (
	// Undefined is a state only occurs in OnStateChange hook,
	// which indicates one of Enable/ForceOpen is called.
	Undefined State = iota
	// Closed indicates the circuit breaker is in "CLOSED" state.
	// Any normal event(request) may happen at time now.
	Closed
	// HalfOpen indicates the circuit breaker is in "HALF_OPEN" state.
	// Only a configurable number of probe events(requests) may happen at time now.
	HalfOpen
	// Open indicates the circuit breaker is in "OPEN" state.
	// All normal events(requests) should be reject at this period.
	Open
	// Disabled indicates the circuit breaker is in "CLOSED" state,
	// but no metrics are recorded so the state is frozen.
	Disabled
	// ForceOpen indicates the circuit breaker is frozen in "OPEN" state,
	// but no metrics are recorded so the state is frozen.
	ForceOpen
)

func (st State) String() string {
	switch st {
	case Undefined:
		return "undefined"
	case Closed:
		return "closed"
	case HalfOpen:
		return "half-open"
	case Open:
		return "open"
	case Disabled:
		return "disabled"
	case ForceOpen:
		return "force-open"
	default:
		panic("circuitbreaker: unknown state")
	}
}

// Config configures the CircuitBreaker.
type Config struct {
	// CooldownDuration is the amount of time the circuit breaker will stay in OPEN state.
	// Incoming events(requests) should be be reject during this period.
	CooldownDuration time.Duration
	// AllowedProbeCalls is the maximum allowed probe events(requests) in HALF_OPEN state
	// (after CooldownDuration).
	AllowedProbeCalls int
	// MetricsSlidingWindow is the time window to keep metrics,
	// any metric before this window will be droped.
	MetricsSlidingWindow time.Duration
	// ObservationsThreshold is the minimum number of required observations (in MetricsSlidingWindow period)
	// to trigger the metrics calculation in "CLOSED" state.
	ObservationsThreshold int
	// ErrorRateThreshold is the percentage of errors which makes the circuit breaker
	// transitions to "OPEN" state.
	// A value equal or greater than 1 disables it.
	ErrorRateThreshold float64
	// SlowCallRateThreshold is the percentage of slow calls which makes the circuit breaker
	// transitions to "OPEN" state.
	// The circuit breaker considers a call as slow when the call duration is equal or
	// greater than SlowCallDurationThreshold.
	// A value equal or greater than 1 disables it.
	SlowCallRateThreshold float64
	// SlowCallDurationThreshold is the maximal tolerable duration, a duration equal or
	// greater than this is slow.
	SlowCallDurationThreshold time.Duration
	// OnStateChange observes state changes, nil to ignore them.
	OnStateChange func(prev, current State)
}

// DefaultConfig returns a default Config.
func DefaultConfig() Config {
	return Config{
		CooldownDuration:          3 * time.Second,
		AllowedProbeCalls:         10,
		MetricsSlidingWindow:      30 * time.Second,
		ObservationsThreshold:     100,
		ErrorRateThreshold:        0.1,
		SlowCallRateThreshold:     0.15,
		SlowCallDurationThreshold: 300 * time.Millisecond,
		OnStateChange:             nil,
	}
}

// CircuitBreaker acts as a proxy for operations that might fail.
//
// A global circuitbreaker for the whole serivce/server is not recommended,
// isolation is a great idea.
type CircuitBreaker struct {
	cfg Config

	state *internalState
}

// New creates a new CircuitBreaker. NOTE the default value (from DefaultConfig())
// used when some of them is illegal or empty.
func New(cfg Config) *CircuitBreaker {
	defaults := DefaultConfig()
	if cfg.CooldownDuration <= 0 {
		cfg.CooldownDuration = defaults.CooldownDuration
	}
	if cfg.AllowedProbeCalls <= 0 {
		cfg.AllowedProbeCalls = defaults.AllowedProbeCalls
	}
	if cfg.MetricsSlidingWindow <= 0 {
		cfg.MetricsSlidingWindow = defaults.MetricsSlidingWindow
	}
	if cfg.ObservationsThreshold <= 0 {
		cfg.ObservationsThreshold = defaults.ObservationsThreshold
	}
	if cfg.ErrorRateThreshold <= 0 {
		cfg.ErrorRateThreshold = defaults.ErrorRateThreshold
	}
	if cfg.SlowCallRateThreshold <= 0 {
		cfg.SlowCallRateThreshold = defaults.SlowCallRateThreshold
	}
	if cfg.SlowCallDurationThreshold <= 0 {
		cfg.SlowCallDurationThreshold = defaults.SlowCallDurationThreshold
	}

	now := time.Now()
	cb := &CircuitBreaker{
		cfg: cfg,
	}
	cb.setState(Closed, now)
	return cb
}

// Enable enables or diables the circuit breaker, "disable" means the circuit breaker
// is in "CLOSED" state until Enable(true) or ForceOpen.
func (cb *CircuitBreaker) Enable(enable bool) {
	if enable {
		cb.setState(Closed, time.Now())
	} else {
		cb.setState(Disabled, time.Now())
	}
}

// ForceOpen puts the circuit breaker always in "OPEN" state.
func (cb *CircuitBreaker) ForceOpen() {
	cb.setState(ForceOpen, time.Now())
}

// Config returns the current Config.
func (cb *CircuitBreaker) Config() Config {
	return cb.cfg
}

// Run acts as a proxy, the circuit breaker counts false status of mainFunc as errors.
// ErrIsOpen is returned if the circuit breaker is open, also the non-nil fallbackFunc
// will be called in such state.
func (cb *CircuitBreaker) Run(mainFunc func() bool, fallbackFunc func()) error {
	state := cb.beforeCall()
	if !state.allowCall() {
		if fallbackFunc != nil {
			fallbackFunc()
		}
		return ErrIsOpen
	}

	statusOk := false
	defer func(startAt time.Time) {
		cb.afterCall(state, startAt, statusOk)
	}(time.Now())

	statusOk = mainFunc()
	return nil
}

// State returns the current state.
func (cb *CircuitBreaker) State() State {
	return cb.loadState().state
}

// Guard returns a DisposableGuard for flat workflow, throw it away after it has been used.
// You can use Run for "convenience".
func (cb *CircuitBreaker) Guard() DisposableGuard {
	state := cb.beforeCall()
	allow := state.allowCall()
	return DisposableGuard{
		state:   state,
		breaker: cb,
		allow:   allow,
	}
}

func (cb *CircuitBreaker) beforeCall() *internalState {
	state := cb.loadState()
	if state.state != Open {
		return state
	}

	now := time.Now()
	if !state.expired(now, cb.cfg.CooldownDuration) {
		return state
	}

	stateHalfOpen := cb.newState(HalfOpen, now)
	if cb.casState(state, stateHalfOpen) { // Transition from OPEN to HALF-OPEN.
		return stateHalfOpen
	}
	return cb.loadState()
}

func (cb *CircuitBreaker) afterCall(state *internalState, startAt time.Time, statusOk bool) {
	if state.state == Open || state.state == Disabled || state.state == ForceOpen {
		return // Skip.
	}

	now := time.Now()
	cbState, changed := state.trace(now, startAt, statusOk, cb.cfg.SlowCallDurationThreshold)
	if !changed {
		return
	}
	newState := cb.newState(cbState, now)
	if !cb.casState(state, newState) {
		// Recycle untouched metric object?
	}
}

var _unixNanoStartTime = time.Now().Add(-time.Hour)

func timeNano(t time.Time) int64 {
	return t.Sub(_unixNanoStartTime).Nanoseconds()
}

func (cb *CircuitBreaker) casState(prev, new *internalState) bool {
	swapped := atomic.CompareAndSwapPointer( //nolint:gosec
		(*unsafe.Pointer)(unsafe.Pointer(&cb.state)), //nolint:gosec
		unsafe.Pointer(prev), unsafe.Pointer(new))    //nolint:gosec
	if swapped && cb.cfg.OnStateChange != nil {
		cb.cfg.OnStateChange(prev.state, new.state)
	}
	return swapped
}

func (cb *CircuitBreaker) setState(st State, now time.Time) {
	state := cb.newState(st, now)
	atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&cb.state)), unsafe.Pointer(state)) //nolint:gosec
	if cb.cfg.OnStateChange != nil {
		cb.cfg.OnStateChange(Undefined, st)
	}
}

func (cb *CircuitBreaker) loadState() *internalState {
	return (*internalState)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&cb.state)))) //nolint:gosec
}

func (cb *CircuitBreaker) newState(cbState State, now time.Time) *internalState {
	state := &internalState{
		timeNano: timeNano(now),
		state:    cbState,
		probe:    nil,
		metrics:  nil,
	}
	switch cbState { //nolint:exhaustive
	case Closed:
		state.metrics = newLockedMetrics(cb.cfg.MetricsSlidingWindow, now,
			cb.cfg.ObservationsThreshold, cb.cfg.ErrorRateThreshold, cb.cfg.SlowCallRateThreshold)
	case HalfOpen:
		// ObservationsThreshold must be 0.
		state.metrics = newLockedMetrics(cb.cfg.MetricsSlidingWindow, now,
			0, cb.cfg.ErrorRateThreshold, cb.cfg.SlowCallRateThreshold)
		state.probe = newProbeCallTracker(uint32(cb.cfg.AllowedProbeCalls))
	default:
	}
	return state
}

type internalState struct {
	timeNano int64
	state    State

	probe   *probeCallTracker
	metrics *lockedMetrics
}

func (st *internalState) allowCall() bool {
	if st.state == HalfOpen {
		return st.probe.allow()
	}
	return st.state != Open && st.state != ForceOpen
}

func (st *internalState) expired(now time.Time, duration time.Duration) bool {
	return st.timeNano+duration.Nanoseconds() <= timeNano(now)
}

func (st *internalState) trace(
	now, startAt time.Time, statusOk bool, slowCallDurationThreshold time.Duration,
) (next State, changed bool) {
	if st.state == Open || st.state == Disabled || st.state == ForceOpen {
		return
	}

	failed := !statusOk
	isSlow := time.Since(startAt) >= slowCallDurationThreshold

	switch st.state { //nolint:exhaustive
	case Closed:
		if !st.metrics.observe(now, failed, isSlow, true) {
			next = Open // Transition from CLOSED to OPEN.
			changed = true
		}
	case HalfOpen:
		done := st.probe.done()
		isHealthy := st.metrics.observe(now, failed, isSlow, done)
		if done {
			changed = true
			if isHealthy {
				next = Closed // Transition from HALF-OPEN to CLOSED.
			} else {
				next = Open // Transition from HALF-OPEN to OPEN.
			}
		}
	default:
	}
	return
}

// DisposableGuard guards the workflow, THROW AWAY after it has been used.
type DisposableGuard struct {
	state   *internalState
	breaker *CircuitBreaker
	allow   bool
}

// Allow indicates whether the circuit breaker is closed,
// true means and any normal event may happen at time now.
// Allow always returns the same result.
func (g DisposableGuard) Allow() bool {
	return g.allow
}

// Observe adds a single observation to metrics, it must be called if Allow returns true
// (use defer to make sure of it), otherwise the circuit breaker's state machine may be broken.
// THROW AWAY after it has been used.
func (g DisposableGuard) Observe(startAt time.Time, statusOk bool) {
	if !g.allow { // Only allowed call can be observed.
		return
	}
	g.breaker.afterCall(g.state, startAt, statusOk)
}

// This is fast enough for most circumstances..
type lockedMetrics struct {
	observationsThreshold int
	errorRateThreshold    float64
	slowCallRateThreshold float64

	lock    sync.Mutex
	metrics *windowedMetrics
}

func newLockedMetrics(
	window time.Duration, now time.Time,
	observationsThreshold int,
	errorRateThreshold float64,
	slowCallRateThreshold float64,
) *lockedMetrics {
	return &lockedMetrics{
		observationsThreshold: observationsThreshold,
		errorRateThreshold:    errorRateThreshold,
		slowCallRateThreshold: slowCallRateThreshold,

		lock:    sync.Mutex{},
		metrics: newWindowedMetrics(window, now),
	}
}

func (m *lockedMetrics) observe(now time.Time, failed, isSlow bool, checkHealthy bool) bool {
	m.lock.Lock()
	total := m.metrics.record(now, failed, isSlow)
	m.lock.Unlock()

	if !checkHealthy {
		return false
	}

	if total.totalcalls < m.observationsThreshold {
		return true
	}
	totalcallsf := float64(total.totalcalls)
	if float64(total.failedcalls)/totalcallsf >= m.errorRateThreshold {
		return false
	}
	return float64(total.slowcalls)/totalcallsf < m.slowCallRateThreshold
}

func (m *lockedMetrics) reset(now time.Time) { //nolint:unused
	m.lock.Lock()
	m.metrics.reset(now)
	m.lock.Unlock()
}

type probeCallTracker struct {
	quota    uint32
	allowed  uint32
	finished uint32
}

func newProbeCallTracker(quota uint32) *probeCallTracker {
	return &probeCallTracker{quota: quota}
}

func (p *probeCallTracker) allow() bool {
	return atomic.AddUint32(&p.allowed, 1) <= p.quota
}

func (p *probeCallTracker) done() bool {
	return atomic.AddUint32(&p.finished, 1) >= p.quota
}
