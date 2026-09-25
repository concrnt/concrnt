// Package health backs the probes served on the internal listener: the
// liveness watchdog over the process's own long-running loops, and the
// readiness sweep over the backends the public API cannot serve without.
package health

import (
	"sync"
	"sync/atomic"
	"time"
)

// Watchdog collects the heartbeats of the process's long-running loops so
// the liveness probe can tell a wedged process (a loop stuck on a lock or
// on a call with no bound) from a healthy one. It says nothing about the
// backends — that is readiness.
type Watchdog struct {
	mu    sync.Mutex
	loops []*Heartbeat
}

func NewWatchdog() *Watchdog {
	return &Watchdog{}
}

// Register adds a loop that, while it runs, must Beat at least once per
// deadline. Pick the deadline from the loop's own cadence with a wide
// margin: a missed deadline restarts the whole process.
//
// A nil Watchdog hands out a nil Heartbeat whose methods are no-ops, so a
// component built without one (tests, conctl) needs no branching.
func (w *Watchdog) Register(name string, deadline time.Duration) *Heartbeat {
	if w == nil {
		return nil
	}
	h := &Heartbeat{name: name, deadline: deadline}
	w.mu.Lock()
	w.loops = append(w.loops, h)
	w.mu.Unlock()
	return h
}

// LoopState is what the watchdog knows about one loop as of a given time.
type LoopState int

const (
	// Idle: registered but not running, e.g. a leader-only loop on a
	// replica that is not the leader. Never a stall.
	Idle LoopState = iota
	Running
	Stalled
)

// Loop is one loop's state plus, when it is running, the time since its
// last beat.
type Loop struct {
	State LoopState
	Since time.Duration
}

// Loops reports every registered loop as of now, keyed by name.
func (w *Watchdog) Loops(now time.Time) map[string]Loop {
	loops := map[string]Loop{}
	if w == nil {
		return loops
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, h := range w.loops {
		last := h.last.Load()
		if last == 0 {
			loops[h.name] = Loop{State: Idle}
			continue
		}
		loop := Loop{State: Running, Since: now.Sub(time.Unix(0, last))}
		if loop.Since > h.deadline {
			loop.State = Stalled
		}
		loops[h.name] = loop
	}
	return loops
}

// Stalled reports the loops that missed their deadline as of now, keyed
// by name with the time since their last beat.
func (w *Watchdog) Stalled(now time.Time) map[string]time.Duration {
	stalled := map[string]time.Duration{}
	for name, loop := range w.Loops(now) {
		if loop.State == Stalled {
			stalled[name] = loop.Since
		}
	}
	return stalled
}

// Heartbeat is one loop's liveness signal. Beat at the top of every
// iteration; Stop when the loop returns on purpose (a leader-only loop
// losing its term) so its silence is not mistaken for a stall.
type Heartbeat struct {
	name     string
	deadline time.Duration
	last     atomic.Int64 // unix nanos of the last Beat; 0 while stopped
}

func (h *Heartbeat) Beat() {
	if h == nil {
		return
	}
	h.last.Store(time.Now().UnixNano())
}

func (h *Heartbeat) Stop() {
	if h == nil {
		return
	}
	h.last.Store(0)
}
