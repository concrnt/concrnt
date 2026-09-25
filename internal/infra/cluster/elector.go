// Package cluster provides the coordination primitives used to run multiple
// replicas of concrnt: leader election (which replica runs the singleton
// workers) and peer discovery (which replicas exist, for aggregating realtime
// subscription demand).
//
// Both are obtained from an external elector service over a small HTTP
// protocol (see HTTPElector), so concrnt itself has no dependency on any
// particular orchestrator. On Kubernetes the cmd/k8s-elector sidecar
// implements the protocol via a coordination.k8s.io Lease and headless-service
// DNS; other backends (consul, etcd, redis, ...) can implement it too.
// Without an elector endpoint configured, AlwaysLeader reproduces the
// single-instance behavior.
package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/concrnt/concrnt/internal/infra/health"
)

// Elector decides which replica runs the singleton workers.
type Elector interface {
	// Run blocks. onLead is called when this replica becomes the leader; the
	// ctx passed to onLead is cancelled when leadership is lost.
	Run(ctx context.Context, onLead func(ctx context.Context))
	IsLeader() bool
	// LeaderURL returns the internal base URL of the current leader.
	LeaderURL() (string, bool)
}

// AlwaysLeader is the standalone (non-clustered) elector: this replica is the
// one and only leader.
type AlwaysLeader struct{}

func (AlwaysLeader) Run(ctx context.Context, onLead func(ctx context.Context)) {
	onLead(ctx)
	<-ctx.Done()
}

func (AlwaysLeader) IsLeader() bool { return true }

func (AlwaysLeader) LeaderURL() (string, bool) { return "", false }

// ElectorStatus is the response of the elector service's GET /status.
type ElectorStatus struct {
	IsLeader  bool     `json:"isLeader"`
	LeaderURL string   `json:"leaderUrl"`
	Peers     []string `json:"peers"`
}

type httpElectorState struct {
	status    ElectorStatus
	fetchedAt time.Time
}

const (
	electorPollInterval = 2 * time.Second
	// liveness deadline of the poll loop: one interval plus one client
	// timeout is the longest legitimate silence between beats
	electorLoopDeadline = 30 * time.Second
	// electorFailureGrace must stay below the elector's lease duration: if the
	// elector service dies it also stops renewing the lease, so dropping
	// leadership locally before another replica can acquire it prevents two
	// leaders from overlapping.
	electorFailureGrace = 10 * time.Second
)

// HTTPElector obtains leadership and peer discovery from an external elector
// service (e.g. the cmd/k8s-elector sidecar) by polling GET /status. It
// implements both Elector and Discovery.
type HTTPElector struct {
	endpoint string
	client   *http.Client

	pollInterval time.Duration
	failureGrace time.Duration

	state     atomic.Pointer[httpElectorState]
	heartbeat *health.Heartbeat
}

// NewHTTPElector polls endpoint; watchdog (nil to skip) gets the poll
// loop's liveness heartbeat.
func NewHTTPElector(endpoint string, watchdog *health.Watchdog) *HTTPElector {
	return &HTTPElector{
		endpoint:     strings.TrimRight(endpoint, "/"),
		client:       &http.Client{Timeout: 2 * time.Second},
		pollInterval: electorPollInterval,
		failureGrace: electorFailureGrace,
		heartbeat:    watchdog.Register("cluster/elector", electorLoopDeadline),
	}
}

func (e *HTTPElector) fetch(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, e.endpoint+"/status", nil)
	if err != nil {
		return err
	}

	resp, err := e.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status %d", resp.StatusCode)
	}

	var status ElectorStatus
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		return err
	}

	e.state.Store(&httpElectorState{status: status, fetchedAt: time.Now()})
	return nil
}

// current returns the last polled status, treating it as void once it is
// older than the failure grace (fail-safe: an unreachable elector means we
// must assume we are not the leader).
func (e *HTTPElector) current() (ElectorStatus, bool) {
	state := e.state.Load()
	if state == nil {
		return ElectorStatus{}, false
	}
	if time.Since(state.fetchedAt) > e.failureGrace {
		return ElectorStatus{}, false
	}
	return state.status, true
}

func (e *HTTPElector) IsLeader() bool {
	status, ok := e.current()
	return ok && status.IsLeader
}

func (e *HTTPElector) LeaderURL() (string, bool) {
	status, ok := e.current()
	if !ok || status.LeaderURL == "" {
		return "", false
	}
	return status.LeaderURL, true
}

func (e *HTTPElector) Peers(ctx context.Context) ([]string, error) {
	status, ok := e.current()
	if !ok {
		return nil, fmt.Errorf("elector status unavailable")
	}
	return status.Peers, nil
}

func (e *HTTPElector) Run(ctx context.Context, onLead func(ctx context.Context)) {
	ticker := time.NewTicker(e.pollInterval)
	defer ticker.Stop()

	var lead leadState
	defer lead.stop()
	defer e.heartbeat.Stop()

	for {
		e.heartbeat.Beat()
		if err := e.fetch(ctx); err != nil && ctx.Err() == nil {
			slog.Warn(
				"failed to poll elector",
				slog.String("endpoint", e.endpoint),
				slog.String("error", err.Error()),
				slog.String("module", "cluster"),
			)
		}

		// IsLeader applies the failure grace, so a dead elector demotes us
		leading := e.IsLeader()
		if leading && !lead.active() {
			slog.Info("acquired cluster leadership", slog.String("module", "cluster"))
			lead.start(ctx, onLead)
		} else if !leading && lead.active() {
			slog.Info("lost cluster leadership", slog.String("module", "cluster"))
			lead.stop()
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// leadState tracks the context handed to the singleton workers while this
// replica is the leader.
type leadState struct {
	cancel context.CancelFunc
}

func (l *leadState) active() bool { return l.cancel != nil }

func (l *leadState) start(ctx context.Context, onLead func(ctx context.Context)) {
	leadCtx, cancel := context.WithCancel(ctx)
	l.cancel = cancel
	go onLead(leadCtx)
}

func (l *leadState) stop() {
	if l.cancel != nil {
		l.cancel()
		l.cancel = nil
	}
}
