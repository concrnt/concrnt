package cluster

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/concrnt/concrnt/internal/infra/health"
	"github.com/concrnt/concrnt/internal/testutil"
)

type fakeElectorService struct {
	mu     sync.Mutex
	status ElectorStatus
}

func (f *fakeElectorService) set(status ElectorStatus) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.status = status
}

func (f *fakeElectorService) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/status" {
			http.NotFound(w, r)
			return
		}
		f.mu.Lock()
		defer f.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(f.status)
	})
}

func newTestElector(endpoint string) *HTTPElector {
	e := NewHTTPElector(endpoint, nil)
	e.pollInterval = 20 * time.Millisecond
	e.failureGrace = 200 * time.Millisecond
	return e
}

func TestHTTPElectorLeadershipTransitions(t *testing.T) {
	fake := &fakeElectorService{}
	fake.set(ElectorStatus{IsLeader: false, LeaderURL: "http://10.0.0.9:8001", Peers: []string{"http://10.0.0.9:8001"}})
	server := httptest.NewServer(fake.handler())
	defer server.Close()

	e := newTestElector(server.URL)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var mu sync.Mutex
	var leadCtx context.Context
	go e.Run(ctx, func(c context.Context) {
		mu.Lock()
		leadCtx = c
		mu.Unlock()
	})

	// non-leader: status is reflected but onLead is not called
	testutil.WaitFor(t, func() bool { _, ok := e.current(); return ok })
	if e.IsLeader() {
		t.Fatal("must not be leader yet")
	}
	if url, ok := e.LeaderURL(); !ok || url != "http://10.0.0.9:8001" {
		t.Fatalf("unexpected leader url: %v %v", url, ok)
	}
	if peers, err := e.Peers(context.Background()); err != nil || !reflect.DeepEqual(peers, []string{"http://10.0.0.9:8001"}) {
		t.Fatalf("unexpected peers: %v %v", peers, err)
	}

	// become leader -> onLead fires with a live context
	fake.set(ElectorStatus{IsLeader: true, LeaderURL: "http://self:8001"})
	testutil.WaitFor(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return leadCtx != nil
	})
	mu.Lock()
	current := leadCtx
	mu.Unlock()
	if current.Err() != nil {
		t.Fatal("lead context must be live while leading")
	}

	// lose leadership -> lead context is cancelled
	fake.set(ElectorStatus{IsLeader: false})
	testutil.WaitFor(t, func() bool { return current.Err() != nil })
}

func TestHTTPElectorDemotesWhenServiceUnreachable(t *testing.T) {
	fake := &fakeElectorService{}
	fake.set(ElectorStatus{IsLeader: true})
	server := httptest.NewServer(fake.handler())

	e := newTestElector(server.URL)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var mu sync.Mutex
	var leadCtx context.Context
	go e.Run(ctx, func(c context.Context) {
		mu.Lock()
		leadCtx = c
		mu.Unlock()
	})

	testutil.WaitFor(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return leadCtx != nil
	})
	mu.Lock()
	current := leadCtx
	mu.Unlock()

	// elector service dies -> after the failure grace we must step down
	server.Close()
	testutil.WaitFor(t, func() bool { return current.Err() != nil })
	if e.IsLeader() {
		t.Fatal("must not report leadership with a dead elector service")
	}
	if _, err := e.Peers(context.Background()); err == nil {
		t.Fatal("peers must be unavailable with a dead elector service")
	}
}

// The poll loop beats the liveness watchdog on every poll and releases the
// heartbeat when Run returns.
func TestHTTPElectorReportsHeartbeat(t *testing.T) {
	fake := &fakeElectorService{}
	fake.set(ElectorStatus{IsLeader: false})
	server := httptest.NewServer(fake.handler())
	defer server.Close()

	watchdog := health.NewWatchdog()
	e := NewHTTPElector(server.URL, watchdog)
	e.pollInterval = 20 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		e.Run(ctx, func(context.Context) {})
		close(done)
	}()

	testutil.WaitFor(t, func() bool {
		_, running := watchdog.Stalled(time.Now().Add(time.Hour))["cluster/elector"]
		return running
	})
	if stalled := watchdog.Stalled(time.Now()); len(stalled) != 0 {
		t.Fatalf("poll loop must not read as stalled while running: %v", stalled)
	}

	cancel()
	<-done
	if stalled := watchdog.Stalled(time.Now().Add(time.Hour)); len(stalled) != 0 {
		t.Fatalf("a returned poll loop must release its heartbeat: %v", stalled)
	}
}
