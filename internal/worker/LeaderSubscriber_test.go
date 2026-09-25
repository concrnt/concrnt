package worker

import (
	"context"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/internal/domain"
	"github.com/concrnt/concrnt/internal/infra/health"
	"github.com/concrnt/concrnt/internal/testutil"
	"github.com/gorilla/websocket"
)

// wsTestServer is the remote end of the subscriber's websocket connections:
// it tracks live server-side connections and the listen requests received.
type wsTestServer struct {
	srv     *httptest.Server
	mu      sync.Mutex
	conns   []*websocket.Conn
	live    int
	listens [][]string
}

func newWSTestServer(t *testing.T) *wsTestServer {
	t.Helper()
	s := &wsTestServer{}
	upgrader := websocket.Upgrader{}
	s.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		s.mu.Lock()
		s.conns = append(s.conns, conn)
		s.live++
		s.mu.Unlock()
		defer func() {
			s.mu.Lock()
			s.live--
			s.mu.Unlock()
			conn.Close()
		}()
		for {
			var req concrnt.RealtimeRequest
			if err := conn.ReadJSON(&req); err != nil {
				return
			}
			s.mu.Lock()
			s.listens = append(s.listens, req.Prefixes)
			s.mu.Unlock()
		}
	}))
	t.Cleanup(s.srv.Close)
	return s
}

func (s *wsTestServer) liveConns() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.live
}

func (s *wsTestServer) closeAll() {
	s.mu.Lock()
	conns := s.conns
	s.conns = nil
	s.mu.Unlock()
	for _, c := range conns {
		c.Close()
	}
}

func (s *wsTestServer) lastListen() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.listens) == 0 {
		return nil
	}
	return s.listens[len(s.listens)-1]
}

// fakeSubClient fakes the SubscriberClient: every prefix resolves to one
// remote host, and Realtime dials the test websocket server, optionally
// blocking on dialGate first.
type fakeSubClient struct {
	ws        *wsTestServer
	dialGate  chan struct{}
	dialCount atomic.Int64
	resolveTo string // host every prefix resolves to; "" means "remote.example"
	dialErr   bool   // Realtime fails instead of connecting
}

func (f *fakeSubClient) ResolveResourceHost(ctx context.Context, uri string) (string, error) {
	if f.resolveTo != "" {
		return f.resolveTo, nil
	}
	return "remote.example", nil
}

func (f *fakeSubClient) Realtime(ctx context.Context, fqdn string) (*websocket.Conn, error) {
	f.dialCount.Add(1)
	if f.dialErr {
		return nil, context.DeadlineExceeded
	}
	if f.dialGate != nil {
		select {
		case <-f.dialGate:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	url := "ws" + strings.TrimPrefix(f.ws.srv.URL, "http")
	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	return conn, err
}

// dynamicDemand is a SubscribeClient whose prefixes can be swapped at runtime.
type dynamicDemand struct {
	mu       sync.Mutex
	prefixes []string
	calls    atomic.Int64
}

func (d *dynamicDemand) CurrentSubscriptions() []string {
	d.calls.Add(1)
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([]string, len(d.prefixes))
	copy(out, d.prefixes)
	return out
}

func (d *dynamicDemand) set(prefixes []string) {
	d.mu.Lock()
	d.prefixes = prefixes
	d.mu.Unlock()
}

type nopPubSub struct{}

func (nopPubSub) Publish(ctx context.Context, channel string, event concrnt.Event) error {
	return nil
}

func (nopPubSub) Subscribe(ctx context.Context, prefixes []string, response chan<- concrnt.Event) error {
	return nil
}

func (nopPubSub) SubscribeAll(ctx context.Context, response chan<- concrnt.Event) error {
	return nil
}

func newTestSubscriber(fake *fakeSubClient) *LeaderSubscriber {
	return NewLeaderSubscriber(&domain.Config{FQDN: "local.example"}, fake, nopPubSub{}, nil, nil)
}

func (s *LeaderSubscriber) trackedEntries() (count int, connected int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, state := range s.Subscriptions {
		count++
		if state.Connection != nil {
			connected++
		}
	}
	return
}

// While a dial is blocked on a slow remote, every other subscriber operation
// must stay responsive: the lock may not be held across network I/O.
func TestEnsureDoesNotBlockSubscriberOnSlowDial(t *testing.T) {
	ws := newWSTestServer(t)
	gate := make(chan struct{})
	fake := &fakeSubClient{ws: ws, dialGate: gate}
	s := newTestSubscriber(fake)

	demand := &dynamicDemand{}
	demand.set([]string{"cckv://alice/home"})
	s.RegisterClient(demand)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Start(ctx)

	testutil.WaitFor(t, func() bool { return fake.dialCount.Load() >= 1 })

	// with the dial still gated, these must all return promptly
	done := make(chan struct{})
	go func() {
		s.EnsureSubscriptions(context.Background(), []string{"cckv://alice/home"})
		s.RegisterClient(&dynamicDemand{})
		s.CurrentSubscriptions()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("subscriber operations blocked behind an in-flight dial")
	}

	close(gate)
	testutil.WaitFor(t, func() bool { return ws.liveConns() == 1 })
}

// Concurrent ensures for the same host must collapse into a single dial whose
// listen request carries the union of all requested prefixes.
func TestConcurrentEnsureDialsOnce(t *testing.T) {
	ws := newWSTestServer(t)
	gate := make(chan struct{})
	fake := &fakeSubClient{ws: ws, dialGate: gate}
	s := newTestSubscriber(fake)

	demand := &dynamicDemand{}
	demand.set([]string{"cckv://alice/home", "cckv://bob/home", "cckv://carol/home"})
	s.RegisterClient(demand)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Start(ctx)

	var wg sync.WaitGroup
	for _, prefix := range []string{"cckv://alice/home", "cckv://bob/home", "cckv://carol/home"} {
		wg.Add(1)
		go func(p string) {
			defer wg.Done()
			s.EnsureSubscriptions(context.Background(), []string{p})
		}(prefix)
	}
	wg.Wait()

	close(gate)
	testutil.WaitFor(t, func() bool { return ws.liveConns() == 1 })

	if got := fake.dialCount.Load(); got != 1 {
		t.Fatalf("expected exactly 1 dial, got %d", got)
	}

	testutil.WaitFor(t, func() bool {
		listen := ws.lastListen()
		found := 0
		for _, p := range []string{"cckv://alice/home", "cckv://bob/home", "cckv://carol/home"} {
			for _, l := range listen {
				if l == p {
					found++
				}
			}
		}
		return found == 3
	})
}

// A dying connection's deferred cleanup must remove only its own map entry:
// a replacement subscription created concurrently must survive, leaving
// exactly one tracked, live connection.
func TestCleanupRemovesOnlyOwnState(t *testing.T) {
	ws := newWSTestServer(t)
	fake := &fakeSubClient{ws: ws}
	s := newTestSubscriber(fake)

	demand := &dynamicDemand{}
	demand.set([]string{"cckv://alice/home"})
	s.RegisterClient(demand)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Start(ctx)

	testutil.WaitFor(t, func() bool { return ws.liveConns() == 1 })

	for range 5 {
		// kill the current connection while hammering ensure from the side,
		// racing the stale defers against fresh replacements
		stop := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					s.EnsureSubscriptions(context.Background(), []string{"cckv://alice/home"})
					time.Sleep(time.Millisecond)
				}
			}
		}()
		ws.closeAll()
		// wait for full recovery while the hammer still runs: a replacement
		// can die too if it was dialed before closeAll's snapshot
		testutil.WaitFor(t, func() bool {
			count, connected := s.trackedEntries()
			return count == 1 && connected == 1 && ws.liveConns() == 1
		})
		close(stop)
		wg.Wait()
	}

	// a connection observed live above may still have been part of the kill
	// snapshot; recover like the keeper would, then require a steady state
	testutil.WaitFor(t, func() bool {
		count, connected := s.trackedEntries()
		if count == 0 {
			s.EnsureSubscriptions(context.Background(), []string{"cckv://alice/home"})
			return false
		}
		return count == 1 && connected == 1 && ws.liveConns() == 1
	})

	// stability: no orphaned/untracked connection may linger
	time.Sleep(200 * time.Millisecond)
	count, connected := s.trackedEntries()
	if count != 1 || connected != 1 || ws.liveConns() != 1 {
		t.Fatalf("expected 1 tracked live connection, got tracked=%d connected=%d live=%d",
			count, connected, ws.liveConns())
	}
}

// A new leader must establish subscriptions immediately on Start, not after
// the first 10s keeper tick.
func TestKeeperRunsImmediatelyOnStart(t *testing.T) {
	ws := newWSTestServer(t)
	fake := &fakeSubClient{ws: ws}
	s := newTestSubscriber(fake)

	demand := &dynamicDemand{}
	demand.set([]string{"cckv://alice/home"})
	s.RegisterClient(demand)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Start(ctx)

	// waitFor's 3s budget is well under the 10s tick, so this only passes
	// with the immediate first pass
	testutil.WaitFor(t, func() bool { return ws.liveConns() == 1 })
}

// Each keeper pass must collect demand exactly once (shared snapshot), not
// once per sub-step.
func TestSingleDemandSnapshotPerTick(t *testing.T) {
	ws := newWSTestServer(t)
	fake := &fakeSubClient{ws: ws}
	s := newTestSubscriber(fake)

	demand := &dynamicDemand{}
	demand.set([]string{"cckv://alice/home"})
	s.RegisterClient(demand)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Start(ctx)

	testutil.WaitFor(t, func() bool { return demand.calls.Load() >= 1 })
	time.Sleep(200 * time.Millisecond) // let the first pass fully settle
	if got := demand.calls.Load(); got != 1 {
		t.Fatalf("expected exactly 1 demand collection in the first keeper pass, got %d", got)
	}
}

// Ensure on a subscriber that has never led must be a safe no-op.
func TestEnsureBeforeStartIsNoop(t *testing.T) {
	ws := newWSTestServer(t)
	fake := &fakeSubClient{ws: ws}
	s := newTestSubscriber(fake)

	s.EnsureSubscriptions(context.Background(), []string{"cckv://alice/home"})

	if got := fake.dialCount.Load(); got != 0 {
		t.Fatalf("expected no dial before Start, got %d", got)
	}
	if count, _ := s.trackedEntries(); count != 0 {
		t.Fatalf("expected no tracked entries before Start, got %d", count)
	}
}

type purgeCall struct {
	prefixes []string
	depth    int
}

type recordingPurger struct {
	mu    sync.Mutex
	calls []purgeCall
}

func (p *recordingPurger) PurgeLatest(prefixes []string, depth int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls = append(p.calls, purgeCall{prefixes: prefixes, depth: depth})
}

func (p *recordingPurger) purged(prefix string, depth int) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, call := range p.calls {
		if call.depth != depth {
			continue
		}
		for _, pf := range call.prefixes {
			if pf == prefix {
				return true
			}
		}
	}
	return false
}

// Dropping demand must purge exactly the dropped prefixes (depth 1), leaving
// still-demanded prefixes alone.
func TestPurgeOnDeleteExcess(t *testing.T) {
	s := newTestSubscriber(&fakeSubClient{})
	purger := &recordingPurger{}
	s.SetCachePurger(purger)

	s.Subscriptions["remote.example"] = &SubState{
		Prefixes: []string{"cckv://alice/home", "cckv://bob/home"},
	}

	s.deleteExcessSubscriptions([]string{"cckv://bob/home"}, time.Now())

	if !purger.purged("cckv://alice/home", 1) {
		t.Fatalf("expected dropped prefix to be purged, calls: %+v", purger.calls)
	}
	if purger.purged("cckv://bob/home", 1) {
		t.Fatalf("still-demanded prefix must not be purged, calls: %+v", purger.calls)
	}
}

// Losing leadership closes every subscription; all their prefixes must be
// purged since the cache stops being maintained.
func TestPurgeOnCloseAll(t *testing.T) {
	s := newTestSubscriber(&fakeSubClient{})
	purger := &recordingPurger{}
	s.SetCachePurger(purger)

	s.Subscriptions["a.example"] = &SubState{Prefixes: []string{"cckv://alice/home"}}
	s.Subscriptions["b.example"] = &SubState{Prefixes: []string{"cckv://bob/home"}}

	// closeAll only sweeps for the term that owns runCtx
	ctx := context.Background()
	s.runCtx = ctx
	s.closeAllSubscriptions(ctx)

	if !purger.purged("cckv://alice/home", 1) || !purger.purged("cckv://bob/home", 1) {
		t.Fatalf("expected all prefixes purged on close-all, calls: %+v", purger.calls)
	}
}

// Establishing a subscription must purge the (possibly gapped) latest chunks
// (depth 2), and only prefixes with an established connection count as open.
func TestPurgeOnOpenAndOpenPrefixes(t *testing.T) {
	ws := newWSTestServer(t)
	fake := &fakeSubClient{ws: ws}
	s := newTestSubscriber(fake)
	purger := &recordingPurger{}
	s.SetCachePurger(purger)

	demand := &dynamicDemand{}
	demand.set([]string{"cckv://alice/home"})
	s.RegisterClient(demand)

	if got := s.OpenPrefixes(); len(got) != 0 {
		t.Fatalf("expected no open prefixes before connecting, got %v", got)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Start(ctx)

	testutil.WaitFor(t, func() bool { return purger.purged("cckv://alice/home", 2) })
	testutil.WaitFor(t, func() bool {
		open := s.OpenPrefixes()
		return len(open) == 1 && open[0] == "cckv://alice/home"
	})
}

// A subscription whose demand disappears while its dial is still in flight
// must not leak the late connection.
func TestDeleteExcessClosesInFlightDialResult(t *testing.T) {
	ws := newWSTestServer(t)
	gate := make(chan struct{})
	fake := &fakeSubClient{ws: ws, dialGate: gate}
	s := newTestSubscriber(fake)

	demand := &dynamicDemand{}
	demand.set([]string{"cckv://alice/home"})
	s.RegisterClient(demand)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Start(ctx)

	testutil.WaitFor(t, func() bool { return fake.dialCount.Load() >= 1 })

	// demand vanishes while the dial is gated
	demand.set(nil)
	s.deleteExcessSubscriptions(nil, time.Now())

	if count, _ := s.trackedEntries(); count != 0 {
		t.Fatalf("expected entry to be removed while dialing, got %d", count)
	}

	close(gate)

	// the late dial result must be closed, not installed
	testutil.WaitFor(t, func() bool { return ws.liveConns() == 0 })
	time.Sleep(100 * time.Millisecond)
	if count, _ := s.trackedEntries(); count != 0 {
		t.Fatalf("expected no tracked entries after late dial completes, got %d", count)
	}
	if live := ws.liveConns(); live != 0 {
		t.Fatalf("expected no live connections after late dial completes, got %d", live)
	}
}

// demandPeerServer fakes a peer replica's GET /internal/demand endpoint; its
// body can be swapped to simulate demand changing between polls.
type demandPeerServer struct {
	srv  *httptest.Server
	body atomic.Value
}

func newDemandPeerServer(t *testing.T, initial string) *demandPeerServer {
	t.Helper()
	p := &demandPeerServer{}
	p.body.Store(initial)
	p.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/internal/demand" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(p.body.Load().(string)))
	}))
	t.Cleanup(p.srv.Close)
	return p
}

// staticPeerDiscovery is a PeerDiscovery whose peer list can be swapped at
// runtime, to simulate a peer joining or leaving the cluster.
type staticPeerDiscovery struct {
	mu    sync.Mutex
	peers []string
}

func (d *staticPeerDiscovery) Peers(ctx context.Context) ([]string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return slices.Clone(d.peers), nil
}

func (d *staticPeerDiscovery) set(peers []string) {
	d.mu.Lock()
	d.peers = peers
	d.mu.Unlock()
}

type failingPeerDiscovery struct{}

func (failingPeerDiscovery) Peers(ctx context.Context) ([]string, error) {
	return nil, context.DeadlineExceeded
}

// pollPeerDemand must union the demand of every peer discovery currently lists.
func TestPollPeerDemandUnion(t *testing.T) {
	peerA := newDemandPeerServer(t, `["cckv://alice/home"]`)
	peerB := newDemandPeerServer(t, `["cckv://bob/home"]`)
	discovery := &staticPeerDiscovery{peers: []string{peerA.srv.URL, peerB.srv.URL}}

	s := NewLeaderSubscriber(&domain.Config{FQDN: "local.example"}, &fakeSubClient{}, nopPubSub{}, discovery, nil)

	got := s.pollPeerDemand(context.Background())
	if !slices.Contains(got, "cckv://alice/home") || !slices.Contains(got, "cckv://bob/home") {
		t.Fatalf("expected union of both peers, got %v", got)
	}
}

// A peer that drops out of discovery must stop contributing demand
// immediately, even though it would still answer if polled directly:
// membership is decided by discovery, not by poll success.
func TestPollPeerDemandDropsPeerGoneFromDiscovery(t *testing.T) {
	peerA := newDemandPeerServer(t, `["cckv://alice/home"]`)
	discovery := &staticPeerDiscovery{peers: []string{peerA.srv.URL}}
	s := NewLeaderSubscriber(&domain.Config{FQDN: "local.example"}, &fakeSubClient{}, nopPubSub{}, discovery, nil)

	got := s.pollPeerDemand(context.Background())
	if !slices.Contains(got, "cckv://alice/home") {
		t.Fatalf("expected initial demand, got %v", got)
	}

	discovery.set(nil)
	got = s.pollPeerDemand(context.Background())
	if slices.Contains(got, "cckv://alice/home") {
		t.Fatalf("expected demand to be dropped once the peer left discovery, got %v", got)
	}
}

// A peer that stays listed by discovery but fails to answer must keep
// contributing its last known demand: redis pubsub delivery is fire-and-
// forget, so tearing its subscriptions down on one failed poll would lose
// events.
func TestPollPeerDemandKeepsLastKnownOnPollFailure(t *testing.T) {
	peerA := newDemandPeerServer(t, `["cckv://alice/home"]`)
	discovery := &staticPeerDiscovery{peers: []string{peerA.srv.URL}}
	s := NewLeaderSubscriber(&domain.Config{FQDN: "local.example"}, &fakeSubClient{}, nopPubSub{}, discovery, nil)

	got := s.pollPeerDemand(context.Background())
	if !slices.Contains(got, "cckv://alice/home") {
		t.Fatalf("expected initial demand, got %v", got)
	}

	peerA.srv.Close()

	got = s.pollPeerDemand(context.Background())
	if !slices.Contains(got, "cckv://alice/home") {
		t.Fatalf("expected last known demand to survive a failed poll, got %v", got)
	}
}

// While discovery itself fails, all last known demand must be frozen rather
// than expired: "peer gone" and "discovery broken" are indistinguishable.
func TestPollPeerDemandFreezesWhileDiscoveryFails(t *testing.T) {
	peerA := newDemandPeerServer(t, `["cckv://alice/home"]`)
	discovery := &staticPeerDiscovery{peers: []string{peerA.srv.URL}}
	s := NewLeaderSubscriber(&domain.Config{FQDN: "local.example"}, &fakeSubClient{}, nopPubSub{}, discovery, nil)

	got := s.pollPeerDemand(context.Background())
	if !slices.Contains(got, "cckv://alice/home") {
		t.Fatalf("expected initial demand, got %v", got)
	}

	s.discovery = failingPeerDiscovery{}

	got = s.pollPeerDemand(context.Background())
	if !slices.Contains(got, "cckv://alice/home") {
		t.Fatalf("expected demand to be frozen while discovery fails, got %v", got)
	}
}

// CurrentSubscriptions must reflect a newly ensured prefix as soon as its
// listen is confirmed on the wire, not waiting for the next keeper tick.
func TestCurrentSubscriptionsAddsOnEnsure(t *testing.T) {
	ws := newWSTestServer(t)
	fake := &fakeSubClient{ws: ws}
	s := newTestSubscriber(fake)

	// registered so the keeper's own demand snapshot agrees with the manual
	// ensure below; otherwise the very next tick's deleteExcessSubscriptions
	// would race to undo it (demand is otherwise empty)
	demand := &dynamicDemand{}
	demand.set([]string{"cckv://alice/home"})
	s.RegisterClient(demand)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Start(ctx)

	s.EnsureSubscriptions(context.Background(), []string{"cckv://alice/home"})

	testutil.WaitFor(t, func() bool {
		return slices.Contains(s.CurrentSubscriptions(), "cckv://alice/home")
	})
}

// CurrentSubscriptions must drop a prefix as soon as its subscription is torn
// down, not waiting for the next keeper tick's demand recomputation.
func TestCurrentSubscriptionsRemovesOnDrop(t *testing.T) {
	ws := newWSTestServer(t)
	fake := &fakeSubClient{ws: ws}
	s := newTestSubscriber(fake)

	demand := &dynamicDemand{}
	demand.set([]string{"cckv://alice/home"})
	s.RegisterClient(demand)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Start(ctx)

	testutil.WaitFor(t, func() bool {
		return slices.Contains(s.CurrentSubscriptions(), "cckv://alice/home")
	})

	demand.set(nil)
	s.deleteExcessSubscriptions(nil, time.Now())

	if slices.Contains(s.CurrentSubscriptions(), "cckv://alice/home") {
		t.Fatal("expected prefix to be removed from the served set once its subscription is dropped")
	}
}

// CurrentSubscriptions must be a pure cache read: it must never itself poll
// peers, or a worker's frequent GETs would turn into a live peer poll storm
// on the leader.
func TestCurrentSubscriptionsDoesNotPollPeers(t *testing.T) {
	var calls atomic.Int64
	discovery := countingDiscovery{calls: &calls}
	s := NewLeaderSubscriber(&domain.Config{FQDN: "local.example"}, &fakeSubClient{}, nopPubSub{}, discovery, nil)

	for range 5 {
		s.CurrentSubscriptions()
	}

	if got := calls.Load(); got != 0 {
		t.Fatalf("expected CurrentSubscriptions to never poll peers, got %d calls", got)
	}
}

type countingDiscovery struct {
	calls *atomic.Int64
}

func (d countingDiscovery) Peers(ctx context.Context) ([]string, error) {
	d.calls.Add(1)
	return nil, nil
}

// Keep-alive demand keeps a subscription open but must never appear in the
// served aggregate: serving it would let reads re-cache latest chunks, which
// feeds the keep-alive again — a loop holding the subscription open forever.
func TestKeepAliveClientExcludedFromServedSet(t *testing.T) {
	ws := newWSTestServer(t)
	fake := &fakeSubClient{ws: ws}
	s := newTestSubscriber(fake)

	keep := &dynamicDemand{}
	keep.set([]string{"cckv://alice/home"})
	s.RegisterKeepAliveClient(keep)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Start(ctx)

	// the keeper honors keep-alive demand: the subscription opens...
	testutil.WaitFor(t, func() bool { return ws.liveConns() == 1 })
	testutil.WaitFor(t, func() bool {
		return slices.Contains(s.OpenPrefixes(), "cckv://alice/home")
	})

	// ...but the served aggregate must not report it
	if got := s.CurrentSubscriptions(); slices.Contains(got, "cckv://alice/home") {
		t.Fatalf("keep-alive demand must not be served, got %v", got)
	}
}

// A prefix whose dial keeps failing must not be served as subscribed: workers
// would cache its latest chunk, which nothing maintains.
func TestUnopenedDemandNotServed(t *testing.T) {
	fake := &fakeSubClient{dialErr: true}
	s := newTestSubscriber(fake)

	demand := &dynamicDemand{}
	demand.set([]string{"cckv://alice/home"})
	s.RegisterClient(demand)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Start(ctx)

	testutil.WaitFor(t, func() bool { return fake.dialCount.Load() >= 1 })
	time.Sleep(100 * time.Millisecond) // let the failed dial settle

	if got := s.CurrentSubscriptions(); slices.Contains(got, "cckv://alice/home") {
		t.Fatalf("demand without an open connection must not be served, got %v", got)
	}
}

// Prefixes resolving to this host need no peer connection (local events
// reach redis via the delivery path and the cache updater maintains them), so
// they are served as soon as demand exists.
func TestLocalPrefixServed(t *testing.T) {
	fake := &fakeSubClient{resolveTo: "local.example"} // == test subscriber's own FQDN
	s := newTestSubscriber(fake)

	demand := &dynamicDemand{}
	demand.set([]string{"cckv://alice/home"})
	s.RegisterClient(demand)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Start(ctx)

	testutil.WaitFor(t, func() bool {
		return slices.Contains(s.CurrentSubscriptions(), "cckv://alice/home")
	})
	if got := fake.dialCount.Load(); got != 0 {
		t.Fatalf("local prefixes must not be dialed, got %d dials", got)
	}
}

// An ensure that lands after the keeper captured its demand snapshot must
// survive that tick's deleteExcessSubscriptions: the snapshot does not know
// the new prefix, but stripping it would tear down a subscription that was
// just opened and lose events until the next tick.
func TestEnsureAfterSnapshotSurvivesDeleteExcess(t *testing.T) {
	ws := newWSTestServer(t)
	fake := &fakeSubClient{ws: ws}
	s := newTestSubscriber(fake)

	demand := &dynamicDemand{}
	demand.set([]string{"cckv://alice/home"})
	s.RegisterClient(demand)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Start(ctx)

	snapshotTime := time.Now() // a tick whose snapshot predates the ensure below
	time.Sleep(10 * time.Millisecond)

	s.EnsureSubscriptions(context.Background(), []string{"cckv://alice/home"})
	testutil.WaitFor(t, func() bool { return ws.liveConns() == 1 })

	// stale snapshot without the prefix: the fresh ensure stamp must spare it
	s.deleteExcessSubscriptions(nil, snapshotTime)

	if count, connected := s.trackedEntries(); count != 1 || connected != 1 {
		t.Fatalf("expected the just-ensured subscription to survive, got tracked=%d connected=%d", count, connected)
	}
}

// A stale keeper from a previous lead term must not tear down the connections
// of the term that replaced it.
func TestCloseAllSkipsNewerTerm(t *testing.T) {
	ws := newWSTestServer(t)
	fake := &fakeSubClient{ws: ws}
	s := newTestSubscriber(fake)

	demand := &dynamicDemand{}
	demand.set([]string{"cckv://alice/home"})
	s.RegisterClient(demand)

	ctx1, cancel1 := context.WithCancel(context.Background())
	defer cancel1()
	s.Start(ctx1)
	testutil.WaitFor(t, func() bool { return ws.liveConns() == 1 })

	// a new term takes over
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	s.mu.Lock()
	s.runCtx = ctx2
	s.mu.Unlock()

	// the old term's closeAll must be a no-op now
	s.closeAllSubscriptions(ctx1)
	if count, connected := s.trackedEntries(); count != 1 || connected != 1 {
		t.Fatalf("expected the new term's connection to survive a stale closeAll, got tracked=%d connected=%d", count, connected)
	}

	// the owning term's closeAll still sweeps
	s.closeAllSubscriptions(ctx2)
	if count, _ := s.trackedEntries(); count != 0 {
		t.Fatalf("expected the owning term's closeAll to sweep, got %d entries", count)
	}
}

// The keeper loop beats the liveness watchdog on every tick and releases the
// heartbeat when its lead term ends, so a demoted replica does not read as
// a wedged process.
func TestLeaderSubscriberKeeperReportsHeartbeat(t *testing.T) {
	watchdog := health.NewWatchdog()
	s := NewLeaderSubscriber(&domain.Config{FQDN: "local.example"}, &fakeSubClient{}, nopPubSub{}, nil, watchdog)

	ctx, cancel := context.WithCancel(context.Background())
	s.Start(ctx)

	testutil.WaitFor(t, func() bool {
		_, running := watchdog.Stalled(time.Now().Add(time.Hour))["worker/leader-subscriber"]
		return running
	})
	if stalled := watchdog.Stalled(time.Now()); len(stalled) != 0 {
		t.Fatalf("keeper must not read as stalled while running: %v", stalled)
	}

	cancel()
	testutil.WaitFor(t, func() bool {
		return len(watchdog.Stalled(time.Now().Add(time.Hour))) == 0
	})
}
