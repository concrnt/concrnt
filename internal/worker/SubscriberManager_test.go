package worker

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	"github.com/concrnt/concrnt/internal/domain"
	"github.com/concrnt/concrnt/internal/testutil"
)

type fakeLeaderState struct{ leader bool }

func (f fakeLeaderState) IsLeader() bool { return f.leader }

// While leading, the manager must delegate to the real LeaderSubscriber
// (which dials peers), not forward over HTTP.
func TestSubscriberManagerDelegatesToLeaderWhenLeading(t *testing.T) {
	ws := newWSTestServer(t)
	fake := &fakeSubClient{ws: ws}
	leaderSub := NewLeaderSubscriber(&domain.Config{FQDN: "local.example"}, fake, nopPubSub{}, nil, nil)

	// registered so the keeper's own demand snapshot agrees with the manual
	// ensure below; otherwise the very next tick's deleteExcessSubscriptions
	// would race to undo it (demand is otherwise empty)
	demand := &dynamicDemand{}
	demand.set([]string{"cckv://alice/home"})
	leaderSub.RegisterClient(demand)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	leaderSub.Start(ctx)

	workerSub := NewWorkerSubscriber(fakeLeaderLocator{ok: false})
	m := NewSubscriberManager(fakeLeaderState{leader: true}, leaderSub, workerSub)

	m.EnsureSubscriptions(context.Background(), []string{"cckv://alice/home"})

	testutil.WaitFor(t, func() bool { return ws.liveConns() == 1 })
	if got := fake.dialCount.Load(); got != 1 {
		t.Fatalf("expected the leader subscriber to dial, got %d dials", got)
	}
}

// While not leading, the manager must delegate to WorkerSubscriber (forward
// to the leader over HTTP), never touching the local LeaderSubscriber.
func TestSubscriberManagerDelegatesToWorkerWhenNotLeading(t *testing.T) {
	received := make(chan []string, 1)
	leader := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		var body EnsureRequest
		if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		received <- body.Prefixes
	}))
	defer leader.Close()

	fake := &fakeSubClient{}
	leaderSub := NewLeaderSubscriber(&domain.Config{FQDN: "local.example"}, fake, nopPubSub{}, nil, nil)
	workerSub := NewWorkerSubscriber(fakeLeaderLocator{url: leader.URL, ok: true})
	m := NewSubscriberManager(fakeLeaderState{leader: false}, leaderSub, workerSub)

	m.EnsureSubscriptions(context.Background(), []string{"cckv://bob/home"})

	select {
	case prefixes := <-received:
		if !reflect.DeepEqual(prefixes, []string{"cckv://bob/home"}) {
			t.Fatalf("unexpected forwarded prefixes: %v", prefixes)
		}
	case <-time.After(time.Second):
		t.Fatal("expected the worker subscriber to forward to the leader")
	}

	if got := fake.dialCount.Load(); got != 0 {
		t.Fatalf("expected the (non-leading) leader subscriber not to be touched, got %d dials", got)
	}
}
