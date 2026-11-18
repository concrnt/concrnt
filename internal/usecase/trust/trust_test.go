package trust

import (
	"context"
	"errors"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/concrnt/concrnt/internal/domain"
)

// stubRepo keeps entities in memory the way the entities table does: a
// computation's result is stored per entity and added up per domain.
type stubRepo struct {
	mu       sync.Mutex
	seeds    []string
	edges    []AckEdge
	domainOf map[string]string
	stored   map[string]float64
	err      error
	saves    int
	loads    int
	schemas  []string
	local    string
}

func (r *stubRepo) GetTrustSeeds(ctx context.Context) ([]string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.seeds, r.err
}

func (r *stubRepo) GetLocalAckEdges(ctx context.Context, localDomain string, schemas []string) ([]AckEdge, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.local = localDomain
	r.schemas = schemas
	for _, edge := range r.edges {
		r.domainOf[edge.To] = edge.ToDomain
	}
	return r.edges, r.err
}

func (r *stubRepo) SaveEntityTrust(ctx context.Context, scores map[string]float64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.err != nil {
		return r.err
	}
	r.saves++
	r.stored = scores
	return nil
}

func (r *stubRepo) GetDomainTrust(ctx context.Context) (map[string]float64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.err != nil {
		return nil, r.err
	}
	r.loads++
	totals := map[string]float64{}
	for entity, score := range r.stored {
		totals[r.domainOf[entity]] += score
	}
	return totals, nil
}

func (r *stubRepo) counts() (saves, loads int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.saves, r.loads
}

// newTrustUsecase wires a usecase whose seeds are local.
func newTrustUsecase(repo *stubRepo, seeds ...string) *Usecase {
	repo.seeds = seeds
	if repo.domainOf == nil {
		repo.domainOf = map[string]string{}
	}
	for _, seed := range seeds {
		repo.domainOf[seed] = "example.com"
	}
	return New(repo, &domain.Config{FQDN: "example.com"})
}

// refresh recomputes, as the leader does on its schedule, and reads the
// result, as every replica does.
func refresh(t *testing.T, uc *Usecase) {
	t.Helper()
	if err := uc.Recompute(context.Background()); err != nil {
		t.Fatalf("Recompute: %v", err)
	}
	if err := uc.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
}

func requireTrust(t *testing.T, uc *Usecase, name string, want float64) {
	t.Helper()
	if got := uc.DomainTrust(context.Background(), name); math.Abs(got-want) > 1e-6 {
		t.Fatalf("trust of %s = %v, want %v", name, got, want)
	}
}

// A remote domain is trusted as much as its users are, added up; trust held
// by local users is not anybody's domain trust.
func TestDomainTrustAddsUpItsUsers(t *testing.T) {
	repo := &stubRepo{edges: []AckEdge{
		{From: "seed", To: "local", ToDomain: "example.com"},
		{From: "seed", To: "r1", ToDomain: "remote.example.net"},
		{From: "seed", To: "r2", ToDomain: "remote.example.net"},
		{From: "seed", To: "o1", ToDomain: "other.example.org"},
		{From: "local", To: "o1", ToDomain: "other.example.org"},
	}}
	uc := newTrustUsecase(repo, "seed")
	refresh(t, uc)

	requireTrust(t, uc, "remote.example.net", 2*damping/4)
	requireTrust(t, uc, "other.example.org", damping/4+damping*damping/4)
	requireTrust(t, uc, "example.com", 0)
	requireTrust(t, uc, "stranger.example", 0)
}

func TestDomainTrustIsCappedAtOne(t *testing.T) {
	repo := &stubRepo{edges: []AckEdge{
		{From: "s1", To: "r1", ToDomain: "remote.example.net"},
		{From: "s2", To: "r2", ToDomain: "remote.example.net"},
	}}
	uc := newTrustUsecase(repo, "s1", "s2")
	refresh(t, uc)

	requireTrust(t, uc, "remote.example.net", 1) // 2 * damping, capped
}

func TestConfiguredSchemasAndLocalDomainReachTheRepository(t *testing.T) {
	repo := &stubRepo{}
	uc := newTrustUsecase(repo, "seed")
	uc.config.IngestLimit.TrustAckSchemas = []string{"https://schema.example/follow.json"}
	refresh(t, uc)

	if repo.local != "example.com" || len(repo.schemas) != 1 || repo.schemas[0] != "https://schema.example/follow.json" {
		t.Fatalf("repository asked for domain %q schemas %v", repo.local, repo.schemas)
	}
}

// Every entity's score is stored, local ones and the seeds included.
func TestRecomputeStoresEveryEntitysScore(t *testing.T) {
	repo := &stubRepo{edges: []AckEdge{
		{From: "seed", To: "local", ToDomain: "example.com"},
		{From: "local", To: "r", ToDomain: "remote.example.net"},
	}}
	uc := newTrustUsecase(repo, "seed")
	if err := uc.Recompute(context.Background()); err != nil {
		t.Fatalf("Recompute: %v", err)
	}

	want := map[string]float64{"seed": 1, "local": damping, "r": damping * damping}
	if len(repo.stored) != len(want) {
		t.Fatalf("stored = %v, want %v", repo.stored, want)
	}
	for entity, score := range want {
		if math.Abs(repo.stored[entity]-score) > 1e-6 {
			t.Fatalf("stored score of %s = %v, want %v", entity, repo.stored[entity], score)
		}
	}
}

// Taking the last seed away must clear the scores it led to.
func TestWithoutSeedsStoredScoresAreCleared(t *testing.T) {
	repo := &stubRepo{edges: []AckEdge{{From: "seed", To: "r", ToDomain: "remote.example.net"}}}
	uc := newTrustUsecase(repo, "seed")
	refresh(t, uc)
	requireTrust(t, uc, "remote.example.net", damping)

	repo.seeds = nil
	refresh(t, uc)
	if len(repo.stored) != 0 {
		t.Fatalf("stored = %v, want nothing", repo.stored)
	}
	requireTrust(t, uc, "remote.example.net", 0)
}

// A remote entity can be a seed, which makes its server fully trusted.
func TestRemoteSeedMakesItsDomainFullyTrusted(t *testing.T) {
	repo := &stubRepo{}
	uc := newTrustUsecase(repo, "friend")
	repo.domainOf["friend"] = "remote.example.net"
	refresh(t, uc)

	requireTrust(t, uc, "remote.example.net", 1)
}

// Only the leader recomputes; every other replica just reads what it stored.
func TestRefreshReadsStoredScoresWithoutRecomputing(t *testing.T) {
	repo := &stubRepo{
		edges:    []AckEdge{{From: "seed", To: "r", ToDomain: "remote.example.net"}},
		domainOf: map[string]string{"r": "remote.example.net"},
		stored:   map[string]float64{"r": 0.25}, // the leader's result
	}
	uc := newTrustUsecase(repo, "seed")

	if err := uc.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	if saves, _ := repo.counts(); saves != 0 {
		t.Fatalf("recomputed %d times, want none", saves)
	}
	requireTrust(t, uc, "remote.example.net", 0.25)
}

// The leader's worker recomputes at once and then on its interval, and stops
// when leadership is lost.
func TestRecomputingRunsUntilLeadershipIsLost(t *testing.T) {
	repo := &stubRepo{edges: []AckEdge{{From: "seed", To: "r", ToDomain: "remote.example.net"}}}
	uc := newTrustUsecase(repo, "seed")

	saves := func() int {
		n, _ := repo.counts()
		return n
	}
	waitForSaves := func(want int) {
		t.Helper()
		deadline := time.Now().Add(2 * time.Second)
		for saves() < want {
			if time.Now().After(deadline) {
				t.Fatalf("recomputed %d times, want at least %d", saves(), want)
			}
			time.Sleep(time.Millisecond)
		}
	}

	leadCtx, loseLeadership := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		uc.recomputeEvery(leadCtx, 5*time.Millisecond)
		close(done)
	}()

	waitForSaves(3)

	loseLeadership()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("worker still running after leadership was lost")
	}
	stopped := saves()
	time.Sleep(30 * time.Millisecond)
	if saves() != stopped {
		t.Fatalf("recomputed after leadership was lost")
	}
}

// DomainTrust never waits on the repository: stale scores are served while a
// single background refresh replaces them.
func TestDomainTrustRefreshesInTheBackgroundWhenStale(t *testing.T) {
	repo := &stubRepo{edges: []AckEdge{{From: "seed", To: "r", ToDomain: "remote.example.net"}}}
	uc := newTrustUsecase(repo, "seed")
	now := time.Unix(1_800_000_000, 0)
	uc.now = func() time.Time { return now }

	waitForLoads := func(want int) {
		t.Helper()
		deadline := time.Now().Add(2 * time.Second)
		for {
			uc.mu.Lock()
			done := !uc.refreshing
			uc.mu.Unlock()
			_, loads := repo.counts()
			if done && loads == want {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("scores read %d times, want %d", loads, want)
			}
			time.Sleep(time.Millisecond)
		}
	}

	if err := uc.Recompute(context.Background()); err != nil {
		t.Fatalf("Recompute: %v", err)
	}

	// nothing read yet: answers 0 and starts the first read
	requireTrust(t, uc, "remote.example.net", 0)
	waitForLoads(1)
	requireTrust(t, uc, "remote.example.net", damping)

	// fresh: no further reads however often it is asked
	for range 100 {
		uc.DomainTrust(context.Background(), "remote.example.net")
	}
	waitForLoads(1)

	// stale: the old score is served, and one read picks up the change
	repo.mu.Lock()
	repo.stored = nil
	repo.mu.Unlock()
	now = now.Add(reloadInterval)
	for range 100 {
		uc.DomainTrust(context.Background(), "remote.example.net")
	}
	waitForLoads(2)
	requireTrust(t, uc, "remote.example.net", 0)
}

func TestFailedRefreshKeepsThePreviousScores(t *testing.T) {
	repo := &stubRepo{edges: []AckEdge{{From: "seed", To: "r", ToDomain: "remote.example.net"}}}
	uc := newTrustUsecase(repo, "seed")
	refresh(t, uc)

	repo.err = errors.New("database down")
	if err := uc.Refresh(context.Background()); err == nil {
		t.Fatalf("Refresh returned no error, want the repository's")
	}
	requireTrust(t, uc, "remote.example.net", damping)
}
