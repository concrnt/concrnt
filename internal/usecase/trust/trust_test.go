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

type stubRepo struct {
	mu      sync.Mutex
	edges   []AckEdge
	err     error
	calls   int
	schemas []string
	local   string
}

func (r *stubRepo) GetLocalAckEdges(ctx context.Context, localDomain string, schemas []string) ([]AckEdge, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls++
	r.local = localDomain
	r.schemas = schemas
	return r.edges, r.err
}

func (r *stubRepo) callCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls
}

func newTrustUsecase(repo *stubRepo, seeds ...string) *Usecase {
	cfg := &domain.Config{FQDN: "example.com"}
	cfg.IngestLimit.Trust.Seeds = seeds
	return New(repo, cfg)
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
	if err := uc.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh: %v", err)
	}

	requireTrust(t, uc, "remote.example.net", 2*damping/4)
	requireTrust(t, uc, "other.example.org", damping/4+damping*damping/4)
	requireTrust(t, uc, "example.com", 0)
	requireTrust(t, uc, "stranger.example", 0)
}

func TestDomainTrustIsCappedAtOne(t *testing.T) {
	repo := &stubRepo{edges: []AckEdge{
		{From: "s1", To: "r", ToDomain: "remote.example.net"},
		{From: "s2", To: "r", ToDomain: "remote.example.net"},
	}}
	uc := newTrustUsecase(repo, "s1", "s2")
	if err := uc.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh: %v", err)
	}

	requireTrust(t, uc, "remote.example.net", 1)
}

func TestConfiguredSchemasAndLocalDomainReachTheRepository(t *testing.T) {
	repo := &stubRepo{}
	uc := newTrustUsecase(repo, "seed")
	uc.config.IngestLimit.Trust.AckSchemas = []string{"https://schema.example/follow.json"}
	if err := uc.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh: %v", err)
	}

	if repo.local != "example.com" || len(repo.schemas) != 1 || repo.schemas[0] != "https://schema.example/follow.json" {
		t.Fatalf("repository asked for domain %q schemas %v", repo.local, repo.schemas)
	}
}

func TestWithoutSeedsNothingIsComputed(t *testing.T) {
	repo := &stubRepo{edges: []AckEdge{{From: "a", To: "r", ToDomain: "remote.example.net"}}}
	uc := newTrustUsecase(repo)

	requireTrust(t, uc, "remote.example.net", 0)
	time.Sleep(20 * time.Millisecond)
	if repo.callCount() != 0 {
		t.Fatalf("repository was read %d times without seeds", repo.callCount())
	}
}

// DomainTrust never waits on the repository: stale scores are served while a
// single background refresh replaces them.
func TestDomainTrustRefreshesInTheBackgroundWhenStale(t *testing.T) {
	repo := &stubRepo{edges: []AckEdge{{From: "seed", To: "r", ToDomain: "remote.example.net"}}}
	uc := newTrustUsecase(repo, "seed")
	now := time.Unix(1_800_000_000, 0)
	uc.now = func() time.Time { return now }

	waitForCalls := func(want int) {
		t.Helper()
		deadline := time.Now().Add(2 * time.Second)
		for {
			uc.mu.Lock()
			done := !uc.refreshing
			uc.mu.Unlock()
			if done && repo.callCount() == want {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("repository read %d times, want %d", repo.callCount(), want)
			}
			time.Sleep(time.Millisecond)
		}
	}

	// nothing computed yet: answers 0 and starts the first computation
	requireTrust(t, uc, "remote.example.net", 0)
	waitForCalls(1)
	requireTrust(t, uc, "remote.example.net", damping)

	// fresh: no further reads however often it is asked
	for range 100 {
		uc.DomainTrust(context.Background(), "remote.example.net")
	}
	waitForCalls(1)

	// stale: the old score is served, and one refresh picks up the change
	repo.mu.Lock()
	repo.edges = nil
	repo.mu.Unlock()
	now = now.Add(refreshInterval)
	for range 100 {
		uc.DomainTrust(context.Background(), "remote.example.net")
	}
	waitForCalls(2)
	requireTrust(t, uc, "remote.example.net", 0)
}

func TestFailedRefreshKeepsThePreviousScores(t *testing.T) {
	repo := &stubRepo{edges: []AckEdge{{From: "seed", To: "r", ToDomain: "remote.example.net"}}}
	uc := newTrustUsecase(repo, "seed")
	if err := uc.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh: %v", err)
	}

	repo.err = errors.New("database down")
	if err := uc.Refresh(context.Background()); err == nil {
		t.Fatalf("Refresh returned no error, want the repository's")
	}
	requireTrust(t, uc, "remote.example.net", damping)
}
