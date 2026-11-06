package record

import (
	"context"
	"maps"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/internal/domain"
)

// policyRepo serves a fixed layer per URI and counts the fetches.
type policyRepo struct {
	Repository
	mu     sync.Mutex
	layers map[string]concrnt.Policy
	single []string
	multi  [][]string
}

// stackOf is the ancestor chain's layers, root first.
func (r *policyRepo) stackOf(uri string) []concrnt.Policy {
	stack := []concrnt.Policy{}
	for _, source := range slices.Sorted(maps.Keys(r.layers)) {
		if uri == source || strings.HasPrefix(uri, source+"/") {
			layer := r.layers[source]
			layer.Source = source
			stack = append(stack, layer)
		}
	}
	return stack
}

func (r *policyRepo) GetHierarchicalRecordPolicies(ctx context.Context, uri string) ([]concrnt.Policy, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.single = append(r.single, uri)
	return r.stackOf(uri), nil
}

func (r *policyRepo) GetHierarchicalRecordPoliciesMulti(ctx context.Context, uris []string) (map[string][]concrnt.Policy, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.multi = append(r.multi, append([]string(nil), uris...))
	stacks := map[string][]concrnt.Policy{}
	for _, uri := range uris {
		stacks[uri] = r.stackOf(uri)
	}
	return stacks, nil
}

func newPolicyCacheUsecase(repo Repository, cache SharedCache) *Usecase {
	uc := New(repo, nil, nil, &domain.Config{FQDN: "example.com"}, nil, nil, nil, nil, nil)
	uc.SetSharedCache(cache)
	return uc
}

const policyOwner = "con1policyowner"

// A stack is fetched once per generation: the second lookup is served from
// the cache, and a second replica sharing the cache never reaches the
// repository either.
func TestPolicyStackCachedAcrossReplicas(t *testing.T) {
	ctx := context.Background()
	repo := &policyRepo{layers: map[string]concrnt.Policy{"cckv://" + policyOwner + "/home": {}}}
	cache := newFakeEntityCache()
	writer := newPolicyCacheUsecase(repo, cache)
	reader := newPolicyCacheUsecase(repo, cache)

	key := "cckv://" + policyOwner + "/home/post1"
	stack, err := writer.policyStack(ctx, key)
	require.NoError(t, err)
	require.Len(t, stack, 1)
	require.Len(t, repo.multi, 1)

	_, err = writer.policyStack(ctx, key)
	require.NoError(t, err)
	_, err = reader.policyStack(ctx, key)
	require.NoError(t, err)
	require.Len(t, repo.multi, 1, "cached stacks must not reach the repository")
	require.Empty(t, repo.single)
}

// Invalidating the owner issues a new generation: every replica refetches
// its stacks, and an owner that was not invalidated keeps its cached ones.
func TestPolicyStackInvalidationIsPerOwner(t *testing.T) {
	ctx := context.Background()
	repo := &policyRepo{layers: map[string]concrnt.Policy{}}
	cache := newFakeEntityCache()
	writer := newPolicyCacheUsecase(repo, cache)
	reader := newPolicyCacheUsecase(repo, cache)

	mine := "cckv://" + policyOwner + "/home/post1"
	other := "cckv://con1otherowner/home/post1"
	for _, uc := range []*Usecase{writer, reader} {
		_, err := uc.policyStacks(ctx, []string{mine, other})
		require.NoError(t, err)
	}
	require.Len(t, repo.multi, 1)

	require.NoError(t, writer.policyStacksInvalidated(policyOwner)(ctx))

	stacks, err := reader.policyStacks(ctx, []string{mine, other})
	require.NoError(t, err)
	require.Len(t, stacks, 2)
	require.Len(t, repo.multi, 2)
	require.Equal(t, []string{mine}, repo.multi[1], "only the invalidated owner's stack is refetched")
}

// A ccfs URI (an association's own document) is never cached.
func TestPolicyStackCCFSBypassesCache(t *testing.T) {
	ctx := context.Background()
	repo := &policyRepo{layers: map[string]concrnt.Policy{}}
	uc := newPolicyCacheUsecase(repo, newFakeEntityCache())

	uri := "ccfs://" + policyOwner + "/concrnt/cdid"
	for range 2 {
		_, err := uc.policyStack(ctx, uri)
		require.NoError(t, err)
	}
	require.Equal(t, []string{uri, uri}, repo.single)
	require.Empty(t, repo.multi)
}

// Without a shared cache every lookup is a repository fetch.
func TestPolicyStackWithoutCache(t *testing.T) {
	ctx := context.Background()
	repo := &policyRepo{layers: map[string]concrnt.Policy{}}
	uc := New(repo, nil, nil, &domain.Config{}, nil, nil, nil, nil, nil)

	key := "cckv://" + policyOwner + "/home/post1"
	for range 2 {
		_, err := uc.policyStack(ctx, key)
		require.NoError(t, err)
	}
	require.Len(t, repo.multi, 2)
}

func TestPolicyLayerAt(t *testing.T) {
	stack := []concrnt.Policy{{Source: "cckv://o/home"}, {Source: "cckv://o/home/tl"}}
	require.True(t, policyLayerAt(stack, "cckv://o/home/tl"))
	require.False(t, policyLayerAt(stack, "cckv://o/home/tl/post"))
	require.False(t, policyLayerAt(nil, "cckv://o/home"))
}
