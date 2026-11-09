package service

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/client"
	"github.com/concrnt/concrnt/policy"
)

// A disallowed api must be rejected before any resolution or network I/O:
// with a nil client, reaching the client would panic.
func TestConcrntCallAllowlist(t *testing.T) {

	s := &PolicyService{client: nil}

	_, err := s.ConcrntCall(context.Background(), "con1alice", "net.concrnt.core.commit", map[string]string{})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not allowed")
}

// CIP-12 §5.3: a virtual parent (distribution destination) forms its own
// layer inserted immediately before the layer that declares it — the
// declaring resource stays last — instead of being merged into an ancestor
// layer. An unreachable parent yields an errored evaluation set in that slot.
func TestResolvePolicyStackVirtualParentPlacement(t *testing.T) {
	s := NewPolicyService(policy.Policy{}, GlobalParameters{}, client.New("example.com"))

	unreachable := ":::not-a-uri"
	stack := []concrnt.Policy{
		{Source: "cckv://con1owner/root"},
		{Source: "cckv://con1owner/root/child", VirtualParents: &[]string{unreachable}},
	}

	result, err := s.resolvePolicyStack(context.Background(), stack)
	assert.NoError(t, err)
	// [root, virtual parent (errored), declaring resource]
	if assert.Len(t, result, 3) {
		assert.Len(t, result[0], 0)
		if assert.Len(t, result[1], 1) {
			assert.True(t, result[1][0].Errored)
		}
		assert.Len(t, result[2], 0)
	}
}

// A policy URL is fetched once; past the fresh window the stale copy is
// served immediately and refreshed in the background, so no evaluation
// waits on the policy host.
func TestResolvePolicyURLStaleWhileRevalidate(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		_ = json.NewEncoder(w).Encode(policy.PolicyDocument{Name: "p", Versions: map[string]any{"2025-12-23": map[string]any{"entries": []any{}}}})
	}))
	defer srv.Close()

	s := NewPolicyService(policy.Policy{}, GlobalParameters{}, client.New("example.com"))
	ctx := context.Background()
	for range 3 {
		_, err := s.ResolvePolicyURL(ctx, srv.URL)
		assert.NoError(t, err)
	}
	assert.EqualValues(t, 1, hits.Load(), "a fresh copy is served without a fetch")

	x, _ := s.cache.Get(srv.URL)
	stale := x.(cachedPolicy)
	stale.fetchedAt = time.Now().Add(-policyFreshFor - time.Minute)
	s.cache.Set(srv.URL, stale, policyStaleFor)

	start := time.Now()
	_, err := s.ResolvePolicyURL(ctx, srv.URL)
	assert.NoError(t, err)
	assert.Less(t, time.Since(start), 500*time.Millisecond, "the stale read does not wait for the refresh")
	assert.Eventually(t, func() bool { return hits.Load() == 2 }, 2*time.Second, 10*time.Millisecond, "the refresh runs in the background")
	assert.Eventually(t, func() bool {
		x, _ := s.cache.Get(srv.URL)
		return time.Since(x.(cachedPolicy).fetchedAt) < policyFreshFor
	}, 2*time.Second, 10*time.Millisecond, "the refreshed copy replaces the stale one")
}
