package record

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/concrnt/concrnt"
)

// The policy stack cache. A record's stack (CIP-12 §5.3) is assembled from
// its ancestor chain, which lies within one owner, so every stack of an
// owner is keyed under that owner's generation: a write that changes any
// layer (a record with a policy or distributions stored, replaced or
// deleted) issues a new generation and every stack of the owner is gone at
// once, while the common write (a post without a policy) leaves the
// generation alone. The generation is read from the shared cache on every
// lookup, since a policy change is access control; only the stacks under a
// generation are also kept in-process, where they can never go stale.

// policyStackTTL bounds how long a stack outlives a change that bypassed
// the generation (an offline tool writing records).
const policyStackTTL = 3600

var policyCacheRequests = promauto.NewCounterVec(prometheus.CounterOpts{
	Namespace: "concrnt",
	Name:      "policy_cache_requests_total",
	Help:      "Policy stack lookups by cckv against the policy cache, by result (local: the in-process copy, hit, miss, error: the cache was unreachable).",
}, []string{"result"})

func policyGenKey(owner string) string {
	return "policy-gen:" + owner
}

func policyStackKey(gen, cckv string) string {
	sum := sha256.Sum256([]byte(cckv))
	return "policy-stack:" + gen + ":" + hex.EncodeToString(sum[:])
}

// policyStack is repo.GetHierarchicalRecordPolicies behind the cache.
func (uc *Usecase) policyStack(ctx context.Context, uri string) ([]concrnt.Policy, error) {
	stacks, err := uc.policyStacks(ctx, []string{uri})
	if err != nil {
		return nil, err
	}
	return stacks[uri], nil
}

// policyStacks is repo.GetHierarchicalRecordPoliciesMulti behind the cache;
// every input URI has an entry. A ccfs URI (an association's document)
// has no cached form and goes to the repository.
func (uc *Usecase) policyStacks(ctx context.Context, uris []string) (map[string][]concrnt.Policy, error) {
	if uc.sharedCache == nil {
		return uc.policyStacksFromRepo(ctx, uris)
	}

	stacks := make(map[string][]concrnt.Policy, len(uris))
	owners := map[string]string{}
	var direct, cached []string
	for _, uri := range uris {
		parsed, err := concrnt.ParseCCURI(uri)
		if err != nil {
			return nil, err
		}
		if parsed.Scheme != "cckv" {
			direct = append(direct, uri)
			continue
		}
		owners[uri] = parsed.Owner
		cached = append(cached, uri)
	}

	if len(direct) > 0 {
		fromRepo, err := uc.policyStacksFromRepo(ctx, direct)
		if err != nil {
			return nil, err
		}
		for uri, stack := range fromRepo {
			stacks[uri] = stack
		}
	}
	if len(cached) == 0 {
		return stacks, nil
	}

	gens, err := uc.policyGenerations(owners)
	if err != nil {
		policyCacheRequests.WithLabelValues("error").Add(float64(len(cached)))
		fromRepo, err := uc.policyStacksFromRepo(ctx, cached)
		if err != nil {
			return nil, err
		}
		for uri, stack := range fromRepo {
			stacks[uri] = stack
		}
		return stacks, nil
	}

	var remote []string
	remoteKeys := map[string]string{}
	for _, uri := range cached {
		key := policyStackKey(gens[owners[uri]], uri)
		if x, found := uc.cache.Get(key); found {
			policyCacheRequests.WithLabelValues("local").Inc()
			stacks[uri] = x.([]concrnt.Policy)
			continue
		}
		remote = append(remote, uri)
		remoteKeys[uri] = key
	}
	if len(remote) == 0 {
		return stacks, nil
	}

	keys := make([]string, 0, len(remote))
	for _, uri := range remote {
		keys = append(keys, remoteKeys[uri])
	}
	items, err := uc.sharedCache.GetMulti(keys)
	if err != nil {
		policyCacheRequests.WithLabelValues("error").Add(float64(len(remote)))
		items = nil
	}
	var missing []string
	for _, uri := range remote {
		item, ok := items[remoteKeys[uri]]
		if ok {
			var stack []concrnt.Policy
			if json.Unmarshal(item.Value, &stack) == nil {
				policyCacheRequests.WithLabelValues("hit").Inc()
				uc.cache.Set(remoteKeys[uri], stack, 0)
				stacks[uri] = stack
				continue
			}
		}
		if err == nil {
			policyCacheRequests.WithLabelValues("miss").Inc()
		}
		missing = append(missing, uri)
	}
	if len(missing) == 0 {
		return stacks, nil
	}

	fromRepo, err := uc.policyStacksFromRepo(ctx, missing)
	if err != nil {
		return nil, err
	}
	for _, uri := range missing {
		stack := fromRepo[uri]
		if stack == nil {
			stack = []concrnt.Policy{}
		}
		stacks[uri] = stack
		uc.cache.Set(remoteKeys[uri], stack, 0)
		uc.sharedCacheSet(remoteKeys[uri], stack, policyStackTTL)
	}
	return stacks, nil
}

// policyStacksFromRepo fetches the cckv URIs in one statement and every
// other URI (ccfs) on its own.
func (uc *Usecase) policyStacksFromRepo(ctx context.Context, uris []string) (map[string][]concrnt.Policy, error) {
	stacks := make(map[string][]concrnt.Policy, len(uris))
	var cckvs []string
	for _, uri := range uris {
		parsed, err := concrnt.ParseCCURI(uri)
		if err != nil {
			return nil, err
		}
		if parsed.Scheme == "cckv" {
			cckvs = append(cckvs, uri)
			continue
		}
		stack, err := uc.repo.GetHierarchicalRecordPolicies(ctx, uri)
		if err != nil {
			return nil, err
		}
		stacks[uri] = stack
	}
	if len(cckvs) > 0 {
		fromRepo, err := uc.repo.GetHierarchicalRecordPoliciesMulti(ctx, cckvs)
		if err != nil {
			return nil, err
		}
		for uri, stack := range fromRepo {
			stacks[uri] = stack
		}
	}
	return stacks, nil
}

// policyGenerations reads the current generation of every owner in owners
// (uri -> owner), issuing one for an owner that has none. A missing
// generation never falls back to a fixed value: stacks cached under an
// earlier, evicted generation must not come back.
func (uc *Usecase) policyGenerations(owners map[string]string) (map[string]string, error) {
	distinct := map[string]bool{}
	keys := []string{}
	for _, owner := range owners {
		if !distinct[owner] {
			distinct[owner] = true
			keys = append(keys, policyGenKey(owner))
		}
	}
	items, err := uc.sharedCache.GetMulti(keys)
	if err != nil {
		return nil, err
	}
	gens := make(map[string]string, len(distinct))
	for owner := range distinct {
		if item, ok := items[policyGenKey(owner)]; ok {
			gens[owner] = string(item.Value)
			continue
		}
		gen := newPolicyGeneration()
		err := uc.sharedCache.Add(&memcache.Item{Key: policyGenKey(owner), Value: []byte(gen)})
		if errors.Is(err, memcache.ErrNotStored) {
			item, err := uc.sharedCache.Get(policyGenKey(owner))
			if err != nil {
				return nil, err
			}
			gen = string(item.Value)
		} else if err != nil {
			return nil, err
		}
		gens[owner] = gen
	}
	return gens, nil
}

func newPolicyGeneration() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(fmt.Sprintf("crypto/rand: %v", err))
	}
	return hex.EncodeToString(b[:])
}

// policyLayerAt reports whether stack has a layer of its own at uri, i.e.
// whether the record stored there carries a policy or distributions.
func policyLayerAt(stack []concrnt.Policy, uri string) bool {
	for _, layer := range stack {
		if layer.Source == uri {
			return true
		}
	}
	return false
}

// policyStacksInvalidated issues owner a new generation. It runs after the
// commit, so a lookup that read the old generation before the write can
// only store under that old generation.
func (uc *Usecase) policyStacksInvalidated(owner string) PostProcessAction {
	return func(ctx context.Context) error {
		if uc.sharedCache == nil {
			return nil
		}
		if err := uc.sharedCache.Set(&memcache.Item{Key: policyGenKey(owner), Value: []byte(newPolicyGeneration())}); err != nil {
			return fmt.Errorf("invalidate policy stacks of %s: %w", owner, err)
		}
		return nil
	}
}
