package record

import (
	"context"
	"fmt"
	"maps"
	"slices"

	"github.com/patrickmn/go-cache"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/internal/utils"
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
// has no cached form and goes to the repository on its own.
func (uc *Usecase) policyStacks(ctx context.Context, uris []string) (map[string][]concrnt.Policy, error) {
	stacks := make(map[string][]concrnt.Policy, len(uris))
	owners := map[string]string{}
	var cckvs []string
	for _, uri := range uris {
		parsed, err := concrnt.ParseCCURI(uri)
		if err != nil {
			return nil, err
		}
		if parsed.Scheme != "cckv" {
			stack, err := uc.repo.GetHierarchicalRecordPolicies(ctx, uri)
			if err != nil {
				return nil, err
			}
			stacks[uri] = stack
			continue
		}
		owners[uri] = parsed.Owner
		cckvs = append(cckvs, uri)
	}
	if len(cckvs) == 0 {
		return stacks, nil
	}

	fromRepo := func(uris []string) error {
		found, err := uc.repo.GetHierarchicalRecordPoliciesMulti(ctx, uris)
		if err != nil {
			return err
		}
		maps.Copy(stacks, found)
		return nil
	}
	if uc.sharedCache == nil {
		return stacks, fromRepo(cckvs)
	}

	genKeys := []string{}
	for _, owner := range owners {
		if !slices.Contains(genKeys, policyGenKey(owner)) {
			genKeys = append(genKeys, policyGenKey(owner))
		}
	}
	gens, err := utils.Generations(uc.sharedCache, genKeys)
	if err != nil {
		policyCacheRequests.WithLabelValues("error").Add(float64(len(cckvs)))
		return stacks, fromRepo(cckvs)
	}

	keys := make([]string, 0, len(cckvs))
	uriOf := make(map[string]string, len(cckvs))
	for _, uri := range cckvs {
		key := "policy-stack:" + gens[policyGenKey(owners[uri])] + ":" + utils.SHA256Hex(uri)
		keys = append(keys, key)
		uriOf[key] = uri
	}
	cached, err := cachedMulti(uc, policyCacheRequests, keys, cache.DefaultExpiration, policyStackTTL, func(missing []string) (map[string][]concrnt.Policy, error) {
		uris := make([]string, 0, len(missing))
		for _, key := range missing {
			uris = append(uris, uriOf[key])
		}
		found, err := uc.repo.GetHierarchicalRecordPoliciesMulti(ctx, uris)
		if err != nil {
			return nil, err
		}
		out := make(map[string][]concrnt.Policy, len(missing))
		for _, key := range missing {
			stack := found[uriOf[key]]
			if stack == nil {
				stack = []concrnt.Policy{}
			}
			out[key] = stack
		}
		return out, nil
	})
	if err != nil {
		return nil, err
	}
	for key, stack := range cached {
		stacks[uriOf[key]] = stack
	}
	return stacks, nil
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
		if err := utils.MoveGeneration(uc.sharedCache, policyGenKey(owner)); err != nil {
			return fmt.Errorf("invalidate policy stacks of %s: %w", owner, err)
		}
		return nil
	}
}
