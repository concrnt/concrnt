package record

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/prometheus/client_golang/prometheus"
)

// SharedCache is the cache shared by every replica (memcached): entities,
// policy stacks and resolved documents live there, so a commit on one
// replica invalidates them for all of them.
type SharedCache interface {
	Get(key string) (*memcache.Item, error)
	GetMulti(keys []string) (map[string]*memcache.Item, error)
	Set(item *memcache.Item) error
	Add(item *memcache.Item) error
	Delete(key string) error
}

// SetSharedCache caches the entity lookups by ccid, the policy stacks by
// cckv and the resolved documents by URI in c.
func (uc *Usecase) SetSharedCache(c SharedCache) {
	uc.sharedCache = c
}

// sharedCacheGet reads the JSON under key into v and reports "hit", "miss"
// (no entry, or one that does not decode) or "error" (the cache was
// unreachable), the labels of the *_cache_requests_total metrics.
func (uc *Usecase) sharedCacheGet(key string, v any) string {
	item, err := uc.sharedCache.Get(key)
	switch {
	case err == nil && json.Unmarshal(item.Value, v) == nil:
		return "hit"
	case err == nil, errors.Is(err, memcache.ErrCacheMiss):
		return "miss"
	default:
		return "error"
	}
}

// sharedCacheSet stores v as JSON under key for ttl seconds. Callers skip
// it after an "error" read: a write to an unreachable cache only costs
// another timeout.
func (uc *Usecase) sharedCacheSet(key string, v any, ttl int32) {
	if value, err := json.Marshal(v); err == nil {
		_ = uc.sharedCache.Set(&memcache.Item{Key: key, Value: value, Expiration: ttl})
	}
}

// sharedCacheDelete drops key; a missing key is fine.
func (uc *Usecase) sharedCacheDelete(key string) error {
	if err := uc.sharedCache.Delete(key); err != nil && !errors.Is(err, memcache.ErrCacheMiss) {
		return err
	}
	return nil
}

// cachedMulti serves keys from the in-process copy, then the shared cache,
// then origin, which is called once with the keys missing from both and
// returns a value per key. Each key counts once in metric (local, hit,
// miss, or error when the shared cache was unreachable). A shared hit is
// copied in-process; an origin value is stored in both tiers, in the shared
// one only after a non-error read (a write to an unreachable cache only
// costs another timeout).
func cachedMulti[T any](uc *Usecase, metric *prometheus.CounterVec, keys []string, localTTL time.Duration, sharedTTL int32, origin func(missing []string) (map[string]T, error)) (map[string]T, error) {
	out := make(map[string]T, len(keys))
	var remote []string
	for _, key := range keys {
		if x, found := uc.cache.Get(key); found {
			metric.WithLabelValues("local").Inc()
			out[key] = x.(T)
			continue
		}
		remote = append(remote, key)
	}
	if len(remote) == 0 {
		return out, nil
	}

	items, sharedErr := uc.sharedCache.GetMulti(remote)
	if sharedErr != nil {
		metric.WithLabelValues("error").Add(float64(len(remote)))
		items = nil
	}
	var missing []string
	for _, key := range remote {
		if item, ok := items[key]; ok {
			var v T
			if json.Unmarshal(item.Value, &v) == nil {
				metric.WithLabelValues("hit").Inc()
				uc.cache.Set(key, v, localTTL)
				out[key] = v
				continue
			}
		}
		if sharedErr == nil {
			metric.WithLabelValues("miss").Inc()
		}
		missing = append(missing, key)
	}
	if len(missing) == 0 {
		return out, nil
	}

	fetched, err := origin(missing)
	if err != nil {
		return nil, err
	}
	for _, key := range missing {
		v, ok := fetched[key]
		if !ok {
			continue
		}
		out[key] = v
		uc.cache.Set(key, v, localTTL)
		if sharedErr == nil {
			uc.sharedCacheSet(key, v, sharedTTL)
		}
	}
	return out, nil
}
