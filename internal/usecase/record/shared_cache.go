package record

import (
	"encoding/json"
	"errors"

	"github.com/bradfitz/gomemcache/memcache"
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
