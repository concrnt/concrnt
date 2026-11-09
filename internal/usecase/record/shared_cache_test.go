package record

import (
	"sync"

	"github.com/bradfitz/gomemcache/memcache"

	"github.com/concrnt/concrnt/internal/domain"
)

type fakeSharedCache struct {
	mu    sync.Mutex
	items map[string][]byte
}

func newFakeSharedCache() *fakeSharedCache {
	return &fakeSharedCache{items: map[string][]byte{}}
}

func (c *fakeSharedCache) Get(key string) (*memcache.Item, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	value, ok := c.items[key]
	if !ok {
		return nil, memcache.ErrCacheMiss
	}
	return &memcache.Item{Key: key, Value: value}, nil
}

func (c *fakeSharedCache) GetMulti(keys []string) (map[string]*memcache.Item, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	items := map[string]*memcache.Item{}
	for _, key := range keys {
		if value, ok := c.items[key]; ok {
			items[key] = &memcache.Item{Key: key, Value: value}
		}
	}
	return items, nil
}

func (c *fakeSharedCache) Add(item *memcache.Item) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.items[item.Key]; ok {
		return memcache.ErrNotStored
	}
	c.items[item.Key] = item.Value
	return nil
}

func (c *fakeSharedCache) Set(item *memcache.Item) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.items[item.Key] = item.Value
	return nil
}

func (c *fakeSharedCache) Delete(key string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.items[key]; !ok {
		return memcache.ErrCacheMiss
	}
	delete(c.items, key)
	return nil
}

func newCachedUsecase(cfg *domain.Config, repo Repository, cache SharedCache) *Usecase {
	uc := New(repo, nil, newTestServerUsecase(cfg), cfg, nil, nopSignalService{}, nopPolicyService{}, nil, nil)
	uc.SetSharedCache(cache)
	return uc
}
