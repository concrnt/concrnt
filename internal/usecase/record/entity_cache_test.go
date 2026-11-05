package record

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/bradfitz/gomemcache/memcache"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/internal/domain"
	"github.com/concrnt/concrnt/schemas"
)

type fakeEntityCache struct {
	mu    sync.Mutex
	items map[string][]byte
}

func newFakeEntityCache() *fakeEntityCache {
	return &fakeEntityCache{items: map[string][]byte{}}
}

func (c *fakeEntityCache) Get(key string) (*memcache.Item, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	value, ok := c.items[key]
	if !ok {
		return nil, memcache.ErrCacheMiss
	}
	return &memcache.Item{Key: key, Value: value}, nil
}

func (c *fakeEntityCache) GetMulti(keys []string) (map[string]*memcache.Item, error) {
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

func (c *fakeEntityCache) Add(item *memcache.Item) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.items[item.Key]; ok {
		return memcache.ErrNotStored
	}
	c.items[item.Key] = item.Value
	return nil
}

func (c *fakeEntityCache) Set(item *memcache.Item) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.items[item.Key] = item.Value
	return nil
}

func (c *fakeEntityCache) Delete(key string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.items[key]; !ok {
		return memcache.ErrCacheMiss
	}
	delete(c.items, key)
	return nil
}

// entityStoreRepo keeps entities the way the real stores do: CreateEntity
// is accept-if-newer and GetEntityByCCID returns the stored document.
type entityStoreRepo struct {
	Repository
	mu        sync.Mutex
	documents map[string]string
	entities  map[string]*domain.Entity
	createdAt map[string]time.Time
	lookups   int
}

func newEntityStoreRepo() *entityStoreRepo {
	return &entityStoreRepo{documents: map[string]string{}, entities: map[string]*domain.Entity{}, createdAt: map[string]time.Time{}}
}

func (r *entityStoreRepo) RunInTx(ctx context.Context, fn func(tx RepositoryTx) error) error {
	return fn(&recordingTx{})
}

func (r *entityStoreRepo) HasCommitLog(ctx context.Context, id string) (bool, error) {
	return false, nil
}

func (r *entityStoreRepo) CreateCommitLog(ctx context.Context, tx RepositoryTx, id string, ip string, document string, proof any, owner string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.documents[id] = document
	return nil
}

func (r *entityStoreRepo) CreateEntity(ctx context.Context, tx RepositoryTx, ccid string, alias *string, domainName string, documentID string, createdAt time.Time) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if stored, ok := r.createdAt[ccid]; ok && !createdAt.After(stored) {
		return false, nil
	}
	r.createdAt[ccid] = createdAt
	r.entities[ccid] = &domain.Entity{
		ID:             ccid,
		Alias:          alias,
		Domain:         domainName,
		SignedDocument: &concrnt.SignedDocument{Document: r.documents[documentID]},
	}
	return true, nil
}

func (r *entityStoreRepo) GetEntityByCCID(ctx context.Context, ccid string) (*domain.Entity, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.lookups++
	entity, ok := r.entities[ccid]
	if !ok {
		return nil, domain.NotFoundError{Resource: ccid}
	}
	copied := *entity
	return &copied, nil
}

func (r *entityStoreRepo) lookupCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.lookups
}

func newEntityCacheUsecase(cfg *domain.Config, repo Repository, cache SharedCache) *Usecase {
	uc := New(repo, nil, newTestServerUsecase(cfg), cfg, nil, nopSignalService{}, nopPolicyService{}, nil, nil)
	uc.SetSharedCache(cache)
	return uc
}

func entityDocAt(t *testing.T, party ackParty, host string, createdAt time.Time) concrnt.SignedDocument {
	t.Helper()
	return signTestDocument(t, concrnt.Document[schemas.Entity]{
		Kind:      "entity",
		Author:    party.ccid,
		Schema:    schemas.EntityURL,
		Value:     schemas.Entity{Domain: host},
		CreatedAt: createdAt,
	}, party.priv)
}

func TestGetEntityServesHitsFromCache(t *testing.T) {
	cfg := &domain.Config{FQDN: "example.com"}
	party := newAckParty(t, remoteEntityHost)
	repo := newEntityStoreRepo()
	uc := newEntityCacheUsecase(cfg, repo, newFakeEntityCache())
	ctx := context.Background()

	doc := entityDocAt(t, party, remoteEntityHost, time.Now().Add(-time.Hour).UTC().Truncate(time.Second))
	if _, err := uc.Commit(ctx, "127.0.0.1", doc, domain.CommitModeExecute); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	uri := concrnt.CCURI{Scheme: "cckv", Owner: party.ccid}.String()
	first, err := uc.GetEntity(ctx, uri)
	if err != nil {
		t.Fatalf("GetEntity: %v", err)
	}
	lookups := repo.lookupCount()

	second, err := uc.GetEntity(ctx, uri)
	if err != nil {
		t.Fatalf("GetEntity: %v", err)
	}
	if got := repo.lookupCount(); got != lookups {
		t.Fatalf("a cache hit reached the repository (%d -> %d lookups)", lookups, got)
	}
	if second.ID != first.ID || second.Domain != remoteEntityHost {
		t.Fatalf("cached entity = %+v, want %+v", second, first)
	}
	if second.SignedDocument == nil || second.SignedDocument.Document != doc.Document {
		t.Fatalf("cached entity lost its signed document: %+v", second.SignedDocument)
	}
}

func TestGetEntityDoesNotCacheNotFound(t *testing.T) {
	cfg := &domain.Config{FQDN: "example.com"}
	repo := newEntityStoreRepo()
	cache := newFakeEntityCache()
	uc := newEntityCacheUsecase(cfg, repo, cache)
	uri := concrnt.CCURI{Scheme: "cckv", Owner: "con1unknown"}.String()

	for range 2 {
		if _, err := uc.GetEntity(context.Background(), uri); err == nil {
			t.Fatal("expected not found")
		}
	}
	if got := repo.lookupCount(); got != 2 {
		t.Fatalf("lookups = %d, want 2 (not found is not cached)", got)
	}
	if len(cache.items) != 0 {
		t.Fatalf("cache holds %v", cache.items)
	}
}

// Two usecases over one shared cache stand for two replicas: an entity
// commit on one must reach the other's next lookup, not wait for the TTL.
func TestEntityCommitInvalidatesSharedCache(t *testing.T) {
	cfg := &domain.Config{FQDN: "example.com"}
	party := newAckParty(t, remoteEntityHost)
	repo := newEntityStoreRepo()
	cache := newFakeEntityCache()
	writer := newEntityCacheUsecase(cfg, repo, cache)
	reader := newEntityCacheUsecase(cfg, repo, cache)
	ctx := context.Background()
	uri := concrnt.CCURI{Scheme: "cckv", Owner: party.ccid}.String()
	storedAt := time.Now().Add(-time.Hour).UTC().Truncate(time.Second)

	if _, err := writer.Commit(ctx, "127.0.0.1", entityDocAt(t, party, remoteEntityHost, storedAt), domain.CommitModeExecute); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if entity, err := reader.GetEntity(ctx, uri); err != nil || entity.Domain != remoteEntityHost {
		t.Fatalf("GetEntity = %+v, %v", entity, err)
	}
	var cached cachedEntity
	item, err := cache.Get(entityCacheKey(party.ccid))
	if err != nil || json.Unmarshal(item.Value, &cached) != nil || cached.Entity.Domain != remoteEntityHost {
		t.Fatalf("the lookup did not populate the cache: %v %+v", err, cached)
	}

	const moved = "moved.example.net"
	if _, err := writer.Commit(ctx, "127.0.0.1", entityDocAt(t, party, moved, storedAt.Add(time.Minute)), domain.CommitModeExecute); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if entity, err := writer.GetEntity(ctx, uri); err != nil || entity.Domain != moved {
		t.Fatalf("the committing replica still serves %+v, %v", entity, err)
	}
	// the other replica keeps its in-process copy for entityLocalTTL
	if entity, err := reader.GetEntity(ctx, uri); err != nil || entity.Domain != remoteEntityHost {
		t.Fatalf("the reading replica's local copy = %+v, %v", entity, err)
	}
	reader.cache.Flush()
	entity, err := reader.GetEntity(ctx, uri)
	if err != nil {
		t.Fatalf("GetEntity: %v", err)
	}
	if entity.Domain != moved {
		t.Fatalf("domain after the entity commit = %q, want %q", entity.Domain, moved)
	}
}

// The in-process copy answers repeated lookups without memcached.
func TestGetEntityLocalCopySkipsSharedCache(t *testing.T) {
	cfg := &domain.Config{FQDN: "example.com"}
	party := newAckParty(t, remoteEntityHost)
	repo := newEntityStoreRepo()
	cache := newFakeEntityCache()
	uc := newEntityCacheUsecase(cfg, repo, cache)
	ctx := context.Background()
	uri := concrnt.CCURI{Scheme: "cckv", Owner: party.ccid}.String()

	if _, err := uc.Commit(ctx, "127.0.0.1", entityDocAt(t, party, remoteEntityHost, time.Now().Add(-time.Hour).UTC().Truncate(time.Second)), domain.CommitModeExecute); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if _, err := uc.GetEntity(ctx, uri); err != nil {
		t.Fatalf("GetEntity: %v", err)
	}
	if err := cache.Delete(entityCacheKey(party.ccid)); err != nil {
		t.Fatalf("the lookup did not populate the shared cache: %v", err)
	}
	lookups := repo.lookupCount()
	entity, err := uc.GetEntity(ctx, uri)
	if err != nil {
		t.Fatalf("GetEntity: %v", err)
	}
	if entity.Domain != remoteEntityHost || repo.lookupCount() != lookups || len(cache.items) != 0 {
		t.Fatalf("the second lookup left the process: %+v, lookups %d -> %d, shared %v", entity, lookups, repo.lookupCount(), cache.items)
	}
}
