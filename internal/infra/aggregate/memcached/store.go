// Package memcached keeps aggregate entries in memcached. Apply is a gets /
// cas loop, like the WATCH / MULTI loop of the Redis store.
//
// memcached cannot list its keys, so InvalidateDefinition and InvalidateAll
// work through generation numbers instead of deleting: every stored key
// embeds the current generation of the whole store and of its definition,
// and dropping moves one of them on, which leaves the old entries
// unreachable until they expire or are evicted.
//
// Each process reads the generations at most every genCacheTTL, so after a
// drop other processes keep serving the old entries for up to that long. A generation key that is evicted comes back with a new value, which
// drops its entries too: the store only errs towards recomputing.
package memcached

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"strconv"
	"sync"
	"time"

	"github.com/bradfitz/gomemcache/memcache"

	"github.com/concrnt/concrnt/internal/usecase/aggregate"
)

const (
	applyAttempts = 32
	putAttempts   = 4
	genCacheTTL   = 5 * time.Second
	// memcached reads an expiration above 30 days as a unix timestamp
	maxRelativeExpiration = 30 * 24 * time.Hour
)

type Store struct {
	mc *memcache.Client
	ns string

	mu   sync.Mutex
	gens map[string]cachedGen // generation key -> value
}

type cachedGen struct {
	value   string
	fetched time.Time
}

// New stores the entries in mc under keys starting with namespace (which
// lets several stores share one memcached).
func New(mc *memcache.Client, namespace string) *Store {
	return &Store{mc: mc, ns: namespace + "agg:", gens: map[string]cachedGen{}}
}

// envelope is the stored item. ExpiresAt is kept alongside the memcached
// expiration because memcached only counts whole seconds and a cas has to
// restate the expiration it keeps.
type envelope struct {
	Value     []byte    `json:"v"`
	AsOf      time.Time `json:"asOf"`
	Deltas    int       `json:"d"`
	ExpiresAt time.Time `json:"exp"`
}

func (e envelope) live(now time.Time) bool {
	return now.Before(e.ExpiresAt)
}

func (e envelope) entry() *aggregate.Entry {
	return &aggregate.Entry{Value: e.Value, AsOf: e.AsOf, Deltas: e.Deltas}
}

func decode(raw []byte) (envelope, error) {
	var e envelope
	err := json.Unmarshal(raw, &e)
	return e, err
}

// expiration is the memcached expiration of an item that lives until at.
func expiration(at, now time.Time) int32 {
	d := at.Sub(now)
	if d > maxRelativeExpiration {
		return int32(at.Unix())
	}
	secs := int32((d + time.Second - 1) / time.Second)
	if secs < 1 {
		secs = 1
	}
	return secs
}

func hash(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

// Keys are hashed: aggregate keys may be longer than memcached's 250 bytes
// and contain spaces.
func (s *Store) genKey(scope string) string {
	return s.ns + "gen:" + hash(scope)[:32]
}

func (s *Store) globalGenKey() string { return s.genKey("") }

func (s *Store) defGenKey(name string) string { return s.genKey("def:" + name) }

// itemKey is where key is stored under the current generations.
func (s *Store) itemKey(key string) (string, error) {
	gk, dk := s.globalGenKey(), s.defGenKey(aggregate.NameOf(key))
	gens, err := s.generations(gk, dk)
	if err != nil {
		return "", err
	}
	return s.ns + "e:" + hash(gens[gk]+"\x00"+gens[dk]+"\x00"+key), nil
}

// generations returns the values of the generation keys, from the process
// cache when fresh, creating a key that does not exist yet.
func (s *Store) generations(keys ...string) (map[string]string, error) {
	now := time.Now()
	out := make(map[string]string, len(keys))
	var stale []string
	s.mu.Lock()
	for _, k := range keys {
		if g, ok := s.gens[k]; ok && now.Sub(g.fetched) < genCacheTTL {
			out[k] = g.value
		} else {
			stale = append(stale, k)
		}
	}
	s.mu.Unlock()
	if len(stale) == 0 {
		return out, nil
	}

	items, err := s.mc.GetMulti(stale)
	if err != nil {
		return nil, fmt.Errorf("aggregate memcached: read generations: %w", err)
	}
	for _, k := range stale {
		if it, ok := items[k]; ok {
			out[k] = string(it.Value)
			continue
		}
		v, err := s.createGeneration(k)
		if err != nil {
			return nil, err
		}
		out[k] = v
	}

	s.mu.Lock()
	for _, k := range stale {
		s.gens[k] = cachedGen{value: out[k], fetched: now}
	}
	s.mu.Unlock()
	return out, nil
}

// newGeneration is unique enough that a recreated generation key never
// brings back the entries of an earlier one.
func newGeneration() string {
	return strconv.FormatInt(time.Now().UnixNano(), 36) + "-" + strconv.FormatUint(rand.Uint64(), 36)
}

func (s *Store) createGeneration(key string) (string, error) {
	v := newGeneration()
	err := s.mc.Add(&memcache.Item{Key: key, Value: []byte(v)})
	if err == nil {
		return v, nil
	}
	if !errors.Is(err, memcache.ErrNotStored) {
		return "", fmt.Errorf("aggregate memcached: create generation: %w", err)
	}
	// another process created it first
	it, err := s.mc.Get(key)
	if err != nil {
		return "", fmt.Errorf("aggregate memcached: read generation: %w", err)
	}
	return string(it.Value), nil
}

func (s *Store) Get(_ context.Context, key string) (*aggregate.Entry, bool, error) {
	k, err := s.itemKey(key)
	if err != nil {
		return nil, false, err
	}
	it, err := s.mc.Get(k)
	if errors.Is(err, memcache.ErrCacheMiss) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	e, err := decode(it.Value)
	if err != nil {
		return nil, false, err
	}
	if !e.live(time.Now()) {
		return nil, false, nil
	}
	return e.entry(), true, nil
}

func (s *Store) PutIfAbsent(_ context.Context, key string, entry aggregate.Entry, ttl time.Duration) (bool, error) {
	k, err := s.itemKey(key)
	if err != nil {
		return false, err
	}
	now := time.Now()
	e := envelope{Value: entry.Value, AsOf: entry.AsOf, Deltas: 0, ExpiresAt: now.Add(ttl)}
	raw, err := json.Marshal(e)
	if err != nil {
		return false, err
	}
	item := &memcache.Item{Key: k, Value: raw, Expiration: expiration(e.ExpiresAt, now)}

	for range putAttempts {
		err := s.mc.Add(item)
		if err == nil {
			return true, nil
		}
		if !errors.Is(err, memcache.ErrNotStored) {
			return false, err
		}
		// memcached rounds expirations up to whole seconds, so the item in
		// the way may already be past its own ExpiresAt: replace it then
		cur, err := s.mc.Get(k)
		if errors.Is(err, memcache.ErrCacheMiss) {
			continue
		}
		if err != nil {
			return false, err
		}
		old, err := decode(cur.Value)
		if err == nil && old.live(time.Now()) {
			return false, nil
		}
		item.CasID = cur.CasID
		err = s.mc.CompareAndSwap(item)
		switch {
		case err == nil:
			return true, nil
		case errors.Is(err, memcache.ErrCASConflict):
			// someone else stored it in the meantime
			return false, nil
		case errors.Is(err, memcache.ErrNotStored):
			continue
		default:
			return false, err
		}
	}
	return false, nil
}

func (s *Store) Apply(_ context.Context, key string, fn func(old []byte) ([]byte, error)) (*aggregate.Entry, bool, error) {
	k, err := s.itemKey(key)
	if err != nil {
		return nil, false, err
	}
	for attempt := range applyAttempts {
		if attempt > 0 {
			// a cas loses to every concurrent writer of the key; a jittered
			// pause spreads a burst of writers out
			time.Sleep(time.Duration(rand.IntN(attempt*500)+100) * time.Microsecond)
		}
		it, err := s.mc.Get(k)
		if errors.Is(err, memcache.ErrCacheMiss) {
			return nil, false, nil
		}
		if err != nil {
			return nil, false, err
		}
		e, err := decode(it.Value)
		if err != nil {
			return nil, false, err
		}
		now := time.Now()
		if !e.live(now) {
			return nil, false, nil
		}
		e.Value, err = fn(e.Value)
		if err != nil {
			return nil, false, err
		}
		e.Deltas++
		it.Value, err = json.Marshal(e)
		if err != nil {
			return nil, false, err
		}
		it.Expiration = expiration(e.ExpiresAt, now)
		err = s.mc.CompareAndSwap(it)
		switch {
		case err == nil:
			return e.entry(), true, nil
		case errors.Is(err, memcache.ErrCASConflict):
			continue
		case errors.Is(err, memcache.ErrNotStored):
			// evicted or dropped since the read
			return nil, false, nil
		default:
			return nil, false, err
		}
	}
	return nil, false, errors.New("aggregate memcached: apply lost the race too many times")
}

func (s *Store) Invalidate(_ context.Context, key string) error {
	k, err := s.itemKey(key)
	if err != nil {
		return err
	}
	if err := s.mc.Delete(k); err != nil && !errors.Is(err, memcache.ErrCacheMiss) {
		return err
	}
	return nil
}

func (s *Store) InvalidateDefinition(_ context.Context, name string) error {
	return s.moveGeneration(s.defGenKey(name))
}

func (s *Store) InvalidateAll(_ context.Context) error {
	return s.moveGeneration(s.globalGenKey())
}

func (s *Store) moveGeneration(gk string) error {
	if err := s.mc.Set(&memcache.Item{Key: gk, Value: []byte(newGeneration())}); err != nil {
		return fmt.Errorf("aggregate memcached: move generation: %w", err)
	}
	s.mu.Lock()
	delete(s.gens, gk)
	s.mu.Unlock()
	return nil
}
