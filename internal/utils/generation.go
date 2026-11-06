package utils

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"math/rand/v2"
	"strconv"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
)

// GenerationCache is the slice of a memcached client the generation keys
// need.
type GenerationCache interface {
	Get(key string) (*memcache.Item, error)
	GetMulti(keys []string) (map[string]*memcache.Item, error)
	Add(item *memcache.Item) error
	Set(item *memcache.Item) error
}

// NewGeneration is unique enough that a recreated generation key never
// brings back the entries cached under an earlier one.
func NewGeneration() string {
	return strconv.FormatInt(time.Now().UnixNano(), 36) + "-" + strconv.FormatUint(rand.Uint64(), 36)
}

// Generations reads the generation under each key, issuing one for a key
// that has none (the first writer wins). A missing generation never falls
// back to a fixed value: entries cached under an earlier, evicted
// generation must not come back.
func Generations(mc GenerationCache, keys []string) (map[string]string, error) {
	items, err := mc.GetMulti(keys)
	if err != nil {
		return nil, err
	}
	gens := make(map[string]string, len(keys))
	for _, key := range keys {
		if item, ok := items[key]; ok {
			gens[key] = string(item.Value)
			continue
		}
		gen := NewGeneration()
		err := mc.Add(&memcache.Item{Key: key, Value: []byte(gen)})
		if errors.Is(err, memcache.ErrNotStored) {
			item, err := mc.Get(key)
			if err != nil {
				return nil, err
			}
			gen = string(item.Value)
		} else if err != nil {
			return nil, err
		}
		gens[key] = gen
	}
	return gens, nil
}

// MoveGeneration issues key a new generation, orphaning everything cached
// under the old one.
func MoveGeneration(mc GenerationCache, key string) error {
	return mc.Set(&memcache.Item{Key: key, Value: []byte(NewGeneration())})
}

// SHA256Hex is the hex SHA-256 of s, the form of a cache key built from a
// URI (memcached keys are at most 250 bytes and may not contain spaces).
func SHA256Hex(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}
