// Package redis keeps aggregate entries as volatile Redis strings; Apply is
// an optimistic WATCH / MULTI loop. A Firestore store (phase 3) follows the
// same shape: a transaction for Apply, a key-range query for InvalidateAll.
package redis

import (
	"context"
	"encoding/json"
	"errors"
	"math/rand/v2"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/concrnt/concrnt/internal/usecase/aggregate"
)

const (
	keyPrefix      = "aggregate:"
	applyAttempts  = 32
	scanBatchCount = 1000
)

type Store struct {
	rdb *redis.Client
}

func New(rdb *redis.Client) *Store {
	return &Store{rdb: rdb}
}

type envelope struct {
	Value  []byte    `json:"v"`
	AsOf   time.Time `json:"asOf"`
	Deltas int       `json:"d"`
}

func (e envelope) entry() *aggregate.Entry {
	return &aggregate.Entry{Value: e.Value, AsOf: e.AsOf, Deltas: e.Deltas}
}

func decode(raw []byte) (envelope, error) {
	var e envelope
	err := json.Unmarshal(raw, &e)
	return e, err
}

func (s *Store) Get(ctx context.Context, key string) (*aggregate.Entry, bool, error) {
	raw, err := s.rdb.Get(ctx, keyPrefix+key).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	e, err := decode(raw)
	if err != nil {
		return nil, false, err
	}
	return e.entry(), true, nil
}

func (s *Store) PutIfAbsent(ctx context.Context, key string, entry aggregate.Entry, ttl time.Duration) (bool, error) {
	raw, err := json.Marshal(envelope{Value: entry.Value, AsOf: entry.AsOf, Deltas: 0})
	if err != nil {
		return false, err
	}
	return s.rdb.SetNX(ctx, keyPrefix+key, raw, ttl).Result()
}

func (s *Store) Apply(ctx context.Context, key string, fn func(old []byte) ([]byte, error)) (*aggregate.Entry, bool, error) {
	k := keyPrefix + key
	for attempt := range applyAttempts {
		if attempt > 0 {
			// WATCH loses to every concurrent writer of the key; a jittered
			// pause spreads a burst of writers out
			time.Sleep(time.Duration(rand.IntN(attempt*500)+100) * time.Microsecond)
		}
		var out *aggregate.Entry
		err := s.rdb.Watch(ctx, func(tx *redis.Tx) error {
			raw, err := tx.Get(ctx, k).Bytes()
			if errors.Is(err, redis.Nil) {
				return nil
			}
			if err != nil {
				return err
			}
			e, err := decode(raw)
			if err != nil {
				return err
			}
			e.Value, err = fn(e.Value)
			if err != nil {
				return err
			}
			e.Deltas++
			next, err := json.Marshal(e)
			if err != nil {
				return err
			}
			_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
				pipe.Set(ctx, k, next, redis.KeepTTL)
				return nil
			})
			if err == nil {
				out = e.entry()
			}
			return err
		}, k)
		if errors.Is(err, redis.TxFailedErr) {
			continue
		}
		return out, out != nil, err
	}
	return nil, false, errors.New("aggregate redis: apply lost the race too many times")
}

func (s *Store) Invalidate(ctx context.Context, key string) error {
	return s.rdb.Del(ctx, keyPrefix+key).Err()
}

var globEscaper = strings.NewReplacer(`\`, `\\`, `*`, `\*`, `?`, `\?`, `[`, `\[`, `]`, `\]`)

func (s *Store) InvalidateAll(ctx context.Context, prefix string) error {
	pattern := globEscaper.Replace(keyPrefix+prefix) + "*"
	var cursor uint64
	for {
		keys, next, err := s.rdb.Scan(ctx, cursor, pattern, scanBatchCount).Result()
		if err != nil {
			return err
		}
		if len(keys) > 0 {
			if err := s.rdb.Unlink(ctx, keys...).Err(); err != nil {
				return err
			}
		}
		if next == 0 {
			return nil
		}
		cursor = next
	}
}
