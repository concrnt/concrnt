// Package redis keeps an aggregate entry as one volatile Redis hash: the
// counters, minima and members are fields of it, so Add is a single Lua
// script of HINCRBY / HSET calls that never reads the entry back to the
// client and needs no retry. InvalidateDefinition / InvalidateAll are a
// SCAN over the key pattern.
package redis

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/concrnt/concrnt/internal/usecase/aggregate"
)

const (
	// the prefix is not the one of the string-valued entries this store
	// kept before: those expire on their own and must not be read as hashes
	keyPrefix      = "aggregate:v2:"
	scanBatchCount = 1000
)

// Hash fields. The first byte tells what the rest names, so arbitrary
// counter names and members cannot collide with one another or with the
// two fields of the entry itself.
const (
	fieldAsOf     = "a" // unix nanoseconds
	fieldDeltas   = "d"
	prefixCount   = "c:"
	prefixMinimum = "m:"
	prefixMember  = "s:"
)

// putScript stores a new hash unless the key exists.
// KEYS[1] the entry; ARGV[1] ttl in milliseconds, then field, value pairs.
var putScript = redis.NewScript(`
if redis.call('EXISTS', KEYS[1]) == 1 then
	return 0
end
redis.call('HSET', KEYS[1], unpack(ARGV, 2))
redis.call('PEXPIRE', KEYS[1], ARGV[1])
return 1
`)

// addScript folds a delta into an existing hash.
// KEYS[1] the entry; ARGV: the number of increments, then field, n pairs;
// the number of minima, then field, value pairs; the member fields.
// Minima are unix nanoseconds, beyond what a Lua number holds exactly, so
// they are compared as decimal strings (canonical: strconv.FormatInt).
var addScript = redis.NewScript(`
if redis.call('EXISTS', KEYS[1]) == 0 then
	return 0
end
local function less(a, b)
	local an, bn = string.sub(a, 1, 1) == '-', string.sub(b, 1, 1) == '-'
	if an ~= bn then
		return an
	end
	if an then
		a, b = string.sub(b, 2), string.sub(a, 2)
	end
	if #a ~= #b then
		return #a < #b
	end
	return a < b
end
local i = 1
local incs = tonumber(ARGV[i])
i = i + 1
for _ = 1, incs do
	redis.call('HINCRBY', KEYS[1], ARGV[i], ARGV[i + 1])
	i = i + 2
end
local mins = tonumber(ARGV[i])
i = i + 1
for _ = 1, mins do
	local cur = redis.call('HGET', KEYS[1], ARGV[i])
	if not cur or less(ARGV[i + 1], cur) then
		redis.call('HSET', KEYS[1], ARGV[i], ARGV[i + 1])
	end
	i = i + 2
end
while i <= #ARGV do
	redis.call('HSETNX', KEYS[1], ARGV[i], '1')
	i = i + 1
end
redis.call('HINCRBY', KEYS[1], 'd', 1)
return 1
`)

type Store struct {
	rdb *redis.Client
}

func New(rdb *redis.Client) *Store {
	return &Store{rdb: rdb}
}

func (s *Store) Get(ctx context.Context, key string) (*aggregate.Counters, bool, error) {
	fields, err := s.rdb.HGetAll(ctx, keyPrefix+key).Result()
	if err != nil {
		return nil, false, err
	}
	if len(fields) == 0 {
		return nil, false, nil
	}
	out := &aggregate.Counters{Counts: map[string]int64{}, Minima: map[string]int64{}}
	for field, value := range fields {
		if name, ok := strings.CutPrefix(field, prefixMember); ok {
			out.Members = append(out.Members, name)
			continue
		}
		n, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return nil, false, fmt.Errorf("aggregate redis: field %q of %q: %w", field, key, err)
		}
		switch {
		case field == fieldAsOf:
			out.AsOf = time.Unix(0, n).UTC()
		case field == fieldDeltas:
			out.Deltas = n
		case strings.HasPrefix(field, prefixCount):
			out.Counts[field[len(prefixCount):]] = n
		case strings.HasPrefix(field, prefixMinimum):
			out.Minima[field[len(prefixMinimum):]] = n
		}
	}
	return out, true, nil
}

func (s *Store) PutIfAbsent(ctx context.Context, key string, entry aggregate.Counters, ttl time.Duration) (bool, error) {
	args := make([]any, 0, 5+2*(len(entry.Counts)+len(entry.Minima)+len(entry.Members)))
	args = append(args, ttl.Milliseconds(), fieldAsOf, entry.AsOf.UnixNano(), fieldDeltas, 0)
	for name, n := range entry.Counts {
		args = append(args, prefixCount+name, n)
	}
	for name, n := range entry.Minima {
		args = append(args, prefixMinimum+name, n)
	}
	for _, member := range entry.Members {
		args = append(args, prefixMember+member, 1)
	}
	return putScript.Run(ctx, s.rdb, []string{keyPrefix + key}, args...).Bool()
}

func (s *Store) Add(ctx context.Context, key string, delta aggregate.Delta) (bool, error) {
	args := make([]any, 0, 2+2*(len(delta.Inc)+len(delta.Min))+len(delta.AddMembers))
	args = append(args, len(delta.Inc))
	for name, n := range delta.Inc {
		args = append(args, prefixCount+name, n)
	}
	args = append(args, len(delta.Min))
	for name, n := range delta.Min {
		args = append(args, prefixMinimum+name, n)
	}
	for _, member := range delta.AddMembers {
		args = append(args, prefixMember+member)
	}
	return addScript.Run(ctx, s.rdb, []string{keyPrefix + key}, args...).Bool()
}

func (s *Store) Invalidate(ctx context.Context, key string) error {
	return s.rdb.Del(ctx, keyPrefix+key).Err()
}

var globEscaper = strings.NewReplacer(`\`, `\\`, `*`, `\*`, `?`, `\?`, `[`, `\[`, `]`, `\]`)

func (s *Store) InvalidateDefinition(ctx context.Context, name string) error {
	return s.unlinkMatching(ctx, globEscaper.Replace(keyPrefix+aggregate.Key(name, ""))+"*")
}

func (s *Store) InvalidateAll(ctx context.Context) error {
	return s.unlinkMatching(ctx, globEscaper.Replace(keyPrefix)+"*")
}

func (s *Store) unlinkMatching(ctx context.Context, pattern string) error {
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
