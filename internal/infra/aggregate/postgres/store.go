// Package postgres keeps aggregate entries in a table of the record
// database, one row per entry with its counters as jsonb. Add is a single
// UPDATE that computes the new values from the row it locks: writers of one
// key queue on the row lock for the length of that statement and none of
// them reads the entry first or retries.
package postgres

import (
	"context"
	"encoding/json"
	"strings"
	"sync/atomic"
	"time"

	"gorm.io/gorm"

	"github.com/concrnt/concrnt/internal/usecase/aggregate"
)

// Schema is the table DDL; the record store's migrations run it. counts and
// minima are objects of name -> integer, members an array of strings.
const Schema = `CREATE TABLE IF NOT EXISTS aggregate_entries (
	"key" text PRIMARY KEY COLLATE "C",
	counts jsonb NOT NULL,
	minima jsonb NOT NULL,
	members jsonb NOT NULL,
	as_of timestamp with time zone NOT NULL,
	deltas bigint NOT NULL DEFAULT 0,
	expires_at timestamp with time zone NOT NULL
)`

// sweepEvery bounds how many expired rows pile up: every sweepEvery writes
// one batch of expired rows is deleted.
const sweepEvery = 1000

type Store struct {
	db     *gorm.DB
	writes atomic.Int64
}

func New(db *gorm.DB) *Store {
	return &Store{db: db}
}

type row struct {
	Counts  []byte
	Minima  []byte
	Members []byte
	AsOf    time.Time
	Deltas  int64
}

func (r row) counters() (*aggregate.Counters, error) {
	out := &aggregate.Counters{AsOf: r.AsOf.UTC(), Deltas: r.Deltas}
	if err := json.Unmarshal(r.Counts, &out.Counts); err != nil {
		return nil, err
	}
	if err := json.Unmarshal(r.Minima, &out.Minima); err != nil {
		return nil, err
	}
	if err := json.Unmarshal(r.Members, &out.Members); err != nil {
		return nil, err
	}
	return out, nil
}

// encode renders the three jsonb parameters; nil maps and slices become
// empty containers, not null.
func encode(counts, minima map[string]int64, members []string) (c, m, s string, err error) {
	if counts == nil {
		counts = map[string]int64{}
	}
	if minima == nil {
		minima = map[string]int64{}
	}
	if members == nil {
		members = []string{}
	}
	var raw [3][]byte
	for i, v := range []any{counts, minima, members} {
		if raw[i], err = json.Marshal(v); err != nil {
			return "", "", "", err
		}
	}
	return string(raw[0]), string(raw[1]), string(raw[2]), nil
}

func (s *Store) Get(ctx context.Context, key string) (*aggregate.Counters, bool, error) {
	var r row
	res := s.db.WithContext(ctx).Raw(`SELECT counts, minima, members, as_of, deltas FROM aggregate_entries WHERE "key" = ? AND expires_at > ?`, key, time.Now()).Scan(&r)
	if res.Error != nil || res.RowsAffected == 0 {
		return nil, false, res.Error
	}
	out, err := r.counters()
	if err != nil {
		return nil, false, err
	}
	return out, true, nil
}

func (s *Store) PutIfAbsent(ctx context.Context, key string, entry aggregate.Counters, ttl time.Duration) (bool, error) {
	counts, minima, members, err := encode(entry.Counts, entry.Minima, entry.Members)
	if err != nil {
		return false, err
	}
	now := time.Now()
	res := s.db.WithContext(ctx).Exec(`INSERT INTO aggregate_entries ("key", counts, minima, members, as_of, deltas, expires_at) VALUES (?, ?::jsonb, ?::jsonb, ?::jsonb, ?, 0, ?)
		ON CONFLICT ("key") DO UPDATE SET counts = excluded.counts, minima = excluded.minima, members = excluded.members, as_of = excluded.as_of, deltas = 0, expires_at = excluded.expires_at
		WHERE aggregate_entries.expires_at <= ?`, key, counts, minima, members, entry.AsOf, now.Add(ttl), now)
	if res.Error != nil {
		return false, res.Error
	}
	if s.writes.Add(1)%sweepEvery == 0 {
		s.db.WithContext(ctx).Exec(`DELETE FROM aggregate_entries WHERE "key" IN (SELECT "key" FROM aggregate_entries WHERE expires_at <= ? LIMIT 1000)`, now)
	}
	return res.RowsAffected > 0, nil
}

// addStatement folds the delta (jsonb parameters shaped like the columns,
// the members given twice) into the live row of a key. Each column is
// merged with what the delta changes of it: || overwrites the touched
// counters and leaves the rest, LEAST skips the NULL of a minimum that is
// not there yet, and members is passed through as it is when it already
// holds every added member, which spares rewriting a large set on every
// write.
const addStatement = `UPDATE aggregate_entries e SET
		counts = e.counts || (
			SELECT COALESCE(jsonb_object_agg(d.key, COALESCE((e.counts ->> d.key)::bigint, 0) + d.value::bigint), '{}'::jsonb)
			FROM jsonb_each_text(?::jsonb) d),
		minima = e.minima || (
			SELECT COALESCE(jsonb_object_agg(d.key, LEAST((e.minima ->> d.key)::bigint, d.value::bigint)), '{}'::jsonb)
			FROM jsonb_each_text(?::jsonb) d),
		members = CASE WHEN e.members @> ?::jsonb THEN e.members ELSE e.members || (
			SELECT jsonb_agg(DISTINCT d.value)
			FROM jsonb_array_elements(?::jsonb) d WHERE NOT e.members @> d.value) END,
		deltas = e.deltas + 1
	WHERE e."key" = ? AND e.expires_at > ?`

func (s *Store) Add(ctx context.Context, key string, delta aggregate.Delta) (bool, error) {
	inc, min, members, err := encode(delta.Inc, delta.Min, delta.AddMembers)
	if err != nil {
		return false, err
	}
	res := s.db.WithContext(ctx).Exec(addStatement, inc, min, members, members, key, time.Now())
	return res.RowsAffected > 0, res.Error
}

func (s *Store) Invalidate(ctx context.Context, key string) error {
	return s.db.WithContext(ctx).Exec(`DELETE FROM aggregate_entries WHERE "key" = ?`, key).Error
}

var likeEscaper = strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`)

// InvalidateDefinition deletes the key range of the definition (the "C"
// collation lets LIKE with a fixed prefix use the primary key).
func (s *Store) InvalidateDefinition(ctx context.Context, name string) error {
	return s.db.WithContext(ctx).Exec(`DELETE FROM aggregate_entries WHERE "key" LIKE ? ESCAPE '\'`, likeEscaper.Replace(aggregate.Key(name, ""))+"%").Error
}

func (s *Store) InvalidateAll(ctx context.Context) error {
	return s.db.WithContext(ctx).Exec(`DELETE FROM aggregate_entries`).Error
}
