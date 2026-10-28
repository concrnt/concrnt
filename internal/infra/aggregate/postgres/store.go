// Package postgres keeps aggregate entries in a table of the record
// database, serialised per key by the row lock.
package postgres

import (
	"context"
	"strings"
	"sync/atomic"
	"time"

	"gorm.io/gorm"

	"github.com/concrnt/concrnt/internal/usecase/aggregate"
)

// Schema is the table DDL; the record store's migrations run it.
const Schema = `CREATE TABLE IF NOT EXISTS aggregate_entries (
	"key" text PRIMARY KEY COLLATE "C",
	value bytea NOT NULL,
	as_of timestamp with time zone NOT NULL,
	deltas integer NOT NULL DEFAULT 0,
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
	Value     []byte
	AsOf      time.Time
	Deltas    int
	ExpiresAt time.Time
}

func (r row) entry() *aggregate.Entry {
	return &aggregate.Entry{Value: r.Value, AsOf: r.AsOf.UTC(), Deltas: r.Deltas}
}

func (s *Store) Get(ctx context.Context, key string) (*aggregate.Entry, bool, error) {
	var r row
	res := s.db.WithContext(ctx).Raw(`SELECT value, as_of, deltas FROM aggregate_entries WHERE "key" = ? AND expires_at > ?`, key, time.Now()).Scan(&r)
	if res.Error != nil || res.RowsAffected == 0 {
		return nil, false, res.Error
	}
	return r.entry(), true, nil
}

func (s *Store) PutIfAbsent(ctx context.Context, key string, entry aggregate.Entry, ttl time.Duration) (bool, error) {
	now := time.Now()
	res := s.db.WithContext(ctx).Exec(`INSERT INTO aggregate_entries ("key", value, as_of, deltas, expires_at) VALUES (?, ?, ?, 0, ?)
		ON CONFLICT ("key") DO UPDATE SET value = excluded.value, as_of = excluded.as_of, deltas = 0, expires_at = excluded.expires_at
		WHERE aggregate_entries.expires_at <= ?`, key, entry.Value, entry.AsOf, now.Add(ttl), now)
	if res.Error != nil {
		return false, res.Error
	}
	if s.writes.Add(1)%sweepEvery == 0 {
		s.db.WithContext(ctx).Exec(`DELETE FROM aggregate_entries WHERE "key" IN (SELECT "key" FROM aggregate_entries WHERE expires_at <= ? LIMIT 1000)`, now)
	}
	return res.RowsAffected > 0, nil
}

func (s *Store) Apply(ctx context.Context, key string, fn func(old []byte) ([]byte, error)) (*aggregate.Entry, bool, error) {
	var out *aggregate.Entry
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var r row
		res := tx.Raw(`SELECT value, as_of, deltas, expires_at FROM aggregate_entries WHERE "key" = ? FOR UPDATE`, key).Scan(&r)
		if res.Error != nil {
			return res.Error
		}
		if res.RowsAffected == 0 || !time.Now().Before(r.ExpiresAt) {
			return nil
		}
		value, err := fn(r.Value)
		if err != nil {
			return err
		}
		if err := tx.Exec(`UPDATE aggregate_entries SET value = ?, deltas = deltas + 1 WHERE "key" = ?`, value, key).Error; err != nil {
			return err
		}
		r.Value = value
		r.Deltas++
		out = r.entry()
		return nil
	})
	if err != nil {
		return nil, false, err
	}
	return out, out != nil, nil
}

func (s *Store) Invalidate(ctx context.Context, key string) error {
	return s.db.WithContext(ctx).Exec(`DELETE FROM aggregate_entries WHERE "key" = ?`, key).Error
}

var likeEscaper = strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`)

func (s *Store) InvalidateAll(ctx context.Context, prefix string) error {
	if prefix == "" {
		return s.db.WithContext(ctx).Exec(`DELETE FROM aggregate_entries`).Error
	}
	return s.db.WithContext(ctx).Exec(`DELETE FROM aggregate_entries WHERE "key" LIKE ? ESCAPE '\'`, likeEscaper.Replace(prefix)+"%").Error
}
