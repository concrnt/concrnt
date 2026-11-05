// Package spanner keeps aggregate entries in a table of the record
// database (aggregate_entries, created by the record store's migrations),
// serialised per key by the read-write transaction: Apply and PutIfAbsent
// read the row under a lock and write it back, so two of them on one key
// abort one another and re-run, which makes the store as strict as the
// Postgres one. Expired rows are dropped by the table's row deletion policy
// (within a few days) and ignored meanwhile.
package spanner

import (
	"context"
	"time"

	"cloud.google.com/go/spanner"
	"google.golang.org/grpc/codes"

	"github.com/concrnt/concrnt/internal/usecase/aggregate"
)

type Store struct {
	client *spanner.Client
}

func New(client *spanner.Client) *Store {
	return &Store{client: client}
}

var columns = []string{"value", "as_of", "deltas", "expires_at"}

type row struct {
	Value     []byte    `spanner:"value"`
	AsOf      time.Time `spanner:"as_of"`
	Deltas    int64     `spanner:"deltas"`
	ExpiresAt time.Time `spanner:"expires_at"`
}

func (r row) entry() *aggregate.Entry {
	return &aggregate.Entry{Value: r.Value, AsOf: r.AsOf.UTC(), Deltas: int(r.Deltas)}
}

func (r row) live(now time.Time) bool {
	return now.Before(r.ExpiresAt)
}

type reader interface {
	ReadRow(ctx context.Context, table string, key spanner.Key, columns []string) (*spanner.Row, error)
}

// read returns the row of key, live or expired; found is false when there
// is none.
func read(ctx context.Context, q reader, key string) (row, bool, error) {
	var r row
	sr, err := q.ReadRow(ctx, "aggregate_entries", spanner.Key{key}, columns)
	if spanner.ErrCode(err) == codes.NotFound {
		return r, false, nil
	}
	if err != nil {
		return r, false, err
	}
	return r, true, sr.ToStruct(&r)
}

func (s *Store) Get(ctx context.Context, key string) (*aggregate.Entry, bool, error) {
	r, found, err := read(ctx, s.client.Single(), key)
	if err != nil || !found || !r.live(time.Now()) {
		return nil, false, err
	}
	return r.entry(), true, nil
}

func (s *Store) PutIfAbsent(ctx context.Context, key string, entry aggregate.Entry, ttl time.Duration) (bool, error) {
	won := false
	_, err := s.client.ReadWriteTransaction(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		won = false
		now := time.Now()
		r, found, err := read(ctx, tx, key)
		if err != nil {
			return err
		}
		if found && r.live(now) {
			return nil
		}
		won = true
		return tx.BufferWrite([]*spanner.Mutation{spanner.InsertOrUpdate("aggregate_entries",
			[]string{"key", "value", "as_of", "deltas", "expires_at"},
			[]any{key, entry.Value, entry.AsOf, int64(0), now.Add(ttl)})})
	})
	if err != nil {
		return false, err
	}
	return won, nil
}

func (s *Store) Apply(ctx context.Context, key string, fn func(old []byte) ([]byte, error)) (*aggregate.Entry, bool, error) {
	var out *aggregate.Entry
	_, err := s.client.ReadWriteTransaction(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		out = nil
		r, found, err := read(ctx, tx, key)
		if err != nil {
			return err
		}
		if !found || !r.live(time.Now()) {
			return nil
		}
		value, err := fn(r.Value)
		if err != nil {
			return err
		}
		r.Value = value
		r.Deltas++
		if err := tx.BufferWrite([]*spanner.Mutation{spanner.Update("aggregate_entries",
			[]string{"key", "value", "deltas"}, []any{key, value, r.Deltas})}); err != nil {
			return err
		}
		out = r.entry()
		return nil
	})
	if err != nil {
		return nil, false, err
	}
	return out, out != nil, nil
}

func (s *Store) Invalidate(ctx context.Context, key string) error {
	_, err := s.client.Apply(ctx, []*spanner.Mutation{spanner.Delete("aggregate_entries", spanner.Key{key})})
	return err
}

func (s *Store) InvalidateDefinition(ctx context.Context, name string) error {
	return s.deleteMatching(ctx, aggregate.Key(name, ""))
}

func (s *Store) InvalidateAll(ctx context.Context) error {
	return s.deleteMatching(ctx, "")
}

// deleteMatching deletes the keys starting with prefix in batches, so
// dropping a large table stays under the mutation limit of one commit.
func (s *Store) deleteMatching(ctx context.Context, prefix string) error {
	for {
		var keys []string
		it := s.client.Single().Query(ctx, spanner.Statement{
			SQL:    "SELECT key FROM aggregate_entries WHERE STARTS_WITH(key, @prefix) LIMIT 1000",
			Params: map[string]any{"prefix": prefix},
		})
		err := it.Do(func(r *spanner.Row) error {
			var key string
			if err := r.Column(0, &key); err != nil {
				return err
			}
			keys = append(keys, key)
			return nil
		})
		if err != nil {
			return err
		}
		if len(keys) == 0 {
			return nil
		}
		ks := make([]spanner.Key, len(keys))
		for i, key := range keys {
			ks[i] = spanner.Key{key}
		}
		if _, err := s.client.Apply(ctx, []*spanner.Mutation{spanner.Delete("aggregate_entries", spanner.KeySetFromKeys(ks...))}); err != nil {
			return err
		}
		if len(keys) < 1000 {
			return nil
		}
	}
}
