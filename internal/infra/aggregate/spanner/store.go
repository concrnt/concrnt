// Package spanner keeps aggregate entries in two tables of the record
// database (created by the record store's migrations): aggregate_entries
// holds an entry's counters as computed, and aggregate_deltas, interleaved
// in it, one row per Add.
//
// Spanner has no blind increment: any statement that adds to a stored
// value reads the row first, and the writers of one hot key then wait on
// each other's locks. An Add is therefore an insert of a delta row with a
// random id, a write that reads nothing; the parent row it needs to exist
// is the "live entry" check, since a child cannot be inserted without one.
// Get reads the entry with its delta rows in one query and folds them, and
// once they are more than compactAfter it folds them into the entry row for
// good. Expired entries are dropped with their deltas by the row deletion
// policy (within a few days) and ignored meanwhile; an Add cannot see the
// expiry and lands on such an entry, which PutIfAbsent then replaces whole.
package spanner

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/google/uuid"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"

	"github.com/concrnt/concrnt/internal/usecase/aggregate"
)

const (
	tableEntries = "aggregate_entries"
	tableDeltas  = "aggregate_deltas"

	// compactAfter is how many delta rows a Get tolerates before it folds
	// them into the entry row; compactBatch bounds one such fold.
	compactAfter = 64
	compactBatch = 1000
)

type Store struct {
	client *spanner.Client
}

func New(client *spanner.Client) *Store {
	return &Store{client: client}
}

// value is the JSON of an entry row's counters.
type value struct {
	Counts  map[string]int64 `json:"c,omitempty"`
	Minima  map[string]int64 `json:"m,omitempty"`
	Members []string         `json:"s,omitempty"`
}

// deltaValue is the JSON of a delta row.
type deltaValue struct {
	Inc        map[string]int64 `json:"i,omitempty"`
	Min        map[string]int64 `json:"m,omitempty"`
	AddMembers []string         `json:"s,omitempty"`
}

func (d deltaValue) delta() aggregate.Delta {
	return aggregate.Delta{Inc: d.Inc, Min: d.Min, AddMembers: d.AddMembers}
}

var entryColumns = []string{"value", "as_of", "deltas", "expires_at"}

// entryRow is an entry as stored: deltas counts the delta rows already
// folded into value.
type entryRow struct {
	Value     []byte    `spanner:"value"`
	AsOf      time.Time `spanner:"as_of"`
	Deltas    int64     `spanner:"deltas"`
	ExpiresAt time.Time `spanner:"expires_at"`
}

func (r entryRow) counters() (aggregate.Counters, error) {
	var v value
	if err := json.Unmarshal(r.Value, &v); err != nil {
		return aggregate.Counters{}, err
	}
	return aggregate.Counters{Counts: v.Counts, Minima: v.Minima, Members: v.Members, AsOf: r.AsOf.UTC(), Deltas: r.Deltas}, nil
}

func (r entryRow) live(now time.Time) bool {
	return now.Before(r.ExpiresAt)
}

type deltaRow struct {
	ID    string `spanner:"id"`
	Delta []byte `spanner:"delta"`
}

// fold adds the delta rows to c.
func fold(c *aggregate.Counters, rows []*deltaRow) error {
	for _, r := range rows {
		var d deltaValue
		if err := json.Unmarshal(r.Delta, &d); err != nil {
			return err
		}
		c.Add(d.delta())
	}
	return nil
}

// getStatement reads an entry and its delta rows in one round trip.
const getStatement = `SELECT e.value, e.as_of, e.deltas, e.expires_at,
		ARRAY(SELECT AS STRUCT d.id, d.delta FROM aggregate_deltas d WHERE d.key = e.key) AS pending
	FROM aggregate_entries e WHERE e.key = @key`

func (s *Store) Get(ctx context.Context, key string) (*aggregate.Counters, bool, error) {
	it := s.client.Single().Query(ctx, spanner.Statement{SQL: getStatement, Params: map[string]any{"key": key}})
	defer it.Stop()
	sr, err := it.Next()
	if err == iterator.Done {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	var r entryRow
	var pending []*deltaRow
	if err := sr.Columns(&r.Value, &r.AsOf, &r.Deltas, &r.ExpiresAt, &pending); err != nil {
		return nil, false, err
	}
	if !r.live(time.Now()) {
		return nil, false, nil
	}
	out, err := r.counters()
	if err != nil {
		return nil, false, err
	}
	if err := fold(&out, pending); err != nil {
		return nil, false, err
	}
	if len(pending) > compactAfter {
		// the entry is already read: a fold that fails is the next
		// reader's to do
		if err := s.compact(ctx, key, pending); err != nil {
			slog.Warn("aggregate spanner: compaction failed", slog.String("key", key), slog.String("error", err.Error()))
		}
	}
	return &out, true, nil
}

// compact folds delta rows into their entry row and deletes them. The rows
// are read again by key inside the transaction, so a row another reader has
// folded meanwhile, or one of an entry that was replaced, is not found and
// not folded twice. Point reads lock those rows only: an Add inserts a new
// id and does not wait for the fold.
func (s *Store) compact(ctx context.Context, key string, pending []*deltaRow) error {
	if len(pending) > compactBatch {
		pending = pending[:compactBatch]
	}
	keys := make([]spanner.Key, len(pending))
	for i, d := range pending {
		keys[i] = spanner.Key{key, d.ID}
	}
	_, err := s.client.ReadWriteTransaction(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		r, found, err := readEntry(ctx, tx, key)
		if err != nil || !found {
			return err
		}
		var rows []*deltaRow
		err = tx.Read(ctx, tableDeltas, spanner.KeySetFromKeys(keys...), []string{"id", "delta"}).Do(func(sr *spanner.Row) error {
			var d deltaRow
			if err := sr.ToStruct(&d); err != nil {
				return err
			}
			rows = append(rows, &d)
			return nil
		})
		if err != nil || len(rows) == 0 {
			return err
		}
		c, err := r.counters()
		if err != nil {
			return err
		}
		if err := fold(&c, rows); err != nil {
			return err
		}
		encoded, err := json.Marshal(value{Counts: c.Counts, Minima: c.Minima, Members: c.Members})
		if err != nil {
			return err
		}
		folded := make([]spanner.Key, len(rows))
		for i, d := range rows {
			folded[i] = spanner.Key{key, d.ID}
		}
		return tx.BufferWrite([]*spanner.Mutation{
			spanner.Update(tableEntries, []string{"key", "value", "deltas"}, []any{key, encoded, c.Deltas}),
			spanner.Delete(tableDeltas, spanner.KeySetFromKeys(folded...)),
		})
	})
	return err
}

// readEntry returns the entry row of key, live or expired; found is false
// when there is none.
func readEntry(ctx context.Context, tx *spanner.ReadWriteTransaction, key string) (entryRow, bool, error) {
	var r entryRow
	sr, err := tx.ReadRow(ctx, tableEntries, spanner.Key{key}, entryColumns)
	if spanner.ErrCode(err) == codes.NotFound {
		return r, false, nil
	}
	if err != nil {
		return r, false, err
	}
	return r, true, sr.ToStruct(&r)
}

func (s *Store) PutIfAbsent(ctx context.Context, key string, entry aggregate.Counters, ttl time.Duration) (bool, error) {
	encoded, err := json.Marshal(value{Counts: entry.Counts, Minima: entry.Minima, Members: entry.Members})
	if err != nil {
		return false, err
	}
	won := false
	_, err = s.client.ReadWriteTransaction(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		won = false
		now := time.Now()
		r, found, err := readEntry(ctx, tx, key)
		if err != nil {
			return err
		}
		if found && r.live(now) {
			return nil
		}
		won = true
		// the delete takes the delta rows of an expired entry with it
		return tx.BufferWrite([]*spanner.Mutation{
			spanner.Delete(tableEntries, spanner.Key{key}),
			spanner.Insert(tableEntries,
				[]string{"key", "value", "as_of", "deltas", "expires_at"},
				[]any{key, encoded, entry.AsOf, int64(0), now.Add(ttl)}),
		})
	})
	if err != nil {
		return false, err
	}
	return won, nil
}

func (s *Store) Add(ctx context.Context, key string, delta aggregate.Delta) (bool, error) {
	encoded, err := json.Marshal(deltaValue{Inc: delta.Inc, Min: delta.Min, AddMembers: delta.AddMembers})
	if err != nil {
		return false, err
	}
	// at-least-once is a single Commit call; the replay of an insert that
	// did land finds its own random id and is told AlreadyExists
	_, err = s.client.Apply(ctx, []*spanner.Mutation{spanner.Insert(tableDeltas,
		[]string{"key", "id", "delta"}, []any{key, uuid.NewString(), encoded})}, spanner.ApplyAtLeastOnce())
	switch spanner.ErrCode(err) {
	case codes.OK, codes.AlreadyExists:
		return true, nil
	case codes.NotFound:
		// no parent row: there is no entry to add to
		return false, nil
	default:
		return false, err
	}
}

func (s *Store) Invalidate(ctx context.Context, key string) error {
	_, err := s.client.Apply(ctx, []*spanner.Mutation{spanner.Delete(tableEntries, spanner.Key{key})})
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
		if _, err := s.client.Apply(ctx, []*spanner.Mutation{spanner.Delete(tableEntries, spanner.KeySetFromKeys(ks...))}); err != nil {
			return err
		}
		if len(keys) < 1000 {
			return nil
		}
	}
}
