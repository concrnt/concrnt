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
// once they are more than compactAfter it replaces them by one delta row
// holding their sum. Expired entries are dropped with their deltas by the
// row deletion policy (within a few days) and ignored meanwhile; an Add
// cannot see the expiry and lands on such an entry, which PutIfAbsent then
// replaces whole.
//
// No write on the path of a request reads in a transaction. A transaction
// that reads a row and then writes it holds a shared lock it has to turn
// into an exclusive one, and when several do so on one row (the readers of
// a hot key all finding it due for a fold, or all finding it missing) all
// but one are ABORTED and re-run, each waiting on the others' locks
// meanwhile. So the entry row is written once, by a plain insert that fails
// when the row is there, and never updated; the fold is a single commit of
// mutations that fails as a whole when another fold got to one of its rows
// first. The one read-write transaction left replaces an entry that has
// expired and is not collected yet.
package spanner

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
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
	// them into one; compactBatch bounds one such fold.
	compactAfter = 64
	compactBatch = 1000
)

type Store struct {
	client *spanner.Client
	// compacting holds the keys this process is folding right now: the
	// readers that find the same rows meanwhile leave them to that fold.
	compacting sync.Map
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

// deltaValue is the JSON of a delta row: one Add, or the sum of several.
type deltaValue struct {
	Inc        map[string]int64 `json:"i,omitempty"`
	Min        map[string]int64 `json:"m,omitempty"`
	AddMembers []string         `json:"s,omitempty"`
	// Folded is how many Adds the row stands for when it is the sum of a
	// fold; a row written by Add leaves it out.
	Folded int64 `json:"n,omitempty"`
}

// entryRow is an entry as computed; the Adds since are its delta rows.
type entryRow struct {
	Value     []byte    `spanner:"value"`
	AsOf      time.Time `spanner:"as_of"`
	ExpiresAt time.Time `spanner:"expires_at"`
}

func (r entryRow) counters() (aggregate.Counters, error) {
	var v value
	if err := json.Unmarshal(r.Value, &v); err != nil {
		return aggregate.Counters{}, err
	}
	return aggregate.Counters{Counts: v.Counts, Minima: v.Minima, Members: v.Members, AsOf: r.AsOf.UTC()}, nil
}

func (r entryRow) live(now time.Time) bool {
	return now.Before(r.ExpiresAt)
}

type deltaRow struct {
	ID    string `spanner:"id"`
	Delta []byte `spanner:"delta"`
}

// fold adds the delta rows to c, each counting for the Adds it stands for.
func fold(c *aggregate.Counters, rows []*deltaRow) error {
	for _, r := range rows {
		var d deltaValue
		if err := json.Unmarshal(r.Delta, &d); err != nil {
			return err
		}
		c.Add(aggregate.Delta{Inc: d.Inc, Min: d.Min, AddMembers: d.AddMembers})
		if d.Folded > 1 {
			c.Deltas += d.Folded - 1
		}
	}
	return nil
}

// getStatement reads an entry and its delta rows in one round trip: a seek
// of the entry row and a scan of the rows interleaved under it.
const getStatement = `SELECT e.value, e.as_of, e.expires_at,
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
	if err := sr.Columns(&r.Value, &r.AsOf, &r.ExpiresAt, &pending); err != nil {
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

// compact replaces delta rows by one row holding their sum, in a single
// commit that reads nothing. Every row is updated (to what it holds)
// before it is deleted, and an update of a missing row fails the commit:
// a row another fold has taken meanwhile, or one of an entry that was
// replaced, makes this fold a no-op instead of counting the row twice.
// Nothing here touches the entry row or a row an Add is inserting, so a
// fold makes neither readers nor writers wait.
func (s *Store) compact(ctx context.Context, key string, pending []*deltaRow) error {
	if _, running := s.compacting.LoadOrStore(key, struct{}{}); running {
		return nil
	}
	defer s.compacting.Delete(key)

	if len(pending) > compactBatch {
		pending = pending[:compactBatch]
	}
	var sum aggregate.Counters
	if err := fold(&sum, pending); err != nil {
		return err
	}
	encoded, err := json.Marshal(deltaValue{Inc: sum.Counts, Min: sum.Minima, AddMembers: sum.Members, Folded: sum.Deltas})
	if err != nil {
		return err
	}
	mutations := make([]*spanner.Mutation, 0, len(pending)+2)
	keys := make([]spanner.Key, len(pending))
	for i, d := range pending {
		keys[i] = spanner.Key{key, d.ID}
		mutations = append(mutations, spanner.Update(tableDeltas, []string{"key", "id", "delta"}, []any{key, d.ID, d.Delta}))
	}
	mutations = append(mutations,
		spanner.Delete(tableDeltas, spanner.KeySetFromKeys(keys...)),
		spanner.Insert(tableDeltas, []string{"key", "id", "delta"}, []any{key, uuid.NewString(), encoded}))
	// at-least-once is a single Commit call; the replay of a fold that did
	// land finds its rows gone like any other late fold
	_, err = s.client.Apply(ctx, mutations, spanner.ApplyAtLeastOnce())
	if spanner.ErrCode(err) == codes.NotFound {
		return nil
	}
	return err
}

// PutIfAbsent inserts the entry row blind: the insert fails when a row is
// there, which is the answer for every caller but one when several computed
// the same missing entry at once. Only a row that is there but expired
// needs more, a transaction that reads it again and replaces it.
func (s *Store) PutIfAbsent(ctx context.Context, key string, entry aggregate.Counters, ttl time.Duration) (bool, error) {
	encoded, err := json.Marshal(value{Counts: entry.Counts, Minima: entry.Minima, Members: entry.Members})
	if err != nil {
		return false, err
	}
	insert := func(now time.Time) *spanner.Mutation {
		return spanner.Insert(tableEntries,
			[]string{"key", "value", "as_of", "expires_at"},
			[]any{key, encoded, entry.AsOf, now.Add(ttl)})
	}

	// at-least-once is a single Commit call. A replay of an insert that did
	// land is told AlreadyExists and reports the entry as someone else's,
	// which the callers take as "read it": they find their own
	_, err = s.client.Apply(ctx, []*spanner.Mutation{insert(time.Now())}, spanner.ApplyAtLeastOnce())
	if err == nil {
		return true, nil
	}
	if spanner.ErrCode(err) != codes.AlreadyExists {
		return false, err
	}

	var expiresAt time.Time
	row, err := s.client.Single().ReadRow(ctx, tableEntries, spanner.Key{key}, []string{"expires_at"})
	switch {
	case spanner.ErrCode(err) == codes.NotFound:
		// dropped since the insert: the transaction below finds no row
	case err != nil:
		return false, err
	default:
		if err := row.Column(0, &expiresAt); err != nil {
			return false, err
		}
	}
	if time.Now().Before(expiresAt) {
		return false, nil
	}

	won := false
	_, err = s.client.ReadWriteTransaction(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		won = false
		now := time.Now()
		row, err := tx.ReadRow(ctx, tableEntries, spanner.Key{key}, []string{"expires_at"})
		if err == nil {
			var expiresAt time.Time
			if err := row.Column(0, &expiresAt); err != nil {
				return err
			}
			if now.Before(expiresAt) {
				return nil
			}
		} else if spanner.ErrCode(err) != codes.NotFound {
			return err
		}
		won = true
		// the delete takes the delta rows of the expired entry with it
		return tx.BufferWrite([]*spanner.Mutation{spanner.Delete(tableEntries, spanner.Key{key}), insert(now)})
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
