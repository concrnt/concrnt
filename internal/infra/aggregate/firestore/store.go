// Package firestore keeps aggregate entries in a collection of the record
// database (aggregate_entries), one document per entry. Add is one Update
// of field transforms (Increment, Minimum, ArrayUnion) under the exists
// precondition an Update carries: the server folds the delta in, so no
// transaction reads the document and concurrent writers of one key do not
// abort one another. Only PutIfAbsent, the cache-miss path, runs a
// transaction. Expired documents are ignored on read and dropped by a TTL
// policy on expiresAt (within a day; deploy/firestore/README.md says how to
// enable it); an Add cannot see the expiry and lands on such a document,
// which PutIfAbsent then replaces whole.
package firestore

import (
	"context"
	"time"

	"cloud.google.com/go/firestore"

	fsrepo "github.com/concrnt/concrnt/internal/infra/repository/firestore"
	"github.com/concrnt/concrnt/internal/usecase/aggregate"
)

type Store struct {
	client *firestore.Client
}

func New(client *firestore.Client) *Store {
	return &Store{client: client}
}

// putMaxAttempts bounds the transaction re-runs of PutIfAbsent: the
// computations racing for one missing key serialise on its document.
const putMaxAttempts = 32

// invalidateBatch is the page of one InvalidateDefinition / InvalidateAll
// sweep.
const invalidateBatch = 500

type entry struct {
	// the definition (aggregate.NameOf) InvalidateDefinition queries by; the
	// key itself is only in the document id
	Name string `firestore:"name"`
	// Counters is keyed by the hash of the counter name: a name is a schema
	// URL joined to a variant, which a field name can hold neither in
	// length nor in characters
	Counters  map[string]counter `firestore:"counters"`
	Members   []string           `firestore:"members"`
	AsOf      time.Time          `firestore:"asOf"`
	Deltas    int64              `firestore:"deltas"`
	ExpiresAt time.Time          `firestore:"expiresAt"`
}

// counter is one name's count and minimum; either may be missing.
type counter struct {
	Name  string `firestore:"name"`
	Count *int64 `firestore:"n,omitempty"`
	Min   *int64 `firestore:"min,omitempty"`
}

const (
	fieldCounters    = "counters"
	fieldMembers     = "members"
	fieldDeltas      = "deltas"
	counterFieldName = "name"
	counterFieldN    = "n"
	counterFieldMin  = "min"
)

func (e entry) counters() *aggregate.Counters {
	out := &aggregate.Counters{
		Counts:  map[string]int64{},
		Minima:  map[string]int64{},
		Members: e.Members,
		AsOf:    e.AsOf.UTC(),
		Deltas:  e.Deltas,
	}
	for _, c := range e.Counters {
		if c.Count != nil {
			out.Counts[c.Name] = *c.Count
		}
		if c.Min != nil {
			out.Minima[c.Name] = *c.Min
		}
	}
	return out
}

func (e entry) live(now time.Time) bool {
	return now.Before(e.ExpiresAt)
}

// keys contain '/' (name:cckv://owner/...), so the document id is a hash.
func (s *Store) ref(key string) *firestore.DocumentRef {
	return s.client.Collection(fsrepo.ColAggregateEntries).Doc(fsrepo.HashID(key))
}

func decodeEntry(snap *firestore.DocumentSnapshot, err error) (entry, bool, error) {
	var e entry
	if err != nil {
		if fsrepo.IsNotFound(err) {
			return e, false, nil
		}
		return e, false, err
	}
	if !snap.Exists() {
		return e, false, nil
	}
	return e, true, snap.DataTo(&e)
}

func (s *Store) Get(ctx context.Context, key string) (*aggregate.Counters, bool, error) {
	e, found, err := decodeEntry(s.ref(key).Get(ctx))
	if err != nil || !found || !e.live(time.Now()) {
		return nil, false, err
	}
	return e.counters(), true, nil
}

func (s *Store) PutIfAbsent(ctx context.Context, key string, in aggregate.Counters, ttl time.Duration) (bool, error) {
	counters := make(map[string]counter, len(in.Counts)+len(in.Minima))
	for name, n := range in.Counts {
		counters[fsrepo.HashID(name)] = counter{Name: name, Count: &n}
	}
	for name, v := range in.Minima {
		c := counters[fsrepo.HashID(name)]
		c.Name, c.Min = name, &v
		counters[fsrepo.HashID(name)] = c
	}

	won := false
	err := s.client.RunTransaction(ctx, func(_ context.Context, tx *firestore.Transaction) error {
		won = false
		now := time.Now()
		e, found, err := decodeEntry(tx.Get(s.ref(key)))
		if err != nil {
			return err
		}
		if found && e.live(now) {
			return nil
		}
		won = true
		return tx.Set(s.ref(key), entry{Name: aggregate.NameOf(key), Counters: counters, Members: in.Members, AsOf: in.AsOf, Deltas: 0, ExpiresAt: now.Add(ttl)})
	}, firestore.MaxAttempts(putMaxAttempts))
	if err != nil {
		return false, err
	}
	return won, nil
}

func (s *Store) Add(ctx context.Context, key string, delta aggregate.Delta) (bool, error) {
	updates := make([]firestore.Update, 0, 2*len(delta.Inc)+2*len(delta.Min)+2)
	named := func(name string) string {
		id := fsrepo.HashID(name)
		updates = append(updates, firestore.Update{FieldPath: firestore.FieldPath{fieldCounters, id, counterFieldName}, Value: name})
		return id
	}
	ids := make(map[string]string, len(delta.Inc))
	for name, n := range delta.Inc {
		ids[name] = named(name)
		updates = append(updates, firestore.Update{FieldPath: firestore.FieldPath{fieldCounters, ids[name], counterFieldN}, Value: firestore.Increment(n)})
	}
	for name, v := range delta.Min {
		id, ok := ids[name]
		if !ok {
			id = named(name)
		}
		updates = append(updates, firestore.Update{FieldPath: firestore.FieldPath{fieldCounters, id, counterFieldMin}, Value: firestore.FieldTransformMinimum(v)})
	}
	if len(delta.AddMembers) > 0 {
		members := make([]any, len(delta.AddMembers))
		for i, m := range delta.AddMembers {
			members[i] = m
		}
		updates = append(updates, firestore.Update{Path: fieldMembers, Value: firestore.ArrayUnion(members...)})
	}
	updates = append(updates, firestore.Update{Path: fieldDeltas, Value: firestore.Increment(1)})

	if _, err := s.ref(key).Update(ctx, updates); err != nil {
		if fsrepo.IsNotFound(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (s *Store) Invalidate(ctx context.Context, key string) error {
	_, err := s.ref(key).Delete(ctx)
	return err
}

func (s *Store) InvalidateDefinition(ctx context.Context, name string) error {
	return s.deleteAll(ctx, s.client.Collection(fsrepo.ColAggregateEntries).Where("name", "==", name))
}

func (s *Store) InvalidateAll(ctx context.Context) error {
	return s.deleteAll(ctx, s.client.Collection(fsrepo.ColAggregateEntries).Query)
}

// deleteAll deletes what q matches a page at a time, one BulkWriter per
// page; a deleted page no longer matches, so every page is the first one.
func (s *Store) deleteAll(ctx context.Context, q firestore.Query) error {
	q = q.Select().Limit(invalidateBatch)
	for {
		snaps, err := q.Documents(ctx).GetAll()
		if err != nil {
			return fsrepo.QueryErr(err)
		}
		if len(snaps) == 0 {
			return nil
		}
		b := fsrepo.NewBulk(ctx, s.client)
		for _, snap := range snaps {
			b.Delete(snap.Ref)
		}
		if err := b.End(); err != nil {
			return err
		}
		if len(snaps) < invalidateBatch {
			return nil
		}
	}
}
