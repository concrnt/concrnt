// Package firestore keeps aggregate entries in a collection of the record
// database (aggregate_entries), serialised per key by the Firestore
// transaction: Apply and PutIfAbsent read the document and write it back in
// one transaction, so two of them on one key abort one another and re-run,
// which makes the store as strict as the Postgres one. Expired documents are
// ignored on read and dropped by a TTL policy on expiresAt (within a day;
// deploy/firestore/README.md says how to enable it).
package firestore

import (
	"context"
	"errors"
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

// applyMaxAttempts bounds the transaction re-runs of Apply and PutIfAbsent:
// the writers of one hot key serialise on its document.
const applyMaxAttempts = 32

// invalidateBatch is the page of one InvalidateDefinition / InvalidateAll
// sweep.
const invalidateBatch = 500

// Name is the definition (aggregate.NameOf) InvalidateDefinition queries
// by; the key itself is only in the document id.
type entry struct {
	Name      string    `firestore:"name"`
	Value     []byte    `firestore:"value"`
	AsOf      time.Time `firestore:"asOf"`
	Deltas    int64     `firestore:"deltas"`
	ExpiresAt time.Time `firestore:"expiresAt"`
}

func (e entry) entry() *aggregate.Entry {
	return &aggregate.Entry{Value: e.Value, AsOf: e.AsOf.UTC(), Deltas: int(e.Deltas)}
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

func (s *Store) Get(ctx context.Context, key string) (*aggregate.Entry, bool, error) {
	e, found, err := decodeEntry(s.ref(key).Get(ctx))
	if err != nil || !found || !e.live(time.Now()) {
		return nil, false, err
	}
	return e.entry(), true, nil
}

func (s *Store) PutIfAbsent(ctx context.Context, key string, in aggregate.Entry, ttl time.Duration) (bool, error) {
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
		return tx.Set(s.ref(key), entry{Name: aggregate.NameOf(key), Value: in.Value, AsOf: in.AsOf, Deltas: 0, ExpiresAt: now.Add(ttl)})
	}, firestore.MaxAttempts(applyMaxAttempts))
	if err != nil {
		return false, err
	}
	return won, nil
}

func (s *Store) Apply(ctx context.Context, key string, fn func(old []byte) ([]byte, error)) (*aggregate.Entry, bool, error) {
	var out *aggregate.Entry
	err := s.client.RunTransaction(ctx, func(_ context.Context, tx *firestore.Transaction) error {
		out = nil
		e, found, err := decodeEntry(tx.Get(s.ref(key)))
		if err != nil {
			return err
		}
		if !found || !e.live(time.Now()) {
			return nil
		}
		value, err := fn(e.Value)
		if err != nil {
			return err
		}
		e.Value = value
		e.Deltas++
		if err := tx.Update(s.ref(key), []firestore.Update{{Path: "value", Value: value}, {Path: "deltas", Value: e.Deltas}}); err != nil {
			return err
		}
		out = e.entry()
		return nil
	}, firestore.MaxAttempts(applyMaxAttempts))
	if err != nil {
		return nil, false, err
	}
	return out, out != nil, nil
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
		bw := s.client.BulkWriter(ctx)
		jobs := make([]*firestore.BulkWriterJob, 0, len(snaps))
		for _, snap := range snaps {
			job, err := bw.Delete(snap.Ref)
			if err != nil {
				return err
			}
			jobs = append(jobs, job)
		}
		bw.End()
		var errs []error
		for _, job := range jobs {
			if _, err := job.Results(); err != nil {
				errs = append(errs, err)
			}
		}
		if err := errors.Join(errs...); err != nil {
			return err
		}
		if len(snaps) < invalidateBatch {
			return nil
		}
	}
}
