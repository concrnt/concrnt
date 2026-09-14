package firestore

import (
	"context"
	"errors"
	"fmt"

	"cloud.google.com/go/firestore"
	"google.golang.org/api/iterator"

	"github.com/concrnt/concrnt/internal/usecase/record"
)

// gcCandidates selects gc-flagged commits whose id sorts below cutoffID.
// Commit ids are time-prefixed CDIDs, so this is "older than the cutoff".
func (r *RecordRepository) gcCandidates(cutoffID string) firestore.Query {
	return r.col(colCommits).
		Where("gcCandidate", "==", true).
		OrderBy(firestore.DocumentID, firestore.Asc).
		EndBefore(cutoffID)
}

func (r *RecordRepository) CountGcCandidates(ctx context.Context, cutoffID string) (int64, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.CountGcCandidates")
	defer span.End()

	q := r.gcCandidates(cutoffID)
	res, err := q.NewAggregationQuery().WithCount("n").Get(ctx)
	if err != nil {
		span.RecordError(err)
		return 0, queryErr(err)
	}
	return aggregationCount(res, "n")
}

func (r *RecordRepository) ListGcCandidateIDs(ctx context.Context, cutoffID string, limit int) ([]string, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.ListGcCandidateIDs")
	defer span.End()

	q := r.gcCandidates(cutoffID).Select()
	if limit > 0 {
		q = q.Limit(limit)
	}
	snaps, err := q.Documents(ctx).GetAll()
	if err != nil {
		span.RecordError(err)
		return nil, queryErr(err)
	}
	ids := make([]string, 0, len(snaps))
	for _, s := range snaps {
		ids = append(ids, s.Ref.ID)
	}
	return ids, nil
}

// DeleteCommitLogs removes the commits and, by hand, everything Postgres
// would drop through ON DELETE CASCADE: the record version, the key pointing
// at it, the association and its uniqueness marker, ack/acked states and
// entities carrying the document id. Every step is idempotent, so a partial
// run is finished by re-running.
func (r *RecordRepository) DeleteCommitLogs(ctx context.Context, ids []string) (int, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.DeleteCommitLogs")
	defer span.End()

	if len(ids) == 0 {
		return 0, nil
	}

	refs := make([]*firestore.DocumentRef, 0, len(ids))
	for _, id := range ids {
		refs = append(refs, r.col(colCommits).Doc(id))
	}
	existing := 0
	for start := 0; start < len(refs); start += getAllBatch {
		snaps, err := r.client.GetAll(ctx, refs[start:min(start+getAllBatch, len(refs))])
		if err != nil {
			span.RecordError(err)
			return 0, err
		}
		for _, s := range snaps {
			if s.Exists() {
				existing++
			}
		}
	}

	b := newBulk(ctx, r.client)
	deleteWhere := func(q firestore.Query) error {
		it := q.Select().Documents(ctx)
		defer it.Stop()
		for {
			snap, err := it.Next()
			if errors.Is(err, iterator.Done) {
				return nil
			}
			if err != nil {
				return queryErr(err)
			}
			b.delete(snap.Ref)
		}
	}

	for _, id := range ids {
		b.delete(r.col(colCommits).Doc(id))
		b.delete(r.col(colRecords).Doc(id))

		// the key pointing at this version goes, and with it the associations
		// targeting the key (record_keys -> associations cascade)
		keySnaps, err := r.col(colRecordKeys).Where("recordID", "==", id).Select("gen").Documents(ctx).GetAll()
		if err != nil {
			span.RecordError(err)
			return 0, queryErr(err)
		}
		for _, keySnap := range keySnaps {
			b.delete(keySnap.Ref)
			gen, _ := keySnap.DataAt("gen")
			if g, ok := gen.(string); ok && g != "" {
				if err := r.forEachAssociationOf(ctx, g, func(ref *firestore.DocumentRef, unique string) {
					b.delete(ref)
					if unique != "" {
						b.delete(r.col(colAssocUniques).Doc(unique))
					}
				}); err != nil {
					span.RecordError(err)
					return 0, err
				}
			}
		}

		assocSnap, err := r.col(colAssociations).Doc(id).Get(ctx)
		if err == nil && assocSnap.Exists() {
			var a associationDoc
			if err := decode(assocSnap, &a); err != nil {
				span.RecordError(err)
				return 0, err
			}
			if a.Unique != "" {
				b.delete(r.col(colAssocUniques).Doc(a.Unique))
			}
			b.delete(assocSnap.Ref)
		} else if err != nil && !isNotFound(err) {
			span.RecordError(err)
			return 0, err
		}

		for _, collection := range []string{colAcks, colAckeds, colEntities} {
			if err := deleteWhere(r.col(collection).Where("documentID", "==", id)); err != nil {
				span.RecordError(err)
				return 0, err
			}
		}
	}
	if err := b.end(); err != nil {
		span.RecordError(err)
		return 0, err
	}
	return existing, nil
}

// forEachAssociationOf enumerates the associations targeting a key instance.
func (r *RecordRepository) forEachAssociationOf(ctx context.Context, targetGen string, fn func(ref *firestore.DocumentRef, unique string)) error {
	it := r.col(colAssociations).Where("targetGen", "==", targetGen).Select("unique").Documents(ctx)
	defer it.Stop()
	for {
		snap, err := it.Next()
		if errors.Is(err, iterator.Done) {
			return nil
		}
		if err != nil {
			return queryErr(err)
		}
		u, _ := snap.DataAt("unique")
		unique, _ := u.(string)
		fn(snap.Ref, unique)
	}
}

func (r *RecordRepository) ListCommitLogs(ctx context.Context, page record.CommitLogPage) ([]record.CommitLogEntry, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.ListCommitLogs")
	defer span.End()

	// document ids are the commit ids; cursors on __name__ take the bare id
	q := r.col(colCommits).OrderBy(firestore.DocumentID, firestore.Asc)
	if page.Owner != "" {
		q = q.Where("owner", "==", page.Owner)
	}
	if page.AfterID != "" {
		q = q.StartAfter(page.AfterID)
	} else if page.FromID != "" {
		q = q.StartAt(page.FromID)
	}
	if page.UntilID != "" {
		q = q.EndAt(page.UntilID)
	}
	if page.Limit > 0 {
		q = q.Limit(page.Limit)
	}

	snaps, err := q.Documents(ctx).GetAll()
	if err != nil {
		span.RecordError(err)
		return nil, queryErr(err)
	}
	entries := make([]record.CommitLogEntry, 0, len(snaps))
	for _, snap := range snaps {
		var c commitDoc
		if err := decode(snap, &c); err != nil {
			span.RecordError(err)
			return nil, err
		}
		proof, err := parseProof(c.Proof)
		if err != nil {
			return nil, fmt.Errorf("failed to parse proof for commit %s: %w", snap.Ref.ID, err)
		}
		entries = append(entries, record.CommitLogEntry{
			ID:       snap.Ref.ID,
			Document: c.Document,
			Proof:    proof,
			Owner:    c.Owner,
		})
	}
	return entries, nil
}

// aggregationCount extracts a count() alias from an aggregation result.
func aggregationCount(res firestore.AggregationResult, alias string) (int64, error) {
	var m struct {
		N int64 `firestore:"n"`
	}
	if alias != "n" {
		return 0, fmt.Errorf("unsupported aggregation alias %q", alias)
	}
	if err := res.DataTo(&m); err != nil {
		return 0, err
	}
	return m.N, nil
}
