package record

import (
	"context"
	"errors"

	"cloud.google.com/go/firestore"
	"google.golang.org/api/iterator"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/internal/domain"
	fs "github.com/concrnt/concrnt/internal/infra/repository/firestore"
	"github.com/concrnt/concrnt/internal/infra/repository/repoutil"
	ucrecord "github.com/concrnt/concrnt/internal/usecase/record"
)

// ApplyEntity reads the commit and the entity in one batch get. CIP-3 §3.4
// accept-if-newer on the document's createdAt: only a strictly newer
// document replaces the stored one; the read is validated at commit, so
// concurrent writers converge on the same winner.
func (r *Repository) ApplyEntity(ctx context.Context, commit ucrecord.EntityCommit) (ucrecord.ApplyStatus, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.ApplyEntity")
	defer span.End()

	entry, err := newCommit(commit.Commit, fs.ColEntities, commit.CCID)
	if err != nil {
		span.RecordError(err)
		return ucrecord.Applied, err
	}
	commitRef := r.col(fs.ColCommits).Doc(commit.ID)
	entityRef := r.col(fs.ColEntities).Doc(commit.CCID)

	status, err := r.apply(ctx, func(tx *firestore.Transaction) (ucrecord.ApplyStatus, error) {
		reads := newReadSet(commitRef, entityRef)
		if err := reads.loadTx(tx); err != nil {
			return ucrecord.Applied, err
		}
		if _, exists := reads.data(commitRef); exists {
			return ucrecord.Duplicate, nil
		}

		fields := map[string]any{
			"alias":      fs.Deref(commit.Alias),
			"domain":     commit.Domain,
			"documentID": commit.ID,
			"createdAt":  commit.CreatedAt,
			"document":   entry.Document,
			"proof":      entry.Proof,
			"mDate":      firestore.ServerTimestamp,
		}
		if m, exists := reads.data(entityRef); exists {
			if !commit.CreatedAt.After(entityFromMap(m).CreatedAt) {
				return ucrecord.Stale, nil
			}
			// what the document does not say (tag, cDate) is kept
			fields["tag"] = str(m, "tag")
			fields["cDate"] = m["cDate"]
		} else {
			fields["tag"] = ""
			fields["cDate"] = firestore.ServerTimestamp
		}

		if err := tx.Create(commitRef, entry.toMap()); err != nil {
			return ucrecord.Applied, err
		}
		return ucrecord.Applied, tx.Set(entityRef, fields)
	})
	if err != nil {
		span.RecordError(err)
	}
	return status, err
}

// MarkCommitLogsGcCandidateByOwner flags every commit log owned by the
// given ccid as a GC candidate. Every commit has exactly one owner (an ack
// between two local users is two commits: the ack owned by the acker and
// the acked owned by the target), so nothing another user holds is touched.
// Idempotent; not atomic across documents, a re-run picks up what was
// missed.
func (r *Repository) MarkCommitLogsGcCandidateByOwner(ctx context.Context, owner string) error {
	ctx, span := tracer.Start(ctx, "Repository.MarkCommitLogsGcCandidateByOwner")
	defer span.End()

	it := r.col(fs.ColCommits).
		Where("owner", "==", owner).
		Where("gcCandidate", "==", false).
		Select().Documents(ctx)
	defer it.Stop()

	b := fs.NewBulk(ctx, r.client)
	for {
		snap, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			span.RecordError(err)
			_ = b.End()
			return fs.QueryErr(err)
		}
		b.Update(snap.Ref, flagGc)
	}
	if err := b.End(); err != nil {
		span.RecordError(err)
		return err
	}
	return nil
}

// entityOf builds the entity from its document alone: it carries the
// signed document that registered it.
func entityOf(ccid string, m map[string]any) (*domain.Entity, error) {
	entity := entityFromMap(m)
	proof, err := repoutil.ParseProof(entity.Proof)
	if err != nil {
		return nil, err
	}
	return &domain.Entity{
		ID:             ccid,
		Domain:         entity.Domain,
		Alias:          fs.StrPtr(entity.Alias),
		TagString:      entity.Tag,
		SignedDocument: &concrnt.SignedDocument{Document: entity.Document, Proof: proof},
	}, nil
}

func (r *Repository) GetEntityByCCID(ctx context.Context, ccid string) (*domain.Entity, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.GetEntityByCCID")
	defer span.End()

	snap, err := r.col(fs.ColEntities).Doc(ccid).Get(ctx)
	if err != nil {
		if fs.IsNotFound(err) {
			return nil, domain.NotFoundError{Resource: "entity"}
		}
		span.RecordError(err)
		return nil, err
	}
	entity, err := entityOf(ccid, snap.Data())
	if err != nil {
		span.RecordError(err)
	}
	return entity, err
}

func (r *Repository) GetEntityByAlias(ctx context.Context, alias string) (*domain.Entity, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.GetEntityByAlias")
	defer span.End()

	if alias == "" {
		return nil, domain.NotFoundError{Resource: "entity"}
	}
	snaps, err := r.col(fs.ColEntities).Where("alias", "==", alias).Limit(1).Documents(ctx).GetAll()
	if err != nil {
		span.RecordError(err)
		return nil, fs.QueryErr(err)
	}
	if len(snaps) == 0 {
		return nil, domain.NotFoundError{Resource: "entity"}
	}
	entity, err := entityOf(snaps[0].Ref.ID, snaps[0].Data())
	if err != nil {
		span.RecordError(err)
	}
	return entity, err
}
