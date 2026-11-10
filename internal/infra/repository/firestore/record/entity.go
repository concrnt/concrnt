//go:build contractv1

package record

import (
	"context"
	"errors"
	"time"

	"cloud.google.com/go/firestore"
	"google.golang.org/api/iterator"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/internal/domain"
	fs "github.com/concrnt/concrnt/internal/infra/repository/firestore"
	ucrecord "github.com/concrnt/concrnt/internal/usecase/record"
)

func (r *Repository) CreateEntity(
	ctx context.Context,
	tx ucrecord.RepositoryTx,
	ccid string,
	alias *string,
	domain string,
	documentID string,
	createdAt time.Time,
) (bool, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.CreateEntity")
	defer span.End()

	t, err := getRecordTx(tx)
	if err != nil {
		span.RecordError(err)
		return false, err
	}

	ref := r.col(fs.ColEntities).Doc(ccid)
	m, exists, err := t.read(ref)
	if err != nil {
		span.RecordError(err)
		return false, err
	}

	if !exists {
		t.set(ref, map[string]any{
			"alias":      fs.Deref(alias),
			"domain":     domain,
			"tag":        "",
			"documentID": documentID,
			"createdAt":  createdAt,
			"cDate":      firestore.ServerTimestamp,
			"mDate":      firestore.ServerTimestamp,
		})
		return true, nil
	}

	// CIP-3 §3.4 accept-if-newer on the document's createdAt: only a strictly
	// newer document replaces the stored one; the read is validated at
	// commit, so concurrent writers converge on the same winner.
	if !createdAt.After(entityFromMap(m).CreatedAt) {
		return false, nil
	}
	t.update(ref, []firestore.Update{
		{Path: "alias", Value: fs.Deref(alias)},
		{Path: "domain", Value: domain},
		{Path: "documentID", Value: documentID},
		{Path: "createdAt", Value: createdAt},
		{Path: "mDate", Value: firestore.ServerTimestamp},
	})
	return true, nil
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
		b.Update(snap.Ref, []firestore.Update{{Path: "gcCandidate", Value: true}})
	}
	if err := b.End(); err != nil {
		span.RecordError(err)
		return err
	}
	return nil
}

// entityWithDocument joins the entity with the commit that registered it;
// an entity whose commit is gone is not served (inner join semantics).
func (r *Repository) entityWithDocument(ctx context.Context, ccid string, m map[string]any) (*domain.Entity, error) {
	entity := entityFromMap(m)
	cm, exists, err := r.getDoc(ctx, r.col(fs.ColCommits).Doc(entity.DocumentID))
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, domain.NotFoundError{Resource: "entity"}
	}
	commit := commitFromMap(cm)
	proof, err := commit.proof()
	if err != nil {
		return nil, err
	}
	return &domain.Entity{
		ID:             ccid,
		Domain:         entity.Domain,
		Alias:          fs.StrPtr(entity.Alias),
		TagString:      entity.Tag,
		SignedDocument: &concrnt.SignedDocument{Document: commit.Document, Proof: proof},
	}, nil
}

func (r *Repository) GetEntityByCCID(ctx context.Context, ccid string) (*domain.Entity, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.GetEntityByCCID")
	defer span.End()

	m, exists, err := r.getDoc(ctx, r.col(fs.ColEntities).Doc(ccid))
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	if !exists {
		return nil, domain.NotFoundError{Resource: "entity"}
	}
	return r.entityWithDocument(ctx, ccid, m)
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
	return r.entityWithDocument(ctx, snaps[0].Ref.ID, snaps[0].Data())
}
