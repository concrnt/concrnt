package firestore

import (
	"context"
	"errors"

	"cloud.google.com/go/firestore"
	"google.golang.org/api/iterator"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/internal/domain"
	"github.com/concrnt/concrnt/internal/usecase/residence"
)

type ResidenceRepository struct {
	client *firestore.Client
}

func NewResidenceRepository(client *firestore.Client) residence.Repository {
	return &ResidenceRepository{client: client}
}

func (r *ResidenceRepository) col(name string) *firestore.CollectionRef {
	return r.client.Collection(name)
}

// SaveMeta upserts inviter and info (cDate is kept from the first write).
func (r *ResidenceRepository) SaveMeta(ctx context.Context, meta domain.EntityMeta) error {
	ctx, span := tracer.Start(ctx, "ResidenceRepository.Register")
	defer span.End()

	ref := r.col(colEntityMetas).Doc(meta.ID)
	fields := map[string]any{
		"inviter": deref(meta.Inviter),
		"info":    meta.Info,
		"mDate":   firestore.ServerTimestamp,
	}
	if err := upsert(ctx, ref, fields); err != nil {
		span.RecordError(err)
		return err
	}
	return nil
}

func (r *ResidenceRepository) GetMeta(ctx context.Context, ccid string) (*domain.EntityMeta, error) {
	ctx, span := tracer.Start(ctx, "ResidenceRepository.GetMeta")
	defer span.End()

	snap, err := r.col(colEntityMetas).Doc(ccid).Get(ctx)
	if err != nil {
		if isNotFound(err) {
			return nil, domain.NotFoundError{Resource: ccid}
		}
		return nil, err
	}
	var meta entityMetaDoc
	if err := decode(snap, &meta); err != nil {
		return nil, err
	}
	return &domain.EntityMeta{
		ID:      ccid,
		Inviter: strPtr(meta.Inviter),
		Info:    meta.Info,
	}, nil
}

// UpdateMetaInfo replaces only info, preserving inviter.
func (r *ResidenceRepository) UpdateMetaInfo(ctx context.Context, ccid string, info string) error {
	ctx, span := tracer.Start(ctx, "ResidenceRepository.UpdateMetaInfo")
	defer span.End()

	_, err := r.col(colEntityMetas).Doc(ccid).Update(ctx, []firestore.Update{
		{Path: "info", Value: info},
		{Path: "mDate", Value: firestore.ServerTimestamp},
	})
	if err != nil {
		if isNotFound(err) {
			return domain.NotFoundError{Resource: "entity meta"}
		}
		span.RecordError(err)
		return err
	}
	return nil
}

func (r *ResidenceRepository) DeleteMeta(ctx context.Context, ccid string) error {
	ctx, span := tracer.Start(ctx, "ResidenceRepository.DeleteMeta")
	defer span.End()

	if _, err := r.col(colEntityMetas).Doc(ccid).Delete(ctx); err != nil {
		span.RecordError(err)
		return err
	}
	return nil
}

func (r *ResidenceRepository) ListMetas(ctx context.Context, owner string) ([]domain.EntityMeta, error) {
	ctx, span := tracer.Start(ctx, "ResidenceRepository.ListMetas")
	defer span.End()

	result := []domain.EntityMeta{}
	if owner != "" {
		meta, err := r.GetMeta(ctx, owner)
		if errors.Is(err, domain.NotFoundError{}) {
			return result, nil
		}
		if err != nil {
			return nil, err
		}
		return append(result, *meta), nil
	}

	it := r.col(colEntityMetas).Documents(ctx)
	defer it.Stop()
	for {
		snap, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			span.RecordError(err)
			return nil, err
		}
		var meta entityMetaDoc
		if err := decode(snap, &meta); err != nil {
			return nil, err
		}
		result = append(result, domain.EntityMeta{
			ID:      snap.Ref.ID,
			Inviter: strPtr(meta.Inviter),
			Info:    meta.Info,
		})
	}
	return result, nil
}

// MarkCommitLogsGcCandidateByOwner flags every commit owned by the ccid.
// Idempotent; not atomic across documents, but every document flips
// independently and a re-run picks up whatever was missed.
func (r *ResidenceRepository) MarkCommitLogsGcCandidateByOwner(ctx context.Context, owner string) error {
	ctx, span := tracer.Start(ctx, "ResidenceRepository.MarkCommitLogsGcCandidateByOwner")
	defer span.End()

	it := r.col(colCommits).
		Where("owner", "==", owner).
		Where("gcCandidate", "==", false).
		Select().Documents(ctx)
	defer it.Stop()

	b := newBulk(ctx, r.client)
	for {
		snap, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			span.RecordError(err)
			_ = b.end()
			return queryErr(err)
		}
		b.update(snap.Ref, []firestore.Update{{Path: "gcCandidate", Value: true}})
	}
	if err := b.end(); err != nil {
		span.RecordError(err)
		return err
	}
	return nil
}

func (r *ResidenceRepository) entityFromSnap(ctx context.Context, snap *firestore.DocumentSnapshot) (*domain.Entity, error) {
	var entity entityDoc
	if err := decode(snap, &entity); err != nil {
		return nil, err
	}

	// the entity's signed document lives in its commit; an entity whose
	// commit is gone is not served (inner join semantics)
	commitSnap, err := r.col(colCommits).Doc(entity.DocumentID).Get(ctx)
	if err != nil {
		if isNotFound(err) {
			return nil, domain.NotFoundError{Resource: "entity"}
		}
		return nil, err
	}
	var commit commitDoc
	if err := decode(commitSnap, &commit); err != nil {
		return nil, err
	}
	proof, err := parseProof(commit.Proof)
	if err != nil {
		return nil, err
	}

	return &domain.Entity{
		ID:        snap.Ref.ID,
		Domain:    entity.Domain,
		Alias:     strPtr(entity.Alias),
		TagString: entity.Tag,
		SignedDocument: &concrnt.SignedDocument{
			Document: commit.Document,
			Proof:    proof,
		},
	}, nil
}

func (r *ResidenceRepository) GetEntityByCCID(ctx context.Context, ccid string) (*domain.Entity, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.GetEntityByCCID")
	defer span.End()

	snap, err := r.col(colEntities).Doc(ccid).Get(ctx)
	if err != nil {
		if isNotFound(err) {
			return nil, domain.NotFoundError{Resource: "entity"}
		}
		span.RecordError(err)
		return nil, err
	}
	return r.entityFromSnap(ctx, snap)
}

func (r *ResidenceRepository) GetEntityByAlias(ctx context.Context, alias string) (*domain.Entity, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.GetEntityByAlias")
	defer span.End()

	if alias == "" {
		return nil, domain.NotFoundError{Resource: "entity"}
	}
	snaps, err := r.col(colEntities).Where("alias", "==", alias).Limit(1).Documents(ctx).GetAll()
	if err != nil {
		span.RecordError(err)
		return nil, queryErr(err)
	}
	if len(snaps) == 0 {
		return nil, domain.NotFoundError{Resource: "entity"}
	}
	return r.entityFromSnap(ctx, snaps[0])
}

// upsert emulates INSERT ... ON CONFLICT DO UPDATE for a handful of fields:
// an existing document is patched (its cDate untouched), a missing one is
// created with cDate set. Not transactional; used for rarely-written,
// last-writer-wins metadata only.
func upsert(ctx context.Context, ref *firestore.DocumentRef, fields map[string]any) error {
	updates := make([]firestore.Update, 0, len(fields))
	for k, v := range fields {
		updates = append(updates, firestore.Update{Path: k, Value: v})
	}
	_, err := ref.Update(ctx, updates)
	if err == nil {
		return nil
	}
	if !isNotFound(err) {
		return err
	}
	created := make(map[string]any, len(fields)+1)
	for k, v := range fields {
		created[k] = v
	}
	created["cDate"] = firestore.ServerTimestamp
	_, err = ref.Set(ctx, created)
	return err
}
