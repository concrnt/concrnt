package firestore

import (
	"context"
	"errors"

	"cloud.google.com/go/firestore"
	"google.golang.org/api/iterator"

	"github.com/concrnt/concrnt/internal/domain"
	"github.com/concrnt/concrnt/internal/usecase"
)

// EntityMetaRepository keeps the local registration state (entity_metas), a
// collection keyed by ccid with no ties to the record store.
type EntityMetaRepository struct {
	client *firestore.Client
}

func NewEntityMetaRepository(client *firestore.Client) usecase.EntityMetaRepository {
	return &EntityMetaRepository{client: client}
}

func (r *EntityMetaRepository) col() *firestore.CollectionRef {
	return r.client.Collection(ColEntityMetas)
}

// SaveMeta upserts inviter and info (cDate is kept from the first write).
func (r *EntityMetaRepository) SaveMeta(ctx context.Context, meta domain.EntityMeta) error {
	ctx, span := tracer.Start(ctx, "Repository.Register")
	defer span.End()

	err := upsert(ctx, r.col().Doc(meta.ID), map[string]any{
		"inviter": deref(meta.Inviter),
		"info":    meta.Info,
		"mDate":   firestore.ServerTimestamp,
	})
	if err != nil {
		span.RecordError(err)
	}
	return err
}

func (r *EntityMetaRepository) GetMeta(ctx context.Context, ccid string) (*domain.EntityMeta, error) {
	ctx, span := tracer.Start(ctx, "Repository.GetMeta")
	defer span.End()

	snap, err := r.col().Doc(ccid).Get(ctx)
	if err != nil {
		if IsNotFound(err) {
			return nil, domain.NotFoundError{Resource: ccid}
		}
		span.RecordError(err)
		return nil, err
	}
	var meta entityMetaDoc
	if err := decode(snap, &meta); err != nil {
		return nil, err
	}
	return &domain.EntityMeta{ID: ccid, Inviter: strPtr(meta.Inviter), Info: meta.Info}, nil
}

// UpdateMetaInfo replaces only info, preserving inviter.
func (r *EntityMetaRepository) UpdateMetaInfo(ctx context.Context, ccid string, info string) error {
	ctx, span := tracer.Start(ctx, "Repository.UpdateMetaInfo")
	defer span.End()

	_, err := r.col().Doc(ccid).Update(ctx, []firestore.Update{
		{Path: "info", Value: info},
		{Path: "mDate", Value: firestore.ServerTimestamp},
	})
	if err != nil {
		if IsNotFound(err) {
			return domain.NotFoundError{Resource: "entity meta"}
		}
		span.RecordError(err)
		return err
	}
	return nil
}

func (r *EntityMetaRepository) DeleteMeta(ctx context.Context, ccid string) error {
	ctx, span := tracer.Start(ctx, "Repository.DeleteMeta")
	defer span.End()

	if _, err := r.col().Doc(ccid).Delete(ctx); err != nil {
		span.RecordError(err)
		return err
	}
	return nil
}

func (r *EntityMetaRepository) ListMetas(ctx context.Context, owner string) ([]domain.EntityMeta, error) {
	ctx, span := tracer.Start(ctx, "Repository.ListMetas")
	defer span.End()

	result := []domain.EntityMeta{}
	if owner != "" {
		meta, err := r.GetMeta(ctx, owner)
		if errors.Is(err, domain.ErrNotFound) {
			return result, nil
		}
		if err != nil {
			return nil, err
		}
		return append(result, *meta), nil
	}

	it := r.col().Documents(ctx)
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
		result = append(result, domain.EntityMeta{ID: snap.Ref.ID, Inviter: strPtr(meta.Inviter), Info: meta.Info})
	}
	return result, nil
}
