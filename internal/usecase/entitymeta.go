package usecase

import (
	"context"

	"github.com/concrnt/concrnt/internal/domain"
)

// EntityMetaRepository stores the local registration state of entities
// (inviter, info). It is keyed by ccid only and never joined with the record
// store, so it may live on a different backend; record consults it for the
// residency check, residence owns it.
type EntityMetaRepository interface {
	Save(ctx context.Context, meta domain.EntityMeta) error
	Get(ctx context.Context, ccid string) (*domain.EntityMeta, error)
	Update(ctx context.Context, ccid string, info string) error
	Delete(ctx context.Context, ccid string) error
	List(ctx context.Context, owner string) ([]domain.EntityMeta, error)
}
