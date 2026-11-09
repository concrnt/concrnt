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
	SaveMeta(ctx context.Context, meta domain.EntityMeta) error
	GetMeta(ctx context.Context, ccid string) (*domain.EntityMeta, error)
	UpdateMetaInfo(ctx context.Context, ccid string, info string) error
	DeleteMeta(ctx context.Context, ccid string) error
	// ListMetas returns every entity meta, or only the one for owner when
	// owner is non-empty (offline tooling).
	ListMetas(ctx context.Context, owner string) ([]domain.EntityMeta, error)
}
