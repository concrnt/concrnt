package entitymeta

import (
	"context"
	"errors"

	"go.opentelemetry.io/otel"
	"gorm.io/gorm"

	"github.com/concrnt/concrnt/internal/domain"
	"github.com/concrnt/concrnt/internal/usecase"
)

var tracer = otel.Tracer("repository")

type Repository struct {
	db *gorm.DB
}

var _ usecase.EntityMetaRepository = (*Repository)(nil)

func New(db *gorm.DB) *Repository {
	return &Repository{db: db}
}

func (r *Repository) Migrate() error {
	return r.db.AutoMigrate(&EntityMeta{})
}

func (r *Repository) Save(ctx context.Context, meta domain.EntityMeta) error {
	ctx, span := tracer.Start(ctx, "Repository.EntityMeta.Save")
	defer span.End()

	// info is jsonb: an empty value is stored as JSON null
	info := meta.Info
	if info == "" {
		info = "null"
	}

	err := gorm.G[any](r.db).Exec(ctx, `
INSERT INTO entity_meta (id, inviter, info, m_date)
VALUES (?, ?, ?, now())
ON CONFLICT (id) DO UPDATE
SET inviter = excluded.inviter,
    info = excluded.info`,
		meta.ID, meta.Inviter, info)
	if err != nil {
		span.RecordError(err)
		return err
	}

	return nil
}

func (r *Repository) Get(ctx context.Context, ccid string) (*domain.EntityMeta, error) {
	ctx, span := tracer.Start(ctx, "Repository.EntityMeta.Get")
	defer span.End()

	meta, err := gorm.G[EntityMeta](r.db).Where("id = ?", ccid).Take(ctx)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, domain.NotFoundError{Resource: ccid}
		}
		return nil, err
	}

	return &domain.EntityMeta{
		ID:      meta.ID,
		Inviter: meta.Inviter,
		Info:    meta.Info,
	}, nil
}

// Update replaces only the info column, preserving inviter (unlike Save,
// whose upsert overwrites both).
func (r *Repository) Update(ctx context.Context, ccid string, info string) error {
	ctx, span := tracer.Start(ctx, "Repository.EntityMeta.Update")
	defer span.End()

	rowsAffected, err := gorm.G[EntityMeta](r.db).Where("id = ?", ccid).Update(ctx, "info", info)
	if err != nil {
		span.RecordError(err)
		return err
	}
	if rowsAffected == 0 {
		return domain.NotFoundError{Resource: "entity meta"}
	}

	return nil
}

func (r *Repository) Delete(ctx context.Context, ccid string) error {
	ctx, span := tracer.Start(ctx, "Repository.EntityMeta.Delete")
	defer span.End()

	if _, err := gorm.G[EntityMeta](r.db).Where("id = ?", ccid).Delete(ctx); err != nil {
		span.RecordError(err)
		return err
	}

	return nil
}

func (r *Repository) List(ctx context.Context, owner string) ([]domain.EntityMeta, error) {
	ctx, span := tracer.Start(ctx, "Repository.EntityMeta.List")
	defer span.End()

	var metas []EntityMeta
	var err error
	if owner != "" {
		metas, err = gorm.G[EntityMeta](r.db).Where("id = ?", owner).Find(ctx)
	} else {
		metas, err = gorm.G[EntityMeta](r.db).Find(ctx)
	}
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	result := make([]domain.EntityMeta, 0, len(metas))
	for _, m := range metas {
		result = append(result, domain.EntityMeta{
			ID:      m.ID,
			Inviter: m.Inviter,
			Info:    m.Info,
		})
	}
	return result, nil
}
