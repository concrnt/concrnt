package entitymeta

import (
	"context"
	"errors"

	"go.opentelemetry.io/otel"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

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

	modelMeta := EntityMeta{
		ID:      meta.ID,
		Inviter: meta.Inviter,
		Info:    meta.Info,
	}

	if err := r.db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "id"}},
		DoUpdates: clause.AssignmentColumns([]string{"inviter", "info"}),
	}).Create(&modelMeta).Error; err != nil {
		return err
	}

	return nil
}

func (r *Repository) Get(ctx context.Context, ccid string) (*domain.EntityMeta, error) {
	ctx, span := tracer.Start(ctx, "Repository.EntityMeta.Get")
	defer span.End()

	var meta EntityMeta
	err := r.db.Where("id = ?", ccid).Take(&meta).Error
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

	result := r.db.WithContext(ctx).Model(&EntityMeta{}).Where("id = ?", ccid).Update("info", info)
	if result.Error != nil {
		span.RecordError(result.Error)
		return result.Error
	}
	if result.RowsAffected == 0 {
		return domain.NotFoundError{Resource: "entity meta"}
	}

	return nil
}

func (r *Repository) Delete(ctx context.Context, ccid string) error {
	ctx, span := tracer.Start(ctx, "Repository.EntityMeta.Delete")
	defer span.End()

	if err := r.db.WithContext(ctx).Where("id = ?", ccid).Delete(&EntityMeta{}).Error; err != nil {
		span.RecordError(err)
		return err
	}

	return nil
}

func (r *Repository) List(ctx context.Context, owner string) ([]domain.EntityMeta, error) {
	ctx, span := tracer.Start(ctx, "Repository.EntityMeta.List")
	defer span.End()

	var metas []EntityMeta
	q := r.db.WithContext(ctx)
	if owner != "" {
		q = q.Where("id = ?", owner)
	}
	if err := q.Find(&metas).Error; err != nil {
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
