package postgres

import (
	"context"
	"errors"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/concrnt/concrnt/internal/domain"
	"github.com/concrnt/concrnt/internal/infra/database/models"
	"github.com/concrnt/concrnt/internal/usecase"
)

// EntityMetaRepository keeps the local registration state (entity_metas), a
// table keyed by ccid with no ties to the record store.
type EntityMetaRepository struct {
	db *gorm.DB
}

func NewEntityMetaRepository(db *gorm.DB) usecase.EntityMetaRepository {
	return &EntityMetaRepository{db: db}
}

func (r *EntityMetaRepository) SaveMeta(ctx context.Context, meta domain.EntityMeta) error {
	ctx, span := tracer.Start(ctx, "Repository.Register")
	defer span.End()

	modelMeta := models.EntityMeta{
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
func (r *EntityMetaRepository) GetMeta(ctx context.Context, ccid string) (*domain.EntityMeta, error) {
	ctx, span := tracer.Start(ctx, "Repository.GetMeta")
	defer span.End()

	var meta models.EntityMeta
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

// UpdateMetaInfo replaces only the info column, preserving inviter (unlike
// SaveMeta, whose upsert overwrites both).
func (r *EntityMetaRepository) UpdateMetaInfo(ctx context.Context, ccid string, info string) error {
	ctx, span := tracer.Start(ctx, "Repository.UpdateMetaInfo")
	defer span.End()

	result := r.db.WithContext(ctx).Model(&models.EntityMeta{}).Where("id = ?", ccid).Update("info", info)
	if result.Error != nil {
		span.RecordError(result.Error)
		return result.Error
	}
	if result.RowsAffected == 0 {
		return domain.NotFoundError{Resource: "entity meta"}
	}

	return nil
}
func (r *EntityMetaRepository) DeleteMeta(ctx context.Context, ccid string) error {
	ctx, span := tracer.Start(ctx, "Repository.DeleteMeta")
	defer span.End()

	if err := r.db.WithContext(ctx).Where("id = ?", ccid).Delete(&models.EntityMeta{}).Error; err != nil {
		span.RecordError(err)
		return err
	}

	return nil
}
func (r *EntityMetaRepository) ListMetas(ctx context.Context, owner string) ([]domain.EntityMeta, error) {
	ctx, span := tracer.Start(ctx, "Repository.ListMetas")
	defer span.End()

	var metas []models.EntityMeta
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
