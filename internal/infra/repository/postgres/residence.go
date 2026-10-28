package postgres

import (
	"context"
	"encoding/json"
	"errors"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/client"
	"github.com/concrnt/concrnt/internal/domain"
	"github.com/concrnt/concrnt/internal/infra/database/models"
	"github.com/concrnt/concrnt/internal/usecase/residence"
)

type ResidenceRepository struct {
	db     *gorm.DB
	client *client.Client
	config domain.Config
}

func NewResidenceRepository(db *gorm.DB, cl *client.Client, config domain.Config) residence.Repository {
	return &ResidenceRepository{db: db, client: cl, config: config}
}

func (r *ResidenceRepository) SaveMeta(ctx context.Context, meta domain.EntityMeta) error {
	ctx, span := tracer.Start(ctx, "ResidenceRepository.Register")
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

func (r *ResidenceRepository) GetMeta(ctx context.Context, ccid string) (*domain.EntityMeta, error) {
	ctx, span := tracer.Start(ctx, "ResidenceRepository.GetMeta")
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
func (r *ResidenceRepository) UpdateMetaInfo(ctx context.Context, ccid string, info string) error {
	ctx, span := tracer.Start(ctx, "ResidenceRepository.UpdateMetaInfo")
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

func (r *ResidenceRepository) DeleteMeta(ctx context.Context, ccid string) error {
	ctx, span := tracer.Start(ctx, "ResidenceRepository.DeleteMeta")
	defer span.End()

	if err := r.db.WithContext(ctx).Where("id = ?", ccid).Delete(&models.EntityMeta{}).Error; err != nil {
		span.RecordError(err)
		return err
	}

	return nil
}

// MarkCommitLogsGcCandidateByOwner flags every commit log owned by the given
// ccid as a GC candidate. Every commit has exactly one owner (an ack between
// two local users is two commits: the ack owned by the acker and the acked
// owned by the target), so nothing another user holds is touched. Idempotent.
func (r *ResidenceRepository) MarkCommitLogsGcCandidateByOwner(ctx context.Context, owner string) error {
	ctx, span := tracer.Start(ctx, "ResidenceRepository.MarkCommitLogsGcCandidateByOwner")
	defer span.End()

	err := r.db.WithContext(ctx).
		Model(&models.CommitLog{}).
		Where("owner = ?", owner).
		Where("NOT gc_candidate").
		Update("gc_candidate", true).Error
	if err != nil {
		span.RecordError(err)
		return err
	}

	return nil
}

func (r *ResidenceRepository) GetEntityByCCID(ctx context.Context, ccid string) (*domain.Entity, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.GetEntityByCCID")
	defer span.End()

	entity, err := gorm.G[models.Entity](r.db).
		Joins(clause.Has("Document"), nil).
		Where("entities.id = ?", ccid).
		Take(ctx)
	if err != nil {
		span.RecordError(err)
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, domain.NotFoundError{Resource: "entity"}
		}
		return nil, err
	}

	var proof concrnt.Proof
	err = json.Unmarshal([]byte(entity.Document.Proof), &proof)
	if err != nil {
		return nil, err
	}

	signedDocument := concrnt.SignedDocument{
		Document: entity.Document.Document,
		Proof:    proof,
	}

	return &domain.Entity{
		ID:             entity.ID,
		Domain:         entity.Domain,
		Alias:          entity.Alias,
		TagString:      entity.Tag,
		SignedDocument: &signedDocument,
	}, nil
}

func (r *ResidenceRepository) GetEntityByAlias(ctx context.Context, alias string) (*domain.Entity, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.GetEntityByAlias")
	defer span.End()

	entity, err := gorm.G[models.Entity](r.db).
		Joins(clause.Has("Document"), nil).
		Where("entities.alias = ?", alias).
		Take(ctx)
	if err != nil {
		span.RecordError(err)
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, domain.NotFoundError{Resource: "entity"}
		}
		return nil, err
	}

	var proof concrnt.Proof
	err = json.Unmarshal([]byte(entity.Document.Proof), &proof)
	if err != nil {
		return nil, err
	}

	signedDocument := concrnt.SignedDocument{
		Document: entity.Document.Document,
		Proof:    proof,
	}

	return &domain.Entity{
		ID:             entity.ID,
		Domain:         entity.Domain,
		Alias:          entity.Alias,
		TagString:      entity.Tag,
		SignedDocument: &signedDocument,
	}, nil
}

func (r *ResidenceRepository) ListMetas(ctx context.Context, owner string) ([]domain.EntityMeta, error) {
	ctx, span := tracer.Start(ctx, "ResidenceRepository.ListMetas")
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
