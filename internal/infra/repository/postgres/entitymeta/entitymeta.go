package entitymeta

import (
	"context"
	"encoding/json"
	"errors"

	"go.opentelemetry.io/otel"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/client"
	"github.com/concrnt/concrnt/internal/domain"
	"github.com/concrnt/concrnt/internal/infra/database/models"
	"github.com/concrnt/concrnt/internal/usecase/residence"
)

var tracer = otel.Tracer("repository")

type Repository struct {
	db     *gorm.DB
	client *client.Client
	config domain.Config
}

func New(db *gorm.DB, cl *client.Client, config domain.Config) residence.Repository {
	return &Repository{db: db, client: cl, config: config}
}

func (r *Repository) SaveMeta(ctx context.Context, meta domain.EntityMeta) error {
	ctx, span := tracer.Start(ctx, "Repository.EntityMeta.SaveMeta")
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

func (r *Repository) GetMeta(ctx context.Context, ccid string) (*domain.EntityMeta, error) {
	ctx, span := tracer.Start(ctx, "Repository.EntityMeta.GetMeta")
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
func (r *Repository) UpdateMetaInfo(ctx context.Context, ccid string, info string) error {
	ctx, span := tracer.Start(ctx, "Repository.EntityMeta.UpdateMetaInfo")
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

func (r *Repository) DeleteMeta(ctx context.Context, ccid string) error {
	ctx, span := tracer.Start(ctx, "Repository.EntityMeta.DeleteMeta")
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
func (r *Repository) MarkCommitLogsGcCandidateByOwner(ctx context.Context, owner string) error {
	ctx, span := tracer.Start(ctx, "Repository.EntityMeta.MarkCommitLogsGcCandidateByOwner")
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

func (r *Repository) GetEntityByCCID(ctx context.Context, ccid string) (*domain.Entity, error) {
	ctx, span := tracer.Start(ctx, "Repository.EntityMeta.GetEntityByCCID")
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

func (r *Repository) GetEntityByAlias(ctx context.Context, alias string) (*domain.Entity, error) {
	ctx, span := tracer.Start(ctx, "Repository.EntityMeta.GetEntityByAlias")
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
