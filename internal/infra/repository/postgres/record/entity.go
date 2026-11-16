package record

import (
	"context"
	"encoding/json"
	"errors"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/internal/domain"
	"github.com/concrnt/concrnt/internal/infra/database/models"
	ucrecord "github.com/concrnt/concrnt/internal/usecase/record"
)

func (r *Repository) ApplyEntity(ctx context.Context, commit ucrecord.EntityCommit) (ucrecord.ApplyStatus, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.ApplyEntity")
	defer span.End()

	status, err := r.apply(ctx, func(tx *gorm.DB) (ucrecord.ApplyStatus, error) {
		inserted, err := insertCommit(tx, commit.Commit)
		if err != nil {
			return ucrecord.Applied, err
		}
		if !inserted {
			return ucrecord.Duplicate, nil
		}
		return createEntity(ctx, tx, commit)
	})
	if err != nil {
		span.RecordError(err)
	}
	return status, err
}

func createEntity(ctx context.Context, db *gorm.DB, commit ucrecord.EntityCommit) (ucrecord.ApplyStatus, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.CreateEntity")
	defer span.End()

	db = db.WithContext(ctx)

	modelEntity := models.Entity{
		ID:         commit.CCID,
		Alias:      commit.Alias,
		Domain:     commit.Domain,
		DocumentID: commit.ID,
		CreatedAt:  commit.CreatedAt,
	}

	// CIP-3 §3.4 accept-if-newer on the document's createdAt: only a strictly
	// newer document replaces the stored one; an equal or older one is a
	// no-op (RowsAffected 0). The conditional upsert makes newer-wins atomic
	// at the row lock, so concurrent writers converge on the same winner.
	result := db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "id"}},
		DoUpdates: clause.AssignmentColumns([]string{"alias", "domain", "document_id", "created_at"}),
		Where:     clause.Where{Exprs: []clause.Expression{gorm.Expr("entities.created_at < excluded.created_at")}},
	}).Create(&modelEntity)
	if result.Error != nil {
		return ucrecord.Applied, result.Error
	}
	if result.RowsAffected == 0 {
		return ucrecord.Stale, nil
	}

	return ucrecord.Applied, nil
}

// MarkCommitLogsGcCandidateByOwner flags every commit log owned by the given
// ccid as a GC candidate. Every commit has exactly one owner (an ack between
// two local users is two commits: the ack owned by the acker and the acked
// owned by the target), so nothing another user holds is touched. Idempotent.
func (r *Repository) MarkCommitLogsGcCandidateByOwner(ctx context.Context, owner string) error {
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

func (r *Repository) GetEntityByCCID(ctx context.Context, ccid string) (*domain.Entity, error) {
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

func (r *Repository) GetEntityByAlias(ctx context.Context, alias string) (*domain.Entity, error) {
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
