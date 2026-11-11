package record

import (
	"context"
	"errors"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/internal/domain"
	"github.com/concrnt/concrnt/internal/infra/repository/postgres"
	"github.com/concrnt/concrnt/internal/infra/repository/repoutil"
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

		modelEntity := postgres.Entity{
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
		result := tx.Clauses(clause.OnConflict{
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
	})
	if err != nil {
		span.RecordError(err)
	}
	return status, err
}

// MarkCommitLogsGcCandidateByOwner flags every commit log owned by the given
// ccid as a GC candidate. Every commit has exactly one owner (an ack between
// two local users is two commits: the ack owned by the acker and the acked
// owned by the target), so nothing another user holds is touched. Idempotent.
func (r *Repository) MarkCommitLogsGcCandidateByOwner(ctx context.Context, owner string) error {
	ctx, span := tracer.Start(ctx, "Repository.MarkCommitLogsGcCandidateByOwner")
	defer span.End()

	err := r.db.WithContext(ctx).
		Model(&postgres.CommitLog{}).
		Where("owner = ?", owner).
		Where("NOT gc_candidate").
		Update("gc_candidate", true).Error
	if err != nil {
		span.RecordError(err)
		return err
	}

	return nil
}

// getEntity reads one entity row with its commit log.
func (r *Repository) getEntity(ctx context.Context, where string, arg any) (*domain.Entity, error) {
	entity, err := gorm.G[postgres.Entity](r.db).
		Joins(clause.Has("Document"), nil).
		Where(where, arg).
		Take(ctx)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, domain.NotFoundError{Resource: "entity"}
		}
		return nil, err
	}
	proof, err := repoutil.ParseProof(entity.Document.Proof)
	if err != nil {
		return nil, err
	}
	return &domain.Entity{
		ID:             entity.ID,
		Domain:         entity.Domain,
		Alias:          entity.Alias,
		TagString:      entity.Tag,
		SignedDocument: &concrnt.SignedDocument{Document: entity.Document.Document, Proof: proof},
	}, nil
}

func (r *Repository) GetEntityByCCID(ctx context.Context, ccid string) (*domain.Entity, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.GetEntityByCCID")
	defer span.End()

	entity, err := r.getEntity(ctx, "entities.id = ?", ccid)
	if err != nil {
		span.RecordError(err)
	}
	return entity, err
}

func (r *Repository) GetEntityByAlias(ctx context.Context, alias string) (*domain.Entity, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.GetEntityByAlias")
	defer span.End()

	entity, err := r.getEntity(ctx, "entities.alias = ?", alias)
	if err != nil {
		span.RecordError(err)
	}
	return entity, err
}
