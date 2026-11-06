package record

import (
	"context"
	"errors"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/internal/domain"
	"github.com/concrnt/concrnt/internal/infra/repository/repoutil"
	sqlitedb "github.com/concrnt/concrnt/internal/infra/repository/sqlite"
	ucrecord "github.com/concrnt/concrnt/internal/usecase/record"
)

func (r *Repository) CreateEntity(
	ctx context.Context,
	tx ucrecord.RepositoryTx,
	ccid string,
	alias *string,
	domain string,
	documentID string,
	createdAt time.Time,
) (bool, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.CreateEntity")
	defer span.End()

	db, err := getRecordTx(ctx, tx)
	if err != nil {
		span.RecordError(err)
		return false, err
	}

	modelEntity := sqlitedb.Entity{
		ID:         ccid,
		Alias:      alias,
		Domain:     domain,
		DocumentID: documentID,
		CreatedAt:  sqlitedb.Micros(createdAt),
	}

	// CIP-3 §3.4 accept-if-newer on the document's createdAt: only a strictly
	// newer document replaces the stored one; an equal or older one is a
	// no-op (RowsAffected 0). The conditional upsert makes newer-wins atomic
	// under the writer lock, so concurrent writers converge on the same winner.
	result := db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "id"}},
		DoUpdates: clause.AssignmentColumns([]string{"alias", "domain", "document_id", "created_at", "m_date"}),
		Where:     clause.Where{Exprs: []clause.Expression{gorm.Expr("entities.created_at < excluded.created_at")}},
	}).Create(&modelEntity)
	if result.Error != nil {
		return false, result.Error
	}

	return result.RowsAffected > 0, nil
}

// MarkCommitLogsGcCandidateByOwner flags every commit log owned by the given
// ccid as a GC candidate. Every commit has exactly one owner (an ack between
// two local users is two commits: the ack owned by the acker and the acked
// owned by the target), so nothing another user holds is touched. Idempotent.
func (r *Repository) MarkCommitLogsGcCandidateByOwner(ctx context.Context, owner string) error {
	ctx, span := tracer.Start(ctx, "Repository.MarkCommitLogsGcCandidateByOwner")
	defer span.End()

	err := r.db.WithContext(ctx).
		Model(&sqlitedb.CommitLog{}).
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
	entity, err := gorm.G[sqlitedb.Entity](r.db).
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
