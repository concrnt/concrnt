package record

import (
	"context"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/concrnt/concrnt/internal/infra/database/models"
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

	modelEntity := models.Entity{
		ID:         ccid,
		Alias:      alias,
		Domain:     domain,
		DocumentID: documentID,
		CreatedAt:  createdAt,
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
		return false, result.Error
	}

	return result.RowsAffected > 0, nil
}
