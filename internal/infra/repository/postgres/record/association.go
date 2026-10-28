package record

import (
	"context"
	"encoding/json"
	"time"

	"gorm.io/gorm/clause"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/internal/infra/database/models"
	ucrecord "github.com/concrnt/concrnt/internal/usecase/record"
	"github.com/concrnt/concrnt/internal/utils"
)

func (r *Repository) CreateAssociation(ctx context.Context, tx ucrecord.RepositoryTx, documentID string, targetURI string, owner string, author string, schema string, variant *string, unique string, createdAt time.Time) (bool, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.CreateAssociation")
	defer span.End()

	db, err := getRecordTx(ctx, tx)
	if err != nil {
		span.RecordError(err)
		return false, err
	}

	targetRK, err := GetRecordKeyByURI(ctx, db, targetURI)
	if err != nil {
		span.RecordError(err)
		return false, err
	}

	association := models.Association{
		TargetID:   targetRK.ID,
		DocumentID: documentID,
		Unique:     unique,

		Owner:     owner,
		Author:    author,
		Variant:   variant,
		Schema:    schema,
		CreatedAt: createdAt,
	}

	// A targetless ON CONFLICT DO NOTHING suppresses duplicates on both the
	// document_id primary key (same document re-delivered) and
	// uni_associations_unique (same logical association under a new document
	// id), making duplicate deliveries a silent success like record/ack.
	result := db.Clauses(clause.OnConflict{
		DoNothing: true,
	}).Create(&association)
	if result.Error != nil {
		span.RecordError(result.Error)
		return false, result.Error
	}

	return result.RowsAffected > 0, nil
}

func (r *Repository) DeleteAssociation(ctx context.Context, tx ucrecord.RepositoryTx, documentID string) error {
	ctx, span := tracer.Start(ctx, "Repository.Record.DeleteAssociation")
	defer span.End()

	db, err := getRecordTx(ctx, tx)
	if err != nil {
		span.RecordError(err)
		return err
	}

	return db.Delete(&models.Association{}, "document_id = ?", documentID).Error
}

func (r *Repository) GetAssociatedRecords(
	ctx context.Context,
	targetURI, schema, variant, author string,
	since, until *time.Time,
	limit int,
	order string,
) ([]ucrecord.QueryRow, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.GetAssociatedRecords")
	defer span.End()

	var associations []models.Association

	query := r.db.WithContext(ctx).
		Model(&models.Association{}).
		Preload("Document").
		Joins("JOIN record_keys rk ON rk.id = associations.target_id").
		Where("rk.uri = ?", targetURI)

	if schema != "" {
		query = query.Where("associations.schema = ?", schema)
	}
	if variant != "" {
		query = query.Where("associations.variant = ?", variant)
	}
	if author != "" {
		query = query.Where("associations.author = ?", author)
	}
	if since != nil {
		query = query.Where("associations.created_at >= ?", *since)
	}
	if until != nil {
		query = query.Where("associations.created_at <= ?", *until)
	}

	if order == "desc" {
		query = query.Order("associations.created_at DESC, associations.document_id DESC")
	} else {
		query = query.Order("associations.created_at ASC, associations.document_id ASC")
	}

	if limit > 0 {
		query = query.Limit(limit)
	}

	if err := query.Find(&associations).Error; err != nil {
		return nil, err
	}

	rows := make([]ucrecord.QueryRow, len(associations))
	for i, assoc := range associations {
		var proof concrnt.Proof
		err := json.Unmarshal([]byte(assoc.Document.Proof), &proof)
		if err != nil {
			span.RecordError(err)
			return nil, err
		}

		ccfs := concrnt.ComposeCCFSURI(assoc.Owner, concrnt.CCFSTypeConcrnt, assoc.DocumentID)

		rows[i] = ucrecord.QueryRow{
			Row: concrnt.SignedDocument{
				CCFS:     &ccfs,
				Document: assoc.Document.Document,
				Proof:    proof,
			},
			CreatedAt: assoc.CreatedAt,
		}
	}

	return rows, nil
}

func (r *Repository) GetAssociatedRecordCountsBySchema(ctx context.Context, targetURI string) (map[string]int64, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.GetAssociatedRecordCountsBySchema")
	defer span.End()

	var counts []struct {
		Schema string
		Count  int64
	}

	err := r.db.WithContext(ctx).
		Model(&models.Association{}).
		Select("schema, COUNT(*) AS count").
		Joins("JOIN record_keys rk ON rk.id = associations.target_id").
		Where("rk.uri = ?", targetURI).
		Group("schema").
		Scan(&counts).Error

	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	result := make(map[string]int64)
	for _, c := range counts {
		result[c.Schema] = c.Count
	}

	return result, nil
}

func (r *Repository) GetAssociatedRecordCountsByVariant(ctx context.Context, targetURI, schema string) (*utils.OrderedKVMap[int64], error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.GetAssociatedRecordCountsByVariant")
	defer span.End()

	var counts []struct {
		Variant  string
		Count    int64
		MinCDate time.Time
	}

	err := r.db.WithContext(ctx).
		Model(&models.Association{}).
		Select("variant, COUNT(*) AS count, MIN(created_at) AS min_created_at").
		Joins("JOIN record_keys rk ON rk.id = associations.target_id").
		Where("rk.uri = ?", targetURI).
		Where("associations.schema = ?", schema).
		Group("variant").
		Order("min_created_at ASC").
		Scan(&counts).Error
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	result := make(utils.OrderedKVMap[int64])
	for _, c := range counts {
		result[c.Variant] = utils.OrderedKV[int64]{
			Value: c.Count,
			Order: c.MinCDate.UnixNano(),
		}
	}

	return &result, nil
}
