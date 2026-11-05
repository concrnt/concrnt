package record

import (
	"context"
	"encoding/json"
	"time"

	"gorm.io/gorm/clause"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/internal/infra/repository/postgres"
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

	association := postgres.Association{
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

func (r *Repository) DeleteAssociation(ctx context.Context, tx ucrecord.RepositoryTx, documentID string) (bool, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.DeleteAssociation")
	defer span.End()

	db, err := getRecordTx(ctx, tx)
	if err != nil {
		span.RecordError(err)
		return false, err
	}

	result := db.Delete(&postgres.Association{}, "document_id = ?", documentID)
	if result.Error != nil {
		span.RecordError(result.Error)
		return false, result.Error
	}
	return result.RowsAffected > 0, nil
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

	query := r.db.WithContext(ctx).
		Model(&postgres.Association{}).
		Joins("JOIN record_keys rk ON rk.id = associations.target_id").
		Joins("JOIN commit_logs c ON c.id = associations.document_id").
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

	var associations []commitRow
	if err := query.Select("associations.document_id AS document_id, associations.owner AS owner, associations.created_at AS created_at, c.document AS document, c.proof AS proof").Scan(&associations).Error; err != nil {
		return nil, err
	}

	return commitRows(associations)
}

// commitRow is a state row (association, ack, acked) joined with its commit
// log; Owner is the namespace its ccfs URI is composed in.
type commitRow struct {
	DocumentID string
	Owner      string
	CreatedAt  time.Time
	Document   string
	Proof      string
}

func commitRows(rows []commitRow) ([]ucrecord.QueryRow, error) {
	out := make([]ucrecord.QueryRow, len(rows))
	for i, row := range rows {
		var proof concrnt.Proof
		if err := json.Unmarshal([]byte(row.Proof), &proof); err != nil {
			return nil, err
		}
		ccfs := concrnt.ComposeCCFSURI(row.Owner, concrnt.CCFSTypeConcrnt, row.DocumentID)
		out[i] = ucrecord.QueryRow{
			Row:       concrnt.SignedDocument{CCFS: &ccfs, Document: row.Document, Proof: proof},
			CreatedAt: row.CreatedAt,
		}
	}
	return out, nil
}

func (r *Repository) ComputeAssociationCounts(ctx context.Context, targetURI string) (map[string]utils.OrderedKVMap[int64], error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.ComputeAssociationCounts")
	defer span.End()

	// the scan target is named after the SELECT alias (gorm maps MinCreatedAt
	// to min_created_at); the earliest association of each variant decides
	// the variant order
	var counts []struct {
		Schema       string
		Variant      *string
		Count        int64
		MinCreatedAt time.Time
	}

	err := r.db.WithContext(ctx).
		Model(&postgres.Association{}).
		Select("associations.schema, associations.variant, COUNT(*) AS count, MIN(associations.created_at) AS min_created_at").
		Joins("JOIN record_keys rk ON rk.id = associations.target_id").
		Where("rk.uri = ?", targetURI).
		Group("associations.schema, associations.variant").
		Scan(&counts).Error
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	result := make(map[string]utils.OrderedKVMap[int64])
	for _, c := range counts {
		variants, ok := result[c.Schema]
		if !ok {
			variants = make(utils.OrderedKVMap[int64])
			result[c.Schema] = variants
		}
		variant := ""
		if c.Variant != nil {
			variant = *c.Variant
		}
		variants[variant] = utils.OrderedKV[int64]{
			Value: c.Count,
			Order: c.MinCreatedAt.UnixNano(),
		}
	}

	return result, nil
}

func (r *Repository) ListAssociationAuthors(ctx context.Context, targetURI string, limit int) ([]string, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.ListAssociationAuthors")
	defer span.End()

	var authors []string
	err := r.db.WithContext(ctx).
		Model(&postgres.Association{}).
		Distinct("associations.author").
		Joins("JOIN record_keys rk ON rk.id = associations.target_id").
		Where("rk.uri = ?", targetURI).
		Limit(limit).
		Pluck("associations.author", &authors).Error
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	return authors, nil
}
