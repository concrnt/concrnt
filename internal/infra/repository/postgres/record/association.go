package record

import (
	"context"
	"encoding/json"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/concrnt/concrnt"
	ucrecord "github.com/concrnt/concrnt/internal/usecase/record"
	"github.com/concrnt/concrnt/internal/utils"
)

func (r *Repository) CreateAssociation(ctx context.Context, tx ucrecord.RepositoryTx, documentID string, targetURI string, owner string, author string, schema string, variant *string, unique string, createdAt time.Time) (bool, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.CreateAssociation")
	defer span.End()

	db, err := getRecordTx(tx)
	if err != nil {
		span.RecordError(err)
		return false, err
	}

	targetRK, err := getRecordKeyByURI(ctx, db, targetURI)
	if err != nil {
		span.RecordError(err)
		return false, err
	}

	// A targetless ON CONFLICT DO NOTHING suppresses duplicates on both the
	// document_id primary key (same document re-delivered) and
	// uni_associations_unique (same logical association under a new document
	// id), making duplicate deliveries a silent success like record/ack.
	result := gorm.WithResult()
	err = gorm.G[any](db, result).Exec(ctx, `
INSERT INTO associations (document_id, target_id, owner, author, schema, variant, "unique", created_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT DO NOTHING`,
		documentID, targetRK.ID, owner, author, schema, variant, unique, createdAt)
	if err != nil {
		span.RecordError(err)
		return false, err
	}

	return result.RowsAffected > 0, nil
}

func (r *Repository) DeleteAssociation(ctx context.Context, tx ucrecord.RepositoryTx, documentID string) error {
	ctx, span := tracer.Start(ctx, "Repository.Record.DeleteAssociation")
	defer span.End()

	db, err := getRecordTx(tx)
	if err != nil {
		span.RecordError(err)
		return err
	}

	_, err = gorm.G[Association](db).Where("document_id = ?", documentID).Delete(ctx)
	return err
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

	query := gorm.G[Association](r.db).
		Joins(clause.LeftJoin.Association("Document"), nil).
		Where("associations.target_id = (SELECT id FROM record_keys WHERE uri = ?)", targetURI)

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

	associations, err := query.Find(ctx)
	if err != nil {
		span.RecordError(err)
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

	type schemaCount struct {
		Schema string
		Count  int64
	}

	counts, err := gorm.G[schemaCount](r.db).Raw(`
SELECT a.schema AS schema, COUNT(*) AS count
FROM associations a
JOIN record_keys rk ON rk.id = a.target_id
WHERE rk.uri = ?
GROUP BY a.schema`, targetURI).Find(ctx)
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

	type variantCount struct {
		Variant  string
		Count    int64
		MinCDate time.Time
	}

	counts, err := gorm.G[variantCount](r.db).Raw(`
SELECT a.variant, COUNT(*) AS count, MIN(a.created_at) AS min_created_at
FROM associations a
JOIN record_keys rk ON rk.id = a.target_id
WHERE rk.uri = ? AND a.schema = ?
GROUP BY a.variant
ORDER BY min_created_at ASC`, targetURI, schema).Find(ctx)
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
