package record

import (
	"context"
	"time"

	sqlitedb "github.com/concrnt/concrnt/internal/infra/repository/sqlite"
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

	target, err := getRecordKey(ctx, db, targetURI)
	if err != nil {
		span.RecordError(err)
		return false, err
	}

	// a bare ON CONFLICT DO NOTHING suppresses duplicates on both the
	// document_id primary key and the unique column, making duplicate
	// deliveries a silent success like record/ack
	res, err := db.ExecContext(ctx, `INSERT INTO associations (document_id, target_id, owner, author, schema, variant, "unique", created_at, c_date) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT DO NOTHING`,
		documentID, target.ID, owner, author, schema, sqlitedb.NullString(variant), unique, sqlitedb.Micros(createdAt), nowMicros())
	if err != nil {
		span.RecordError(err)
		return false, err
	}
	affected, err := res.RowsAffected()
	return affected > 0, err
}

func (r *Repository) DeleteAssociation(ctx context.Context, tx ucrecord.RepositoryTx, documentID string) error {
	ctx, span := tracer.Start(ctx, "Repository.Record.DeleteAssociation")
	defer span.End()

	db, err := getRecordTx(tx)
	if err != nil {
		span.RecordError(err)
		return err
	}
	_, err = db.ExecContext(ctx, "DELETE FROM associations WHERE document_id = ?", documentID)
	return err
}

func (r *Repository) GetAssociatedRecords(ctx context.Context, targetURI, schema, variant, author string, since, until *time.Time, limit int, order string) ([]ucrecord.QueryRow, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.GetAssociatedRecords")
	defer span.End()

	query := `SELECT a.document_id, a.owner, a.created_at, c.document, c.proof FROM associations a
		JOIN record_keys rk ON rk.id = a.target_id
		JOIN commit_logs c ON c.id = a.document_id
		WHERE rk.uri = ?`
	args := []any{targetURI}
	if schema != "" {
		query += " AND a.schema = ?"
		args = append(args, schema)
	}
	if variant != "" {
		query += " AND a.variant = ?"
		args = append(args, variant)
	}
	if author != "" {
		query += " AND a.author = ?"
		args = append(args, author)
	}
	if since != nil {
		query += " AND a.created_at >= ?"
		args = append(args, sqlitedb.Micros(*since))
	}
	if until != nil {
		query += " AND a.created_at <= ?"
		args = append(args, sqlitedb.Micros(*until))
	}
	if order == "desc" {
		query += " ORDER BY a.created_at DESC, a.document_id DESC"
	} else {
		query += " ORDER BY a.created_at ASC, a.document_id ASC"
	}
	if limit > 0 {
		query += " LIMIT ?"
		args = append(args, limit)
	}

	rows, err := scanCommitRows(ctx, r.db, query, args...)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	return rows, nil
}

func (r *Repository) ComputeAssociationCountsBySchema(ctx context.Context, targetURI string) (map[string]int64, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.ComputeAssociationCountsBySchema")
	defer span.End()

	rows, err := r.db.QueryContext(ctx, "SELECT a.schema, COUNT(*) FROM associations a JOIN record_keys rk ON rk.id = a.target_id WHERE rk.uri = ? GROUP BY a.schema", targetURI)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	defer rows.Close()

	result := map[string]int64{}
	for rows.Next() {
		var schema string
		var count int64
		if err := rows.Scan(&schema, &count); err != nil {
			return nil, err
		}
		result[schema] = count
	}
	return result, rows.Err()
}

func (r *Repository) ComputeAssociationCountsByVariant(ctx context.Context, targetURI, schema string) (*utils.OrderedKVMap[int64], error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.ComputeAssociationCountsByVariant")
	defer span.End()

	// the earliest association of each variant decides the variant order
	rows, err := r.db.QueryContext(ctx, `SELECT COALESCE(a.variant, ''), COUNT(*), MIN(a.created_at) AS min_created_at FROM associations a
		JOIN record_keys rk ON rk.id = a.target_id WHERE rk.uri = ? AND a.schema = ? GROUP BY a.variant ORDER BY min_created_at ASC`, targetURI, schema)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	defer rows.Close()

	result := utils.OrderedKVMap[int64]{}
	for rows.Next() {
		var variant string
		var count, minCreatedAt int64
		if err := rows.Scan(&variant, &count, &minCreatedAt); err != nil {
			return nil, err
		}
		result[variant] = utils.OrderedKV[int64]{Value: count, Order: sqlitedb.Time(minCreatedAt).UnixNano()}
	}
	return &result, rows.Err()
}
