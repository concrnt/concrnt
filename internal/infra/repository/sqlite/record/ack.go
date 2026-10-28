package record

import (
	"context"
	"errors"
	"time"

	sqlitedb "github.com/concrnt/concrnt/internal/infra/repository/sqlite"
	ucrecord "github.com/concrnt/concrnt/internal/usecase/record"
)

// saveAckState is the accept-if-newer upsert shared by the acks and ackeds
// tables (CIP-10 §4): only a document with a strictly newer createdAt moves
// the (from, to, schema) state; an equal or older one changes nothing.
func saveAckState(ctx context.Context, tx ucrecord.RepositoryTx, table, documentID, from, to, schema string, createdAt time.Time, valid bool) (bool, error) {
	db, err := getRecordTx(tx)
	if err != nil {
		return false, err
	}
	res, err := db.ExecContext(ctx, `INSERT INTO `+table+` ("from", "to", schema, document_id, valid, created_at, c_date) VALUES (?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT ("from", "to", schema) DO UPDATE SET valid = excluded.valid, document_id = excluded.document_id, created_at = excluded.created_at
		WHERE `+table+`.created_at < excluded.created_at`,
		from, to, schema, documentID, valid, sqlitedb.Micros(createdAt), nowMicros())
	if err != nil {
		return false, err
	}
	affected, err := res.RowsAffected()
	return affected > 0, err
}

func (r *Repository) processAck(ctx context.Context, tx ucrecord.RepositoryTx, documentID string, from string, to string, schema string, createdAt time.Time, valid bool) (bool, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.processAck")
	defer span.End()

	applied, err := saveAckState(ctx, tx, "acks", documentID, from, to, schema, createdAt, valid)
	if err != nil {
		span.RecordError(err)
	}
	return applied, err
}

func (r *Repository) Acknowledge(ctx context.Context, tx ucrecord.RepositoryTx, documentID string, from string, to string, schema string, createdAt time.Time) (bool, error) {
	return r.processAck(ctx, tx, documentID, from, to, schema, createdAt, true)
}

func (r *Repository) UnAcknowledge(ctx context.Context, tx ucrecord.RepositoryTx, documentID string, from string, to string, schema string, createdAt time.Time) (bool, error) {
	return r.processAck(ctx, tx, documentID, from, to, schema, createdAt, false)
}

func (r *Repository) processAcked(ctx context.Context, tx ucrecord.RepositoryTx, documentID string, from string, to string, schema string, createdAt time.Time, valid bool) (bool, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.processAcked")
	defer span.End()

	applied, err := saveAckState(ctx, tx, "ackeds", documentID, from, to, schema, createdAt, valid)
	if err != nil {
		span.RecordError(err)
	}
	return applied, err
}

func (r *Repository) Acknowledged(ctx context.Context, tx ucrecord.RepositoryTx, documentID string, from string, to string, schema string, createdAt time.Time) (bool, error) {
	return r.processAcked(ctx, tx, documentID, from, to, schema, createdAt, true)
}

func (r *Repository) UnAcknowledged(ctx context.Context, tx ucrecord.RepositoryTx, documentID string, from string, to string, schema string, createdAt time.Time) (bool, error) {
	return r.processAcked(ctx, tx, documentID, from, to, schema, createdAt, false)
}

// ackRecords lists the valid state rows of one side: table acks keyed by
// "from" (the ccfs namespace is the acker's), table ackeds keyed by "to"
// (the associate owner's).
func (r *Repository) ackRecords(ctx context.Context, table, sideColumn, side, schema string, since, until *time.Time, limit int, order string) ([]ucrecord.QueryRow, error) {
	query := `SELECT s.document_id, s."` + sideColumn + `", s.created_at, c.document, c.proof FROM ` + table + ` s
		JOIN commit_logs c ON c.id = s.document_id
		WHERE s."` + sideColumn + `" = ? AND s.valid = 1`
	args := []any{side}
	if schema != "" {
		query += " AND s.schema = ?"
		args = append(args, schema)
	}
	if since != nil {
		query += " AND s.created_at >= ?"
		args = append(args, sqlitedb.Micros(*since))
	}
	if until != nil {
		query += " AND s.created_at <= ?"
		args = append(args, sqlitedb.Micros(*until))
	}
	if order == "desc" {
		query += " ORDER BY s.created_at DESC, s.document_id DESC"
	} else {
		query += " ORDER BY s.created_at ASC, s.document_id ASC"
	}
	if limit > 0 {
		query += " LIMIT ?"
		args = append(args, limit)
	}
	return scanCommitRows(ctx, r.db, query, args...)
}

func (r *Repository) GetAcknowledgeRecords(ctx context.Context, from, to, schema string, since, until *time.Time, limit int, order string) ([]ucrecord.QueryRow, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.GetAcknowledgeRecords")
	defer span.End()

	var rows []ucrecord.QueryRow
	var err error
	switch {
	case from != "":
		rows, err = r.ackRecords(ctx, "acks", "from", from, schema, since, until, limit, order)
	case to != "":
		rows, err = r.ackRecords(ctx, "ackeds", "to", to, schema, since, until, limit, order)
	default:
		err = errors.New("either 'from' or 'to' must be specified")
	}
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	return rows, nil
}

func (r *Repository) ackCounts(ctx context.Context, table, sideColumn, side, schema string) (map[string]int64, error) {
	query := `SELECT schema, COUNT(*) FROM ` + table + ` WHERE "` + sideColumn + `" = ? AND valid = 1`
	args := []any{side}
	if schema != "" {
		query += " AND schema = ?"
		args = append(args, schema)
	}
	rows, err := r.db.QueryContext(ctx, query+" GROUP BY schema", args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	counts := map[string]int64{}
	for rows.Next() {
		var s string
		var n int64
		if err := rows.Scan(&s, &n); err != nil {
			return nil, err
		}
		counts[s] = n
	}
	return counts, rows.Err()
}

func (r *Repository) ComputeAcknowledgeCounts(ctx context.Context, from, to, schema string) (map[string]int64, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.ComputeAcknowledgeCounts")
	defer span.End()

	var counts map[string]int64
	var err error
	switch {
	case from != "":
		counts, err = r.ackCounts(ctx, "acks", "from", from, schema)
	case to != "":
		counts, err = r.ackCounts(ctx, "ackeds", "to", to, schema)
	default:
		err = errors.New("either 'from' or 'to' must be specified")
	}
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	return counts, nil
}
