package record

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/concrnt/concrnt"
	ucrecord "github.com/concrnt/concrnt/internal/usecase/record"
)

func (r *Repository) processAck(ctx context.Context, tx ucrecord.RepositoryTx, documentID string, from string, to string, schema string, createdAt time.Time, valid bool) (bool, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.processAck")
	defer span.End()

	db, err := getRecordTx(tx)
	if err != nil {
		return false, err
	}

	// CIP-10 §4: only a document with a strictly newer createdAt may move
	// the (from, to, schema) state; an equal or older one is a no-op
	// (RowsAffected 0). The key is createdAt alone — never the document id —
	// so the acker's and the associate owner's servers, which hold different
	// documents with the same createdAt, reach the same decision.
	result := gorm.WithResult()
	err = gorm.G[any](db, result).Exec(ctx, `
INSERT INTO acks ("from", "to", schema, document_id, valid, created_at)
VALUES (?, ?, ?, ?, ?, ?)
ON CONFLICT ("from", "to", schema) DO UPDATE
SET valid = excluded.valid,
    document_id = excluded.document_id,
    created_at = excluded.created_at
WHERE acks.created_at < excluded.created_at`,
		from, to, schema, documentID, valid, createdAt)
	if err != nil {
		span.RecordError(err)
		return false, err
	}

	return result.RowsAffected > 0, nil

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

	db, err := getRecordTx(tx)
	if err != nil {
		return false, err
	}

	// CIP-10 §4: only a document with a strictly newer createdAt may move
	// the (from, to, schema) state; an equal or older one is a no-op
	// (RowsAffected 0). The key is createdAt alone — never the document id —
	// so the acker's and the associate owner's servers, which hold different
	// documents with the same createdAt, reach the same decision.
	result := gorm.WithResult()
	err = gorm.G[any](db, result).Exec(ctx, `
INSERT INTO ackeds ("from", "to", schema, document_id, valid, created_at)
VALUES (?, ?, ?, ?, ?, ?)
ON CONFLICT ("from", "to", schema) DO UPDATE
SET valid = excluded.valid,
    document_id = excluded.document_id,
    created_at = excluded.created_at
WHERE ackeds.created_at < excluded.created_at`,
		from, to, schema, documentID, valid, createdAt)
	if err != nil {
		span.RecordError(err)
		return false, err
	}

	return result.RowsAffected > 0, nil

}

func (r *Repository) Acknowledged(ctx context.Context, tx ucrecord.RepositoryTx, documentID string, from string, to string, schema string, createdAt time.Time) (bool, error) {
	return r.processAcked(ctx, tx, documentID, from, to, schema, createdAt, true)

}

func (r *Repository) UnAcknowledged(ctx context.Context, tx ucrecord.RepositoryTx, documentID string, from string, to string, schema string, createdAt time.Time) (bool, error) {
	return r.processAcked(ctx, tx, documentID, from, to, schema, createdAt, false)
}

func (r *Repository) getAckRecords(ctx context.Context, from, schema string, since, until *time.Time, limit int, order string) ([]ucrecord.QueryRow, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.getAckRecords")
	defer span.End()

	query := gorm.G[Ack](r.db).
		Joins(clause.LeftJoin.Association("Document"), nil).
		Where("acks.from = ?", from).
		Where("acks.valid = ?", true)

	if schema != "" {
		query = query.Where("acks.schema = ?", schema)
	}

	if since != nil {
		query = query.Where("acks.created_at >= ?", *since)
	}
	if until != nil {
		query = query.Where("acks.created_at <= ?", *until)
	}

	if order == "desc" {
		query = query.Order("acks.created_at DESC, acks.document_id DESC")
	} else {
		query = query.Order("acks.created_at ASC, acks.document_id ASC")
	}

	if limit > 0 {
		query = query.Limit(limit)
	}

	acks, err := query.Find(ctx)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	rows := make([]ucrecord.QueryRow, len(acks))
	for i, ack := range acks {
		var proof concrnt.Proof
		err := json.Unmarshal([]byte(ack.Document.Proof), &proof)
		if err != nil {
			span.RecordError(err)
			return nil, err
		}

		ccfs := concrnt.ComposeCCFSURI(ack.From, concrnt.CCFSTypeConcrnt, ack.DocumentID)

		rows[i] = ucrecord.QueryRow{
			Row: concrnt.SignedDocument{
				CCFS:     &ccfs,
				Document: ack.Document.Document,
				Proof:    proof,
			},
			CreatedAt: ack.CreatedAt,
		}
	}

	return rows, nil
}

func (r *Repository) getAckedRecords(ctx context.Context, to, schema string, since, until *time.Time, limit int, order string) ([]ucrecord.QueryRow, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.getAckedRecords")
	defer span.End()

	query := gorm.G[Acked](r.db).
		Joins(clause.LeftJoin.Association("Document"), nil).
		Where("ackeds.to = ?", to).
		Where("ackeds.valid = ?", true)

	if schema != "" {
		query = query.Where("ackeds.schema = ?", schema)
	}

	if since != nil {
		query = query.Where("ackeds.created_at >= ?", *since)
	}
	if until != nil {
		query = query.Where("ackeds.created_at <= ?", *until)
	}

	if order == "desc" {
		query = query.Order("ackeds.created_at DESC, ackeds.document_id DESC")
	} else {
		query = query.Order("ackeds.created_at ASC, ackeds.document_id ASC")
	}

	if limit > 0 {
		query = query.Limit(limit)
	}

	acked, err := query.Find(ctx)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	rows := make([]ucrecord.QueryRow, len(acked))
	for i, ack := range acked {
		var proof concrnt.Proof
		err := json.Unmarshal([]byte(ack.Document.Proof), &proof)
		if err != nil {
			span.RecordError(err)
			return nil, err
		}

		// CIP-3 §3.4: an acked's ccfs identity is held by the associate
		// owner's server
		ccfs := concrnt.ComposeCCFSURI(ack.To, concrnt.CCFSTypeConcrnt, ack.DocumentID)

		rows[i] = ucrecord.QueryRow{
			Row: concrnt.SignedDocument{
				CCFS:     &ccfs,
				Document: ack.Document.Document,
				Proof:    proof,
			},
			CreatedAt: ack.CreatedAt,
		}
	}

	return rows, nil
}

func (r *Repository) GetAcknowledgeRecords(ctx context.Context, from, to, schema string, since, until *time.Time, limit int, order string) ([]ucrecord.QueryRow, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.GetAcknowledgeRecords")
	defer span.End()

	if from != "" {
		return r.getAckRecords(ctx, from, schema, since, until, limit, order)
	} else if to != "" {
		return r.getAckedRecords(ctx, to, schema, since, until, limit, order)
	} else {
		err := errors.New("either 'from' or 'to' must be specified")
		span.RecordError(err)
		return nil, err
	}
}

func (r *Repository) getAckRecordCounts(ctx context.Context, from, schema string) (map[string]int64, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.getAckRecordCounts")
	defer span.End()

	type result struct {
		Schema string
		Count  int64
	}

	var results []result

	query := gorm.G[Ack](r.db).
		Select("schema, COUNT(*) AS count").
		Where("valid = ?", true).
		Where("acks.from = ?", from)

	if schema != "" {
		query = query.Where("acks.schema = ?", schema)
	}

	err := query.Group("schema").Scan(ctx, &results)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	counts := make(map[string]int64)
	for _, r := range results {
		counts[r.Schema] = r.Count
	}

	return counts, nil
}

func (r *Repository) getAckedRecordCounts(ctx context.Context, to, schema string) (map[string]int64, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.getAckedRecordCounts")
	defer span.End()

	type result struct {
		Schema string
		Count  int64
	}

	var results []result

	query := gorm.G[Acked](r.db).
		Select("schema, COUNT(*) AS count").
		Where("valid = ?", true).
		Where("ackeds.to = ?", to)

	if schema != "" {
		query = query.Where("ackeds.schema = ?", schema)
	}

	err := query.Group("schema").Scan(ctx, &results)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	counts := make(map[string]int64)
	for _, r := range results {
		counts[r.Schema] = r.Count
	}

	return counts, nil
}

func (r *Repository) GetAcknowledgeRecordCounts(ctx context.Context, from, to, schema string) (map[string]int64, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.GetAcknowledgeRecordCounts")
	defer span.End()

	if from != "" {
		return r.getAckRecordCounts(ctx, from, schema)
	} else if to != "" {
		return r.getAckedRecordCounts(ctx, to, schema)
	} else {
		err := errors.New("either 'from' or 'to' must be specified")
		span.RecordError(err)
		return nil, err
	}
}
