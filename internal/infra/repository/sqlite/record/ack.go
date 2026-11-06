package record

import (
	"context"
	"errors"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	sqlitedb "github.com/concrnt/concrnt/internal/infra/repository/sqlite"
	ucrecord "github.com/concrnt/concrnt/internal/usecase/record"
)

// ackSide names the two sides of an ack relationship: the acker's rows
// (acks, keyed and owned by from) and the target's (ackeds, by to).
type ackSide struct {
	table string
	owner string // the column the ccfs namespace is composed from (CIP-3 §3.4)
	model any
	row   func(documentID, from, to, schema string, createdAt time.Time, valid bool) any
}

var (
	ackerSide = ackSide{"acks", "from", &sqlitedb.Ack{}, func(documentID, from, to, schema string, createdAt time.Time, valid bool) any {
		return &sqlitedb.Ack{From: from, To: to, Schema: schema, DocumentID: documentID, Valid: valid, CreatedAt: sqlitedb.Micros(createdAt)}
	}}
	targetSide = ackSide{"ackeds", "to", &sqlitedb.Acked{}, func(documentID, from, to, schema string, createdAt time.Time, valid bool) any {
		return &sqlitedb.Acked{From: from, To: to, Schema: schema, DocumentID: documentID, Valid: valid, CreatedAt: sqlitedb.Micros(createdAt)}
	}}
)

// upsertAck is the CIP-10 §4 state move: only a document with a strictly
// newer createdAt may move the (from, to, schema) state; an equal or older
// one is a no-op (RowsAffected 0). The key is createdAt alone — never the
// document id — so the acker's and the associate owner's servers, which
// hold different documents with the same createdAt, reach the same
// decision.
func (r *Repository) upsertAck(ctx context.Context, tx ucrecord.RepositoryTx, side ackSide, documentID, from, to, schema string, createdAt time.Time, valid bool) (bool, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.upsertAck")
	defer span.End()

	db, err := getRecordTx(ctx, tx)
	if err != nil {
		return false, err
	}
	result := db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "from"}, {Name: "to"}, {Name: "schema"}},
		DoUpdates: clause.Assignments(map[string]any{"valid": valid, "document_id": documentID, "created_at": sqlitedb.Micros(createdAt)}),
		Where:     clause.Where{Exprs: []clause.Expression{gorm.Expr(side.table + ".created_at < excluded.created_at")}},
	}).Create(side.row(documentID, from, to, schema, createdAt, valid))
	if result.Error != nil {
		span.RecordError(result.Error)
		return false, result.Error
	}
	return result.RowsAffected > 0, nil
}

func (r *Repository) Acknowledge(ctx context.Context, tx ucrecord.RepositoryTx, documentID string, from string, to string, schema string, createdAt time.Time) (bool, error) {
	return r.upsertAck(ctx, tx, ackerSide, documentID, from, to, schema, createdAt, true)
}

func (r *Repository) UnAcknowledge(ctx context.Context, tx ucrecord.RepositoryTx, documentID string, from string, to string, schema string, createdAt time.Time) (bool, error) {
	return r.upsertAck(ctx, tx, ackerSide, documentID, from, to, schema, createdAt, false)
}

func (r *Repository) Acknowledged(ctx context.Context, tx ucrecord.RepositoryTx, documentID string, from string, to string, schema string, createdAt time.Time) (bool, error) {
	return r.upsertAck(ctx, tx, targetSide, documentID, from, to, schema, createdAt, true)
}

func (r *Repository) UnAcknowledged(ctx context.Context, tx ucrecord.RepositoryTx, documentID string, from string, to string, schema string, createdAt time.Time) (bool, error) {
	return r.upsertAck(ctx, tx, targetSide, documentID, from, to, schema, createdAt, false)
}

// ackRecords lists the valid acks of one side owned by ccid, each with its
// commit (the side's ccfs identity is held by its owner's server).
func (r *Repository) ackRecords(ctx context.Context, side ackSide, ccid, schema string, since, until *time.Time, limit int, order string) ([]ucrecord.QueryRow, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.ackRecords")
	defer span.End()

	t := side.table
	query := r.db.WithContext(ctx).
		Model(side.model).
		Joins("JOIN commit_logs c ON c.id = "+t+".document_id").
		Where(t+`."`+side.owner+`" = ?`, ccid).
		Where(t+".valid = ?", true)
	if schema != "" {
		query = query.Where(t+".schema = ?", schema)
	}
	if since != nil {
		query = query.Where(t+".created_at >= ?", sqlitedb.Micros(*since))
	}
	if until != nil {
		query = query.Where(t+".created_at <= ?", sqlitedb.Micros(*until))
	}
	if order == "desc" {
		query = query.Order(t + ".created_at DESC, " + t + ".document_id DESC")
	} else {
		query = query.Order(t + ".created_at ASC, " + t + ".document_id ASC")
	}
	if limit > 0 {
		query = query.Limit(limit)
	}

	var rows []commitRow
	if err := query.Select(t + `.document_id AS document_id, ` + t + `."` + side.owner + `" AS owner, ` + t + `.created_at AS created_at, c.document AS document, c.proof AS proof`).Scan(&rows).Error; err != nil {
		span.RecordError(err)
		return nil, err
	}
	return commitRows(rows)
}

func (r *Repository) GetAcknowledgeRecords(ctx context.Context, from, to, schema string, since, until *time.Time, limit int, order string) ([]ucrecord.QueryRow, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.GetAcknowledgeRecords")
	defer span.End()

	switch {
	case from != "":
		return r.ackRecords(ctx, ackerSide, from, schema, since, until, limit, order)
	case to != "":
		return r.ackRecords(ctx, targetSide, to, schema, since, until, limit, order)
	default:
		err := errors.New("either 'from' or 'to' must be specified")
		span.RecordError(err)
		return nil, err
	}
}

func (r *Repository) ackCounts(ctx context.Context, side ackSide, ccid, schema string) (map[string]int64, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.ackCounts")
	defer span.End()

	t := side.table
	query := r.db.WithContext(ctx).
		Model(side.model).
		Select("schema, COUNT(*) AS count").
		Where("valid = ?", true).
		Where(t+`."`+side.owner+`" = ?`, ccid)
	if schema != "" {
		query = query.Where(t+".schema = ?", schema)
	}
	var results []struct {
		Schema string
		Count  int64
	}
	if err := query.Group("schema").Scan(&results).Error; err != nil {
		span.RecordError(err)
		return nil, err
	}
	counts := make(map[string]int64, len(results))
	for _, r := range results {
		counts[r.Schema] = r.Count
	}
	return counts, nil
}

func (r *Repository) ComputeAcknowledgeCounts(ctx context.Context, from, to, schema string) (map[string]int64, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.ComputeAcknowledgeCounts")
	defer span.End()

	switch {
	case from != "":
		return r.ackCounts(ctx, ackerSide, from, schema)
	case to != "":
		return r.ackCounts(ctx, targetSide, to, schema)
	default:
		err := errors.New("either 'from' or 'to' must be specified")
		span.RecordError(err)
		return nil, err
	}
}
