package record

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/internal/infra/database/models"
	ucrecord "github.com/concrnt/concrnt/internal/usecase/record"
)

func (r *Repository) ApplyAck(ctx context.Context, commit ucrecord.AckCommit) (ucrecord.AckOutcome, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.ApplyAck")
	defer span.End()

	var outcome ucrecord.AckOutcome
	status, err := r.apply(ctx, func(tx *gorm.DB) (ucrecord.ApplyStatus, error) {
		outcome = ucrecord.AckOutcome{}

		inserted, err := insertCommit(tx, commit.Commit)
		if err != nil {
			return ucrecord.Applied, err
		}
		if !inserted {
			return ucrecord.Duplicate, nil
		}
		if commit.Side == ucrecord.TargetSide {
			outcome, err = processAcked(ctx, tx, commit)
		} else {
			outcome, err = processAck(ctx, tx, commit)
		}
		return outcome.Status, err
	})
	if err != nil {
		span.RecordError(err)
		return ucrecord.AckOutcome{}, err
	}
	outcome.Status = status
	return outcome, nil
}

func (r *Repository) legacyAck(ctx context.Context, tx ucrecord.RepositoryTx, commit ucrecord.AckCommit) (bool, error) {
	db, err := getRecordTx(ctx, tx)
	if err != nil {
		return false, err
	}
	process := processAck
	if commit.Side == ucrecord.TargetSide {
		process = processAcked
	}
	outcome, err := process(ctx, db, commit)
	return outcome.Status == ucrecord.Applied, err
}

func processAck(ctx context.Context, db *gorm.DB, commit ucrecord.AckCommit) (ucrecord.AckOutcome, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.processAck")
	defer span.End()

	db = db.WithContext(ctx)

	// CIP-10 §4: only a document with a strictly newer createdAt may move
	// the (from, to, schema) state; an equal or older one is a no-op.
	// The key is createdAt alone — never the document id —
	// so the acker's and the associate owner's servers, which hold different
	// documents with the same createdAt, reach the same decision.
	// Not a conditional upsert: it cannot tell which validity it replaced.
	var state models.Ack
	found := db.Clauses(clause.Locking{Strength: "UPDATE"}).
		Where(`"from" = ? AND "to" = ? AND schema = ?`, commit.From, commit.To, commit.Schema).
		Limit(1).
		Find(&state)
	if found.Error != nil {
		span.RecordError(found.Error)
		return ucrecord.AckOutcome{}, found.Error
	}

	if found.RowsAffected == 0 {
		// Not the model: gorm would swap a false valid for the column
		// default, and an unack may be the first state.
		result := db.Model(&models.Ack{}).Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "from"}, {Name: "to"}, {Name: "schema"}},
			DoNothing: true,
		}).Create(map[string]any{
			"from": commit.From, "to": commit.To, "schema": commit.Schema,
			"document_id": commit.ID, "valid": commit.Valid, "created_at": commit.CreatedAt,
		})
		if result.Error != nil {
			span.RecordError(result.Error)
			return ucrecord.AckOutcome{}, result.Error
		}
		if result.RowsAffected == 0 {
			// a concurrent writer created the state first: start over
			// with the row there to lock
			return ucrecord.AckOutcome{}, errRetryTx
		}
		return ucrecord.AckOutcome{Status: ucrecord.Applied, IsValid: commit.Valid}, nil
	}

	if !commit.CreatedAt.After(state.CreatedAt) {
		return ucrecord.AckOutcome{Status: ucrecord.Stale, WasValid: state.Valid, IsValid: state.Valid}, nil
	}

	err := db.Model(&models.Ack{}).
		Where(`"from" = ? AND "to" = ? AND schema = ?`, commit.From, commit.To, commit.Schema).
		Updates(map[string]any{"valid": commit.Valid, "document_id": commit.ID, "created_at": commit.CreatedAt}).Error
	if err != nil {
		span.RecordError(err)
		return ucrecord.AckOutcome{}, err
	}

	return ucrecord.AckOutcome{Status: ucrecord.Applied, WasValid: state.Valid, IsValid: commit.Valid}, nil
}

func (r *Repository) Acknowledge(ctx context.Context, tx ucrecord.RepositoryTx, documentID string, from string, to string, schema string, createdAt time.Time) (bool, error) {
	return r.legacyAck(ctx, tx, ucrecord.AckCommit{Commit: ucrecord.Commit{ID: documentID}, Side: ucrecord.AckerSide, From: from, To: to, Schema: schema, Valid: true, CreatedAt: createdAt})
}

func (r *Repository) UnAcknowledge(ctx context.Context, tx ucrecord.RepositoryTx, documentID string, from string, to string, schema string, createdAt time.Time) (bool, error) {
	return r.legacyAck(ctx, tx, ucrecord.AckCommit{Commit: ucrecord.Commit{ID: documentID}, Side: ucrecord.AckerSide, From: from, To: to, Schema: schema, Valid: false, CreatedAt: createdAt})
}

func processAcked(ctx context.Context, db *gorm.DB, commit ucrecord.AckCommit) (ucrecord.AckOutcome, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.processAcked")
	defer span.End()

	db = db.WithContext(ctx)

	// CIP-10 §4: only a document with a strictly newer createdAt may move
	// the (from, to, schema) state; an equal or older one is a no-op.
	// The key is createdAt alone — never the document id —
	// so the acker's and the associate owner's servers, which hold different
	// documents with the same createdAt, reach the same decision.
	// Not a conditional upsert: it cannot tell which validity it replaced.
	var state models.Acked
	found := db.Clauses(clause.Locking{Strength: "UPDATE"}).
		Where(`"from" = ? AND "to" = ? AND schema = ?`, commit.From, commit.To, commit.Schema).
		Limit(1).
		Find(&state)
	if found.Error != nil {
		span.RecordError(found.Error)
		return ucrecord.AckOutcome{}, found.Error
	}

	if found.RowsAffected == 0 {
		// Not the model: gorm would swap a false valid for the column
		// default, and an unack may be the first state.
		result := db.Model(&models.Acked{}).Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "from"}, {Name: "to"}, {Name: "schema"}},
			DoNothing: true,
		}).Create(map[string]any{
			"from": commit.From, "to": commit.To, "schema": commit.Schema,
			"document_id": commit.ID, "valid": commit.Valid, "created_at": commit.CreatedAt,
		})
		if result.Error != nil {
			span.RecordError(result.Error)
			return ucrecord.AckOutcome{}, result.Error
		}
		if result.RowsAffected == 0 {
			// a concurrent writer created the state first: start over
			// with the row there to lock
			return ucrecord.AckOutcome{}, errRetryTx
		}
		return ucrecord.AckOutcome{Status: ucrecord.Applied, IsValid: commit.Valid}, nil
	}

	if !commit.CreatedAt.After(state.CreatedAt) {
		return ucrecord.AckOutcome{Status: ucrecord.Stale, WasValid: state.Valid, IsValid: state.Valid}, nil
	}

	err := db.Model(&models.Acked{}).
		Where(`"from" = ? AND "to" = ? AND schema = ?`, commit.From, commit.To, commit.Schema).
		Updates(map[string]any{"valid": commit.Valid, "document_id": commit.ID, "created_at": commit.CreatedAt}).Error
	if err != nil {
		span.RecordError(err)
		return ucrecord.AckOutcome{}, err
	}

	return ucrecord.AckOutcome{Status: ucrecord.Applied, WasValid: state.Valid, IsValid: commit.Valid}, nil
}

func (r *Repository) Acknowledged(ctx context.Context, tx ucrecord.RepositoryTx, documentID string, from string, to string, schema string, createdAt time.Time) (bool, error) {
	return r.legacyAck(ctx, tx, ucrecord.AckCommit{Commit: ucrecord.Commit{ID: documentID}, Side: ucrecord.TargetSide, From: from, To: to, Schema: schema, Valid: true, CreatedAt: createdAt})
}

func (r *Repository) UnAcknowledged(ctx context.Context, tx ucrecord.RepositoryTx, documentID string, from string, to string, schema string, createdAt time.Time) (bool, error) {
	return r.legacyAck(ctx, tx, ucrecord.AckCommit{Commit: ucrecord.Commit{ID: documentID}, Side: ucrecord.TargetSide, From: from, To: to, Schema: schema, Valid: false, CreatedAt: createdAt})
}

func (r *Repository) getAckRecords(ctx context.Context, from, schema string, since, until *time.Time, limit int, order string) ([]ucrecord.QueryRow, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.getAckRecords")
	defer span.End()

	var acks []models.Ack
	query := r.db.WithContext(ctx).
		Model(&models.Ack{}).
		Joins("Document").
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

	err := query.Find(&acks).Error
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

	var acked []models.Acked
	query := r.db.WithContext(ctx).
		Model(&models.Acked{}).
		Joins("Document").
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

	err := query.Find(&acked).Error
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

	query := r.db.WithContext(ctx).
		Model(&models.Ack{}).
		Select("schema, COUNT(*) AS count").
		Where("valid = ?", true).
		Where("acks.from = ?", from)

	if schema != "" {
		query = query.Where("acks.schema = ?", schema)
	}

	err := query.Group("schema").Scan(&results).Error
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

	query := r.db.WithContext(ctx).
		Model(&models.Acked{}).
		Select("schema, COUNT(*) AS count").
		Where("valid = ?", true).
		Where("ackeds.to = ?", to)

	if schema != "" {
		query = query.Where("ackeds.schema = ?", schema)
	}

	err := query.Group("schema").Scan(&results).Error
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

func (r *Repository) ComputeAcknowledgeCounts(ctx context.Context, from, to, schema string) (map[string]int64, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.ComputeAcknowledgeCounts")
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
