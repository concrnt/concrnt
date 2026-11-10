package record

import (
	"context"
	"errors"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/concrnt/concrnt/internal/infra/repository/postgres"
	ucrecord "github.com/concrnt/concrnt/internal/usecase/record"
)

// ackSide names the two sides of an ack relationship: the acker's rows
// (acks, keyed and owned by from) and the target's (ackeds, by to).
type ackSide struct {
	table string
	owner string // the column the ccfs namespace is composed from (CIP-3 §3.4)
	model any
}

var (
	ackerSide  = ackSide{"acks", "from", &postgres.Ack{}}
	targetSide = ackSide{"ackeds", "to", &postgres.Acked{}}
)

func sideOf(side ucrecord.AckSide) ackSide {
	if side == ucrecord.TargetSide {
		return targetSide
	}
	return ackerSide
}

// ackState is the stored state of one (from, to, schema) on one side.
type ackState struct {
	Valid     bool
	CreatedAt time.Time
}

// lockAckState reads the state under a row lock; false when there is none.
func lockAckState(tx *gorm.DB, side ackSide, commit ucrecord.AckCommit) (ackState, bool, error) {
	var state ackState
	result := tx.Table(side.table).
		Select("valid, created_at").
		Clauses(clause.Locking{Strength: "UPDATE"}).
		Where(`"from" = ? AND "to" = ? AND schema = ?`, commit.From, commit.To, commit.Schema).
		Limit(1).
		Scan(&state)
	return state, result.RowsAffected > 0, result.Error
}

// ApplyAck is the CIP-10 §4 state move: only a document with a strictly
// newer createdAt may move the (from, to, schema) state; an equal or older
// one is Stale. The key is createdAt alone — never the document id — so
// the acker's and the associate owner's servers, which hold different
// documents with the same createdAt, reach the same decision. The state is
// read under its row lock, so the validity reported as the previous one is
// the one this write replaced.
func (r *Repository) ApplyAck(ctx context.Context, commit ucrecord.AckCommit) (ucrecord.AckOutcome, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.ApplyAck")
	defer span.End()

	side := sideOf(commit.Side)
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

		state, found, err := lockAckState(tx, side, commit)
		if err != nil {
			return ucrecord.Applied, err
		}
		if !found {
			// no row to lock: the insert either creates the state or meets
			// the one a concurrent writer committed first, which is then
			// read like any existing state
			// (a map, not the model: gorm would swap a false valid for the
			// column default, and an unack may be the first state)
			result := tx.Table(side.table).Clauses(clause.OnConflict{
				Columns:   []clause.Column{{Name: "from"}, {Name: "to"}, {Name: "schema"}},
				DoNothing: true,
			}).Create(map[string]any{
				"from": commit.From, "to": commit.To, "schema": commit.Schema,
				"document_id": commit.ID, "valid": commit.Valid, "created_at": commit.CreatedAt,
			})
			if result.Error != nil {
				return ucrecord.Applied, result.Error
			}
			if result.RowsAffected > 0 {
				outcome.IsValid = commit.Valid
				return ucrecord.Applied, nil
			}
			if state, found, err = lockAckState(tx, side, commit); err != nil {
				return ucrecord.Applied, err
			}
			if !found {
				// created and removed again in between
				return ucrecord.Applied, errRetryTx
			}
		}

		outcome.WasValid = state.Valid
		if !commit.CreatedAt.After(state.CreatedAt) {
			outcome.IsValid = state.Valid
			return ucrecord.Stale, nil
		}
		err = tx.Table(side.table).
			Where(`"from" = ? AND "to" = ? AND schema = ?`, commit.From, commit.To, commit.Schema).
			Updates(map[string]any{"valid": commit.Valid, "document_id": commit.ID, "created_at": commit.CreatedAt}).Error
		if err != nil {
			return ucrecord.Applied, err
		}
		outcome.IsValid = commit.Valid
		return ucrecord.Applied, nil
	})
	if err != nil {
		span.RecordError(err)
		return ucrecord.AckOutcome{}, err
	}
	outcome.Status = status
	return outcome, nil
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
		query = query.Where(t+".created_at >= ?", *since)
	}
	if until != nil {
		query = query.Where(t+".created_at <= ?", *until)
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
