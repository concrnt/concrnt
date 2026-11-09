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

func (r *Repository) HasCommitLog(ctx context.Context, id string) (bool, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.HasCommitLog")
	defer span.End()

	var commitLog models.CommitLog
	err := r.db.WithContext(ctx).
		Select("id").
		Where("id = ?", id).
		Take(&commitLog).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return false, nil
	}
	if err != nil {
		span.RecordError(err)
		return false, err
	}

	return true, nil
}

func (r *Repository) CreateCommitLog(ctx context.Context, tx ucrecord.RepositoryTx, id string, ip string, document string, proof any, owner string) error {
	ctx, span := tracer.Start(ctx, "Repository.Record.CreateCommitLog")
	defer span.End()

	db, err := getRecordTx(ctx, tx)
	if err != nil {
		span.RecordError(err)
		return err
	}

	proofBytes, err := json.Marshal(proof)
	if err != nil {
		span.RecordError(err)
		return err
	}

	commitLog := models.CommitLog{
		ID:       id,
		IP:       ip,
		Document: document,
		Owner:    owner,
		Proof:    string(proofBytes),
	}

	if err := db.Clauses(clause.OnConflict{
		DoNothing: true,
	}).Create(&commitLog).Error; err != nil {
		span.RecordError(err)
		return err
	}

	return nil
}

func (r *Repository) MarkCommitLogGcCandidate(ctx context.Context, tx ucrecord.RepositoryTx, documentID string) error {
	ctx, span := tracer.Start(ctx, "Repository.Record.MarkCommitLogGcCandidate")
	defer span.End()

	db, err := getRecordTx(ctx, tx)
	if err != nil {
		span.RecordError(err)
		return err
	}

	if err := db.Model(&models.CommitLog{}).
		Where("id = ?", documentID).
		Update("gc_candidate", true).Error; err != nil {
		span.RecordError(err)
		return err
	}

	return nil
}

func (r *Repository) GetAllCommitLogs(ctx context.Context, owner string) ([]concrnt.SignedDocument, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.GetAllCommitLogs")
	defer span.End()

	var commitLogs []models.CommitLog
	err := r.db.WithContext(ctx).
		Where("owner = ?", owner).
		Order("c_date ASC").
		Find(&commitLogs).Error
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	sds := make([]concrnt.SignedDocument, len(commitLogs))
	for i, cl := range commitLogs {
		var proof concrnt.Proof
		err := json.Unmarshal([]byte(cl.Proof), &proof)
		if err != nil {
			span.RecordError(err)
			return nil, err
		}
		sds[i] = concrnt.SignedDocument{
			Document: cl.Document,
			Proof:    proof,
		}
	}

	return sds, nil
}

func (r *Repository) QueryCommitLogs(ctx context.Context, owner string, since, until *time.Time, limit int, order string) ([]ucrecord.QueryRow, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.QueryCommitLogs")
	defer span.End()

	var commitLogs []models.CommitLog

	query := r.db.WithContext(ctx).Model(&models.CommitLog{})

	if owner != "" {
		query = query.Where("owner = ?", owner)
	}
	if since != nil {
		query = query.Where("c_date >= ?", *since)
	}
	if until != nil {
		query = query.Where("c_date <= ?", *until)
	}

	if order == "desc" {
		query = query.Order("c_date DESC, id DESC")
	} else {
		query = query.Order("c_date ASC, id ASC")
	}

	if limit > 0 {
		query = query.Limit(limit)
	}

	if err := query.Find(&commitLogs).Error; err != nil {
		span.RecordError(err)
		return nil, err
	}

	rows := make([]ucrecord.QueryRow, 0, len(commitLogs))
	for _, cl := range commitLogs {
		var proof concrnt.Proof
		err := json.Unmarshal([]byte(cl.Proof), &proof)
		if err != nil {
			span.RecordError(err)
			return nil, err
		}

		row := ucrecord.QueryRow{
			Row: concrnt.SignedDocument{
				Document: cl.Document,
				Proof:    proof,
			},
			CreatedAt: cl.CDate,
		}
		// an ownerless commit (unrepaired legacy row) has no ccfs namespace to
		// compose; ComposeCCFSURI("") would yield a URI that misparses
		if cl.Owner != "" {
			ccfs := concrnt.ComposeCCFSURI(cl.Owner, concrnt.CCFSTypeConcrnt, cl.ID)
			row.Row.CCFS = &ccfs
		}

		rows = append(rows, row)
	}

	return rows, nil
}
