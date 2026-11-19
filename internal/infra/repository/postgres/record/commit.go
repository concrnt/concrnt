package record

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"gorm.io/gorm"

	"github.com/concrnt/concrnt"
	ucrecord "github.com/concrnt/concrnt/internal/usecase/record"
)

func (r *Repository) HasCommitLog(ctx context.Context, id string) (bool, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.HasCommitLog")
	defer span.End()

	_, err := gorm.G[CommitLog](r.db).
		Select("id").
		Where("id = ?", id).
		Take(ctx)
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

	db, err := getRecordTx(tx)
	if err != nil {
		span.RecordError(err)
		return err
	}

	proofBytes, err := json.Marshal(proof)
	if err != nil {
		span.RecordError(err)
		return err
	}

	err = gorm.G[any](db).Exec(ctx, `
INSERT INTO commit_logs (id, ip, document, proof, owner)
VALUES (?, ?, ?, ?, ?)
ON CONFLICT DO NOTHING`,
		id, ip, document, string(proofBytes), owner)
	if err != nil {
		span.RecordError(err)
		return err
	}

	return nil
}

func (r *Repository) MarkCommitLogGcCandidate(ctx context.Context, tx ucrecord.RepositoryTx, documentID string) error {
	ctx, span := tracer.Start(ctx, "Repository.Record.MarkCommitLogGcCandidate")
	defer span.End()

	db, err := getRecordTx(tx)
	if err != nil {
		span.RecordError(err)
		return err
	}

	if _, err := gorm.G[CommitLog](db).
		Where("id = ?", documentID).
		Update(ctx, "gc_candidate", true); err != nil {
		span.RecordError(err)
		return err
	}

	return nil
}

func (r *Repository) GetAllCommitLogs(ctx context.Context, owner string) ([]concrnt.SignedDocument, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.GetAllCommitLogs")
	defer span.End()

	commitLogs, err := gorm.G[CommitLog](r.db).
		Where("owner = ?", owner).
		Order("c_date ASC").
		Find(ctx)
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

	orderBy := "c_date ASC, id ASC"
	if order == "desc" {
		orderBy = "c_date DESC, id DESC"
	}

	query := gorm.G[CommitLog](r.db).Order(orderBy)

	if owner != "" {
		query = query.Where("owner = ?", owner)
	}
	if since != nil {
		query = query.Where("c_date >= ?", *since)
	}
	if until != nil {
		query = query.Where("c_date <= ?", *until)
	}

	if limit > 0 {
		query = query.Limit(limit)
	}

	commitLogs, err := query.Find(ctx)
	if err != nil {
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
