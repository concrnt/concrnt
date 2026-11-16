package record

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/internal/infra/database/models"
	ucrecord "github.com/concrnt/concrnt/internal/usecase/record"
)

// insertCommit writes the commit log entry; false when the id is already
// in the log, which is left as first written (Duplicate).
func insertCommit(tx *gorm.DB, commit ucrecord.Commit) (bool, error) {
	proofBytes, err := json.Marshal(commit.Proof)
	if err != nil {
		return false, err
	}

	commitLog := models.CommitLog{
		ID:       commit.ID,
		IP:       commit.IP,
		Document: commit.Document,
		Owner:    commit.Owner,
		Proof:    string(proofBytes),
	}

	result := tx.Clauses(clause.OnConflict{
		DoNothing: true,
	}).Create(&commitLog)
	if result.Error != nil {
		return false, result.Error
	}
	return result.RowsAffected > 0, nil
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

func (r *Repository) CountGcCandidates(ctx context.Context, cutoffID string) (int64, error) {
	var count int64
	err := r.db.WithContext(ctx).
		Model(&models.CommitLog{}).
		Where("gc_candidate AND id < ?", cutoffID).
		Count(&count).Error
	return count, err
}

func (r *Repository) ListGcCandidates(ctx context.Context, cutoffID string, afterID string, limit int) ([]ucrecord.CommitLogEntry, error) {
	q := r.db.WithContext(ctx).
		Where("gc_candidate AND id < ?", cutoffID).
		Order("id ASC").
		Limit(limit)
	if afterID != "" {
		q = q.Where("id > ?", afterID)
	}
	var logs []models.CommitLog
	if err := q.Find(&logs).Error; err != nil {
		return nil, err
	}
	return commitLogEntries(logs)
}

func (r *Repository) DeleteCommitLogs(ctx context.Context, ids []string) (int, error) {
	if len(ids) == 0 {
		return 0, nil
	}
	res := r.db.WithContext(ctx).Where("id IN ?", ids).Delete(&models.CommitLog{})
	return int(res.RowsAffected), res.Error
}

// ListCommitLogs orders, paginates and filters on the id column's own
// collation so the primary-key index backs the range scan (forcing COLLATE
// "C" would defeat the index and full-scan the table on every page). This is
// correct because time-kind CDIDs are fixed-length lowercase base32 whose byte
// order encodes the embedded timestamp, and every standard collation orders
// that alphabet identically to byte order.
func (r *Repository) ListCommitLogs(ctx context.Context, page ucrecord.CommitLogPage) ([]ucrecord.CommitLogEntry, error) {
	q := r.db.WithContext(ctx).Order("commit_logs.id ASC")
	if page.Limit > 0 {
		q = q.Limit(page.Limit)
	}
	if page.AfterID != "" {
		q = q.Where("commit_logs.id > ?", page.AfterID)
	} else if page.FromID != "" {
		q = q.Where("commit_logs.id >= ?", page.FromID)
	}
	if page.UntilID != "" {
		q = q.Where("commit_logs.id <= ?", page.UntilID)
	}
	if page.Owner != "" {
		q = q.Where("commit_logs.owner = ?", page.Owner)
	}

	var logs []models.CommitLog
	if err := q.Find(&logs).Error; err != nil {
		return nil, err
	}
	return commitLogEntries(logs)
}

func commitLogEntries(logs []models.CommitLog) ([]ucrecord.CommitLogEntry, error) {
	entries := make([]ucrecord.CommitLogEntry, 0, len(logs))
	for _, cl := range logs {
		var proof concrnt.Proof
		if cl.Proof != "" {
			if err := json.Unmarshal([]byte(cl.Proof), &proof); err != nil {
				return nil, fmt.Errorf("failed to parse proof for commit %s: %w", cl.ID, err)
			}
		}
		entries = append(entries, ucrecord.CommitLogEntry{
			ID:       cl.ID,
			IP:       cl.IP,
			Document: cl.Document,
			Proof:    proof,
			Owner:    cl.Owner,
			CDate:    cl.CDate,
		})
	}
	return entries, nil
}
