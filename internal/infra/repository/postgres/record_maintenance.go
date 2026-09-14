package postgres

import (
	"context"
	"encoding/json"
	"fmt"

	"gorm.io/gorm"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/internal/infra/database/models"
	"github.com/concrnt/concrnt/internal/usecase/record"
)

// NewRecordMaintenanceRepository exposes the offline-tooling slice of the
// record store (conctl gc/dump) over the same connection.
func NewRecordMaintenanceRepository(db *gorm.DB) record.MaintenanceRepository {
	return &RecordRepository{db: db}
}

func (r *RecordRepository) CountGcCandidates(ctx context.Context, cutoffID string) (int64, error) {
	var count int64
	err := r.db.WithContext(ctx).
		Model(&models.CommitLog{}).
		Where("gc_candidate AND id < ?", cutoffID).
		Count(&count).Error
	return count, err
}

func (r *RecordRepository) ListGcCandidateIDs(ctx context.Context, cutoffID string, limit int) ([]string, error) {
	var ids []string
	err := r.db.WithContext(ctx).
		Model(&models.CommitLog{}).
		Where("gc_candidate AND id < ?", cutoffID).
		Order("id ASC").
		Limit(limit).
		Pluck("id", &ids).Error
	return ids, err
}

// DeleteCommitLogs deletes the commit logs; records, record keys, acks,
// ackeds, associations and entities go with them via ON DELETE CASCADE.
func (r *RecordRepository) DeleteCommitLogs(ctx context.Context, ids []string) (int, error) {
	if len(ids) == 0 {
		return 0, nil
	}
	res := r.db.WithContext(ctx).Where("id IN ?", ids).Delete(&models.CommitLog{})
	return int(res.RowsAffected), res.Error
}

// ListCommitLogs orders, paginates and filters on the id column's own
// collation so the primary-key index backs the range scan (forcing COLLATE
// "C" would defeat the index). This is correct because time-kind CDIDs are
// fixed-length lowercase base32 whose byte order encodes the embedded
// timestamp, and every standard collation orders that alphabet identically to
// byte order.
func (r *RecordRepository) ListCommitLogs(ctx context.Context, page record.CommitLogPage) ([]record.CommitLogEntry, error) {
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

	entries := make([]record.CommitLogEntry, 0, len(logs))
	for _, cl := range logs {
		var proof concrnt.Proof
		if err := json.Unmarshal([]byte(cl.Proof), &proof); err != nil {
			return nil, fmt.Errorf("failed to parse proof for commit %s: %w", cl.ID, err)
		}
		entries = append(entries, record.CommitLogEntry{
			ID:       cl.ID,
			Document: cl.Document,
			Proof:    proof,
			Owner:    cl.Owner,
		})
	}
	return entries, nil
}
