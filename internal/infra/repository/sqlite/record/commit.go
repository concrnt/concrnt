package record

import (
	"context"
	"encoding/json"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/internal/infra/repository/repoutil"
	sqlitedb "github.com/concrnt/concrnt/internal/infra/repository/sqlite"
	ucrecord "github.com/concrnt/concrnt/internal/usecase/record"
)

// insertCommit writes the commit log entry; false when the id is already
// in the log, which is left as first written (Duplicate).
func insertCommit(tx *gorm.DB, commit ucrecord.Commit) (bool, error) {
	proofBytes, err := json.Marshal(commit.Proof)
	if err != nil {
		return false, err
	}

	commitLog := sqlitedb.CommitLog{
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

// markGcCandidates flags the commit logs ids for `conctl op gc-commitlog`;
// unknown ids are ignored.
func markGcCandidates(tx *gorm.DB, ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	return tx.Model(&sqlitedb.CommitLog{}).
		Where("id IN ?", ids).
		Update("gc_candidate", true).Error
}

func (r *Repository) GetAllCommitLogs(ctx context.Context, owner string) ([]concrnt.SignedDocument, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.GetAllCommitLogs")
	defer span.End()

	var commitLogs []sqlitedb.CommitLog
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

	var commitLogs []sqlitedb.CommitLog

	query := r.db.WithContext(ctx).Model(&sqlitedb.CommitLog{})

	if owner != "" {
		query = query.Where("owner = ?", owner)
	}
	if since != nil {
		query = query.Where("c_date >= ?", sqlitedb.Micros(*since))
	}
	if until != nil {
		query = query.Where("c_date <= ?", sqlitedb.Micros(*until))
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
		sd, err := repoutil.CommitDocument(cl.Owner, cl.ID, cl.Document, cl.Proof)
		if err != nil {
			span.RecordError(err)
			return nil, err
		}
		rows = append(rows, ucrecord.QueryRow{Row: sd, CreatedAt: sqlitedb.Time(cl.CDate)})
	}

	return rows, nil
}

func (r *Repository) CountGcCandidates(ctx context.Context, cutoffID string) (int64, error) {
	var count int64
	err := r.db.WithContext(ctx).
		Model(&sqlitedb.CommitLog{}).
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
	var logs []sqlitedb.CommitLog
	if err := q.Find(&logs).Error; err != nil {
		return nil, err
	}
	return commitLogEntries(logs)
}

// DeleteCommitLogs deletes the commit logs; records, record keys, acks,
// ackeds, associations and entities go with them via ON DELETE CASCADE.
func (r *Repository) DeleteCommitLogs(ctx context.Context, ids []string) (int, error) {
	if len(ids) == 0 {
		return 0, nil
	}
	res := r.db.WithContext(ctx).Where("id IN ?", ids).Delete(&sqlitedb.CommitLog{})
	return int(res.RowsAffected), res.Error
}

// ListCommitLogs orders, paginates and filters on the id column so the
// primary-key index backs the range scan; time-kind CDIDs are fixed-length
// lowercase base32 whose byte order encodes the embedded timestamp.
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

	var logs []sqlitedb.CommitLog
	if err := q.Find(&logs).Error; err != nil {
		return nil, err
	}
	return commitLogEntries(logs)
}

func commitLogEntries(logs []sqlitedb.CommitLog) ([]ucrecord.CommitLogEntry, error) {
	entries := make([]ucrecord.CommitLogEntry, 0, len(logs))
	for _, cl := range logs {
		entry, err := repoutil.CommitLogEntry(cl.ID, cl.IP, cl.Document, cl.Proof, cl.Owner, sqlitedb.Time(cl.CDate))
		if err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}
	return entries, nil
}
