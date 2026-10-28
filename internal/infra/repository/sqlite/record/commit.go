package record

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/concrnt/concrnt"
	sqlitedb "github.com/concrnt/concrnt/internal/infra/repository/sqlite"
	ucrecord "github.com/concrnt/concrnt/internal/usecase/record"
)

func (r *Repository) HasCommitLog(ctx context.Context, id string) (bool, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.HasCommitLog")
	defer span.End()

	var one int
	err := r.db.QueryRowContext(ctx, "SELECT 1 FROM commit_logs WHERE id = ?", id).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
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

	_, err = db.ExecContext(ctx, `INSERT INTO commit_logs (id, ip, document, proof, owner, gc_candidate, c_date) VALUES (?, ?, ?, ?, ?, 0, ?)
		ON CONFLICT (id) DO NOTHING`, id, ip, document, string(proofBytes), owner, nowMicros())
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
	_, err = db.ExecContext(ctx, "UPDATE commit_logs SET gc_candidate = 1 WHERE id = ?", documentID)
	if err != nil {
		span.RecordError(err)
	}
	return err
}

func (r *Repository) GetAllCommitLogs(ctx context.Context, owner string) ([]concrnt.SignedDocument, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.GetAllCommitLogs")
	defer span.End()

	rows, err := r.db.QueryContext(ctx, "SELECT document, proof FROM commit_logs WHERE owner = ? ORDER BY c_date ASC", owner)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	defer rows.Close()

	sds := []concrnt.SignedDocument{}
	for rows.Next() {
		var document, proofJSON string
		if err := rows.Scan(&document, &proofJSON); err != nil {
			return nil, err
		}
		var proof concrnt.Proof
		if err := json.Unmarshal([]byte(proofJSON), &proof); err != nil {
			span.RecordError(err)
			return nil, err
		}
		sds = append(sds, concrnt.SignedDocument{Document: document, Proof: proof})
	}
	return sds, rows.Err()
}

func (r *Repository) QueryCommitLogs(ctx context.Context, owner string, since, until *time.Time, limit int, order string) ([]ucrecord.QueryRow, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.QueryCommitLogs")
	defer span.End()

	query := "SELECT id, document, proof, owner, c_date FROM commit_logs WHERE 1 = 1"
	args := []any{}
	if owner != "" {
		query += " AND owner = ?"
		args = append(args, owner)
	}
	if since != nil {
		query += " AND c_date >= ?"
		args = append(args, sqlitedb.Micros(*since))
	}
	if until != nil {
		query += " AND c_date <= ?"
		args = append(args, sqlitedb.Micros(*until))
	}
	if order == "desc" {
		query += " ORDER BY c_date DESC, id DESC"
	} else {
		query += " ORDER BY c_date ASC, id ASC"
	}
	if limit > 0 {
		query += " LIMIT ?"
		args = append(args, limit)
	}

	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	defer rows.Close()

	out := []ucrecord.QueryRow{}
	for rows.Next() {
		var id, document, proofJSON, cOwner string
		var cDate int64
		if err := rows.Scan(&id, &document, &proofJSON, &cOwner, &cDate); err != nil {
			return nil, err
		}
		var proof concrnt.Proof
		if err := json.Unmarshal([]byte(proofJSON), &proof); err != nil {
			span.RecordError(err)
			return nil, err
		}
		row := ucrecord.QueryRow{
			Row:       concrnt.SignedDocument{Document: document, Proof: proof},
			CreatedAt: sqlitedb.Time(cDate),
		}
		// an ownerless commit (unrepaired legacy row) has no ccfs namespace
		if cOwner != "" {
			ccfs := concrnt.ComposeCCFSURI(cOwner, concrnt.CCFSTypeConcrnt, id)
			row.Row.CCFS = &ccfs
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

func (r *Repository) CountGcCandidates(ctx context.Context, cutoffID string) (int64, error) {
	var count int64
	err := r.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM commit_logs WHERE gc_candidate = 1 AND id < ?", cutoffID).Scan(&count)
	return count, err
}

func (r *Repository) ListGcCandidates(ctx context.Context, cutoffID string, afterID string, limit int) ([]ucrecord.CommitLogEntry, error) {
	query := "SELECT id, ip, document, proof, owner, c_date FROM commit_logs WHERE gc_candidate = 1 AND id < ?"
	args := []any{cutoffID}
	if afterID != "" {
		query += " AND id > ?"
		args = append(args, afterID)
	}
	query += " ORDER BY id ASC LIMIT ?"
	args = append(args, limit)
	return r.commitLogEntries(ctx, query, args...)
}

// DeleteCommitLogs deletes the commit logs; records, record keys, acks,
// ackeds, associations and entities go with them via ON DELETE CASCADE.
func (r *Repository) DeleteCommitLogs(ctx context.Context, ids []string) (int, error) {
	if len(ids) == 0 {
		return 0, nil
	}
	res, err := r.db.ExecContext(ctx, "DELETE FROM commit_logs WHERE id IN ("+sqlitedb.Placeholders(len(ids))+")", sqlitedb.Args(ids)...)
	if err != nil {
		return 0, err
	}
	n, err := res.RowsAffected()
	return int(n), err
}

// ListCommitLogs pages by id: time-kind CDIDs are fixed-length lowercase
// base32 whose byte order encodes the embedded timestamp.
func (r *Repository) ListCommitLogs(ctx context.Context, page ucrecord.CommitLogPage) ([]ucrecord.CommitLogEntry, error) {
	query := "SELECT id, ip, document, proof, owner, c_date FROM commit_logs WHERE 1 = 1"
	args := []any{}
	if page.AfterID != "" {
		query += " AND id > ?"
		args = append(args, page.AfterID)
	} else if page.FromID != "" {
		query += " AND id >= ?"
		args = append(args, page.FromID)
	}
	if page.UntilID != "" {
		query += " AND id <= ?"
		args = append(args, page.UntilID)
	}
	if page.Owner != "" {
		query += " AND owner = ?"
		args = append(args, page.Owner)
	}
	query += " ORDER BY id ASC"
	if page.Limit > 0 {
		query += " LIMIT ?"
		args = append(args, page.Limit)
	}
	return r.commitLogEntries(ctx, query, args...)
}

func (r *Repository) commitLogEntries(ctx context.Context, query string, args ...any) ([]ucrecord.CommitLogEntry, error) {
	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	entries := []ucrecord.CommitLogEntry{}
	for rows.Next() {
		var e ucrecord.CommitLogEntry
		var proofJSON string
		var cDate int64
		if err := rows.Scan(&e.ID, &e.IP, &e.Document, &proofJSON, &e.Owner, &cDate); err != nil {
			return nil, err
		}
		if proofJSON != "" {
			if err := json.Unmarshal([]byte(proofJSON), &e.Proof); err != nil {
				return nil, fmt.Errorf("failed to parse proof for commit %s: %w", e.ID, err)
			}
		}
		e.CDate = sqlitedb.Time(cDate)
		entries = append(entries, e)
	}
	return entries, rows.Err()
}
