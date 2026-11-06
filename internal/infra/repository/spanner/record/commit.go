package record

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	spannerdb "github.com/concrnt/concrnt/internal/infra/repository/spanner"

	"cloud.google.com/go/spanner"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/internal/infra/repository/repoutil"
	ucrecord "github.com/concrnt/concrnt/internal/usecase/record"
)

type commitLogRow struct {
	ID          string    `spanner:"id"`
	IP          string    `spanner:"ip"`
	Document    string    `spanner:"document"`
	Proof       string    `spanner:"proof"`
	Owner       string    `spanner:"owner"`
	GcCandidate bool      `spanner:"gc_candidate"`
	CDate       time.Time `spanner:"c_date"`
}

const commitLogSelect = "SELECT id, ip, document, proof, owner, gc_candidate, c_date FROM commit_logs"

// CreateCommitLog stamps c_date with the commit timestamp, so the receive
// order is the database's commit order; the column cannot be read again in
// the same transaction.
func (r *Repository) CreateCommitLog(ctx context.Context, tx ucrecord.RepositoryTx, id string, ip string, document string, proof any, owner string) error {
	ctx, span := tracer.Start(ctx, "Repository.Record.CreateCommitLog")
	defer span.End()

	stx, err := getRecordTx(tx)
	if err != nil {
		span.RecordError(err)
		return err
	}

	proofBytes, err := json.Marshal(proof)
	if err != nil {
		span.RecordError(err)
		return err
	}

	rows, err := stx.Update(ctx, spanner.Statement{
		SQL: `INSERT OR IGNORE INTO commit_logs (id, ip, document, proof, owner, gc_candidate, c_date)
			VALUES (@id, @ip, @document, @proof, @owner, false, PENDING_COMMIT_TIMESTAMP())`,
		Params: map[string]any{"id": id, "ip": ip, "document": document, "proof": string(proofBytes), "owner": owner},
	})
	if err != nil {
		span.RecordError(err)
		return err
	}
	if rows == 0 {
		return ucrecord.ErrAlreadyCommitted
	}
	return nil
}

func (r *Repository) MarkCommitLogGcCandidate(ctx context.Context, tx ucrecord.RepositoryTx, documentID string) error {
	ctx, span := tracer.Start(ctx, "Repository.Record.MarkCommitLogGcCandidate")
	defer span.End()

	stx, err := getRecordTx(tx)
	if err != nil {
		span.RecordError(err)
		return err
	}

	_, err = stx.Update(ctx, spanner.Statement{
		SQL:    "UPDATE commit_logs SET gc_candidate = true WHERE id = @id",
		Params: map[string]any{"id": documentID},
	})
	if err != nil {
		span.RecordError(err)
		return err
	}
	return nil
}

func (r *Repository) GetAllCommitLogs(ctx context.Context, owner string) ([]concrnt.SignedDocument, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.GetAllCommitLogs")
	defer span.End()

	logs, err := spannerdb.QueryAll[commitLogRow](ctx, r.client.Single(), spanner.Statement{
		SQL:    commitLogSelect + " WHERE owner = @owner ORDER BY c_date ASC",
		Params: map[string]any{"owner": owner},
	})
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	sds := make([]concrnt.SignedDocument, len(logs))
	for i, cl := range logs {
		var proof concrnt.Proof
		if err := json.Unmarshal([]byte(cl.Proof), &proof); err != nil {
			span.RecordError(err)
			return nil, err
		}
		sds[i] = concrnt.SignedDocument{Document: cl.Document, Proof: proof}
	}
	return sds, nil
}

func (r *Repository) QueryCommitLogs(ctx context.Context, owner string, since, until *time.Time, limit int, order string) ([]ucrecord.QueryRow, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.QueryCommitLogs")
	defer span.End()

	params := map[string]any{}
	where := []string{"true"}
	if owner != "" {
		where = append(where, "owner = @owner")
		params["owner"] = owner
	}
	where = timeRange(where, params, "c_date", since, until)
	sql := commitLogSelect + " WHERE " + strings.Join(where, " AND ") + orderLimit("c_date", "id", order, limit, params)

	logs, err := spannerdb.QueryAll[commitLogRow](ctx, r.client.Single(), spanner.Statement{SQL: sql, Params: params})
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	rows := make([]ucrecord.QueryRow, 0, len(logs))
	for _, cl := range logs {
		sd, err := repoutil.CommitDocument(cl.Owner, cl.ID, cl.Document, cl.Proof)
		if err != nil {
			span.RecordError(err)
			return nil, err
		}
		rows = append(rows, ucrecord.QueryRow{Row: sd, CreatedAt: cl.CDate.UTC()})
	}
	return rows, nil
}

func (r *Repository) CountGcCandidates(ctx context.Context, cutoffID string) (int64, error) {
	return scalar[int64](ctx, r.client.Single(), spanner.Statement{
		SQL:    "SELECT COUNT(*) FROM commit_logs WHERE gc_candidate AND id < @cutoff",
		Params: map[string]any{"cutoff": cutoffID},
	})
}

func (r *Repository) ListGcCandidates(ctx context.Context, cutoffID string, afterID string, limit int) ([]ucrecord.CommitLogEntry, error) {
	params := map[string]any{"cutoff": cutoffID, "after": afterID, "limit": int64(limit)}
	logs, err := spannerdb.QueryAll[commitLogRow](ctx, r.client.Single(), spanner.Statement{
		SQL:    commitLogSelect + " WHERE gc_candidate AND id < @cutoff AND id > @after ORDER BY id ASC LIMIT @limit",
		Params: params,
	})
	if err != nil {
		return nil, err
	}
	return commitLogEntries(logs)
}

// deleteBatch bounds the rows one DeleteCommitLogs transaction removes:
// the cascaded state rows and their index entries all count against the
// commit's mutation limit.
const deleteBatch = 100

// DeleteCommitLogs deletes the commit logs in batches of deleteBatch, one
// transaction each (the caller is offline tooling, so the batches need not
// be atomic together); records, acks, ackeds and associations go with them
// as interleaved children, entities and record keys (and those keys'
// associations) through the foreign keys.
func (r *Repository) DeleteCommitLogs(ctx context.Context, ids []string) (int, error) {
	deleted := 0
	for start := 0; start < len(ids); start += deleteBatch {
		batch := ids[start:min(start+deleteBatch, len(ids))]
		var n int64
		_, err := r.client.ReadWriteTransaction(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
			var err error
			n, err = tx.Update(ctx, spanner.Statement{
				SQL:    "DELETE FROM commit_logs WHERE id IN UNNEST(@ids)",
				Params: map[string]any{"ids": batch},
			})
			return err
		})
		if err != nil {
			return deleted, err
		}
		deleted += int(n)
	}
	return deleted, nil
}

// ListCommitLogs orders, paginates and filters on the id column so the
// primary key backs the range scan; time-kind CDIDs are fixed-length
// lowercase base32 whose byte order encodes the embedded timestamp.
func (r *Repository) ListCommitLogs(ctx context.Context, page ucrecord.CommitLogPage) ([]ucrecord.CommitLogEntry, error) {
	params := map[string]any{}
	where := []string{"true"}
	if page.AfterID != "" {
		where = append(where, "id > @after")
		params["after"] = page.AfterID
	} else if page.FromID != "" {
		where = append(where, "id >= @from")
		params["from"] = page.FromID
	}
	if page.UntilID != "" {
		where = append(where, "id <= @until")
		params["until"] = page.UntilID
	}
	if page.Owner != "" {
		where = append(where, "owner = @owner")
		params["owner"] = page.Owner
	}
	sql := commitLogSelect + " WHERE " + strings.Join(where, " AND ") + " ORDER BY id ASC"
	if page.Limit > 0 {
		sql += " LIMIT @limit"
		params["limit"] = int64(page.Limit)
	}
	logs, err := spannerdb.QueryAll[commitLogRow](ctx, r.client.Single(), spanner.Statement{SQL: sql, Params: params})
	if err != nil {
		return nil, err
	}
	return commitLogEntries(logs)
}

func commitLogEntries(logs []commitLogRow) ([]ucrecord.CommitLogEntry, error) {
	entries := make([]ucrecord.CommitLogEntry, 0, len(logs))
	for _, cl := range logs {
		entry, err := repoutil.CommitLogEntry(cl.ID, cl.IP, cl.Document, cl.Proof, cl.Owner, cl.CDate)
		if err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}
	return entries, nil
}
