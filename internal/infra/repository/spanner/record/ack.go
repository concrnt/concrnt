//go:build contractv1

package record

import (
	"context"
	"errors"
	"strings"
	"time"

	spannerdb "github.com/concrnt/concrnt/internal/infra/repository/spanner"

	"cloud.google.com/go/spanner"

	ucrecord "github.com/concrnt/concrnt/internal/usecase/record"
)

// ackTable names the two sides of an ack relationship: the acker's rows
// (acks, keyed and owned by from) and the target's (ackeds, by to).
type ackTable struct {
	name  string
	owner string // the column the ccfs namespace is composed from
}

var (
	ackSide   = ackTable{name: "acks", owner: "from_ccid"}
	ackedSide = ackTable{name: "ackeds", owner: "to_ccid"}
)

// upsertAck is the CIP-10 §4 state move: only a document with a strictly
// newer createdAt may move the (from, to, schema) state; an equal or older
// one is a no-op (false). The key is createdAt alone — never the document
// id — so the acker's and the associate owner's servers, which hold
// different documents with the same createdAt, reach the same decision.
// The state row is keyed by the document id (its commit), so a move is a
// delete of the old row and an insert of the new one.
func upsertAck(ctx context.Context, stx *spanner.ReadWriteTransaction, table ackTable, documentID, from, to, schema string, createdAt time.Time, valid bool) (bool, error) {
	old, found, err := spannerdb.QueryOne[struct {
		ID        string    `spanner:"id"`
		CreatedAt time.Time `spanner:"created_at"`
	}](ctx, stx, spanner.Statement{
		SQL:    "SELECT id, created_at FROM " + table.name + " WHERE from_ccid = @from AND to_ccid = @to AND schema = @schema",
		Params: map[string]any{"from": from, "to": to, "schema": schema},
	})
	if err != nil {
		return false, err
	}
	if found {
		if !createdAt.After(old.CreatedAt) {
			return false, nil
		}
		if _, err := stx.Update(ctx, spanner.Statement{
			SQL:    "DELETE FROM " + table.name + " WHERE id = @id",
			Params: map[string]any{"id": old.ID},
		}); err != nil {
			return false, err
		}
	}
	affected, err := stx.Update(ctx, spanner.Statement{
		SQL: "INSERT OR IGNORE INTO " + table.name + ` (id, from_ccid, to_ccid, schema, valid, created_at, c_date)
			VALUES (@id, @from, @to, @schema, @valid, @created_at, CURRENT_TIMESTAMP())`,
		Params: map[string]any{"id": documentID, "from": from, "to": to, "schema": schema, "valid": valid, "created_at": createdAt},
	})
	if err != nil {
		return false, err
	}
	return affected > 0, nil
}

func (r *Repository) processAck(ctx context.Context, tx ucrecord.RepositoryTx, documentID string, from string, to string, schema string, createdAt time.Time, valid bool) (bool, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.processAck")
	defer span.End()

	stx, err := getRecordTx(tx)
	if err != nil {
		return false, err
	}
	moved, err := upsertAck(ctx, stx, ackSide, documentID, from, to, schema, createdAt, valid)
	if err != nil {
		span.RecordError(err)
	}
	return moved, err
}

func (r *Repository) Acknowledge(ctx context.Context, tx ucrecord.RepositoryTx, documentID string, from string, to string, schema string, createdAt time.Time) (bool, error) {
	return r.processAck(ctx, tx, documentID, from, to, schema, createdAt, true)
}

func (r *Repository) UnAcknowledge(ctx context.Context, tx ucrecord.RepositoryTx, documentID string, from string, to string, schema string, createdAt time.Time) (bool, error) {
	return r.processAck(ctx, tx, documentID, from, to, schema, createdAt, false)
}

func (r *Repository) processAcked(ctx context.Context, tx ucrecord.RepositoryTx, documentID string, from string, to string, schema string, createdAt time.Time, valid bool) (bool, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.processAcked")
	defer span.End()

	stx, err := getRecordTx(tx)
	if err != nil {
		return false, err
	}
	moved, err := upsertAck(ctx, stx, ackedSide, documentID, from, to, schema, createdAt, valid)
	if err != nil {
		span.RecordError(err)
	}
	return moved, err
}

func (r *Repository) Acknowledged(ctx context.Context, tx ucrecord.RepositoryTx, documentID string, from string, to string, schema string, createdAt time.Time) (bool, error) {
	return r.processAcked(ctx, tx, documentID, from, to, schema, createdAt, true)
}

func (r *Repository) UnAcknowledged(ctx context.Context, tx ucrecord.RepositoryTx, documentID string, from string, to string, schema string, createdAt time.Time) (bool, error) {
	return r.processAcked(ctx, tx, documentID, from, to, schema, createdAt, false)
}

// ackRecords lists the valid states of one side filtered by its owning
// column (from for acks, to for ackeds); CIP-3 §3.4: the ccfs identity of
// an ack is held by the acker's server, of an acked by the associate
// owner's.
func (r *Repository) ackRecords(ctx context.Context, table ackTable, ccid, schema string, since, until *time.Time, limit int, order string) ([]ucrecord.QueryRow, error) {
	params := map[string]any{"ccid": ccid}
	where := []string{"a." + table.owner + " = @ccid", "a.valid"}
	if schema != "" {
		where = append(where, "a.schema = @schema")
		params["schema"] = schema
	}
	where = timeRange(where, params, "a.created_at", since, until)

	sql := "SELECT a.id AS document_id, a." + table.owner + ` AS owner, a.created_at AS created_at, c.document AS document, c.proof AS proof
		FROM ` + table.name + " a JOIN commit_logs c ON c.id = a.id WHERE " + strings.Join(where, " AND ") +
		orderLimit("a.created_at", "a.id", order, limit, params)
	rows, err := spannerdb.QueryAll[commitRow](ctx, r.client.Single(), spanner.Statement{SQL: sql, Params: params})
	if err != nil {
		return nil, err
	}
	return commitRows(rows)
}

func (r *Repository) GetAcknowledgeRecords(ctx context.Context, from, to, schema string, since, until *time.Time, limit int, order string) ([]ucrecord.QueryRow, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.GetAcknowledgeRecords")
	defer span.End()

	var rows []ucrecord.QueryRow
	var err error
	switch {
	case from != "":
		rows, err = r.ackRecords(ctx, ackSide, from, schema, since, until, limit, order)
	case to != "":
		rows, err = r.ackRecords(ctx, ackedSide, to, schema, since, until, limit, order)
	default:
		err = errors.New("either 'from' or 'to' must be specified")
	}
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	return rows, nil
}

func (r *Repository) ackCounts(ctx context.Context, table ackTable, ccid, schema string) (map[string]int64, error) {
	params := map[string]any{"ccid": ccid}
	where := []string{table.owner + " = @ccid", "valid"}
	if schema != "" {
		where = append(where, "schema = @schema")
		params["schema"] = schema
	}
	results, err := spannerdb.QueryAll[struct {
		Schema string `spanner:"schema"`
		Count  int64  `spanner:"count"`
	}](ctx, r.client.Single(), spanner.Statement{
		SQL:    "SELECT schema, COUNT(*) AS count FROM " + table.name + " WHERE " + strings.Join(where, " AND ") + " GROUP BY schema",
		Params: params,
	})
	if err != nil {
		return nil, err
	}
	counts := make(map[string]int64)
	for _, res := range results {
		counts[res.Schema] = res.Count
	}
	return counts, nil
}

func (r *Repository) ComputeAcknowledgeCounts(ctx context.Context, from, to, schema string) (map[string]int64, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.ComputeAcknowledgeCounts")
	defer span.End()

	var counts map[string]int64
	var err error
	switch {
	case from != "":
		counts, err = r.ackCounts(ctx, ackSide, from, schema)
	case to != "":
		counts, err = r.ackCounts(ctx, ackedSide, to, schema)
	default:
		err = errors.New("either 'from' or 'to' must be specified")
	}
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	return counts, nil
}
