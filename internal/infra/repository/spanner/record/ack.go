package record

import (
	"context"
	"strings"
	"time"

	"cloud.google.com/go/spanner"

	"github.com/concrnt/concrnt/internal/infra/repository/repoutil"
	spannerdb "github.com/concrnt/concrnt/internal/infra/repository/spanner"
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

func tableOf(side ucrecord.AckSide) ackTable {
	if side == ucrecord.TargetSide {
		return ackedSide
	}
	return ackSide
}

// ApplyAck is the CIP-10 §4 state move: only a document with a strictly
// newer createdAt may move the (from, to, schema) state; an equal or older
// one is Stale. The key is createdAt alone — never the document id — so
// the acker's and the associate owner's servers, which hold different
// documents with the same createdAt, reach the same decision. The state row
// is keyed by the document id (its commit), so a move is a delete of the
// old row and an insert of the new one. The read locks the state (or its
// absence), so the validity reported as the previous one is the one this
// write replaced.
func (r *Repository) ApplyAck(ctx context.Context, commit ucrecord.AckCommit) (ucrecord.AckOutcome, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.ApplyAck")
	defer span.End()

	table := tableOf(commit.Side)
	var outcome ucrecord.AckOutcome
	status, err := r.apply(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) (ucrecord.ApplyStatus, error) {
		outcome = ucrecord.AckOutcome{}

		read, err := readSet[struct {
			Duplicate bool `spanner:"duplicate"`
			States    []*struct {
				ID        string    `spanner:"id"`
				Valid     bool      `spanner:"valid"`
				CreatedAt time.Time `spanner:"created_at"`
			} `spanner:"states"`
		}](ctx, tx, spanner.Statement{
			SQL: "SELECT " + duplicateCommit + `, ARRAY(SELECT AS STRUCT id, valid, created_at FROM ` + table.name + `
				WHERE from_ccid = @from AND to_ccid = @to AND schema = @schema) AS states`,
			Params: map[string]any{"id": commit.ID, "from": commit.From, "to": commit.To, "schema": commit.Schema},
		})
		if err != nil {
			return ucrecord.Applied, err
		}
		if read.Duplicate {
			return ucrecord.Duplicate, nil
		}

		insert, err := insertCommit(commit.Commit, false)
		if err != nil {
			return ucrecord.Applied, err
		}
		mutations := []*spanner.Mutation{insert}
		if len(read.States) > 0 {
			state := read.States[0]
			outcome.WasValid = state.Valid
			if !commit.CreatedAt.After(state.CreatedAt) {
				outcome.IsValid = state.Valid
				return ucrecord.Stale, nil
			}
			mutations = append(mutations, spanner.Delete(table.name, spanner.Key{state.ID}))
		}
		outcome.IsValid = commit.Valid
		mutations = append(mutations, spanner.Insert(table.name,
			[]string{"id", "from_ccid", "to_ccid", "schema", "valid", "created_at", "c_date"},
			[]any{commit.ID, commit.From, commit.To, commit.Schema, commit.Valid, commit.CreatedAt, time.Now().UTC()}))
		return ucrecord.Applied, tx.BufferWrite(mutations)
	})
	if err != nil {
		span.RecordError(err)
		return ucrecord.AckOutcome{}, err
	}
	outcome.Status = status
	return outcome, nil
}

// GetAcknowledgeRecords lists the valid states of one side by its owning
// column (from for acks, to for ackeds); CIP-3 §3.4: the ccfs identity of
// an ack is held by the acker's server, of an acked by the associate
// owner's.
func (r *Repository) GetAcknowledgeRecords(ctx context.Context, from, to, schema string, since, until *time.Time, limit int, order string) ([]ucrecord.QueryRow, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.GetAcknowledgeRecords")
	defer span.End()

	side, ccid, err := repoutil.AckSideOf(from, to)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	table := tableOf(side)

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
		span.RecordError(err)
		return nil, err
	}
	return commitRows(rows)
}

func (r *Repository) ComputeAcknowledgeCounts(ctx context.Context, from, to, schema string) (map[string]int64, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.ComputeAcknowledgeCounts")
	defer span.End()

	side, ccid, err := repoutil.AckSideOf(from, to)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	table := tableOf(side)

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
		span.RecordError(err)
		return nil, err
	}
	counts := make(map[string]int64)
	for _, res := range results {
		counts[res.Schema] = res.Count
	}
	return counts, nil
}
