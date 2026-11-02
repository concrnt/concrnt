package record

import (
	"context"
	"time"

	"cloud.google.com/go/spanner"

	"github.com/concrnt/concrnt/internal/infra/repository/repotest"
)

// inspector is the Spanner repotest.Inspector: white-box reads of the
// tables the contract suites assert on, for tests only.
type inspector struct {
	client *spanner.Client
}

func newInspector(client *spanner.Client) repotest.Inspector {
	return &inspector{client: client}
}

func (i *inspector) Commit(ctx context.Context, id string) (*repotest.CommitState, error) {
	commit, found, err := queryOne[commitLogRow](ctx, i.client.Single(), spanner.Statement{
		SQL: commitLogSelect + " WHERE id = @id", Params: map[string]any{"id": id},
	})
	if err != nil || !found {
		return nil, err
	}
	return &repotest.CommitState{
		IP:          commit.IP,
		Document:    commit.Document,
		Proof:       commit.Proof,
		Owner:       commit.Owner,
		GcCandidate: commit.GcCandidate,
		CDate:       commit.CDate.UTC(),
	}, nil
}

// SetCommitCDate writes an explicit (past) timestamp into the commit
// timestamp column, which allow_commit_timestamp permits.
func (i *inspector) SetCommitCDate(ctx context.Context, id string, cDate time.Time) error {
	_, err := i.client.ReadWriteTransaction(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		_, err := tx.Update(ctx, spanner.Statement{
			SQL: "UPDATE commit_logs SET c_date = @c_date WHERE id = @id", Params: map[string]any{"id": id, "c_date": cDate},
		})
		return err
	})
	return err
}

func (i *inspector) Record(ctx context.Context, documentID string) (*repotest.RecordState, error) {
	rec, found, err := queryOne[struct {
		Owner         string    `spanner:"owner"`
		Schema        string    `spanner:"schema"`
		Redirect      *string   `spanner:"redirect"`
		Distributions []string  `spanner:"distributions"`
		CreatedAt     time.Time `spanner:"created_at"`
	}](ctx, i.client.Single(), spanner.Statement{
		SQL: "SELECT owner, schema, redirect, distributions, created_at FROM records WHERE id = @id", Params: map[string]any{"id": documentID},
	})
	if err != nil || !found {
		return nil, err
	}
	distributions := rec.Distributions
	if distributions == nil {
		distributions = []string{}
	}
	return &repotest.RecordState{
		Owner:         rec.Owner,
		Schema:        rec.Schema,
		Redirect:      rec.Redirect,
		Distributions: distributions,
		CreatedAt:     rec.CreatedAt.UTC(),
	}, nil
}

func (i *inspector) RecordKey(ctx context.Context, uri string) (*repotest.KeyState, error) {
	key, err := readRecordKey(ctx, i.client.Single(), uri)
	if err != nil || key == nil {
		return nil, err
	}
	var createdAt *time.Time
	if key.RecordCreatedAt != nil {
		t := key.RecordCreatedAt.UTC()
		createdAt = &t
	}
	return &repotest.KeyState{
		RecordID:        key.RecordID,
		RecordCreatedAt: createdAt,
		Redirect:        key.Redirect,
	}, nil
}

func (i *inspector) Entity(ctx context.Context, ccid string) (*repotest.EntityState, error) {
	entity, found, err := queryOne[struct {
		DocumentID string    `spanner:"document_id"`
		Domain     string    `spanner:"domain"`
		CreatedAt  time.Time `spanner:"created_at"`
	}](ctx, i.client.Single(), spanner.Statement{
		SQL: "SELECT document_id, domain, created_at FROM entities WHERE ccid = @ccid", Params: map[string]any{"ccid": ccid},
	})
	if err != nil || !found {
		return nil, err
	}
	return &repotest.EntityState{
		DocumentID: entity.DocumentID,
		Domain:     entity.Domain,
		CreatedAt:  entity.CreatedAt.UTC(),
	}, nil
}

func (i *inspector) Association(ctx context.Context, documentID string) (*repotest.AssociationState, error) {
	assoc, found, err := queryOne[struct {
		Owner   string  `spanner:"owner"`
		Author  string  `spanner:"author"`
		Variant *string `spanner:"variant"`
		Unique  string  `spanner:"unique_key"`
	}](ctx, i.client.Single(), spanner.Statement{
		SQL: "SELECT owner, author, variant, unique_key FROM associations WHERE id = @id", Params: map[string]any{"id": documentID},
	})
	if err != nil || !found {
		return nil, err
	}
	return &repotest.AssociationState{
		Owner:   assoc.Owner,
		Author:  assoc.Author,
		Variant: assoc.Variant,
		Unique:  assoc.Unique,
	}, nil
}

func (i *inspector) AssociationCountByUnique(ctx context.Context, unique string) (int64, error) {
	return scalar[int64](ctx, i.client.Single(), spanner.Statement{
		SQL: "SELECT COUNT(*) FROM associations WHERE unique_key = @unique", Params: map[string]any{"unique": unique},
	})
}

func (i *inspector) ackState(ctx context.Context, table ackTable, from, to, schema string) (*repotest.AckState, error) {
	ack, found, err := queryOne[struct {
		ID        string    `spanner:"id"`
		Valid     bool      `spanner:"valid"`
		CreatedAt time.Time `spanner:"created_at"`
	}](ctx, i.client.Single(), spanner.Statement{
		SQL:    "SELECT id, valid, created_at FROM " + table.name + " WHERE from_ccid = @from AND to_ccid = @to AND schema = @schema",
		Params: map[string]any{"from": from, "to": to, "schema": schema},
	})
	if err != nil || !found {
		return nil, err
	}
	return &repotest.AckState{DocumentID: ack.ID, Valid: ack.Valid, CreatedAt: ack.CreatedAt.UTC()}, nil
}

func (i *inspector) Ack(ctx context.Context, from, to, schema string) (*repotest.AckState, error) {
	return i.ackState(ctx, ackSide, from, to, schema)
}

func (i *inspector) Acked(ctx context.Context, from, to, schema string) (*repotest.AckState, error) {
	return i.ackState(ctx, ackedSide, from, to, schema)
}

func (i *inspector) AckCount(ctx context.Context) (int64, error) {
	return scalar[int64](ctx, i.client.Single(), spanner.Statement{SQL: "SELECT COUNT(*) FROM acks"})
}

func (i *inspector) AbuseReportCount(ctx context.Context) (int64, error) {
	return scalar[int64](ctx, i.client.Single(), spanner.Statement{SQL: "SELECT COUNT(*) FROM abuse_reports"})
}
