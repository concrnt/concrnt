package record

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/concrnt/concrnt/internal/infra/repository/repotest"
	sqlitedb "github.com/concrnt/concrnt/internal/infra/repository/sqlite"
)

// inspector is the SQLite repotest.Inspector: white-box reads of the tables
// the contract suites assert on, for tests only.
type inspector struct {
	db *sqlitedb.DB
}

func newInspector(db *sqlitedb.DB) repotest.Inspector {
	return &inspector{db: db}
}

// absent maps a missing row to nil, nil.
func absent(err error) error {
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	return err
}

func (i *inspector) Commit(ctx context.Context, id string) (*repotest.CommitState, error) {
	var c repotest.CommitState
	var cDate int64
	err := i.db.QueryRowContext(ctx, "SELECT ip, document, proof, owner, gc_candidate, c_date FROM commit_logs WHERE id = ?", id).
		Scan(&c.IP, &c.Document, &c.Proof, &c.Owner, &c.GcCandidate, &cDate)
	if err != nil {
		return nil, absent(err)
	}
	c.CDate = sqlitedb.Time(cDate)
	return &c, nil
}

func (i *inspector) SetCommitCDate(ctx context.Context, id string, cDate time.Time) error {
	_, err := i.db.ExecContext(ctx, "UPDATE commit_logs SET c_date = ? WHERE id = ?", sqlitedb.Micros(cDate), id)
	return err
}

func (i *inspector) Record(ctx context.Context, documentID string) (*repotest.RecordState, error) {
	var r repotest.RecordState
	var redirect sql.NullString
	var distributions string
	var createdAt int64
	err := i.db.QueryRowContext(ctx, "SELECT owner, schema, redirect, distributions, created_at FROM records WHERE document_id = ?", documentID).
		Scan(&r.Owner, &r.Schema, &redirect, &distributions, &createdAt)
	if err != nil {
		return nil, absent(err)
	}
	r.Redirect = sqlitedb.StringPtr(redirect)
	if r.Distributions, err = sqlitedb.Strings(distributions); err != nil {
		return nil, err
	}
	r.CreatedAt = sqlitedb.Time(createdAt)
	return &r, nil
}

func (i *inspector) RecordKey(ctx context.Context, uri string) (*repotest.KeyState, error) {
	var recordID, redirect sql.NullString
	var recordCreatedAt sql.NullInt64
	err := i.db.QueryRowContext(ctx, "SELECT record_id, record_created_at, redirect FROM record_keys WHERE uri = ?", uri).
		Scan(&recordID, &recordCreatedAt, &redirect)
	if err != nil {
		return nil, absent(err)
	}
	return &repotest.KeyState{
		RecordID:        sqlitedb.StringPtr(recordID),
		RecordCreatedAt: sqlitedb.NullTime(recordCreatedAt),
		Redirect:        sqlitedb.StringPtr(redirect),
	}, nil
}

func (i *inspector) Entity(ctx context.Context, ccid string) (*repotest.EntityState, error) {
	var e repotest.EntityState
	var createdAt int64
	err := i.db.QueryRowContext(ctx, "SELECT document_id, domain, created_at FROM entities WHERE id = ?", ccid).
		Scan(&e.DocumentID, &e.Domain, &createdAt)
	if err != nil {
		return nil, absent(err)
	}
	e.CreatedAt = sqlitedb.Time(createdAt)
	return &e, nil
}

func (i *inspector) Association(ctx context.Context, documentID string) (*repotest.AssociationState, error) {
	var a repotest.AssociationState
	var variant sql.NullString
	err := i.db.QueryRowContext(ctx, `SELECT owner, author, variant, "unique" FROM associations WHERE document_id = ?`, documentID).
		Scan(&a.Owner, &a.Author, &variant, &a.Unique)
	if err != nil {
		return nil, absent(err)
	}
	a.Variant = sqlitedb.StringPtr(variant)
	return &a, nil
}

func (i *inspector) AssociationCountByUnique(ctx context.Context, unique string) (int64, error) {
	var count int64
	err := i.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM associations WHERE "unique" = ?`, unique).Scan(&count)
	return count, err
}

func (i *inspector) ackState(ctx context.Context, table, from, to, schema string) (*repotest.AckState, error) {
	var a repotest.AckState
	var createdAt int64
	err := i.db.QueryRowContext(ctx, `SELECT document_id, valid, created_at FROM `+table+` WHERE "from" = ? AND "to" = ? AND schema = ?`, from, to, schema).
		Scan(&a.DocumentID, &a.Valid, &createdAt)
	if err != nil {
		return nil, absent(err)
	}
	a.CreatedAt = sqlitedb.Time(createdAt)
	return &a, nil
}

func (i *inspector) Ack(ctx context.Context, from, to, schema string) (*repotest.AckState, error) {
	return i.ackState(ctx, "acks", from, to, schema)
}

func (i *inspector) Acked(ctx context.Context, from, to, schema string) (*repotest.AckState, error) {
	return i.ackState(ctx, "ackeds", from, to, schema)
}

func (i *inspector) AckCount(ctx context.Context) (int64, error) {
	var count int64
	err := i.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM acks").Scan(&count)
	return count, err
}

func (i *inspector) AbuseReportCount(ctx context.Context) (int64, error) {
	var count int64
	err := i.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM abuse_reports").Scan(&count)
	return count, err
}
