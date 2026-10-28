// Package record is the SQLite implementation of record.Repository, file
// for file and method for method the same as the Postgres one
// (internal/infra/repository/postgres/record), written against
// database/sql instead of GORM.
package record

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"go.opentelemetry.io/otel"
	"modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"

	"github.com/concrnt/concrnt/client"
	"github.com/concrnt/concrnt/internal/domain"
	sqlitedb "github.com/concrnt/concrnt/internal/infra/repository/sqlite"
	ucrecord "github.com/concrnt/concrnt/internal/usecase/record"
)

type Repository struct {
	db     *sqlitedb.DB
	client *client.Client
	config domain.Config
}

func New(db *sqlitedb.DB, cl *client.Client, config domain.Config) ucrecord.Repository {
	return &Repository{db: db, client: cl, config: config}
}

var tracer = otel.Tracer("repository")

// querier is what *sql.DB and *sql.Tx share: every read helper takes one so
// it serves both the plain and the transactional paths.
type querier interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

type recordTx struct {
	tx *sql.Tx
}

func (*recordTx) IsRepositoryTx() {}

const (
	txMaxAttempts  = 8
	txBackoffStart = 10 * time.Millisecond
	txBackoffMax   = 500 * time.Millisecond
)

// isRetryableTxError reports SQLITE_BUSY / SQLITE_LOCKED (extended codes
// included): the writer lock could not be taken within busy_timeout, so the
// transaction is retried from scratch.
func isRetryableTxError(err error) bool {
	var sqliteErr *sqlite.Error
	if !errors.As(err, &sqliteErr) {
		return false
	}
	switch sqliteErr.Code() & 0xff {
	case sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED:
		return true
	}
	return false
}

func (r *Repository) RunInTx(ctx context.Context, fn func(tx ucrecord.RepositoryTx) error) error {
	backoff := txBackoffStart
	for attempt := 1; ; attempt++ {
		err := r.runOnce(ctx, fn)
		if err == nil || !isRetryableTxError(err) || attempt >= txMaxAttempts {
			return err
		}
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return errors.Join(ctx.Err(), err)
		case <-timer.C:
		}
		backoff = min(backoff*2, txBackoffMax)
	}
}

func (r *Repository) runOnce(ctx context.Context, fn func(tx ucrecord.RepositoryTx) error) error {
	tx, err := r.db.BeginTx(ctx, nil) // BEGIN IMMEDIATE (DSN _txlock)
	if err != nil {
		return err
	}
	if err := fn(&recordTx{tx: tx}); err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit()
}

func getRecordTx(tx ucrecord.RepositoryTx) (*sql.Tx, error) {
	recordTx, ok := tx.(*recordTx)
	if !ok || recordTx == nil || recordTx.tx == nil {
		return nil, errors.New("invalid record repository transaction")
	}
	return recordTx.tx, nil
}

func nowMicros() int64 {
	return sqlitedb.Micros(time.Now())
}
