// Package record is the SQLite implementation of record.Repository, laid
// out file for file like the Postgres one
// (internal/infra/repository/postgres/record). The differences are the
// storage types (int64 microsecond timestamps, JSON text arrays, no
// COLLATE) and the locking: the IMMEDIATE transaction holds the single
// writer lock, so no row is locked explicitly and what a transaction reads
// cannot be overtaken before it writes.
package record

import (
	"context"
	"errors"

	"github.com/ncruces/go-sqlite3"
	"go.opentelemetry.io/otel"
	"gorm.io/gorm"

	"github.com/concrnt/concrnt/internal/infra/repository/repoutil"
	ucrecord "github.com/concrnt/concrnt/internal/usecase/record"
)

// Repository implements record.Repository. Every Apply* method is one
// IMMEDIATE transaction. Dependents (CONTRACT.md C11) are removed by ON
// DELETE CASCADE inside the transaction that removes what they hang off,
// as on Postgres, so nothing is left for Cleanup.
type Repository struct {
	db *gorm.DB
	// writer admits one Apply* transaction at a time. SQLite has a single
	// writer anyway, and a transaction waiting for its lock does so on a
	// pooled connection: a burst of commits would park every connection
	// in BEGIN IMMEDIATE and the reads, which WAL lets run beside the
	// writer, would queue behind them for a connection. Waiting here
	// holds none. (Another process writing the file is still met by
	// busy_timeout and the retry in apply.)
	writer chan struct{}
}

func New(db *gorm.DB) ucrecord.Repository {
	return &Repository{db: db, writer: make(chan struct{}, 1)}
}

var tracer = otel.Tracer("repository")

// errNotStored rolls back a transaction whose write ended Stale or
// Duplicate: neither leaves a trace, not even the commit log entry.
var errNotStored = errors.New("not stored")

// isRetryableTxError reports SQLITE_BUSY / SQLITE_LOCKED (extended codes
// included): the writer lock could not be taken within busy_timeout, so the
// transaction is retried from scratch.
func isRetryableTxError(err error) bool {
	return errors.Is(err, sqlite3.BUSY) || errors.Is(err, sqlite3.LOCKED)
}

// apply runs fn as one transaction and returns its status. fn is re-run
// from scratch when the writer lock could not be taken, so it must reset
// whatever it reports through captured variables at its start. Anything
// but Applied rolls the transaction back.
func (r *Repository) apply(ctx context.Context, fn func(tx *gorm.DB) (ucrecord.ApplyStatus, error)) (ucrecord.ApplyStatus, error) {
	status := ucrecord.Applied
	err := repoutil.RetryTx(ctx, isRetryableTxError, func() error {
		select {
		case r.writer <- struct{}{}:
			defer func() { <-r.writer }()
		case <-ctx.Done():
			return ctx.Err()
		}
		return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			var err error
			status, err = fn(tx)
			if err == nil && status != ucrecord.Applied {
				return errNotStored
			}
			return err
		})
	})
	if errors.Is(err, errNotStored) {
		return status, nil
	}
	return status, err
}

// Cleanup has nothing to do: ApplyDelete removes dependents itself (ON
// DELETE CASCADE) and issues no token.
func (r *Repository) Cleanup(ctx context.Context, token string) (string, error) {
	return "", nil
}
