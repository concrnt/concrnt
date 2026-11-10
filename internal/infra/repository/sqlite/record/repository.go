// Package record is the SQLite implementation of record.Repository, file
// for file and method for method the same as the Postgres one
// (internal/infra/repository/postgres/record). The differences are the
// storage types (int64 microsecond timestamps, JSON text arrays, no
// COLLATE) and the locking: the IMMEDIATE transaction holds the single
// writer lock, so no row is locked explicitly and what a transaction reads
// cannot be overtaken before it writes.
package record

import (
	"context"
	"errors"
	"time"

	"github.com/ncruces/go-sqlite3"
	"go.opentelemetry.io/otel"
	"gorm.io/gorm"

	ucrecord "github.com/concrnt/concrnt/internal/usecase/record"
)

// Repository implements record.Repository. Every Apply* method is one
// IMMEDIATE transaction. Dependents (CONTRACT.md P4) are removed by ON
// DELETE CASCADE inside the transaction that removes what they hang off,
// as on Postgres, so nothing is left for Cleanup.
type Repository struct {
	db *gorm.DB
}

func New(db *gorm.DB) ucrecord.Repository {
	return &Repository{db: db}
}

var tracer = otel.Tracer("repository")

const (
	txMaxAttempts  = 8
	txBackoffStart = 10 * time.Millisecond
	txBackoffMax   = 500 * time.Millisecond
)

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
	backoff := txBackoffStart
	for attempt := 1; ; attempt++ {
		status := ucrecord.Applied
		err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			var err error
			status, err = fn(tx)
			if err == nil && status != ucrecord.Applied {
				return errNotStored
			}
			return err
		})
		if err == nil || errors.Is(err, errNotStored) {
			return status, nil
		}
		if !isRetryableTxError(err) || attempt >= txMaxAttempts {
			return status, err
		}
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return status, errors.Join(ctx.Err(), err)
		case <-timer.C:
		}
		backoff = min(backoff*2, txBackoffMax)
	}
}

// Cleanup has nothing to do: ApplyDelete removes dependents itself (ON
// DELETE CASCADE) and issues no token.
func (r *Repository) Cleanup(ctx context.Context, token string) (string, error) {
	return "", nil
}
