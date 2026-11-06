// Package record is the SQLite implementation of record.Repository, file
// for file and method for method the same as the Postgres one
// (internal/infra/repository/postgres/record). The differences are the
// storage types (int64 microsecond timestamps, JSON text arrays, no
// COLLATE) and the locking: the IMMEDIATE transaction holds the single
// writer lock, so no row is locked explicitly.
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

type Repository struct {
	db *gorm.DB
}

func New(db *gorm.DB) ucrecord.Repository {
	return &Repository{db: db}
}

var tracer = otel.Tracer("repository")

type recordTx struct {
	tx *gorm.DB
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
	return errors.Is(err, sqlite3.BUSY) || errors.Is(err, sqlite3.LOCKED)
}

func (r *Repository) RunInTx(ctx context.Context, fn func(tx ucrecord.RepositoryTx) error) error {
	backoff := txBackoffStart
	for attempt := 1; ; attempt++ {
		err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			return fn(&recordTx{tx: tx})
		})
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

func getRecordTx(ctx context.Context, tx ucrecord.RepositoryTx) (*gorm.DB, error) {
	recordTx, ok := tx.(*recordTx)
	if !ok || recordTx == nil || recordTx.tx == nil {
		return nil, errors.New("invalid record repository transaction")
	}
	return recordTx.tx.WithContext(ctx), nil
}
