package record

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"go.opentelemetry.io/otel"
	"gorm.io/gorm"

	"github.com/concrnt/concrnt/client"
	"github.com/concrnt/concrnt/internal/domain"
	ucrecord "github.com/concrnt/concrnt/internal/usecase/record"
)

// Repository is the Postgres implementation of the usecase record.Repository: commit log,
// records and keys, associations, acks, entities with their registration
// state, and the chunkline views, all over one connection.
type Repository struct {
	db     *gorm.DB
	client *client.Client
	config domain.Config
}

func New(db *gorm.DB, cl *client.Client, config domain.Config) ucrecord.Repository {
	return &Repository{db: db, client: cl, config: config}
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

// isRetryableTxError reports serialization_failure / deadlock_detected, the
// two outcomes Postgres asks the client to retry from scratch.
func isRetryableTxError(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && (pgErr.Code == "40001" || pgErr.Code == "40P01")
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
