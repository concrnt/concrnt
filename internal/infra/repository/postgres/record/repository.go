package record

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.opentelemetry.io/otel"
	"gorm.io/gorm"

	ucrecord "github.com/concrnt/concrnt/internal/usecase/record"
)

// Repository is the Postgres implementation of the usecase record.Repository: commit log,
// records and keys, associations, acks, entities with their registration
// state, and the chunkline views, all over one connection.
type Repository struct {
	db *gorm.DB
	// txSlots caps the transactions running at once. A transaction pins one
	// pooled connection and the reads fn issues take a second one (C4:
	// reads return committed state), so the cap must stay at half the pool
	// or every transaction can wait for a connection another one holds.
	txSlots chan struct{}
}

// New builds the repository over db, whose pool must hold at least
// 2*maxConcurrentTx connections.
func New(db *gorm.DB, maxConcurrentTx int) ucrecord.Repository {
	txSlotCapacity.Set(float64(maxConcurrentTx))
	return &Repository{db: db, txSlots: make(chan struct{}, maxConcurrentTx)}
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

var (
	txSlotWait = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "concrnt",
		Subsystem: "record",
		Name:      "tx_slot_wait_seconds",
		Help:      "Time a transaction waited for one of the maxConcurrentTx slots.",
		Buckets:   []float64{0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5},
	})
	txSlotsInUse = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "concrnt",
		Subsystem: "record",
		Name:      "tx_slots_in_use",
		Help:      "Transactions currently holding a slot.",
	})
	txSlotCapacity = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "concrnt",
		Subsystem: "record",
		Name:      "tx_slots",
		Help:      "Configured maxConcurrentTx.",
	})
)

func (r *Repository) RunInTx(ctx context.Context, fn func(tx ucrecord.RepositoryTx) error) error {
	waitStart := time.Now()
	select {
	case r.txSlots <- struct{}{}:
		txSlotWait.Observe(time.Since(waitStart).Seconds())
		txSlotsInUse.Inc()
		defer func() {
			txSlotsInUse.Dec()
			<-r.txSlots
		}()
	case <-ctx.Done():
		return ctx.Err()
	}
	backoff := txBackoffStart
	for attempt := 1; ; attempt++ {
		err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			return fn(&recordTx{tx: tx})
		})
		// serialization_failure / deadlock_detected are the two outcomes
		// Postgres asks the client to retry from scratch
		var pgErr *pgconn.PgError
		retryable := errors.As(err, &pgErr) && (pgErr.Code == "40001" || pgErr.Code == "40P01")
		if err == nil || !retryable || attempt >= txMaxAttempts {
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
