//go:build contractv1

// Package record is the Firestore implementation of record.Repository, file
// for file and method for method the same as the Postgres, SQLite and
// Spanner ones. A Firestore transaction must issue every read before its
// first write and never sees its own writes, so the transaction handle
// buffers the writes until the usecase closure returns and lays them over
// the reads it serves meanwhile (tx.go); a method therefore sees what the
// earlier methods of the same transaction wrote, as it does on the SQL
// backends. Reads outside a transaction are plain strong reads. The client
// re-runs the closure when the transaction aborts on contention, which is
// RunInTx's retry.
package record

import (
	"context"
	"errors"
	"time"

	"cloud.google.com/go/firestore"
	"go.opentelemetry.io/otel"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	fs "github.com/concrnt/concrnt/internal/infra/repository/firestore"
	ucrecord "github.com/concrnt/concrnt/internal/usecase/record"
)

type Repository struct {
	client *firestore.Client
}

func New(client *firestore.Client) ucrecord.Repository {
	return &Repository{client: client}
}

var tracer = otel.Tracer("repository")

func (r *Repository) col(name string) *firestore.CollectionRef {
	return r.client.Collection(name)
}

// txMaxAttempts bounds the closure re-runs on contention (Firestore aborts
// the loser of two overlapping transactions; the SDK retries with backoff).
const txMaxAttempts = 8

// RunInTx runs fn inside a Firestore transaction. fn is re-run when the
// transaction aborts on contention; a non-nil error from fn rolls back and
// is returned unchanged.
func (r *Repository) RunInTx(ctx context.Context, fn func(tx ucrecord.RepositoryTx) error) error {
	ctx, span := tracer.Start(ctx, "Repository.Record.RunInTx")
	defer span.End()

	start := time.Now()
	attempts := 0
	err := r.client.RunTransaction(ctx, func(_ context.Context, ftx *firestore.Transaction) error {
		attempts++
		tx := &recordTx{ftx: ftx, ops: map[string]*pendingWrite{}}
		if err := fn(tx); err != nil {
			return err
		}
		return tx.flush()
	}, firestore.MaxAttempts(txMaxAttempts))

	fs.ObserveTx(start, attempts, err)
	if err != nil && status.Code(err) == codes.Aborted {
		err = errors.Join(errTxContention, err)
	}
	return err
}

// errTxContention marks a transaction that still aborted after every retry.
var errTxContention = errors.New("firestore transaction aborted after retries")

var errInvalidTx = errors.New("invalid record repository transaction")

func getRecordTx(tx ucrecord.RepositoryTx) (*recordTx, error) {
	t, ok := tx.(*recordTx)
	if !ok || t == nil || t.ftx == nil {
		return nil, errInvalidTx
	}
	return t, nil
}
