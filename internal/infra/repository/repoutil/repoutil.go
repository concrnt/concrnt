// Package repoutil holds the backend-independent logic of the repository
// contract: what every backend does with a row once it has fetched it, and
// how the URIs the contract is keyed by relate to each other. Nothing here
// is a model, a table or an index (CONTRACT.md C5).
package repoutil

import (
	"context"
	"errors"
	"time"
)

// A transaction that lost to contention is run again, this many times at
// most, waiting from the first backoff up to the last between attempts.
const (
	txMaxAttempts  = 8
	txBackoffStart = 10 * time.Millisecond
	txBackoffMax   = 500 * time.Millisecond
)

// RetryTx runs tx, one whole transaction, again from scratch while it fails
// with an error retryable accepts (the store's contention errors). The SQL
// backends use it; the Spanner and Firestore clients retry by themselves.
func RetryTx(ctx context.Context, retryable func(error) bool, tx func() error) error {
	backoff := txBackoffStart
	for attempt := 1; ; attempt++ {
		err := tx()
		if err == nil || !retryable(err) || attempt >= txMaxAttempts {
			return err
		}
		select {
		case <-ctx.Done():
			return errors.Join(ctx.Err(), err)
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, txBackoffMax)
	}
}
