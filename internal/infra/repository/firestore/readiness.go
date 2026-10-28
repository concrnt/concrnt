package firestore

import (
	"context"
	"sync"
	"time"

	"cloud.google.com/go/firestore"
)

// Readiness probes the database with one cheap document read, memoized so a
// fleet of replicas polled every few seconds neither burns reads nor
// amplifies an outage.
type Readiness struct {
	client *firestore.Client
	ttl    time.Duration

	mu      sync.Mutex
	checked time.Time
	last    error
}

func NewReadiness(client *firestore.Client, ttl time.Duration) *Readiness {
	return &Readiness{client: client, ttl: ttl}
}

// Check returns nil when Firestore answered (a missing probe document still
// proves authentication and connectivity).
func (r *Readiness) Check(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.checked.IsZero() && time.Since(r.checked) < r.ttl {
		return r.last
	}

	_, err := r.client.Collection(colMeta).Doc("ready").Get(ctx)
	if err != nil && isNotFound(err) {
		err = nil
	}
	r.checked = time.Now()
	r.last = err
	return err
}
