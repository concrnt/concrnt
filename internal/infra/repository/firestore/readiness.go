package firestore

import (
	"context"

	"cloud.google.com/go/firestore"
)

// Ping checks that the database answers with one document read (a missing
// probe document still proves authentication and connectivity). Every call
// is a billed read, so the caller sets the cadence: the readiness watcher
// runs it on an interval of its own, not once per probe.
func Ping(ctx context.Context, client *firestore.Client) error {
	_, err := client.Collection(ColMeta).Doc("ready").Get(ctx)
	if err != nil && IsNotFound(err) {
		return nil
	}
	return err
}
