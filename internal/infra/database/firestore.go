package database

import (
	"context"

	"cloud.google.com/go/firestore"
)

// NewFirestore opens a Firestore client with Application Default Credentials.
// An empty projectID is detected from the environment (metadata server /
// ADC), an empty databaseID means "(default)". FIRESTORE_EMULATOR_HOST in the
// environment redirects the client to an emulator.
func NewFirestore(ctx context.Context, projectID, databaseID string) (*firestore.Client, error) {
	if projectID == "" {
		projectID = firestore.DetectProjectID
	}
	if databaseID == "" {
		databaseID = firestore.DefaultDatabaseID
	}
	return firestore.NewClientWithDatabase(ctx, projectID, databaseID)
}
