package database

import (
	"context"
	"os"

	"cloud.google.com/go/firestore"

	"github.com/concrnt/concrnt/internal/infra/config"
)

// NewFirestore opens a Firestore client with Application Default
// Credentials. An empty project id is detected from the environment
// (metadata server / ADC), an empty database id means "(default)". An
// emulator is reached through FIRESTORE_EMULATOR_HOST, which the client
// library honours by itself; backends.firestore.emulatorHost sets it.
func NewFirestore(ctx context.Context, conf config.Backends) (*firestore.Client, error) {
	if conf.Firestore.EmulatorHost != "" {
		if err := os.Setenv("FIRESTORE_EMULATOR_HOST", conf.Firestore.EmulatorHost); err != nil {
			return nil, err
		}
	}
	projectID := conf.Firestore.ProjectID
	if projectID == "" {
		projectID = firestore.DetectProjectID
	}
	databaseID := conf.Firestore.DatabaseID
	if databaseID == "" {
		databaseID = firestore.DefaultDatabaseID
	}
	return firestore.NewClientWithDatabase(ctx, projectID, databaseID)
}
