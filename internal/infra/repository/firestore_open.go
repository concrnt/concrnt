package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/concrnt/concrnt/internal/infra/config"
	"github.com/concrnt/concrnt/internal/infra/database"
	fsrepo "github.com/concrnt/concrnt/internal/infra/repository/firestore"
)

// readinessTTL memoizes the Firestore readiness probe between /ready polls.
const readinessTTL = 15 * time.Second

func openFirestore(ctx context.Context, conf config.Backends, deps Deps) (*Set, error) {
	client, err := database.NewFirestore(ctx, conf.Firestore.ProjectID, conf.Firestore.DatabaseID)
	if err != nil {
		return nil, fmt.Errorf("failed to connect firestore: %w", err)
	}

	// best effort, off the startup path: surfaces missing composite indexes
	// (with their creation URLs) in the log before users hit them
	go fsrepo.VerifyIndexes(context.WithoutCancel(ctx), client)

	domainConfig := deps.DomainConfig
	return &Set{
		Record:            fsrepo.NewRecordRepository(client),
		RecordMaintenance: fsrepo.NewRecordMaintenanceRepository(client),
		Residence:         fsrepo.NewResidenceRepository(client),
		Chunkline:         fsrepo.NewChunklineRepository(client),
		Server:            fsrepo.NewServerRepository(&domainConfig, client, deps.Client),
		Notification:      fsrepo.NewNotificationRepository(client),
		Abuse:             fsrepo.NewAbuseRepository(client),

		Name:       "firestore",
		Collectors: fsrepo.Collectors(),
		Ready:      fsrepo.NewReadiness(client, readinessTTL).Check,
		Close:      client.Close,
	}, nil
}
