// Package repository selects and opens the persistence backend. Everything
// above the infra layer talks to the store through the usecase repository
// interfaces; this package is the one place that knows which implementation
// (Postgres or Firestore) is behind them.
package repository

import (
	"context"
	"fmt"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"gorm.io/gorm"

	"github.com/concrnt/concrnt/client"
	"github.com/concrnt/concrnt/internal/domain"
	"github.com/concrnt/concrnt/internal/infra/config"
	"github.com/concrnt/concrnt/internal/infra/database"
	"github.com/concrnt/concrnt/internal/infra/repository/postgres"
	"github.com/concrnt/concrnt/internal/usecase/abuse"
	"github.com/concrnt/concrnt/internal/usecase/chunkline"
	"github.com/concrnt/concrnt/internal/usecase/notification"
	"github.com/concrnt/concrnt/internal/usecase/record"
	"github.com/concrnt/concrnt/internal/usecase/residence"
	"github.com/concrnt/concrnt/internal/usecase/server"
)

// Set bundles the persistence adapters of one backend.
type Set struct {
	Record            record.Repository
	RecordMaintenance record.MaintenanceRepository
	Residence         residence.Repository
	Chunkline         chunkline.Repository
	Server            server.Repository
	Notification      notification.Repository
	Abuse             abuse.Repository

	// Name is "postgres" or "firestore", for logs.
	Name string
	// Collectors are backend-specific Prometheus collectors (connection pool
	// stats for Postgres, operation latency for Firestore).
	Collectors []prometheus.Collector
	// Ready reports whether the store is reachable; it gates /ready.
	Ready func(ctx context.Context) error
	Close func() error

	// SQL is the raw GORM handle, non-nil only for the Postgres backend. It
	// exists for conctl operations that still speak SQL (legacy repairs and
	// v1 migrations).
	SQL *gorm.DB
}

// Deps are the collaborators repositories need beyond the store itself.
type Deps struct {
	Client       *client.Client
	DomainConfig domain.Config
}

// Open connects to the backend named by conf.Database ("" and "postgres"
// select Postgres) and, for Postgres, runs the schema migration.
func Open(ctx context.Context, conf config.Backends, deps Deps) (*Set, error) {
	switch strings.ToLower(conf.Database) {
	case "", "postgres":
		return openPostgres(conf, deps)
	case "firestore":
		return openFirestore(ctx, conf, deps)
	default:
		return nil, fmt.Errorf("unknown backends.database %q (expected postgres or firestore)", conf.Database)
	}
}

func openPostgres(conf config.Backends, deps Deps) (*Set, error) {
	db, err := database.NewPostgres(conf.PostgresDsn)
	if err != nil {
		return nil, fmt.Errorf("failed to connect database: %w", err)
	}
	if err := database.MigratePostgres(db); err != nil {
		return nil, fmt.Errorf("failed to migrate database: %w", err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		return nil, fmt.Errorf("failed to get sql.DB: %w", err)
	}

	domainConfig := deps.DomainConfig
	return &Set{
		Record:            postgres.NewRecordRepository(db),
		RecordMaintenance: postgres.NewRecordMaintenanceRepository(db),
		Residence:         postgres.NewResidenceRepository(db, deps.Client, domainConfig),
		Chunkline:         postgres.NewChunklineRepository(db),
		Server:            postgres.NewServerRepository(&domainConfig, db, deps.Client),
		Notification:      postgres.NewNotificationRepository(db),
		Abuse:             postgres.NewAbuseRepository(db),

		Name: "postgres",
		// connection pool usage (go_sql_*): the pool is small, so saturation
		// and wait time are the first thing to check when everything gets slow
		Collectors: []prometheus.Collector{collectors.NewDBStatsCollector(sqlDB, "concrnt")},
		Ready:      sqlDB.PingContext,
		Close:      sqlDB.Close,
		SQL:        db,
	}, nil
}
