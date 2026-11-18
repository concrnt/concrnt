package migrations

import (
	"context"
	"log/slog"
	"maps"
	"slices"
	"sort"

	"golang.org/x/mod/semver"

	"github.com/concrnt/concrnt/internal/usecase/lease"
)

const VERSION_KEY = "net.concrnt.version"

// leaseKey serializes the migrators of replicas starting at the same time.
const leaseKey = "net.concrnt.migration"

var migrations = make(map[string]func(ctx context.Context) error)

type ContextKey string

const PostgresKey ContextKey = "postgres"

type Repository interface {
	GetState(ctx context.Context, name string) (string, error)
	SetState(ctx context.Context, name string, state string) error
}

type Migrator struct {
	repo  Repository
	lease lease.Lease
}

func NewMigrator(repo Repository, lease lease.Lease) *Migrator {
	return &Migrator{
		repo:  repo,
		lease: lease,
	}
}

type Migratable interface {
	Migrate() error
}

func Migrate(ctx context.Context, lease lease.Lease, migratables ...Migratable) error {

	release, err := lease.Acquire(ctx, leaseKey)
	if err != nil {
		return err
	}
	defer release()

	for _, m := range migratables {
		err := m.Migrate()
		if err != nil {
			return err
		}
	}

	return nil
}

func (m *Migrator) Do(ctx context.Context, version string) error {

	if !semver.IsValid(version) {
		slog.Warn("invalid version format. Skipping migration", "version", version)
		return nil
	}

	release, err := m.lease.Acquire(ctx, leaseKey)
	if err != nil {
		return err
	}
	defer release()

	currentVersion, err := m.repo.GetState(ctx, VERSION_KEY)
	if err != nil {
		return err
	}

	if currentVersion == "" {
		slog.Info("no current version found, setting to", "version", version)
		err = m.repo.SetState(ctx, VERSION_KEY, version)
		if err != nil {
			return err
		}
		return nil
	}

	allMigrations := slices.Collect(maps.Keys(migrations))
	sort.Slice(allMigrations, func(i, j int) bool {
		return semver.Compare(allMigrations[i], allMigrations[j]) < 0
	})

	splitter := sort.Search(len(allMigrations), func(i int) bool {
		return semver.Compare(allMigrations[i], currentVersion) > 0
	})

	toRun := allMigrations[splitter:]

	if len(toRun) == 0 {
		slog.Info("no migrations to run")
		return nil
	} else {
		slog.Info("migrations to run", "count", len(toRun), "versions", toRun)
	}

	for _, v := range toRun {
		slog.Info("running migration", "version", v)
		err = migrations[v](ctx)
		if err != nil {
			return err
		}

		err = m.repo.SetState(ctx, VERSION_KEY, v)
		if err != nil {
			return err
		}
	}

	slog.Info("migration completed", "version", version)

	return nil
}
