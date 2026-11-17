package migrations

import (
	"context"
	"log/slog"
	"maps"
	"slices"
	"sort"

	"golang.org/x/mod/semver"
)

const VERSION_KEY = "net.concrnt.version"

var migrations = make(map[string]func(ctx context.Context) error)

type Repository interface {
	GetState(ctx context.Context, name string) (string, error)
	SetState(ctx context.Context, name string, state string) error
}

type Migrator struct {
	repo Repository
}

func NewMigrator(repo Repository) *Migrator {
	return &Migrator{
		repo: repo,
	}
}

func (m *Migrator) Do(ctx context.Context, version string) error {

	if !semver.IsValid(version) {
		slog.Warn("invalid version format. Skipping migration", "version", version)
		return nil
	}

	// accuire lock
	// defer release lock

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
