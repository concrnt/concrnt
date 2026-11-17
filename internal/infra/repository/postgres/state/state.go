package state

import (
	"context"

	"gorm.io/gorm"
)

type Repository struct {
	db *gorm.DB
}

func New(
	db *gorm.DB,
) *Repository {
	return &Repository{
		db: db,
	}
}

func (r *Repository) Migrate() error {
	return r.db.AutoMigrate(&State{})
}

func (r *Repository) GetState(ctx context.Context, name string) (string, error) {
	var state string
	err := gorm.G[string](r.db).Raw("SELECT value FROM states WHERE name = ?", name).Scan(ctx, &state)
	if err != nil {
		return "", err
	}

	return state, nil
}

func (r *Repository) SetState(ctx context.Context, name string, value string) error {
	err := gorm.G[any](r.db).Exec(
		ctx,
		"INSERT INTO states (name, value) VALUES (?, ?) ON CONFLICT (name) DO UPDATE SET value = EXCLUDED.value",
		name,
		value,
	)
	if err != nil {
		return err
	}

	return nil
}
