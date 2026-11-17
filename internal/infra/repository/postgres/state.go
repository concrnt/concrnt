package postgres

import (
	"context"
	"database/sql/driver"
	"log/slog"
	"time"

	"gorm.io/gorm"
)

type StateRepository struct {
	db *gorm.DB
}

func NewStateRepository(
	db *gorm.DB,
) *StateRepository {
	return &StateRepository{
		db: db,
	}
}

func (r *StateRepository) GetState(ctx context.Context, name string) (string, error) {
	var state string
	err := gorm.G[string](r.db).Raw("SELECT value FROM states WHERE name = ?", name).Scan(ctx, &state)
	if err != nil {
		return "", err
	}

	return state, nil
}

func (r *StateRepository) SetState(ctx context.Context, name string, value string) error {
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

const stateUnlockTimeout = 10 * time.Second

func (r *StateRepository) Lock(ctx context.Context, name string) (func(), error) {
	sqlDB, err := r.db.DB()
	if err != nil {
		return nil, err
	}

	conn, err := sqlDB.Conn(ctx)
	if err != nil {
		return nil, err
	}

	// this session is never returned to the pool: closing it drops both the
	// lock and the keepalive settings below
	discard := func() {
		_ = conn.Raw(func(any) error { return driver.ErrBadConn })
		conn.Close()
	}

	// the lock session sits idle while migrations run on other connections,
	// so only keepalive can tell postgres the holder vanished without a FIN
	// (node loss, network cut); the OS default would keep the lock ~2h
	for _, stmt := range []string{
		"SET tcp_keepalives_idle = 10",
		"SET tcp_keepalives_interval = 5",
		"SET tcp_keepalives_count = 3",
	} {
		if _, err = conn.ExecContext(ctx, stmt); err != nil {
			discard()
			return nil, err
		}
	}

	_, err = conn.ExecContext(ctx, "SELECT pg_advisory_lock(hashtextextended($1, 0))", name)
	if err != nil {
		discard()
		return nil, err
	}

	unlock := func() {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), stateUnlockTimeout)
		defer cancel()

		_, err := conn.ExecContext(ctx, "SELECT pg_advisory_unlock(hashtextextended($1, 0))", name)
		if err != nil {
			slog.Warn("failed to release state lock; closing its session instead", "name", name, "error", err)
		}
		discard()
	}

	return unlock, nil
}
