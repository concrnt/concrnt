package lease

import (
	"context"
	"database/sql/driver"
	"log/slog"
	"time"

	"gorm.io/gorm"

	"github.com/concrnt/concrnt/internal/usecase/lease"
)

type Lease struct {
	db *gorm.DB
}

var _ lease.Lease = (*Lease)(nil)

func NewLease(db *gorm.DB) *Lease {
	return &Lease{db: db}
}

const leaseReleaseTimeout = 10 * time.Second

func (l *Lease) Acquire(ctx context.Context, name string) (func(), error) {
	sqlDB, err := l.db.DB()
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

	// the lease session sits idle while the holder works on other
	// connections, so only keepalive can tell postgres the holder vanished
	// without a FIN (node loss, network cut); the OS default would keep the
	// lease ~2h
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

	release := func() {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), leaseReleaseTimeout)
		defer cancel()

		_, err := conn.ExecContext(ctx, "SELECT pg_advisory_unlock(hashtextextended($1, 0))", name)
		if err != nil {
			slog.Warn("failed to release lease; closing its session instead", "name", name, "error", err)
		}
		discard()
	}

	return release, nil
}
