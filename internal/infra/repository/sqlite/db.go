// Package sqlite is the SQLite backend of the repository contract over
// database/sql and the pure-Go modernc.org/sqlite driver (no GORM, no cgo):
// the reference for writing a backend without an ORM, and the backend that
// runs the contract suites without Docker.
package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/XSAM/otelsql"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	_ "modernc.org/sqlite"
)

// DB is an open database. Close removes the file again when the path was
// ":memory:" (an ephemeral database; a real in-memory one cannot be shared
// across the pool's connections).
type DB struct {
	*sql.DB
	ephemeral string
}

// Open opens (creating if needed) the database at path. Every pooled
// connection gets the same pragmas through the DSN: WAL so readers never
// block the writer, a busy timeout so writers queue instead of failing,
// foreign keys for the cascades the contract relies on, and case-sensitive
// LIKE (keys are case-sensitive). Transactions start IMMEDIATE, so the
// single writer lock is taken up front and a transaction never has to
// upgrade (the only way to hit SQLITE_BUSY mid-transaction). A pragma given
// here replaces the default of the same name (tests shorten the busy
// timeout); the driver applies them in its own order, so a duplicate name
// would not reliably win.
func Open(path string, pragmas ...string) (*DB, error) {
	ephemeral := ""
	if path == ":memory:" {
		dir, err := os.MkdirTemp("", "concrnt-sqlite-")
		if err != nil {
			return nil, err
		}
		ephemeral = dir
		path = filepath.Join(dir, "concrnt.db")
	}

	defaults := []string{
		"busy_timeout(5000)",
		"journal_mode(WAL)",
		"synchronous(NORMAL)",
		"foreign_keys(1)",
		"case_sensitive_like(1)",
	}
	for _, pragma := range pragmas {
		name, _, _ := strings.Cut(pragma, "(")
		replaced := false
		for i, def := range defaults {
			if strings.HasPrefix(def, name+"(") {
				defaults[i] = pragma
				replaced = true
			}
		}
		if !replaced {
			defaults = append(defaults, pragma)
		}
	}

	q := url.Values{}
	q.Set("_txlock", "immediate")
	q["_pragma"] = defaults
	dsn := "file:" + path + "?" + q.Encode()

	sqlDB, err := otelsql.Open("sqlite", dsn,
		otelsql.WithAttributes(semconv.DBSystemSqlite),
		otelsql.WithSpanOptions(otelsql.SpanOptions{
			OmitConnResetSession: true,
			OmitConnPrepare:      true,
			OmitConnQuery:        true,
			OmitRows:             true,
			OmitConnectorConnect: true,
		}),
	)
	if err != nil {
		return nil, err
	}
	if _, err := otelsql.RegisterDBStatsMetrics(sqlDB, otelsql.WithAttributes(semconv.DBSystemSqlite)); err != nil {
		sqlDB.Close()
		return nil, err
	}
	// never a single connection: usecase code reads outside the transaction
	// it has open (through ctx, not tx), which needs a second connection
	sqlDB.SetMaxOpenConns(8)
	sqlDB.SetMaxIdleConns(8)

	if err := sqlDB.PingContext(context.Background()); err != nil {
		sqlDB.Close()
		return nil, fmt.Errorf("sqlite %s: %w", path, err)
	}
	return &DB{DB: sqlDB, ephemeral: ephemeral}, nil
}

func (db *DB) Close() error {
	err := db.DB.Close()
	if db.ephemeral != "" {
		os.RemoveAll(db.ephemeral)
	}
	return err
}

// Ready reports whether the database answers.
func (db *DB) Ready(ctx context.Context) error {
	var one int
	return db.QueryRowContext(ctx, "SELECT 1").Scan(&one)
}

// Placeholders renders n comma-separated "?" for an IN list.
func Placeholders(n int) string {
	return strings.TrimSuffix(strings.Repeat("?, ", n), ", ")
}

// Args widens a string slice to query arguments.
func Args(values []string) []any {
	out := make([]any, len(values))
	for i, v := range values {
		out[i] = v
	}
	return out
}
