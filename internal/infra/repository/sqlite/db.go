// Package sqlite is the SQLite backend of the repository contract: GORM over
// the pure-Go (wasm, no cgo) github.com/ncruces/go-sqlite3 driver, with its
// own models (models.go) and migrations (migrations.go), and the backend
// that runs the contract suites without Docker.
package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/XSAM/otelsql"
	"github.com/ncruces/go-sqlite3/gormlite"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"gorm.io/plugin/opentelemetry/tracing"

	_ "github.com/ncruces/go-sqlite3/driver"
)

// DB is an open database. Close removes the file again when the path was
// ":memory:" (an ephemeral database; a real in-memory one cannot be shared
// across the pool's connections).
type DB struct {
	*gorm.DB
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

	sqlDB, err := otelsql.Open("sqlite3", dsn,
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
		if ephemeral != "" {
			os.RemoveAll(ephemeral)
		}
		return nil, fmt.Errorf("sqlite %s: %w", path, err)
	}

	gormLogger := logger.New(
		log.New(os.Stdout, "\r\n", log.LstdFlags),
		logger.Config{
			SlowThreshold:             300 * time.Millisecond,
			LogLevel:                  logger.Warn,
			IgnoreRecordNotFoundError: true,
			Colorful:                  true,
		},
	)
	db, err := gorm.Open(gormlite.OpenDB(sqlDB), &gorm.Config{
		TranslateError: true,
		Logger:         gormLogger,
	})
	if err != nil {
		sqlDB.Close()
		return nil, err
	}
	if err := db.Use(tracing.NewPlugin()); err != nil {
		slog.Error("failed to enable tracing plugin for sqlite", slog.String("error", err.Error()))
	}

	return &DB{DB: db, ephemeral: ephemeral}, nil
}

// SQL is the underlying connection pool (the embedded field hides gorm's
// DB method of the same name).
func (db *DB) SQL() *sql.DB {
	sqlDB, err := db.DB.DB()
	if err != nil {
		panic(err) // gormlite.OpenDB always sets a *sql.DB
	}
	return sqlDB
}

func (db *DB) Close() error {
	err := db.SQL().Close()
	if db.ephemeral != "" {
		os.RemoveAll(db.ephemeral)
	}
	return err
}

// Ready reports whether the database answers.
func (db *DB) Ready(ctx context.Context) error {
	return db.SQL().PingContext(ctx)
}
