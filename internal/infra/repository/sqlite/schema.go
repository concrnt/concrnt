package sqlite

import (
	"context"
	"database/sql"
	"fmt"
)

// The schema mirrors the Postgres one table for table (see
// internal/infra/database/models): text ids, INTEGER microsecond
// timestamps, INTEGER booleans, JSON text for arrays, and the same
// foreign-key cascades. Indexes come from the shape registry (indexes.go);
// only primary keys and unique constraints are declared here.
var tables = []string{
	`CREATE TABLE IF NOT EXISTS commit_logs (
		id TEXT PRIMARY KEY,
		ip TEXT NOT NULL DEFAULT '',
		document TEXT NOT NULL DEFAULT '',
		proof TEXT NOT NULL DEFAULT '',
		owner TEXT NOT NULL DEFAULT '',
		gc_candidate INTEGER NOT NULL DEFAULT 0,
		c_date INTEGER NOT NULL
	)`,
	`CREATE TABLE IF NOT EXISTS records (
		document_id TEXT PRIMARY KEY REFERENCES commit_logs(id) ON DELETE CASCADE,
		owner TEXT NOT NULL DEFAULT '',
		author TEXT NOT NULL DEFAULT '',
		redirect TEXT,
		schema TEXT NOT NULL DEFAULT '',
		policies TEXT,
		distributions TEXT NOT NULL DEFAULT '[]',
		created_at INTEGER NOT NULL,
		c_date INTEGER NOT NULL
	)`,
	`CREATE TABLE IF NOT EXISTS record_keys (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		parent_id INTEGER,
		uri TEXT NOT NULL UNIQUE,
		record_id TEXT UNIQUE REFERENCES records(document_id) ON DELETE CASCADE,
		record_created_at INTEGER,
		redirect TEXT,
		clean_on_update INTEGER NOT NULL DEFAULT 0
	)`,
	`CREATE TABLE IF NOT EXISTS associations (
		document_id TEXT PRIMARY KEY REFERENCES commit_logs(id) ON DELETE CASCADE,
		target_id INTEGER NOT NULL REFERENCES record_keys(id) ON DELETE CASCADE,
		owner TEXT NOT NULL DEFAULT '',
		author TEXT NOT NULL DEFAULT '',
		schema TEXT NOT NULL DEFAULT '',
		variant TEXT,
		"unique" TEXT NOT NULL UNIQUE,
		created_at INTEGER NOT NULL,
		c_date INTEGER NOT NULL
	)`,
	`CREATE TABLE IF NOT EXISTS acks (
		"from" TEXT NOT NULL,
		"to" TEXT NOT NULL,
		schema TEXT NOT NULL,
		document_id TEXT PRIMARY KEY REFERENCES commit_logs(id) ON DELETE CASCADE,
		valid INTEGER NOT NULL DEFAULT 1,
		created_at INTEGER NOT NULL,
		c_date INTEGER NOT NULL,
		UNIQUE ("from", "to", schema)
	)`,
	`CREATE TABLE IF NOT EXISTS ackeds (
		"from" TEXT NOT NULL,
		"to" TEXT NOT NULL,
		schema TEXT NOT NULL,
		document_id TEXT PRIMARY KEY REFERENCES commit_logs(id) ON DELETE CASCADE,
		valid INTEGER NOT NULL DEFAULT 1,
		created_at INTEGER NOT NULL,
		c_date INTEGER NOT NULL,
		UNIQUE ("from", "to", schema)
	)`,
	`CREATE TABLE IF NOT EXISTS entities (
		id TEXT PRIMARY KEY,
		alias TEXT,
		domain TEXT NOT NULL DEFAULT '',
		tag TEXT NOT NULL DEFAULT '',
		document_id TEXT NOT NULL REFERENCES commit_logs(id) ON DELETE CASCADE,
		created_at INTEGER NOT NULL DEFAULT 0,
		c_date INTEGER NOT NULL,
		m_date INTEGER NOT NULL
	)`,
	`CREATE TABLE IF NOT EXISTS entity_metas (
		id TEXT PRIMARY KEY,
		inviter TEXT,
		info TEXT NOT NULL DEFAULT 'null',
		c_date INTEGER NOT NULL,
		m_date INTEGER NOT NULL
	)`,
	`CREATE TABLE IF NOT EXISTS servers (
		id TEXT PRIMARY KEY,
		cs_id TEXT NOT NULL DEFAULT '',
		tag TEXT NOT NULL DEFAULT '',
		layer TEXT NOT NULL DEFAULT '',
		well_known TEXT NOT NULL DEFAULT '',
		c_date INTEGER NOT NULL,
		m_date INTEGER NOT NULL,
		last_scraped INTEGER
	)`,
	`CREATE TABLE IF NOT EXISTS subscriptions (
		vendor_id TEXT NOT NULL,
		owner TEXT NOT NULL,
		schemas TEXT NOT NULL DEFAULT '[]',
		prefixes TEXT NOT NULL DEFAULT '[]',
		subscription TEXT NOT NULL DEFAULT '',
		c_date INTEGER NOT NULL,
		m_date INTEGER NOT NULL,
		PRIMARY KEY (vendor_id, owner)
	)`,
	`CREATE TABLE IF NOT EXISTS abuse_reports (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		ip TEXT NOT NULL DEFAULT '',
		reporter TEXT NOT NULL DEFAULT '',
		target_uri TEXT NOT NULL DEFAULT '',
		body TEXT NOT NULL DEFAULT '',
		c_date INTEGER NOT NULL
	)`,
}

type migration struct {
	Version int
	Name    string
	Up      func(ctx context.Context, tx *sql.Tx) error
}

var migrations = []migration{
	{1, "baseline_tables", func(ctx context.Context, tx *sql.Tx) error {
		for _, stmt := range tables {
			if _, err := tx.ExecContext(ctx, stmt); err != nil {
				return err
			}
		}
		return nil
	}},
}

// Migrate applies the pending numbered steps and then creates every shape
// index that is missing, all in one IMMEDIATE transaction (the writer lock
// serialises concurrent starters).
func Migrate(ctx context.Context, db *DB) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		version INTEGER PRIMARY KEY,
		name TEXT NOT NULL,
		applied_at INTEGER NOT NULL
	)`); err != nil {
		return err
	}
	rows, err := tx.QueryContext(ctx, "SELECT version FROM schema_migrations")
	if err != nil {
		return err
	}
	applied := map[int]bool{}
	for rows.Next() {
		var v int
		if err := rows.Scan(&v); err != nil {
			rows.Close()
			return err
		}
		applied[v] = true
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}

	for _, m := range migrations {
		if applied[m.Version] {
			continue
		}
		if err := m.Up(ctx, tx); err != nil {
			return fmt.Errorf("migration %d (%s): %w", m.Version, m.Name, err)
		}
		if _, err := tx.ExecContext(ctx, "INSERT INTO schema_migrations (version, name, applied_at) VALUES (?, ?, ?)", m.Version, m.Name, nowMicros()); err != nil {
			return err
		}
	}

	ddl, err := IndexDDL()
	if err != nil {
		return err
	}
	for _, stmt := range ddl {
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("%s: %w", stmt, err)
		}
	}
	return tx.Commit()
}
