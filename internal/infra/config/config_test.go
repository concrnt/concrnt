package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestPostgresMaxOpenConnsOrDefault(t *testing.T) {
	if got := (Postgres{}).MaxOpenConnsOrDefault(); got != 64 {
		t.Fatalf("default = %d, want 64", got)
	}
	if got := (Postgres{MaxOpenConns: 20}).MaxOpenConnsOrDefault(); got != 20 {
		t.Fatalf("configured = %d, want 20", got)
	}
}

func TestJobQueueConcurrencyOrDefault(t *testing.T) {
	if got := (JobQueue{}).ConcurrencyOrDefault(); got != 16 {
		t.Fatalf("default = %d, want 16", got)
	}
	if got := (JobQueue{Concurrency: 3}).ConcurrencyOrDefault(); got != 3 {
		t.Fatalf("configured = %d, want 3", got)
	}
}

func TestLoadPoolSettings(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	yaml := `backends:
  postgresDsn: "host=db"
  postgres:
    maxOpenConns: 40
  jobqueue:
    concurrency: 12
`
	if err := os.WriteFile(path, []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}
	conf, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := conf.Backends.Postgres.MaxOpenConnsOrDefault(); got != 40 {
		t.Fatalf("postgres maxOpenConns = %d", got)
	}
	if got := conf.Backends.JobQueue.ConcurrencyOrDefault(); got != 12 {
		t.Fatalf("jobqueue concurrency = %d", got)
	}
}
