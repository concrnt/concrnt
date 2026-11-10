package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestPostgresWithDefaults(t *testing.T) {
	cases := []struct {
		name string
		in   Postgres
		want Postgres
	}{
		{"unset", Postgres{}, Postgres{MaxOpenConns: 64}},
		{"configured", Postgres{MaxOpenConns: 20}, Postgres{MaxOpenConns: 20}},
		{"pool of one", Postgres{MaxOpenConns: 1}, Postgres{MaxOpenConns: 1}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := c.in.WithDefaults(); got != c.want {
				t.Fatalf("WithDefaults(%+v) = %+v; want %+v", c.in, got, c.want)
			}
		})
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
    maxConcurrentTx: 30
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
	// maxConcurrentTx is a key of older configs: it is ignored, not an error
	if pg := conf.Backends.Postgres.WithDefaults(); pg.MaxOpenConns != 40 {
		t.Fatalf("postgres = %+v", pg)
	}
	if got := conf.Backends.JobQueue.ConcurrencyOrDefault(); got != 12 {
		t.Fatalf("jobqueue concurrency = %d", got)
	}
}
