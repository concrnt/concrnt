package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestPostgresWithDefaults(t *testing.T) {
	cases := []struct {
		name    string
		in      Postgres
		want    Postgres
		clamped bool
	}{
		{"unset", Postgres{}, Postgres{MaxOpenConns: 64, MaxConcurrentTx: 32}, false},
		{"pool only", Postgres{MaxOpenConns: 20}, Postgres{MaxOpenConns: 20, MaxConcurrentTx: 10}, false},
		{"both within bound", Postgres{MaxOpenConns: 32, MaxConcurrentTx: 8}, Postgres{MaxOpenConns: 32, MaxConcurrentTx: 8}, false},
		{"tx at bound", Postgres{MaxOpenConns: 32, MaxConcurrentTx: 16}, Postgres{MaxOpenConns: 32, MaxConcurrentTx: 16}, false},
		{"tx above pool", Postgres{MaxOpenConns: 16, MaxConcurrentTx: 64}, Postgres{MaxOpenConns: 16, MaxConcurrentTx: 8}, true},
		{"tx above half", Postgres{MaxOpenConns: 16, MaxConcurrentTx: 9}, Postgres{MaxOpenConns: 16, MaxConcurrentTx: 8}, true},
		{"tx above default pool", Postgres{MaxConcurrentTx: 40}, Postgres{MaxOpenConns: 64, MaxConcurrentTx: 32}, true},
		{"pool of one", Postgres{MaxOpenConns: 1}, Postgres{MaxOpenConns: 2, MaxConcurrentTx: 1}, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, clamped := c.in.WithDefaults()
			if got != c.want || clamped != c.clamped {
				t.Fatalf("WithDefaults(%+v) = %+v, %v; want %+v, %v", c.in, got, clamped, c.want, c.clamped)
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
	pg, clamped := conf.Backends.Postgres.WithDefaults()
	if pg.MaxOpenConns != 40 || pg.MaxConcurrentTx != 20 || !clamped {
		t.Fatalf("postgres = %+v clamped=%v", pg, clamped)
	}
	if got := conf.Backends.JobQueue.ConcurrencyOrDefault(); got != 12 {
		t.Fatalf("jobqueue concurrency = %d", got)
	}
}
