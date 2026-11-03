package config

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"time"

	"github.com/go-yaml/yaml"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/impl/interop"
	"github.com/concrnt/concrnt/internal/domain"
)

type Config struct {
	Concrnt       Concrnt           `yaml:"concrnt"`
	Backends      Backends          `yaml:"backends"`
	Observability Observability     `yaml:"observability"`
	Integrations  Integrations      `yaml:"integrations"`
	Services      []interop.Service `yaml:"services"`
	Meta          map[string]any    `yaml:"meta"`
	// AdditionalWellKnown serves static documents under /.well-known/<name>.
	// Each entry is either a plain string (text/plain) or a json document
	// (application/json), see WellKnownEntry.
	AdditionalWellKnown map[string]WellKnownEntry `yaml:"additionalWellKnown"`
	// Static points at the files served under the fixed top-level paths
	// (/tos, /code-of-conduct, /register-template, /robots.txt).
	Static Static `yaml:"static"`
}

// DefaultStaticDir is where the static files live unless Static says
// otherwise.
const DefaultStaticDir = "/etc/concrnt/static"

// Static is the file behind each static route. Every entry is optional: an
// empty one falls back to its file under DefaultStaticDir, and a path whose
// file does not exist makes the route answer 404.
type Static struct {
	TOS              string `yaml:"tos"`              // /tos
	CodeOfConduct    string `yaml:"codeOfConduct"`    // /code-of-conduct
	RegisterTemplate string `yaml:"registerTemplate"` // /register-template
	RobotsTxt        string `yaml:"robotsTxt"`        // /robots.txt
}

// WithDefaults fills the unset entries with their default paths.
func (s Static) WithDefaults() Static {
	fill := func(path *string, name string) {
		if *path == "" {
			*path = filepath.Join(DefaultStaticDir, name)
		}
	}
	fill(&s.TOS, "tos.txt")
	fill(&s.CodeOfConduct, "code-of-conduct.txt")
	fill(&s.RegisterTemplate, "register-template.json")
	fill(&s.RobotsTxt, "robots.txt")
	return s
}

// WellKnownEntry is one static /.well-known/<name> document.
type WellKnownEntry struct {
	// Type is "string" (served as text/plain) or "json" (served as
	// application/json).
	Type string `yaml:"type"`
	// Value is the document body. For "string" it must be a string. For
	// "json" it is either a string holding a JSON document, served as is, or
	// a YAML mapping/sequence that is encoded to JSON.
	Value any `yaml:"value"`
}

type Concrnt struct {
	FQDN         string  `yaml:"fqdn"`
	PrivateKey   string  `yaml:"privatekey"`
	Registration string  `yaml:"registration"` // open, invite, close
	Layer        string  `yaml:"layer"`
	Debug        bool    `yaml:"debug"`
	Cluster      Cluster `yaml:"cluster"`
}

type Cluster struct {
	// Enable turns on multi-replica coordination: leadership decides which
	// replica runs the singleton workers, and replicas exchange realtime
	// subscription demand over the internal listener. Off means standalone:
	// this instance is the one and only replica.
	Enable bool `yaml:"enable"`
	// ElectorEndpoint is the leader-election / peer-discovery service (e.g.
	// the k8s-elector sidecar). Required when Enable is true.
	ElectorEndpoint string `yaml:"electorEndpoint"`
}

type Backends struct {
	// Database selects the record store: "postgres" (default), "sqlite",
	// "spanner" or "firestore".
	Database      string    `yaml:"database"`
	PostgresDsn   string    `yaml:"postgresDsn"`
	Postgres      Postgres  `yaml:"postgres"`
	SQLite        SQLite    `yaml:"sqlite"`
	Spanner       Spanner   `yaml:"spanner"`
	Firestore     Firestore `yaml:"firestore"`
	GatewayAddr   string    `yaml:"gatewayAddr"`
	RedisAddr     string    `yaml:"redisAddr"`
	RedisDB       int       `yaml:"redisDB"`
	MemcachedAddr string    `yaml:"memcachedAddr"`
	Aggregate     Aggregate `yaml:"aggregate"`
	JobQueue      JobQueue  `yaml:"jobqueue"`
}

// Postgres sizes the connection pool of one process.
type Postgres struct {
	// MaxOpenConns is the pool size (default 64).
	MaxOpenConns int `yaml:"maxOpenConns"`
	// MaxConcurrentTx caps the transactions running at once (default and
	// upper bound maxOpenConns/2).
	MaxConcurrentTx int `yaml:"maxConcurrentTx"`
}

const (
	DefaultPostgresMaxOpenConns = 64
	DefaultJobQueueConcurrency  = 16
)

// WithDefaults fills the unset values and caps MaxConcurrentTx at half the
// pool: a transaction's reads take a second connection (CONTRACT.md C4), so
// more transactions than that can leave every one waiting for a connection
// another holds. clamped reports that a configured value was lowered.
func (p Postgres) WithDefaults() (resolved Postgres, clamped bool) {
	if p.MaxOpenConns <= 0 {
		p.MaxOpenConns = DefaultPostgresMaxOpenConns
	}
	if p.MaxOpenConns < 2 {
		p.MaxOpenConns = 2
		clamped = true
	}
	limit := p.MaxOpenConns / 2
	if p.MaxConcurrentTx <= 0 {
		p.MaxConcurrentTx = limit
	}
	if p.MaxConcurrentTx > limit {
		p.MaxConcurrentTx = limit
		clamped = true
	}
	return p, clamped
}

// JobQueue configures the background job consumers of one process.
type JobQueue struct {
	// Concurrency is the number of consumer goroutines (default 16).
	Concurrency int `yaml:"concurrency"`
}

// ConcurrencyOrDefault is Concurrency, or its default when unset.
func (j JobQueue) ConcurrencyOrDefault() int {
	if j.Concurrency <= 0 {
		return DefaultJobQueueConcurrency
	}
	return j.Concurrency
}

// SQLite configures the sqlite record store.
type SQLite struct {
	// Path is the database file (default concrnt.db in the working
	// directory); ":memory:" is an ephemeral file removed on exit.
	Path string `yaml:"path"`
}

// SQLitePath is the configured database file or its default.
func (b Backends) SQLitePath() string {
	if b.SQLite.Path == "" {
		return "concrnt.db"
	}
	return b.SQLite.Path
}

// Spanner configures the spanner record store.
type Spanner struct {
	// Mode is "cloud" (default; Cloud Spanner with the ambient credentials),
	// "omni" (Spanner Omni at Endpoint, plain text) or "emulator" (the
	// Cloud Spanner emulator at Endpoint).
	Mode string `yaml:"mode"`
	// Endpoint is the host:port of the omni / emulator server.
	Endpoint string `yaml:"endpoint"`
	// Project and Instance name the database's instance; Spanner Omni
	// fixes both to "default", which is the default here.
	Project  string `yaml:"project"`
	Instance string `yaml:"instance"`
	// Database is the database name (default concrnt). It must exist
	// (conctl / the Omni CLI create it); the server creates its schema.
	Database string `yaml:"database"`
}

// Firestore configures the firestore record store.
type Firestore struct {
	// ProjectID is the Google Cloud project; empty means detect it from the
	// environment (metadata server, ADC).
	ProjectID string `yaml:"projectID"`
	// DatabaseID is the Firestore database (default "(default)").
	DatabaseID string `yaml:"databaseID"`
	// EmulatorHost is the host:port of a Firestore emulator; it sets
	// FIRESTORE_EMULATOR_HOST (which can also be set directly).
	EmulatorHost string `yaml:"emulatorHost"`
}

// SpannerDatabasePath is the full database resource name.
func (b Backends) SpannerDatabasePath() string {
	project, instance, db := b.Spanner.Project, b.Spanner.Instance, b.Spanner.Database
	if project == "" {
		project = "default"
	}
	if instance == "" {
		instance = "default"
	}
	if db == "" {
		db = "concrnt"
	}
	return "projects/" + project + "/instances/" + instance + "/databases/" + db
}

// Aggregate configures the count cache (internal/usecase/aggregate).
type Aggregate struct {
	// Store is "postgres" (a table in the record database), "spanner" (same)
	// or "redis"; empty means the store matching the record database.
	Store string `yaml:"store"`
	// TTL is how long a computed value is served before it is recomputed
	// (default 24h); MaxDeltas how many writes an entry absorbs before it is
	// recomputed (default 10000).
	TTL       time.Duration `yaml:"ttl"`
	MaxDeltas int           `yaml:"maxDeltas"`
}

type Observability struct {
	EnableTrace   bool   `yaml:"enableTrace"`
	TraceEndpoint string `yaml:"traceEndpoint"`
}

type Integrations struct {
	CaptchaSitekey  string `yaml:"captchaSiteKey"`
	CaptchaSecret   string `yaml:"captchaSecret"`
	VapidPublicKey  string `yaml:"vapidPublicKey"`
	VapidPrivateKey string `yaml:"vapidPrivateKey"`
}

func DeepMerge(dst, src any) error {
	dstPtr := reflect.ValueOf(dst)
	srcPtr := reflect.ValueOf(src)

	if dstPtr.Kind() != reflect.Pointer || srcPtr.Kind() != reflect.Pointer {
		return fmt.Errorf("both arguments must be pointers")
	}

	dstElem := dstPtr.Elem()
	srcElem := srcPtr.Elem()
	if dstElem.Kind() != reflect.Struct || srcElem.Kind() != reflect.Struct {
		return fmt.Errorf("both arguments must be pointers to structs")
	}

	dstType := dstElem.Type()
	for i := range dstElem.NumField() {
		dstField := dstElem.Field(i)
		srcField := srcElem.Field(i)
		structField := dstType.Field(i)

		if !dstField.CanSet() {
			continue
		}

		if isZeroValue(srcField) {
			continue
		}

		switch dstField.Kind() {
		case reflect.Struct:
			if err := DeepMerge(dstField.Addr().Interface(), srcField.Addr().Interface()); err != nil {
				return fmt.Errorf("error merging field %s: %w", structField.Name, err)
			}
		default:
			dstField.Set(srcField)
		}
	}

	return nil
}

func isZeroValue(v reflect.Value) bool {
	return reflect.DeepEqual(v.Interface(), reflect.Zero(v.Type()).Interface())
}

func Load(path string) (Config, error) {

	info, err := os.Stat(path)
	if err != nil {
		return Config{}, err
	}

	var config Config
	if info.IsDir() {
		files, err := os.ReadDir(path)
		if err != nil {
			slog.Error("Failed to read config directory", "path", path, "error", err)
			return Config{}, err
		}

		for _, file := range files {
			if file.IsDir() {
				continue
			}

			entryPath := filepath.Join(path, file.Name())
			slog.Debug("Loading config from file", "path", entryPath)

			f, err := os.Open(entryPath)
			if err != nil {
				slog.Warn("Failed to open config file, skipping", "path", entryPath, "error", err)
				continue
			}
			defer f.Close()

			var tmp Config
			err = yaml.NewDecoder(f).Decode(&tmp)
			if err != nil {
				slog.Warn("Failed to decode config file, skipping", "path", entryPath, "error", err)
				continue
			}

			err = DeepMerge(&config, &tmp)
			if err != nil {
				slog.Warn("Failed to merge config file, skipping", "path", entryPath, "error", err)
				continue
			}
		}
	} else {
		file, err := os.Open(path)
		if err != nil {
			return Config{}, err
		}

		slog.Debug("Loading config from file", "path", path)

		err = yaml.NewDecoder(file).Decode(&config)
		if err != nil {
			return Config{}, err
		}

		return config, nil
	}

	return config, nil
}

func (c Config) DomainConfig() domain.Config {

	csid, err := concrnt.PrivKeyToAddr(c.Concrnt.PrivateKey, "ccs")
	if err != nil {
		panic(err)
	}

	return domain.Config{
		FQDN:         c.Concrnt.FQDN,
		PrivateKey:   c.Concrnt.PrivateKey,
		Registration: c.Concrnt.Registration,
		Layer:        c.Concrnt.Layer,
		CSID:         csid,
		Meta:         c.Meta,
		Debug:        c.Concrnt.Debug,
	}
}
