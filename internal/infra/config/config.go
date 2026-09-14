package config

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"

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
	// Database selects the persistence backend: "postgres" (the default when
	// empty) or "firestore".
	Database      string    `yaml:"database"`
	PostgresDsn   string    `yaml:"postgresDsn"`
	Firestore     Firestore `yaml:"firestore"`
	GatewayAddr   string    `yaml:"gatewayAddr"`
	RedisAddr     string    `yaml:"redisAddr"`
	RedisDB       int       `yaml:"redisDB"`
	MemcachedAddr string    `yaml:"memcachedAddr"`
}

// Firestore configures the Cloud Firestore (Native mode) backend, used when
// Backends.Database is "firestore". Credentials come from Application Default
// Credentials (Workload Identity on GKE, `gcloud auth application-default
// login` locally); FIRESTORE_EMULATOR_HOST in the environment redirects the
// client to an emulator.
type Firestore struct {
	// ProjectID is the GCP project; empty means detect it from the environment
	// (metadata server / ADC).
	ProjectID string `yaml:"projectID"`
	// DatabaseID names the Firestore database; empty means "(default)".
	DatabaseID string `yaml:"databaseID"`
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
