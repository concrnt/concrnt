package database

import (
	"context"
	"fmt"
	"os"

	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"google.golang.org/api/option"

	"github.com/concrnt/concrnt/internal/infra/config"
)

// NewSpanner opens the data and admin clients of the configured database.
// Three targets share the code: Cloud Spanner (ambient credentials), Spanner
// Omni (plain text at the endpoint, Type OMNI so the client skips the Cloud
// Monitoring export) and the Cloud Spanner emulator (SPANNER_EMULATOR_HOST,
// which the client library honours by itself).
func NewSpanner(ctx context.Context, conf config.Backends) (*spanner.Client, *database.DatabaseAdminClient, error) {
	cfg := spanner.ClientConfig{DisableNativeMetrics: true}
	var opts []option.ClientOption
	switch conf.Spanner.Mode {
	case "", "cloud":
	case "omni":
		if conf.Spanner.Endpoint == "" {
			return nil, nil, fmt.Errorf("backends.spanner.endpoint is required in omni mode")
		}
		cfg.Type = spanner.OMNI
		cfg.UsePlainText = true
		opts = append(opts, option.WithEndpoint(conf.Spanner.Endpoint))
	case "emulator":
		if conf.Spanner.Endpoint != "" {
			if err := os.Setenv("SPANNER_EMULATOR_HOST", conf.Spanner.Endpoint); err != nil {
				return nil, nil, err
			}
		}
	default:
		return nil, nil, fmt.Errorf("unknown backends.spanner.mode %q (cloud, omni, emulator)", conf.Spanner.Mode)
	}

	admin, err := database.NewDatabaseAdminClientWithConfig(ctx, cfg, opts...)
	if err != nil {
		return nil, nil, err
	}
	client, err := spanner.NewClientWithConfig(ctx, conf.SpannerDatabasePath(), cfg, opts...)
	if err != nil {
		admin.Close()
		return nil, nil, err
	}
	return client, admin, nil
}
