package testutil

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"github.com/ory/dockertest"
	dc "github.com/ory/dockertest/docker"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"

	spannerrepo "github.com/concrnt/concrnt/internal/infra/repository/spanner"
)

// SpannerOmniImage is the Spanner Omni image the tests start; the tag is
// pinned because the evaluation license and the DDL surface are per release.
const (
	SpannerOmniImage = "us-docker.pkg.dev/spanner-omni/images/spanner-omni"
	SpannerOmniTag   = "2026.r4-lts"
	// SpannerInstance is the instance path Spanner Omni serves; it accepts
	// no other project or instance name.
	SpannerInstance = "projects/default/instances/default"
)

var (
	spannerOnce     sync.Once
	spannerEndpoint string
	spannerResource *dockertest.Resource
)

// spannerServer returns the endpoint of a Spanner Omni server: the one named
// by CONCRNT_TEST_SPANNER_ENDPOINT, or one started here once per test
// process (one container, one database per CreateSpanner call).
func spannerServer(t *testing.T) string {
	t.Helper()
	if os.Getenv("CONCRNT_TEST_SPANNER") == "0" {
		t.Skip("CONCRNT_TEST_SPANNER=0")
	}
	spannerOnce.Do(func() {
		if ep := os.Getenv("CONCRNT_TEST_SPANNER_ENDPOINT"); ep != "" {
			spannerEndpoint = ep
			return
		}
		pool := getPool()
		resource, err := pool.RunWithOptions(&dockertest.RunOptions{
			Name:         "concrnt_spanner_omni_" + getSuffix() + fmt.Sprintf("_%d", os.Getpid()),
			Repository:   SpannerOmniImage,
			Tag:          SpannerOmniTag,
			Cmd:          []string{"start-single-server", "--listen-addresses", "0.0.0.0"},
			ExposedPorts: []string{"15000/tcp"},
		}, func(hc *dc.HostConfig) {
			hc.AutoRemove = true
		})
		if err != nil {
			log.Fatalf("Could not start spanner omni: %s", err)
		}
		// the container is removed when it stops; the expiry is the safety
		// net for a test process killed before it could purge
		_ = resource.Expire(3600)
		spannerResource = resource
		spannerEndpoint = "localhost:" + resource.GetPort("15000/tcp")
		log.Printf("Spanner Omni running on %s\n", spannerEndpoint)

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		admin, err := database.NewDatabaseAdminClientWithConfig(ctx, spannerClientConfig(), option.WithEndpoint(spannerEndpoint))
		if err != nil {
			log.Fatalf("Could not create spanner admin client: %s", err)
		}
		defer admin.Close()
		if err := pool.Retry(func() error {
			it := admin.ListDatabases(ctx, &databasepb.ListDatabasesRequest{Parent: SpannerInstance})
			_, err := it.Next()
			if err == iterator.Done {
				return nil
			}
			return err
		}); err != nil {
			log.Fatalf("Spanner Omni did not become ready: %s", err)
		}
	})
	return spannerEndpoint
}

func spannerClientConfig() spanner.ClientConfig {
	return spanner.ClientConfig{Type: spanner.OMNI, UsePlainText: true, DisableNativeMetrics: true}
}

// PurgeSpanner drops the test database and stops the Spanner Omni container
// this process started, if any. Test packages call it from TestMain after
// m.Run.
func PurgeSpanner() {
	if spannerDB != nil {
		spannerDB.client.Close()
		_ = spannerDB.admin.DropDatabase(context.Background(), &databasepb.DropDatabaseRequest{Database: spannerDB.name})
		spannerDB.admin.Close()
		spannerDB = nil
	}
	if spannerResource != nil {
		_ = getPool().Purge(spannerResource)
		spannerResource = nil
	}
}

// CreateSpanner returns clients for a migrated test database holding no
// rows. Migrating a database takes Spanner Omni the better part of a minute,
// so one database is created per test process and emptied (every table but
// schema_migrations) before each call; the database is dropped by
// PurgeSpanner. Tests sharing it must not run in parallel.
func CreateSpanner(t *testing.T) (*spanner.Client, *database.DatabaseAdminClient) {
	t.Helper()
	endpoint := spannerServer(t)
	ctx := context.Background()

	spannerDBOnce.Do(func() {
		admin, err := database.NewDatabaseAdminClientWithConfig(ctx, spannerClientConfig(), option.WithEndpoint(endpoint))
		if err != nil {
			log.Fatalf("spanner admin client: %s", err)
		}
		name := strings.ToLower(fmt.Sprintf("t%d_%d", os.Getpid()%100000, time.Now().UnixNano()%1000000))
		op, err := admin.CreateDatabase(ctx, &databasepb.CreateDatabaseRequest{
			Parent:          SpannerInstance,
			CreateStatement: "CREATE DATABASE " + name,
		})
		if err != nil {
			log.Fatalf("create spanner database: %s", err)
		}
		db, err := op.Wait(ctx)
		if err != nil {
			log.Fatalf("create spanner database: %s", err)
		}
		client, err := spanner.NewClientWithConfig(ctx, db.Name, spannerClientConfig(), option.WithEndpoint(endpoint))
		if err != nil {
			log.Fatalf("spanner client: %s", err)
		}
		if err := spannerrepo.Migrate(ctx, admin, client); err != nil {
			log.Fatalf("migrate spanner database: %s", err)
		}
		spannerDB = &spannerDatabase{client: client, admin: admin, name: db.Name}
	})

	// commit_logs takes its interleaved rows (records, acks, ackeds,
	// associations) and foreign-keyed rows (entities, the record_keys rows
	// pointing at a record) with it; what is left in record_keys are the
	// placeholders, which nothing cascades from any more
	for _, table := range []string{"commit_logs", "record_keys", "entity_meta", "servers", "subscriptions", "abuse_reports", "aggregate_entries"} {
		_, err := spannerDB.client.ReadWriteTransaction(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
			_, err := tx.Update(ctx, spanner.Statement{SQL: "DELETE FROM " + table + " WHERE true"})
			return err
		})
		if err != nil {
			t.Fatalf("empty %s: %s", table, err)
		}
	}
	return spannerDB.client, spannerDB.admin
}

type spannerDatabase struct {
	client *spanner.Client
	admin  *database.DatabaseAdminClient
	name   string
}

var (
	spannerDBOnce sync.Once
	spannerDB     *spannerDatabase
)
