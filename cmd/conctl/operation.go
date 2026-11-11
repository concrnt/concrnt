package main

import (
	"cmp"
	"context"
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"gorm.io/gorm"

	"github.com/concrnt/concrnt/client"
	"github.com/concrnt/concrnt/internal/domain"
	aggfirestore "github.com/concrnt/concrnt/internal/infra/aggregate/firestore"
	aggmemcached "github.com/concrnt/concrnt/internal/infra/aggregate/memcached"
	aggpostgres "github.com/concrnt/concrnt/internal/infra/aggregate/postgres"
	aggredis "github.com/concrnt/concrnt/internal/infra/aggregate/redis"
	aggspanner "github.com/concrnt/concrnt/internal/infra/aggregate/spanner"
	"github.com/concrnt/concrnt/internal/infra/config"
	"github.com/concrnt/concrnt/internal/infra/database"
	fsrepo "github.com/concrnt/concrnt/internal/infra/repository/firestore"
	fsrecord "github.com/concrnt/concrnt/internal/infra/repository/firestore/record"
	"github.com/concrnt/concrnt/internal/infra/repository/postgres"
	pgrecord "github.com/concrnt/concrnt/internal/infra/repository/postgres/record"
	spannerrepo "github.com/concrnt/concrnt/internal/infra/repository/spanner"
	spannerrecord "github.com/concrnt/concrnt/internal/infra/repository/spanner/record"
	"github.com/concrnt/concrnt/internal/infra/repository/sqlite"
	sqliterecord "github.com/concrnt/concrnt/internal/infra/repository/sqlite/record"
	"github.com/concrnt/concrnt/internal/usecase"
	"github.com/concrnt/concrnt/internal/usecase/aggregate"
	"github.com/concrnt/concrnt/internal/usecase/notification"
	"github.com/concrnt/concrnt/internal/usecase/record"
	"github.com/concrnt/concrnt/internal/usecase/server"
)

type operationContext struct {
	Config       config.Config
	GlobalConfig domain.Config
	// DB is the Postgres connection; nil on any other backend. Commands
	// that speak SQL directly check RequirePostgres first.
	DB           *gorm.DB
	Client       *client.Client
	Aggregate    *aggregate.Usecase
	Record       record.Repository
	EntityMeta   usecase.EntityMetaRepository
	Server       server.Repository
	Notification notification.Repository
	close        func() error
}

// RequirePostgres fails a command that works on the Postgres tables
// directly when another backend is configured.
func (o *operationContext) RequirePostgres() error {
	if o.DB == nil {
		return fmt.Errorf("this command speaks SQL directly and only runs on the postgres backend (backends.database is %q)", o.Config.Backends.Database)
	}
	return nil
}

func loadConcrntConfig() (config.Config, error) {
	configPath := os.Getenv("CONCRNT_CONFIG")
	if configPath == "" {
		configPath = "/etc/concrnt/config"
	}

	conf, err := config.Load(configPath)
	if err != nil {
		return config.Config{}, fmt.Errorf("failed to load config: %w", err)
	}

	return conf, nil
}

func newOperationContext() (*operationContext, error) {
	conf, err := loadConcrntConfig()
	if err != nil {
		return nil, err
	}

	globalConfig := conf.DomainConfig()

	cl := client.New(globalConfig.FQDN)
	if conf.Backends.GatewayAddr != "" {
		cl.AddHostRemapping(globalConfig.FQDN, conf.Backends.GatewayAddr)
	}

	op := &operationContext{Config: conf, GlobalConfig: globalConfig, Client: cl}
	// the count cache table in the record database, under its
	// backends.aggregate.store name; nil on sqlite, which has none
	var (
		databaseAggregate     aggregate.Store
		databaseAggregateName string
	)
	// the same backend selection as cmd/concrnt/main.go; conctl never
	// migrates, the server does that at start
	switch conf.Backends.Database {
	case "", "postgres":
		db, err := database.NewPostgres(conf.Backends.PostgresDsn, conf.Backends.Postgres.MaxOpenConnsOrDefault())
		if err != nil {
			return nil, fmt.Errorf("failed to connect database: %w", err)
		}
		op.DB = db
		op.Record = pgrecord.New(db)
		op.EntityMeta = postgres.NewEntityMetaRepository(db)
		op.Server = postgres.NewServerRepository(db, cl)
		op.Notification = postgres.NewNotificationRepository(db)
		databaseAggregate, databaseAggregateName = aggpostgres.New(db), "postgres"
		op.close = func() error {
			sqlDB, err := db.DB()
			if err != nil {
				return err
			}
			return sqlDB.Close()
		}
	case "sqlite":
		lite, err := sqlite.Open(conf.Backends.SQLitePath())
		if err != nil {
			return nil, fmt.Errorf("failed to open database: %w", err)
		}
		op.Record = sqliterecord.New(lite.DB)
		op.EntityMeta = sqlite.NewEntityMetaRepository(lite.DB)
		op.Server = sqlite.NewServerRepository(lite.DB, cl)
		op.Notification = sqlite.NewNotificationRepository(lite.DB)
		op.close = lite.Close
	case "spanner":
		sp, admin, err := database.NewSpanner(context.Background(), conf.Backends)
		if err != nil {
			return nil, fmt.Errorf("failed to connect database: %w", err)
		}
		op.Record = spannerrecord.New(sp)
		op.EntityMeta = spannerrepo.NewEntityMetaRepository(sp)
		op.Server = spannerrepo.NewServerRepository(sp, cl)
		op.Notification = spannerrepo.NewNotificationRepository(sp)
		databaseAggregate, databaseAggregateName = aggspanner.New(sp), "spanner"
		op.close = func() error {
			sp.Close()
			return admin.Close()
		}
	case "firestore":
		fsc, err := database.NewFirestore(context.Background(), conf.Backends)
		if err != nil {
			return nil, fmt.Errorf("failed to connect database: %w", err)
		}
		op.Record = fsrecord.New(fsc)
		op.EntityMeta = fsrepo.NewEntityMetaRepository(fsc)
		op.Server = fsrepo.NewServerRepository(fsc, cl)
		op.Notification = fsrepo.NewNotificationRepository(fsc)
		databaseAggregate, databaseAggregateName = aggfirestore.New(fsc), "firestore"
		op.close = fsc.Close
	default:
		return nil, fmt.Errorf("unknown backends.database %q (postgres, sqlite, spanner, firestore)", conf.Backends.Database)
	}

	// the same aggregate store selection as cmd/concrnt/main.go
	aggregateName := conf.Backends.Aggregate.Store
	if aggregateName == "" {
		aggregateName = cmp.Or(databaseAggregateName, "redis")
	}
	var aggregateStore aggregate.Store
	switch name := aggregateName; name {
	case "postgres", "spanner", "firestore":
		if name != databaseAggregateName {
			op.Close()
			return nil, fmt.Errorf("backends.aggregate.store %s needs backends.database %s", name, name)
		}
		aggregateStore = databaseAggregate
	case "redis":
		aggregateStore = aggredis.New(database.NewRedis(conf.Backends.RedisAddr, "", conf.Backends.RedisDB))
	case "memcached":
		aggregateStore = aggmemcached.New(database.NewMemcached(conf.Backends.MemcachedAddr), "")
	default:
		op.Close()
		return nil, fmt.Errorf("unknown backends.aggregate.store %q (postgres, spanner, firestore, redis, memcached)", name)
	}
	op.Aggregate = aggregate.New(aggregateStore, conf.Backends.Aggregate.TTL, conf.Backends.Aggregate.MaxDeltas)
	return op, nil
}

func (o *operationContext) Close() error {
	if o == nil || o.close == nil {
		return nil
	}
	return o.close()
}

func withOperationContext(run func(cmd *cobra.Command, args []string, op *operationContext) error) func(cmd *cobra.Command, args []string) error {
	return func(cmd *cobra.Command, args []string) error {
		op, err := newOperationContext()
		if err != nil {
			return err
		}
		defer op.Close()

		return run(cmd, args, op)
	}
}

var operationCmd = &cobra.Command{
	Use:     "operation",
	Aliases: []string{"op"},
	Short:   "Execute local server operations",
}

func init() {
	rootCmd.AddCommand(operationCmd)
}
