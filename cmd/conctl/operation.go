package main

import (
	"context"
	"fmt"
	"os"

	gfirestore "cloud.google.com/go/firestore"
	gspanner "cloud.google.com/go/spanner"
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
	spanner      *gspanner.Client   // spanner only; the aggregate store's default home
	firestore    *gfirestore.Client // firestore only; same role
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
	// the same backend selection as cmd/concrnt/main.go; conctl never
	// migrates, the server does that at start
	switch conf.Backends.Database {
	case "", "postgres":
		pool, _ := conf.Backends.Postgres.WithDefaults()
		db, err := database.NewPostgres(conf.Backends.PostgresDsn, pool.MaxOpenConns)
		if err != nil {
			return nil, fmt.Errorf("failed to connect database: %w", err)
		}
		op.DB = db
		op.Record = pgrecord.New(db, pool.MaxConcurrentTx)
		op.EntityMeta = postgres.NewEntityMetaRepository(db)
		op.Server = postgres.NewServerRepository(db, cl)
		op.Notification = postgres.NewNotificationRepository(db)
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
		op.spanner = sp
		op.Record = spannerrecord.New(sp)
		op.EntityMeta = spannerrepo.NewEntityMetaRepository(sp)
		op.Server = spannerrepo.NewServerRepository(sp, cl)
		op.Notification = spannerrepo.NewNotificationRepository(sp)
		op.close = func() error {
			sp.Close()
			return admin.Close()
		}
	case "firestore":
		fsc, err := database.NewFirestore(context.Background(), conf.Backends)
		if err != nil {
			return nil, fmt.Errorf("failed to connect database: %w", err)
		}
		op.firestore = fsc
		op.Record = fsrecord.New(fsc)
		op.EntityMeta = fsrepo.NewEntityMetaRepository(fsc)
		op.Server = fsrepo.NewServerRepository(fsc, cl)
		op.Notification = fsrepo.NewNotificationRepository(fsc)
		op.close = fsc.Close
	default:
		return nil, fmt.Errorf("unknown backends.database %q (postgres, sqlite, spanner, firestore)", conf.Backends.Database)
	}

	rdb := database.NewRedis(conf.Backends.RedisAddr, "", conf.Backends.RedisDB)
	var aggregateStore aggregate.Store
	switch conf.Backends.Aggregate.Store {
	case "postgres":
		if op.DB == nil {
			op.Close()
			return nil, fmt.Errorf("backends.aggregate.store postgres needs the postgres database")
		}
		aggregateStore = aggpostgres.New(op.DB)
	case "spanner":
		if op.spanner == nil {
			op.Close()
			return nil, fmt.Errorf("backends.aggregate.store spanner needs the spanner database")
		}
		aggregateStore = aggspanner.New(op.spanner)
	case "firestore":
		if op.firestore == nil {
			op.Close()
			return nil, fmt.Errorf("backends.aggregate.store firestore needs the firestore database")
		}
		aggregateStore = aggfirestore.New(op.firestore)
	case "redis":
		aggregateStore = aggredis.New(rdb)
	case "memcached":
		aggregateStore = aggmemcached.New(database.NewMemcached(conf.Backends.MemcachedAddr), "")
	case "":
		switch {
		case op.DB != nil:
			aggregateStore = aggpostgres.New(op.DB)
		case op.spanner != nil:
			aggregateStore = aggspanner.New(op.spanner)
		case op.firestore != nil:
			aggregateStore = aggfirestore.New(op.firestore)
		default:
			aggregateStore = aggredis.New(rdb)
		}
	default:
		op.Close()
		return nil, fmt.Errorf("unknown backends.aggregate.store %q (postgres, spanner, firestore, redis, memcached)", conf.Backends.Aggregate.Store)
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
