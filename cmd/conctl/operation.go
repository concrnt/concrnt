package main

import (
	"cmp"
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"gorm.io/gorm"

	"github.com/concrnt/concrnt/client"
	"github.com/concrnt/concrnt/internal/domain"
	aggmemcached "github.com/concrnt/concrnt/internal/infra/aggregate/memcached"
	aggpostgres "github.com/concrnt/concrnt/internal/infra/aggregate/postgres"
	aggredis "github.com/concrnt/concrnt/internal/infra/aggregate/redis"
	"github.com/concrnt/concrnt/internal/infra/config"
	"github.com/concrnt/concrnt/internal/infra/database"
	"github.com/concrnt/concrnt/internal/infra/repository/postgres"
	pgrecord "github.com/concrnt/concrnt/internal/infra/repository/postgres/record"
	"github.com/concrnt/concrnt/internal/usecase"
	"github.com/concrnt/concrnt/internal/usecase/aggregate"
	"github.com/concrnt/concrnt/internal/usecase/record"
	"github.com/concrnt/concrnt/internal/usecase/server"
)

type operationContext struct {
	Config       config.Config
	GlobalConfig domain.Config
	DB           *gorm.DB
	Client       *client.Client
	Aggregate    *aggregate.Usecase
	Record       record.Repository
	EntityMeta   usecase.EntityMetaRepository
	Server       server.Repository
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

	db, err := database.NewPostgres(conf.Backends.PostgresDsn, conf.Backends.Postgres.MaxOpenConnsOrDefault())
	if err != nil {
		return nil, fmt.Errorf("failed to connect database: %w", err)
	}

	cl := client.New(globalConfig.FQDN)
	if conf.Backends.GatewayAddr != "" {
		cl.AddHostRemapping(globalConfig.FQDN, conf.Backends.GatewayAddr)
	}

	// the same aggregate store selection as cmd/concrnt/main.go
	var aggregateStore aggregate.Store
	switch name := cmp.Or(conf.Backends.Aggregate.Store, "postgres"); name {
	case "postgres":
		aggregateStore = aggpostgres.New(db)
	case "redis":
		aggregateStore = aggredis.New(database.NewRedis(conf.Backends.RedisAddr, "", conf.Backends.RedisDB))
	case "memcached":
		aggregateStore = aggmemcached.New(database.NewMemcached(conf.Backends.MemcachedAddr), "")
	default:
		return nil, fmt.Errorf("unknown backends.aggregate.store %q (postgres, redis, memcached)", name)
	}

	return &operationContext{
		Config:       conf,
		GlobalConfig: globalConfig,
		DB:           db,
		Client:       cl,
		Aggregate:    aggregate.New(aggregateStore, conf.Backends.Aggregate.TTL, conf.Backends.Aggregate.MaxDeltas),
		Record:       pgrecord.New(db),
		EntityMeta:   postgres.NewEntityMetaRepository(db),
		Server:       postgres.NewServerRepository(&globalConfig, db, cl),
	}, nil
}

func (o *operationContext) Close() error {
	if o == nil || o.DB == nil {
		return nil
	}
	sqlDB, err := o.DB.DB()
	if err != nil {
		return err
	}
	return sqlDB.Close()
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
