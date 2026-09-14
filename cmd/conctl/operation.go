package main

import (
	"context"
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"gorm.io/gorm"

	"github.com/concrnt/concrnt/client"
	"github.com/concrnt/concrnt/internal/domain"
	"github.com/concrnt/concrnt/internal/infra/config"
	"github.com/concrnt/concrnt/internal/infra/repository"
)

type operationContext struct {
	Config       config.Config
	GlobalConfig domain.Config
	// Repos is the persistence backend selected by backends.database.
	Repos *repository.Set
	// DB is the raw Postgres handle; nil when the backend is not Postgres.
	// Operations that still speak SQL must go through RequirePostgres.
	DB     *gorm.DB
	Client *client.Client
}

// RequirePostgres returns the raw SQL handle for operations that are only
// implemented against Postgres (legacy repairs).
func (o *operationContext) RequirePostgres() (*gorm.DB, error) {
	if o.DB == nil {
		return nil, fmt.Errorf("this operation requires backends.database=postgres (current backend: %s)", o.Repos.Name)
	}
	return o.DB, nil
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

	repos, err := repository.Open(context.Background(), conf.Backends, repository.Deps{Client: cl, DomainConfig: globalConfig})
	if err != nil {
		return nil, err
	}

	return &operationContext{
		Config:       conf,
		GlobalConfig: globalConfig,
		Repos:        repos,
		DB:           repos.SQL,
		Client:       cl,
	}, nil
}

func (o *operationContext) Close() error {
	if o == nil || o.Repos == nil || o.Repos.Close == nil {
		return nil
	}
	return o.Repos.Close()
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
