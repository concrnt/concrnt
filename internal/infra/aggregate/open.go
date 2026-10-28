// Package aggregatestore picks the aggregate.Store implementation named in
// the configuration.
package aggregatestore

import (
	"fmt"

	goredis "github.com/redis/go-redis/v9"
	"gorm.io/gorm"

	"github.com/concrnt/concrnt/internal/infra/aggregate/postgres"
	"github.com/concrnt/concrnt/internal/infra/aggregate/redis"
	"github.com/concrnt/concrnt/internal/usecase/aggregate"
)

// Open returns the store for kind: "postgres" over db (which must then be
// non-nil), "redis" over rdb. "" means the store matching the record
// database: postgres when db is given, redis otherwise.
func Open(kind string, db *gorm.DB, rdb *goredis.Client) (aggregate.Store, error) {
	if kind == "" {
		if db != nil {
			kind = "postgres"
		} else {
			kind = "redis"
		}
	}
	switch kind {
	case "postgres":
		if db == nil {
			return nil, fmt.Errorf("aggregate store %q needs the postgres database", kind)
		}
		return postgres.New(db), nil
	case "redis":
		return redis.New(rdb), nil
	default:
		return nil, fmt.Errorf("unknown aggregate store %q (postgres, redis)", kind)
	}
}
