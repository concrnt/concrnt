// Package stores picks the aggregate store backends.aggregate.store names.
package stores

import (
	"fmt"

	"cloud.google.com/go/firestore"
	"cloud.google.com/go/spanner"
	"github.com/bradfitz/gomemcache/memcache"
	"github.com/redis/go-redis/v9"
	"gorm.io/gorm"

	aggfirestore "github.com/concrnt/concrnt/internal/infra/aggregate/firestore"
	aggmemcached "github.com/concrnt/concrnt/internal/infra/aggregate/memcached"
	aggpostgres "github.com/concrnt/concrnt/internal/infra/aggregate/postgres"
	aggredis "github.com/concrnt/concrnt/internal/infra/aggregate/redis"
	aggspanner "github.com/concrnt/concrnt/internal/infra/aggregate/spanner"
	"github.com/concrnt/concrnt/internal/usecase/aggregate"
)

// Open builds the store named by name over the connections the caller
// holds (nil for a database that is not the record backend); "" means the
// store matching the record database, redis when none is a database.
func Open(name string, db *gorm.DB, sp *spanner.Client, fsc *firestore.Client, rdb *redis.Client, mc *memcache.Client) (aggregate.Store, error) {
	switch name {
	case "postgres":
		if db == nil {
			return nil, fmt.Errorf("backends.aggregate.store postgres needs the postgres database")
		}
		return aggpostgres.New(db), nil
	case "spanner":
		if sp == nil {
			return nil, fmt.Errorf("backends.aggregate.store spanner needs the spanner database")
		}
		return aggspanner.New(sp), nil
	case "firestore":
		if fsc == nil {
			return nil, fmt.Errorf("backends.aggregate.store firestore needs the firestore database")
		}
		return aggfirestore.New(fsc), nil
	case "redis":
		return aggredis.New(rdb), nil
	case "memcached":
		return aggmemcached.New(mc, ""), nil
	case "":
		switch {
		case db != nil:
			return aggpostgres.New(db), nil
		case sp != nil:
			return aggspanner.New(sp), nil
		case fsc != nil:
			return aggfirestore.New(fsc), nil
		default:
			return aggredis.New(rdb), nil
		}
	default:
		return nil, fmt.Errorf("unknown backends.aggregate.store %q (postgres, spanner, firestore, redis, memcached)", name)
	}
}
