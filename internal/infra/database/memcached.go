package database

import (
	"time"

	"github.com/bradfitz/gomemcache/memcache"
)

func NewMemcached(server string) *memcache.Client {
	mc := memcache.New(server)
	// every authenticated request reads the entity cache; the default of 2
	// idle connections makes concurrent requests dial a new one each time
	mc.MaxIdleConns = 64
	// the default 100ms makes a throttled memcached look unreachable and
	// sends every entity lookup to the database instead
	mc.Timeout = 500 * time.Millisecond
	return mc
}
