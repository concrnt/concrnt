package database

import (
	"github.com/bradfitz/gomemcache/memcache"
)

func NewMemcached(server string) *memcache.Client {
	mc := memcache.New(server)
	// every authenticated request reads the entity cache; the default of 2
	// idle connections makes concurrent requests dial a new one each time
	mc.MaxIdleConns = 64
	return mc
}
