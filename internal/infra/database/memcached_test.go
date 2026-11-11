package database

import (
	"errors"
	"testing"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
)

// A client created while its server address does not resolve must not stay
// without a server for the life of the process, and must not pass the
// readiness ping meanwhile: memcache.New does both, which left one replica
// of a load test reading every timeline chunk from the database.
func TestMemcachedResolvesLate(t *testing.T) {
	// a port name that does not exist fails resolution without any DNS
	servers := &memcachedServers{server: "127.0.0.1:no-such-service"}
	mc := memcache.NewFromSelector(servers)

	if err := mc.Ping(); !errors.Is(err, memcache.ErrNoServers) {
		t.Fatalf("Ping without a resolved server = %v, want ErrNoServers", err)
	}
	if _, err := mc.Get("key"); !errors.Is(err, memcache.ErrNoServers) {
		t.Fatalf("Get without a resolved server = %v, want ErrNoServers", err)
	}

	// the name starts resolving; the next attempt is due after the retry
	// interval
	servers.server = "127.0.0.1:11211"
	if _, err := servers.PickServer("key"); !errors.Is(err, memcache.ErrNoServers) {
		t.Fatalf("PickServer inside the retry interval = %v, want ErrNoServers", err)
	}
	servers.lastTry = time.Now().Add(-memcachedResolveRetry)
	addr, err := servers.PickServer("key")
	if err != nil || addr.String() != "127.0.0.1:11211" {
		t.Fatalf("PickServer after the name resolves = %v, %v", addr, err)
	}
}

func TestNewMemcachedUnresolvable(t *testing.T) {
	mc := NewMemcached("127.0.0.1:no-such-service")
	if err := mc.Ping(); !errors.Is(err, memcache.ErrNoServers) {
		t.Fatalf("Ping = %v, want ErrNoServers", err)
	}
}
