package database

import (
	"fmt"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
)

// memcachedResolveRetry spaces the attempts to resolve the server address
// while it does not resolve.
const memcachedResolveRetry = time.Second

func NewMemcached(server string) *memcache.Client {
	servers := &memcachedServers{server: server}
	if err := servers.resolve(); err != nil {
		slog.Warn("memcached: server address does not resolve yet, retried as requests arrive",
			slog.String("server", server), slog.String("error", err.Error()))
	}
	mc := memcache.NewFromSelector(servers)
	// every authenticated request reads the entity cache; the default of 2
	// idle connections makes concurrent requests dial a new one each time
	mc.MaxIdleConns = 64
	// the default 100ms makes a throttled memcached look unreachable and
	// sends every entity lookup to the database instead
	mc.Timeout = 500 * time.Millisecond
	return mc
}

// memcachedServers is the server list of the client. memcache.New resolves
// the address once and ignores a failure, which leaves a process started
// before its memcached was resolvable (a rollout that recreates both)
// without a server for its whole life: every operation fails with
// ErrNoServers, every cached read goes to the database, and Ping, which
// visits each server of an empty list, reports success. This list keeps
// trying to resolve until it has an address and fails Ping until then, so
// readiness tells the truth.
type memcachedServers struct {
	server string
	list   memcache.ServerList

	resolved atomic.Bool
	mu       sync.Mutex // one resolution at a time
	lastTry  time.Time
}

func (s *memcachedServers) resolve() error {
	if err := s.list.SetServers(s.server); err != nil {
		return err
	}
	s.resolved.Store(true)
	return nil
}

// ensure reports whether the address is resolved, resolving it when the
// last attempt is old enough. Callers that arrive during an attempt are
// not held up behind the name lookup.
func (s *memcachedServers) ensure() error {
	if s.resolved.Load() {
		return nil
	}
	if !s.mu.TryLock() {
		return memcache.ErrNoServers
	}
	defer s.mu.Unlock()
	if s.resolved.Load() {
		return nil
	}
	if time.Since(s.lastTry) < memcachedResolveRetry {
		return memcache.ErrNoServers
	}
	s.lastTry = time.Now()
	if err := s.resolve(); err != nil {
		return fmt.Errorf("%w: %s does not resolve: %v", memcache.ErrNoServers, s.server, err)
	}
	slog.Info("memcached: server address resolved", slog.String("server", s.server))
	return nil
}

func (s *memcachedServers) PickServer(key string) (net.Addr, error) {
	if err := s.ensure(); err != nil {
		return nil, err
	}
	return s.list.PickServer(key)
}

func (s *memcachedServers) Each(f func(net.Addr) error) error {
	if err := s.ensure(); err != nil {
		return err
	}
	return s.list.Each(f)
}
