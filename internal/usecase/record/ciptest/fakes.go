package ciptest

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/internal/domain"
	"github.com/concrnt/concrnt/internal/usecase/aggregate"
	"github.com/concrnt/concrnt/policy"
)

// job is one enqueued job as the real queue carries it: a type and the
// serialized payload.
type job struct {
	Type    string
	Payload json.RawMessage
}

// jobQueue is a usecase.JobQueue that keeps the jobs until the test drains
// it, so a test can observe the state between a commit and its background
// work (delivery, range-delete continuation, cleanup).
type jobQueue struct {
	mu       sync.Mutex
	handlers map[string]func(ctx context.Context, payload json.RawMessage) error
	pending  []job
	// finished counts the jobs run to completion, by type.
	finished map[string]int
}

func newJobQueue() *jobQueue {
	return &jobQueue{
		handlers: map[string]func(ctx context.Context, payload json.RawMessage) error{},
		finished: map[string]int{},
	}
}

func (q *jobQueue) Enqueue(_ context.Context, jobType string, payload any) error {
	raw, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	q.pending = append(q.pending, job{Type: jobType, Payload: raw})
	return nil
}

func (q *jobQueue) RegisterHandler(jobType string, handler func(ctx context.Context, payload json.RawMessage) error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.handlers[jobType] = handler
}

func (q *jobQueue) pop() (job, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.pending) == 0 {
		return job{}, false
	}
	next := q.pending[0]
	q.pending = q.pending[1:]
	return next, true
}

// drainLimit stops a job that keeps re-enqueueing itself from hanging the
// suite.
const drainLimit = 10000

// drain runs the pending jobs in order, including the ones they enqueue,
// until the queue is empty. The first failing job is the error: the suite
// has no retries, and every job a conforming backend produces succeeds.
func (q *jobQueue) drain(ctx context.Context) error {
	for range drainLimit {
		next, ok := q.pop()
		if !ok {
			return nil
		}
		q.mu.Lock()
		handler := q.handlers[next.Type]
		q.mu.Unlock()
		if handler == nil {
			return fmt.Errorf("no handler registered for job type %q", next.Type)
		}
		if err := handler(ctx, next.Payload); err != nil {
			return fmt.Errorf("job %s %s: %w", next.Type, next.Payload, err)
		}
		q.mu.Lock()
		q.finished[next.Type]++
		q.mu.Unlock()
	}
	return fmt.Errorf("job queue still not empty after %d jobs", drainLimit)
}

// pendingTypes lists the types of the jobs waiting, in order.
func (q *jobQueue) pendingTypes() []string {
	q.mu.Lock()
	defer q.mu.Unlock()
	types := make([]string, 0, len(q.pending))
	for _, j := range q.pending {
		types = append(types, j.Type)
	}
	return types
}

func (q *jobQueue) finishedCount(jobType string) int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.finished[jobType]
}

// publication is one realtime event as a subscriber of channel gets it.
type publication struct {
	Channel string
	Event   concrnt.Event
}

// signalRecorder is a record.SignalService that keeps what was published.
type signalRecorder struct {
	mu        sync.Mutex
	published []publication
}

func (s *signalRecorder) Publish(_ context.Context, channel string, event concrnt.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.published = append(s.published, publication{Channel: channel, Event: event})
	return nil
}

// channels lists, in publish order, the channels that got an event of
// eventType about uri ("" = any).
func (s *signalRecorder) channels(eventType, uri string) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	channels := []string{}
	for _, p := range s.published {
		if p.Event.Type == eventType && (uri == "" || p.Event.URI == uri) {
			channels = append(channels, p.Channel)
		}
	}
	return channels
}

func (s *signalRecorder) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.published)
}

// memoryKVS is a usecase.KVS without expiry: no test outlives a ttl.
type memoryKVS struct {
	mu    sync.Mutex
	sets  map[string][]string
	flags map[string]string
}

func newMemoryKVS() *memoryKVS {
	return &memoryKVS{sets: map[string][]string{}, flags: map[string]string{}}
}

func (k *memoryKVS) SetAdd(_ context.Context, key string, value string, _ time.Duration) error {
	k.mu.Lock()
	defer k.mu.Unlock()
	if !slices.Contains(k.sets[key], value) {
		k.sets[key] = append(k.sets[key], value)
	}
	return nil
}

func (k *memoryKVS) SetMembers(_ context.Context, key string) ([]string, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	return slices.Clone(k.sets[key]), nil
}

func (k *memoryKVS) SetNX(_ context.Context, key string, value string, _ time.Duration) (bool, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	if _, ok := k.flags[key]; ok {
		return false, nil
	}
	k.flags[key] = value
	return true, nil
}

// policySwitch is a record.PolicyService that allows everything except the
// (action, key) pairs a test denies. The suite is about what the store
// does with a decision, not about how CIP-12 reaches one.
type policySwitch struct {
	mu     sync.Mutex
	denied map[string]bool
}

func newPolicySwitch() *policySwitch {
	return &policySwitch{denied: map[string]bool{}}
}

// deny makes action on key fail with a permission error from now on.
func (p *policySwitch) deny(action, key string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.denied[action+" "+key] = true
}

func (p *policySwitch) Eval(_ context.Context, _ policy.RequestContext, _ []concrnt.Policy, action string, key string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.denied[action+" "+key] {
		return domain.PermissionError{Reason: "denied by the test: " + action + " on " + key}
	}
	return nil
}

// peerServers is a server.Repository in which every other domain is a
// reachable server of this network's layer, so an entity may name any
// domain as its home.
type peerServers struct {
	layer string
}

func (p peerServers) Resolve(_ context.Context, identifier string, _ *string) (*domain.Server, error) {
	return &domain.Server{WellKnown: concrnt.WellKnownConcrnt{Version: "2.0", Domain: identifier, Layer: p.layer}}, nil
}

func (p peerServers) List(context.Context) ([]*concrnt.WellKnownConcrnt, error) {
	return nil, nil
}

// memoryAggregates is an aggregate.Store held in a map. The count cache
// sits in front of every count read, so the suite runs the usecase with a
// real one: a backend whose Apply* outcomes are wrong shows up as a cached
// count that disagrees with the primary data.
type memoryAggregates struct {
	mu      sync.Mutex
	entries map[string]memoryAggregate
}

type memoryAggregate struct {
	counters  aggregate.Counters
	expiresAt time.Time
}

func newMemoryAggregates() *memoryAggregates {
	return &memoryAggregates{entries: map[string]memoryAggregate{}}
}

func cloneCounters(c aggregate.Counters) aggregate.Counters {
	c.Counts, c.Minima, c.Members = maps.Clone(c.Counts), maps.Clone(c.Minima), slices.Clone(c.Members)
	return c
}

// live returns the unexpired entry under key; the caller holds the lock.
func (s *memoryAggregates) live(key string) (memoryAggregate, bool) {
	entry, ok := s.entries[key]
	if ok && !time.Now().Before(entry.expiresAt) {
		delete(s.entries, key)
		return memoryAggregate{}, false
	}
	return entry, ok
}

func (s *memoryAggregates) Get(_ context.Context, key string) (*aggregate.Counters, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	entry, ok := s.live(key)
	if !ok {
		return nil, false, nil
	}
	counters := cloneCounters(entry.counters)
	return &counters, true, nil
}

func (s *memoryAggregates) PutIfAbsent(_ context.Context, key string, entry aggregate.Counters, ttl time.Duration) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.live(key); ok {
		return false, nil
	}
	entry = cloneCounters(entry)
	entry.Deltas = 0
	s.entries[key] = memoryAggregate{counters: entry, expiresAt: time.Now().Add(ttl)}
	return true, nil
}

func (s *memoryAggregates) Add(_ context.Context, key string, delta aggregate.Delta) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	entry, ok := s.live(key)
	if !ok {
		return false, nil
	}
	entry.counters.Add(delta)
	s.entries[key] = entry
	return true, nil
}

func (s *memoryAggregates) Invalidate(_ context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.entries, key)
	return nil
}

func (s *memoryAggregates) InvalidateDefinition(_ context.Context, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	maps.DeleteFunc(s.entries, func(key string, _ memoryAggregate) bool {
		return strings.HasPrefix(key, aggregate.Key(name, ""))
	})
	return nil
}

func (s *memoryAggregates) InvalidateAll(context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	clear(s.entries)
	return nil
}
