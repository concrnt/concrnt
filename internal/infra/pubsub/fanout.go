package pubsub

import (
	"context"
	"log/slog"
	"slices"
	"strings"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/concrnt/concrnt"
)

var (
	droppedEvents = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "concrnt",
		Subsystem: "pubsub",
		Name:      "dropped_total",
		Help:      "Events a local subscriber could not take because its channel was full.",
	})
	reconnects = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "concrnt",
		Subsystem: "pubsub",
		Name:      "reconnects_total",
		Help:      "Times the backend subscription was re-established after an error; events published in between were lost.",
	})
	subscribers = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "concrnt",
		Subsystem: "pubsub",
		Name:      "subscribers",
		Help:      "Local subscriptions (realtime sessions, push subscriptions, the cache updater) served by this replica's backend subscription.",
	})
)

// fanout distributes the events of one backend subscription to the local
// subscribers by channel prefix. It is for a backend whose server-side
// pattern matching is per connection and linear in the number of patterns
// (Redis): one connection per replica receives everything and the matching
// happens here. Delivery to a subscriber never blocks: a full channel drops
// the event, so one stalled session cannot hold the others back.
type fanout struct {
	mu     sync.Mutex
	nextID uint64
	subs   map[uint64]*subscriber
	onGap  []func()
}

type subscriber struct {
	prefixes []string // nil: every channel
	out      chan<- concrnt.Event
}

func newFanout() *fanout {
	return &fanout{subs: map[uint64]*subscriber{}}
}

// add registers out for the channels starting with one of prefixes (nil for
// all of them) until ctx is done.
func (f *fanout) add(ctx context.Context, prefixes []string, out chan<- concrnt.Event) {
	f.mu.Lock()
	f.nextID++
	id := f.nextID
	f.subs[id] = &subscriber{prefixes: prefixes, out: out}
	subscribers.Set(float64(len(f.subs)))
	f.mu.Unlock()

	go func() {
		<-ctx.Done()
		f.mu.Lock()
		delete(f.subs, id)
		subscribers.Set(float64(len(f.subs)))
		f.mu.Unlock()
	}()
}

// dispatch hands event, published on channel, to every matching subscriber.
func (f *fanout) dispatch(channel string, event concrnt.Event) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, sub := range f.subs {
		if sub.prefixes != nil && !slices.ContainsFunc(sub.prefixes, func(p string) bool { return strings.HasPrefix(channel, p) }) {
			continue
		}
		select {
		case sub.out <- event:
		default:
			droppedEvents.Inc()
			slog.Warn("pubsub: subscriber is not keeping up, event dropped", slog.String("channel", channel))
		}
	}
}

// addGapHook registers fn to run after the backend subscription was lost
// and re-established.
func (f *fanout) addGapHook(fn func()) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.onGap = append(f.onGap, fn)
}

func (f *fanout) gap() {
	f.mu.Lock()
	hooks := append([]func(){}, f.onGap...)
	f.mu.Unlock()
	reconnects.Inc()
	for _, fn := range hooks {
		fn()
	}
}
