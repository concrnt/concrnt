package gateway

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// one series per (cache, result); the hit rate is
// rate(result="hit") / rate(all) over a window
var cacheRequests = promauto.NewCounterVec(prometheus.CounterOpts{
	Namespace: "concrnt",
	Subsystem: "chunkline",
	Name:      "cache_requests_total",
	Help:      "Chunkline cache lookups by cache (itr: chunk iterators, body: chunk bodies) and result.",
}, []string{"cache", "result"})

// the cache updater is the leader's mirror of the event stream into the
// chunk cache; "dropped" events are counted by the pubsub package
var cacheUpdaterEvents = promauto.NewCounterVec(prometheus.CounterOpts{
	Namespace: "concrnt",
	Subsystem: "chunkline",
	Name:      "cache_updater_events_total",
	Help:      "Events the cache updater took from the stream, by result (applied, skipped: not a timeline record, error).",
}, []string{"result"})

var cacheUpdaterQueued = promauto.NewGauge(prometheus.GaugeOpts{
	Namespace: "concrnt",
	Subsystem: "chunkline",
	Name:      "cache_updater_queued",
	Help:      "Events waiting for a cache updater worker, sampled as each one is taken.",
})

// origin "local" is this server's own timelines read without HTTP; "http"
// counts the requests to other servers
var originFetches = promauto.NewCounterVec(prometheus.CounterOpts{
	Namespace: "concrnt",
	Subsystem: "chunkline",
	Name:      "origin_fetches_total",
	Help:      "Chunkline reads that missed the cache, by kind (manifest, itr, body, removed) and origin (local, http).",
}, []string{"kind", "origin"})
