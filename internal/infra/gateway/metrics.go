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

// origin "local" is this server's own timelines read without HTTP; "http"
// counts the requests to other servers (and to this one before SetLocal)
var originFetches = promauto.NewCounterVec(prometheus.CounterOpts{
	Namespace: "concrnt",
	Subsystem: "chunkline",
	Name:      "origin_fetches_total",
	Help:      "Chunkline reads that missed the cache, by kind (manifest, itr, body, removed) and origin (local, http).",
}, []string{"kind", "origin"})
