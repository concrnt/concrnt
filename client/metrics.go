package client

import (
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Every request this server makes to another concrnt server (resource
// fetches, resolver lookups, health checks) goes through Client.RoundTrip,
// so this is the single place to account for federation traffic. `host` is
// the peer's logical domain (before any dev-time remapping), which bounds
// the cardinality to the number of servers this one talks to.
var (
	peerRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "concrnt",
		Subsystem: "peer",
		Name:      "requests_total",
		Help:      "Requests made to other concrnt servers by peer host and HTTP status code (\"error\" when no response was received).",
	}, []string{"host", "code"})

	// the client times out at 3s, so buckets need not go much beyond that
	peerRequestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "concrnt",
		Subsystem: "peer",
		Name:      "request_duration_seconds",
		Help:      "Duration of requests made to other concrnt servers, by peer host.",
		Buckets:   []float64{0.05, 0.1, 0.25, 0.5, 1, 2.5, 5},
	}, []string{"host"})
)

// localResolves counts the cckv fetches the LocalResolver answered (hit),
// settled as absent (notfound) or passed on to HTTP (miss).
var localResolves = promauto.NewCounterVec(prometheus.CounterOpts{
	Namespace: "concrnt",
	Subsystem: "peer",
	Name:      "local_resolves_total",
	Help:      "cckv document fetches answered by this server's own store (hit), settled as absent by it (notfound) or sent over HTTP (miss).",
}, []string{"result"})

func observePeerRequest(host string, code int, err error, seconds float64) {
	codeLabel := "error"
	if err == nil {
		codeLabel = strconv.Itoa(code)
	}
	peerRequests.WithLabelValues(host, codeLabel).Inc()
	peerRequestDuration.WithLabelValues(host).Observe(seconds)
}

// peerOfflineCollector exports one series per peer host the client currently
// considers offline (value 1). Reading the client's own state at scrape time
// means a peer that recovers simply disappears from the output — no stale
// series to clean up.
type peerOfflineCollector struct {
	client *Client
	desc   *prometheus.Desc
}

func newPeerOfflineCollector(c *Client) *peerOfflineCollector {
	return &peerOfflineCollector{
		client: c,
		desc: prometheus.NewDesc(
			prometheus.BuildFQName("concrnt", "peer", "offline"),
			"Peer hosts currently considered offline by the federation client; the value is always 1.",
			[]string{"host"}, nil,
		),
	}
}

func (p *peerOfflineCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- p.desc
}

func (p *peerOfflineCollector) Collect(ch chan<- prometheus.Metric) {
	for _, host := range p.client.offlineHosts() {
		ch <- prometheus.MustNewConstMetric(p.desc, prometheus.GaugeValue, 1, host)
	}
}
