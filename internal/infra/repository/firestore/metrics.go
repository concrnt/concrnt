package firestore

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	txDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "concrnt",
		Subsystem: "firestore",
		Name:      "tx_duration_seconds",
		Help:      "Wall time of commit transactions against Firestore, including retries.",
		Buckets:   prometheus.ExponentialBuckets(0.01, 2, 12),
	}, []string{"status"})
	txAttempts = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "concrnt",
		Subsystem: "firestore",
		Name:      "tx_attempts_total",
		Help:      "Transaction closure runs; more than one per transaction means contention retries.",
	})
)

// Collectors are the backend's Prometheus collectors for main to register.
func Collectors() []prometheus.Collector {
	return []prometheus.Collector{txDuration, txAttempts}
}

func observeTx(start time.Time, attempts int, err error) {
	status := "ok"
	if err != nil {
		status = "error"
	}
	txDuration.WithLabelValues(status).Observe(time.Since(start).Seconds())
	txAttempts.Add(float64(attempts))
}
