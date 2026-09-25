package jobqueue

import (
	"context"
	"log/slog"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/concrnt/concrnt/internal/infra/health"
)

const (
	resultOK    = "ok"
	resultRetry = "retry"
	resultDLQ   = "dlq"

	backlogReady   = "ready"   // in the stream, not yet claimed by a consumer
	backlogPending = "pending" // claimed by a consumer, not yet acked
	backlogRetry   = "retry"   // waiting in the delayed-retry set
	backlogDLQ     = "dlq"     // dead-lettered

	statsInterval = 15 * time.Second
)

var (
	jobsEnqueued = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "concrnt",
		Subsystem: "jobqueue",
		Name:      "enqueued_total",
		Help:      "Jobs added to the queue, by job type.",
	}, []string{"type"})

	jobsProcessed = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "concrnt",
		Subsystem: "jobqueue",
		Name:      "jobs_total",
		Help:      "Job attempts finished on this replica, by job type and outcome (ok, retry: rescheduled, dlq: dead-lettered).",
	}, []string{"type", "result"})

	jobDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "concrnt",
		Subsystem: "jobqueue",
		Name:      "job_duration_seconds",
		Help:      "Handler run time per job attempt, by job type.",
		Buckets:   []float64{0.01, 0.05, 0.1, 0.5, 1, 5, 10, 30},
	}, []string{"type"})

	// the backlog lives in Redis and is shared by every replica, so each
	// replica reports the same numbers; aggregate with max, not sum
	backlog = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "concrnt",
		Subsystem: "jobqueue",
		Name:      "backlog",
		Help:      "Jobs waiting in the shared queue by state (ready, pending, retry, dlq); identical on every replica.",
	}, []string{"state"})
)

// statsLoop refreshes the backlog gauges from Redis until ctx is cancelled.
func (q *RedisJobQueue) statsLoop(ctx context.Context, heartbeat *health.Heartbeat) {
	ticker := time.NewTicker(statsInterval)
	defer ticker.Stop()
	defer heartbeat.Stop()

	heartbeat.Beat()
	q.updateBacklog(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			heartbeat.Beat()
			q.updateBacklog(ctx)
		}
	}
}

func (q *RedisJobQueue) updateBacklog(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, bookkeepingTimeout)
	defer cancel()

	streamLen, err := q.rdb.XLen(ctx, streamKey).Result()
	if err != nil {
		if ctx.Err() == nil {
			slog.Warn("job queue: failed to read stream length", slog.String("error", err.Error()))
		}
		return
	}
	pending, err := q.rdb.XPending(ctx, streamKey, groupName).Result()
	if err != nil {
		if ctx.Err() == nil {
			slog.Warn("job queue: failed to read pending count", slog.String("error", err.Error()))
		}
		return
	}
	retry, err := q.rdb.ZCard(ctx, retryZSetKey).Result()
	if err != nil {
		if ctx.Err() == nil {
			slog.Warn("job queue: failed to read retry set size", slog.String("error", err.Error()))
		}
		return
	}
	dlq, err := q.rdb.XLen(ctx, dlqStreamKey).Result()
	if err != nil {
		if ctx.Err() == nil {
			slog.Warn("job queue: failed to read DLQ length", slog.String("error", err.Error()))
		}
		return
	}

	// a claimed message stays in the stream until it is acked, so the
	// stream length counts both ready and pending jobs
	backlog.WithLabelValues(backlogReady).Set(float64(max(streamLen-pending.Count, 0)))
	backlog.WithLabelValues(backlogPending).Set(float64(pending.Count))
	backlog.WithLabelValues(backlogRetry).Set(float64(retry))
	backlog.WithLabelValues(backlogDLQ).Set(float64(dlq))
}
