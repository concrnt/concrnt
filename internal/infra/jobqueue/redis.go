// Package jobqueue provides an at-least-once, retrying background job queue.
// RedisJobQueue is the current (Redis Streams-backed) implementation. The
// queue is agnostic of what a job means: a job is a (type, serialized
// payload) envelope, producers enqueue through the small interface they
// declare themselves (internal/usecase.JobQueue), and whoever owns a job type
// registers the handler that interprets its payload (RegisterHandler) — so
// this backend can be swapped later without touching either side.
package jobqueue

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"github.com/concrnt/concrnt/internal/infra/health"
)

const (
	streamKey    = "jobqueue:stream"
	retryZSetKey = "jobqueue:retry"
	dlqStreamKey = "jobqueue:dlq"
	groupName    = "jobqueue"

	defaultConcurrency     = 4
	defaultMaxAttempts     = 8
	defaultBaseBackoff     = 10 * time.Second
	defaultBackoffCap      = time.Hour
	defaultClaimMinIdle    = 5 * time.Minute
	defaultMoverInterval   = time.Second
	defaultReclaimInterval = time.Minute
	defaultStreamMaxLen    = 100_000

	bookkeepingTimeout   = 5 * time.Second
	handlerTimeout       = 30 * time.Second
	groupCreateRetryWait = 2 * time.Second

	// liveness deadlines for the queue's loops: each beats between jobs, so
	// the longest legitimate silence is one blocking read or one tick plus
	// one handler timeout; the margin on top covers a slow Redis round trip
	consumerDeadline    = 2 * time.Minute
	bookkeepingDeadline = time.Minute
)

// Job is the envelope the queue persists: a job type, its serialized
// payload, and the queue's own bookkeeping. The queue never looks inside
// Payload; the handler registered for Type does.
type Job struct {
	ID      string          `json:"id"`
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`

	Attempt   int       `json:"attempt"`
	CreatedAt time.Time `json:"createdAt"`
	LastError string    `json:"lastError,omitempty"`
}

// Handler interprets the payload of one job type. A returned error
// reschedules the job (up to maxAttempts) and then dead-letters it.
type Handler func(ctx context.Context, payload json.RawMessage) error

// RedisJobQueue is a Redis Streams-backed job queue: a consumer group drains
// streamKey, failures are rescheduled via a delayed-retry ZSET (Redis
// Streams has no native delayed delivery), and jobs that exhaust their
// attempts land in a dead-letter stream for later inspection/replay.
type RedisJobQueue struct {
	rdb *redis.Client

	handlersMu sync.RWMutex
	handlers   map[string]Handler

	consumerName    string
	concurrency     int
	maxAttempts     int
	baseBackoff     time.Duration
	backoffCap      time.Duration
	claimMinIdle    time.Duration
	moverInterval   time.Duration
	reclaimInterval time.Duration
	streamMaxLen    int64

	watchdog *health.Watchdog
}

type Option func(*RedisJobQueue)

func WithConcurrency(n int) Option {
	return func(q *RedisJobQueue) { q.concurrency = n }
}

func WithMaxAttempts(n int) Option {
	return func(q *RedisJobQueue) { q.maxAttempts = n }
}

func WithBaseBackoff(d time.Duration) Option {
	return func(q *RedisJobQueue) { q.baseBackoff = d }
}

func WithBackoffCap(d time.Duration) Option {
	return func(q *RedisJobQueue) { q.backoffCap = d }
}

func WithClaimMinIdle(d time.Duration) Option {
	return func(q *RedisJobQueue) { q.claimMinIdle = d }
}

func WithMoverInterval(d time.Duration) Option {
	return func(q *RedisJobQueue) { q.moverInterval = d }
}

func WithReclaimInterval(d time.Duration) Option {
	return func(q *RedisJobQueue) { q.reclaimInterval = d }
}

func WithConsumerName(name string) Option {
	return func(q *RedisJobQueue) { q.consumerName = name }
}

// WithWatchdog reports the queue's loops to the liveness watchdog.
func WithWatchdog(w *health.Watchdog) Option {
	return func(q *RedisJobQueue) { q.watchdog = w }
}

func NewRedisJobQueue(rdb *redis.Client, opts ...Option) *RedisJobQueue {
	hostname, err := os.Hostname()
	if err != nil || hostname == "" {
		hostname = "worker"
	}

	q := &RedisJobQueue{
		rdb:             rdb,
		handlers:        map[string]Handler{},
		consumerName:    hostname,
		concurrency:     defaultConcurrency,
		maxAttempts:     defaultMaxAttempts,
		baseBackoff:     defaultBaseBackoff,
		backoffCap:      defaultBackoffCap,
		claimMinIdle:    defaultClaimMinIdle,
		moverInterval:   defaultMoverInterval,
		reclaimInterval: defaultReclaimInterval,
		streamMaxLen:    defaultStreamMaxLen,
	}

	for _, opt := range opts {
		opt(q)
	}

	return q
}

// RegisterHandler installs the handler for jobType. Register every type
// before Run: a job whose type has no handler on this consumer fails and is
// retried, so another replica (e.g. a newer one mid-rollout) can pick it up.
func (q *RedisJobQueue) RegisterHandler(jobType string, handler func(ctx context.Context, payload json.RawMessage) error) {
	q.handlersMu.Lock()
	defer q.handlersMu.Unlock()
	q.handlers[jobType] = handler
}

func (q *RedisJobQueue) handlerFor(jobType string) (Handler, bool) {
	q.handlersMu.RLock()
	defer q.handlersMu.RUnlock()
	handler, ok := q.handlers[jobType]
	return handler, ok
}

// Enqueue serializes payload into a new job of jobType and adds it to the
// stream.
func (q *RedisJobQueue) Enqueue(ctx context.Context, jobType string, payload any) error {
	raw, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	err = q.enqueueRaw(ctx, Job{
		ID:        uuid.NewString(),
		Type:      jobType,
		Payload:   raw,
		CreatedAt: time.Now(),
	})
	if err == nil {
		jobsEnqueued.WithLabelValues(jobType).Inc()
	}
	return err
}

func (q *RedisJobQueue) enqueueRaw(ctx context.Context, job Job) error {
	payload, err := json.Marshal(job)
	if err != nil {
		return err
	}
	return q.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: streamKey,
		MaxLen: q.streamMaxLen,
		Approx: true,
		Values: map[string]interface{}{"payload": string(payload)},
	}).Err()
}

// Run drains the stream with the configured number of consumer goroutines,
// plus a mover goroutine (moves due retries back onto the stream) and a
// reclaimer goroutine (recovers messages left pending by a crashed
// consumer), dispatching each job to the handler registered for its type.
// It blocks until ctx is cancelled.
func (q *RedisJobQueue) Run(ctx context.Context) error {
	if err := q.ensureGroup(ctx); err != nil {
		return err
	}

	var wg sync.WaitGroup

	for i := 0; i < q.concurrency; i++ {
		consumer := q.consumerName + "-" + strconv.Itoa(i)
		heartbeat := q.watchdog.Register("jobqueue/consumer-"+strconv.Itoa(i), consumerDeadline)
		wg.Add(1)
		go func(consumer string, heartbeat *health.Heartbeat) {
			defer wg.Done()
			q.consumeLoop(ctx, consumer, heartbeat)
		}(consumer, heartbeat)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		q.moverLoop(ctx, q.watchdog.Register("jobqueue/mover", bookkeepingDeadline))
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		q.reclaimLoop(ctx, q.watchdog.Register("jobqueue/reclaimer", consumerDeadline))
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		q.statsLoop(ctx, q.watchdog.Register("jobqueue/stats", bookkeepingDeadline))
	}()

	<-ctx.Done()
	wg.Wait()
	return nil
}

// ensureGroup creates the consumer group, retrying on transient errors (e.g.
// Redis not yet reachable at process startup) instead of giving up, so a
// brief Redis outage during boot doesn't permanently disable the queue.
func (q *RedisJobQueue) ensureGroup(ctx context.Context) error {
	for {
		err := q.rdb.XGroupCreateMkStream(ctx, streamKey, groupName, "0").Err()
		if err == nil || strings.Contains(err.Error(), "BUSYGROUP") {
			return nil
		}
		slog.Error("job queue: failed to create consumer group, retrying", slog.String("error", err.Error()))
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(groupCreateRetryWait):
		}
	}
}

func (q *RedisJobQueue) consumeLoop(ctx context.Context, consumer string, heartbeat *health.Heartbeat) {
	defer heartbeat.Stop()
	for ctx.Err() == nil {
		heartbeat.Beat()
		streams, err := q.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    groupName,
			Consumer: consumer,
			Streams:  []string{streamKey, ">"},
			Count:    1,
			Block:    5 * time.Second,
		}).Result()
		if err != nil {
			if ctx.Err() != nil || err == redis.Nil {
				continue
			}
			slog.Error("job queue: XReadGroup failed", slog.String("error", err.Error()))
			time.Sleep(time.Second)
			continue
		}
		for _, stream := range streams {
			for _, msg := range stream.Messages {
				heartbeat.Beat()
				q.process(ctx, msg)
			}
		}
	}
}

func (q *RedisJobQueue) reclaimLoop(ctx context.Context, heartbeat *health.Heartbeat) {
	ticker := time.NewTicker(q.reclaimInterval)
	defer ticker.Stop()
	defer heartbeat.Stop()
	consumer := q.consumerName + "-reclaimer"

	heartbeat.Beat()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			heartbeat.Beat()
			messages, _, err := q.rdb.XAutoClaim(ctx, &redis.XAutoClaimArgs{
				Stream:   streamKey,
				Group:    groupName,
				Consumer: consumer,
				MinIdle:  q.claimMinIdle,
				Start:    "0",
				Count:    100,
			}).Result()
			if err != nil {
				if ctx.Err() == nil {
					slog.Error("job queue: XAutoClaim failed", slog.String("error", err.Error()))
				}
				continue
			}
			for _, msg := range messages {
				heartbeat.Beat()
				q.process(ctx, msg)
			}
		}
	}
}

func (q *RedisJobQueue) moverLoop(ctx context.Context, heartbeat *health.Heartbeat) {
	ticker := time.NewTicker(q.moverInterval)
	defer ticker.Stop()
	defer heartbeat.Stop()

	heartbeat.Beat()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			heartbeat.Beat()
			q.moveDueRetries(ctx)
		}
	}
}

func (q *RedisJobQueue) moveDueRetries(ctx context.Context) {
	now := strconv.FormatInt(time.Now().UnixMilli(), 10)
	members, err := q.rdb.ZRangeByScore(ctx, retryZSetKey, &redis.ZRangeBy{
		Min:   "-inf",
		Max:   now,
		Count: 100,
	}).Result()
	if err != nil {
		if ctx.Err() == nil {
			slog.Error("job queue: ZRangeByScore failed", slog.String("error", err.Error()))
		}
		return
	}

	for _, member := range members {
		if err := q.rdb.XAdd(ctx, &redis.XAddArgs{
			Stream: streamKey,
			MaxLen: q.streamMaxLen,
			Approx: true,
			Values: map[string]interface{}{"payload": member},
		}).Err(); err != nil {
			slog.Error("job queue: failed to re-enqueue due retry", slog.String("error", err.Error()))
			continue
		}
		// A crash between XAdd and ZRem duplicates the job rather than
		// losing it, which is acceptable under at-least-once delivery.
		q.rdb.ZRem(ctx, retryZSetKey, member)
	}
}

// process handles a single claimed stream message: run the handler
// registered for the job's type, then ack+delete it from the stream and
// either drop it (success), reschedule it (retryable failure, including a
// type this consumer has no handler for), or dead-letter it (attempts
// exhausted). Bookkeeping writes use their own short-lived context so a
// cancelled loop ctx can't lose accounting for a job that already ran.
func (q *RedisJobQueue) process(ctx context.Context, msg redis.XMessage) {
	raw, _ := msg.Values["payload"].(string)

	var job Job
	if err := json.Unmarshal([]byte(raw), &job); err != nil {
		slog.Error("job queue: dropping unparseable job", slog.String("streamId", msg.ID), slog.String("error", err.Error()))
		jobsProcessed.WithLabelValues("unparseable", resultDLQ).Inc()
		q.deadLetterRaw(raw)
		q.ackAndDel(msg.ID)
		return
	}

	var err error
	if handler, ok := q.handlerFor(job.Type); ok {
		handlerCtx, cancel := context.WithTimeout(ctx, handlerTimeout)
		start := time.Now()
		err = handler(handlerCtx, job.Payload)
		jobDuration.WithLabelValues(job.Type).Observe(time.Since(start).Seconds())
		cancel()
	} else {
		err = fmt.Errorf("job queue: no handler registered for job type %q", job.Type)
	}

	if err == nil {
		jobsProcessed.WithLabelValues(job.Type, resultOK).Inc()
		q.ackAndDel(msg.ID)
		return
	}

	if ctx.Err() != nil {
		// The loop is shutting down: leave the message unacked/pending
		// rather than counting this as a genuine attempt. It will be
		// picked back up (via XAutoClaim) once claimMinIdle elapses,
		// with its retry budget intact.
		return
	}

	job.Attempt++
	job.LastError = err.Error()

	if job.Attempt >= q.maxAttempts {
		slog.Error("job queue: exhausted retries, moving to DLQ",
			slog.String("jobId", job.ID), slog.String("type", job.Type), slog.Int("attempt", job.Attempt), slog.String("error", err.Error()))
		jobsProcessed.WithLabelValues(job.Type, resultDLQ).Inc()
		q.deadLetterJob(job)
		q.ackAndDel(msg.ID)
		return
	}

	jobsProcessed.WithLabelValues(job.Type, resultRetry).Inc()
	q.scheduleRetry(job)
	q.ackAndDel(msg.ID)
}

func (q *RedisJobQueue) ackAndDel(streamID string) {
	ctx, cancel := context.WithTimeout(context.Background(), bookkeepingTimeout)
	defer cancel()
	if err := q.rdb.XAck(ctx, streamKey, groupName, streamID).Err(); err != nil {
		slog.Error("job queue: XAck failed", slog.String("streamId", streamID), slog.String("error", err.Error()))
	}
	q.rdb.XDel(ctx, streamKey, streamID)
}

func (q *RedisJobQueue) scheduleRetry(job Job) {
	payload, err := json.Marshal(job)
	if err != nil {
		slog.Error("job queue: failed to marshal job for retry", slog.String("error", err.Error()))
		return
	}

	nextAttempt := time.Now().Add(backoffDuration(job.Attempt, q.baseBackoff, q.backoffCap))

	ctx, cancel := context.WithTimeout(context.Background(), bookkeepingTimeout)
	defer cancel()
	if err := q.rdb.ZAdd(ctx, retryZSetKey, redis.Z{
		Score:  float64(nextAttempt.UnixMilli()),
		Member: string(payload),
	}).Err(); err != nil {
		slog.Error("job queue: failed to schedule retry", slog.String("error", err.Error()))
	}
}

func (q *RedisJobQueue) deadLetterJob(job Job) {
	payload, err := json.Marshal(job)
	if err != nil {
		slog.Error("job queue: failed to marshal job for DLQ", slog.String("error", err.Error()))
		return
	}
	q.deadLetterRaw(string(payload))
}

func (q *RedisJobQueue) deadLetterRaw(payload string) {
	ctx, cancel := context.WithTimeout(context.Background(), bookkeepingTimeout)
	defer cancel()
	if err := q.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: dlqStreamKey,
		MaxLen: q.streamMaxLen,
		Approx: true,
		Values: map[string]interface{}{"payload": payload},
	}).Err(); err != nil {
		slog.Error("job queue: failed to write to DLQ", slog.String("error", err.Error()))
	}
}

// DeadLetter is one dead-letter stream entry as read back for inspection:
// the stream id it sits under and the Job envelope it holds. Raw is set
// instead of Job when the stored payload is not a valid envelope (see
// process: an unparseable stream message is dead-lettered as-is).
type DeadLetter struct {
	StreamID string          `json:"streamId"`
	Job      json.RawMessage `json:"job,omitempty"`
	Raw      string          `json:"raw,omitempty"`
}

const dlqScanBatch = 1000

// ScanDLQ walks the dead-letter stream oldest-first without modifying it,
// calling fn for each entry; the first error fn returns stops the scan.
func (q *RedisJobQueue) ScanDLQ(ctx context.Context, fn func(DeadLetter) error) error {
	start := "-"
	for {
		entries, err := q.rdb.XRangeN(ctx, dlqStreamKey, start, "+", dlqScanBatch).Result()
		if err != nil {
			return err
		}
		if len(entries) == 0 {
			return nil
		}
		for _, entry := range entries {
			raw, _ := entry.Values["payload"].(string)
			dl := DeadLetter{StreamID: entry.ID}
			if json.Valid([]byte(raw)) {
				dl.Job = json.RawMessage(raw)
			} else {
				dl.Raw = raw
			}
			if err := fn(dl); err != nil {
				return err
			}
		}
		// exclusive lower bound: resume after the last id of this batch
		start = "(" + entries[len(entries)-1].ID
	}
}

// PurgeDLQ removes every dead-letter entry that was dead-lettered before
// the given time (stream ids are the XADD timestamp, so this is a MINID
// trim) and returns how many were removed.
func (q *RedisJobQueue) PurgeDLQ(ctx context.Context, before time.Time) (int64, error) {
	return q.rdb.XTrimMinID(ctx, dlqStreamKey, strconv.FormatInt(before.UnixMilli(), 10)).Result()
}

// ReinjectDLQ re-enqueues every job currently in the dead-letter stream
// (attempt counter and last error reset) and removes it from the DLQ.
func (q *RedisJobQueue) ReinjectDLQ(ctx context.Context) (int, error) {
	entries, err := q.rdb.XRange(ctx, dlqStreamKey, "-", "+").Result()
	if err != nil {
		return 0, err
	}

	count := 0
	for _, entry := range entries {
		raw, _ := entry.Values["payload"].(string)

		var job Job
		if err := json.Unmarshal([]byte(raw), &job); err != nil {
			slog.Error("job queue: failed to unmarshal DLQ entry, skipping", slog.String("streamId", entry.ID), slog.String("error", err.Error()))
			continue
		}
		job.Attempt = 0
		job.LastError = ""

		if err := q.enqueueRaw(ctx, job); err != nil {
			return count, err
		}
		if err := q.rdb.XDel(ctx, dlqStreamKey, entry.ID).Err(); err != nil {
			slog.Error("job queue: failed to delete reinjected DLQ entry", slog.String("streamId", entry.ID), slog.String("error", err.Error()))
		}
		count++
	}
	return count, nil
}

// backoffDuration returns base*3^(attempt-1), capped at capD.
func backoffDuration(attempt int, base, capD time.Duration) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	d := base
	for i := 1; i < attempt; i++ {
		if d >= capD {
			return capD
		}
		d *= 3
	}
	if d > capD {
		return capD
	}
	return d
}
