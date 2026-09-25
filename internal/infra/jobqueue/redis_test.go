package jobqueue_test

import (
	"context"
	"encoding/json"
	"errors"
	"maps"
	"reflect"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/concrnt/concrnt/internal/infra/health"
	"github.com/concrnt/concrnt/internal/infra/jobqueue"
	"github.com/concrnt/concrnt/internal/testutil"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/redis/go-redis/v9"
)

type testPayload struct {
	Host string `json:"host"`
}

func TestRedisJobQueue_EnqueueAndProcess(t *testing.T) {
	rdb, cleanup := testutil.CreateRDB()
	defer cleanup()

	q := jobqueue.NewRedisJobQueue(rdb, jobqueue.WithConsumerName("test-success"), jobqueue.WithConcurrency(2))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	received := make(chan testPayload, 1)
	q.RegisterHandler("test", func(ctx context.Context, payload json.RawMessage) error {
		var p testPayload
		if err := json.Unmarshal(payload, &p); err != nil {
			return err
		}
		received <- p
		return nil
	})
	go q.Run(ctx)

	if err := q.Enqueue(context.Background(), "test", testPayload{Host: "example.com"}); err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}

	select {
	case p := <-received:
		if p.Host != "example.com" {
			t.Fatalf("handler received payload %+v, want the enqueued one", p)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for job to be processed")
	}
	select {
	case p := <-received:
		t.Fatalf("job processed twice: %+v", p)
	case <-time.After(200 * time.Millisecond):
	}
}

func TestRedisJobQueue_RetryThenDeadLetter(t *testing.T) {
	rdb, cleanup := testutil.CreateRDB()
	defer cleanup()

	q := jobqueue.NewRedisJobQueue(rdb,
		jobqueue.WithConsumerName("test-dlq"),
		jobqueue.WithConcurrency(1),
		jobqueue.WithMaxAttempts(3),
		jobqueue.WithBaseBackoff(50*time.Millisecond),
		jobqueue.WithBackoffCap(50*time.Millisecond),
		jobqueue.WithMoverInterval(20*time.Millisecond),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var mu sync.Mutex
	var calls int

	q.RegisterHandler("test", func(ctx context.Context, payload json.RawMessage) error {
		mu.Lock()
		calls++
		mu.Unlock()
		return errors.New("boom")
	})
	go q.Run(ctx)

	if err := q.Enqueue(context.Background(), "test", testPayload{Host: "example.com"}); err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}

	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		c := calls
		mu.Unlock()
		if c >= 3 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	mu.Lock()
	finalCalls := calls
	mu.Unlock()
	if finalCalls != 3 {
		t.Fatalf("handler called %d times, want 3", finalCalls)
	}

	// The 3rd failure dead-letters synchronously inside process(); give it a
	// moment to land before asserting on the DLQ.
	time.Sleep(200 * time.Millisecond)

	// two rescheduled attempts, then the third dead-letters
	if got := promtestutil.ToFloat64(jobqueue.JobsProcessedForTest("test", "retry")); got != 2 {
		t.Fatalf("jobs_total{type=test,result=retry} = %v, want 2", got)
	}
	if got := promtestutil.ToFloat64(jobqueue.JobsProcessedForTest("test", "dlq")); got != 1 {
		t.Fatalf("jobs_total{type=test,result=dlq} = %v, want 1", got)
	}

	var dead []jobqueue.DeadLetter
	if err := q.ScanDLQ(context.Background(), func(dl jobqueue.DeadLetter) error {
		dead = append(dead, dl)
		return nil
	}); err != nil {
		t.Fatalf("ScanDLQ failed: %v", err)
	}
	if len(dead) != 1 {
		t.Fatalf("ScanDLQ returned %d entries, want 1", len(dead))
	}
	var deadJob jobqueue.Job
	if err := json.Unmarshal(dead[0].Job, &deadJob); err != nil {
		t.Fatalf("ScanDLQ entry is not a job envelope: %v", err)
	}
	if deadJob.Type != "test" || deadJob.Attempt != 3 || deadJob.LastError != "boom" {
		t.Fatalf("ScanDLQ entry = %+v, want type=test attempt=3 lastError=boom", deadJob)
	}

	n, err := q.ReinjectDLQ(context.Background())
	if err != nil {
		t.Fatalf("ReinjectDLQ failed: %v", err)
	}
	if n != 1 {
		t.Fatalf("ReinjectDLQ moved %d jobs, want 1", n)
	}

	// scanning is read-only, so reinject saw exactly what scan reported
	if err := q.ScanDLQ(context.Background(), func(jobqueue.DeadLetter) error {
		return errors.New("DLQ should be empty after reinject")
	}); err != nil {
		t.Fatal(err)
	}
}

// ScanDLQ pages through the stream and reports entries that were never a
// valid envelope as raw text rather than dropping them.
func TestRedisJobQueue_ScanDLQPagesAndKeepsRaw(t *testing.T) {
	rdb, cleanup := testutil.CreateRDB()
	defer cleanup()

	const total = 2500
	for i := 0; i < total; i++ {
		payload := `{"id":"job-` + strconv.Itoa(i) + `","type":"test","payload":{},"attempt":8,"createdAt":"2026-01-01T00:00:00Z"}`
		if i == 7 {
			payload = "not json"
		}
		if err := rdb.XAdd(context.Background(), &redis.XAddArgs{
			Stream: "jobqueue:dlq",
			Values: map[string]interface{}{"payload": payload},
		}).Err(); err != nil {
			t.Fatalf("XAdd failed: %v", err)
		}
	}

	q := jobqueue.NewRedisJobQueue(rdb, jobqueue.WithConsumerName("test-scan"))

	var dead []jobqueue.DeadLetter
	if err := q.ScanDLQ(context.Background(), func(dl jobqueue.DeadLetter) error {
		dead = append(dead, dl)
		return nil
	}); err != nil {
		t.Fatalf("ScanDLQ failed: %v", err)
	}
	if len(dead) != total {
		t.Fatalf("ScanDLQ returned %d entries, want %d", len(dead), total)
	}
	for i, dl := range dead {
		if i > 0 && dl.StreamID <= dead[i-1].StreamID {
			t.Fatalf("entry %d (%s) is not after entry %d (%s)", i, dl.StreamID, i-1, dead[i-1].StreamID)
		}
		if i == 7 {
			if dl.Raw != "not json" || dl.Job != nil {
				t.Fatalf("unparseable entry = %+v, want raw text", dl)
			}
			continue
		}
		var job jobqueue.Job
		if err := json.Unmarshal(dl.Job, &job); err != nil {
			t.Fatalf("entry %d is not a job envelope: %v", i, err)
		}
		if job.ID != "job-"+strconv.Itoa(i) {
			t.Fatalf("entry %d has id %s, want job-%d", i, job.ID, i)
		}
	}
}

// A job type this consumer has no handler for is retried like any failure
// (another replica may own the handler mid-rollout), not dropped or
// dead-lettered on first sight; it reaches the DLQ only once the retry
// budget is exhausted.
func TestRedisJobQueue_UnknownTypeIsRetried(t *testing.T) {
	rdb, cleanup := testutil.CreateRDB()
	defer cleanup()

	q := jobqueue.NewRedisJobQueue(rdb,
		jobqueue.WithConsumerName("test-unknown"),
		jobqueue.WithConcurrency(1),
		jobqueue.WithMaxAttempts(2),
		jobqueue.WithBaseBackoff(50*time.Millisecond),
		jobqueue.WithBackoffCap(50*time.Millisecond),
		jobqueue.WithMoverInterval(20*time.Millisecond),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go q.Run(ctx)

	if err := q.Enqueue(context.Background(), "unknown", testPayload{Host: "example.com"}); err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}

	// first attempt fails immediately, the retry is due after the backoff;
	// well before that the job must not be in the DLQ yet
	time.Sleep(20 * time.Millisecond)
	if n, err := q.ReinjectDLQ(context.Background()); err != nil || n != 0 {
		t.Fatalf("job dead-lettered on first sight (reinjected %d, err %v), want retry", n, err)
	}

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		n, err := q.ReinjectDLQ(context.Background())
		if err != nil {
			t.Fatalf("ReinjectDLQ failed: %v", err)
		}
		if n == 1 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("job never reached the DLQ after exhausting retries")
}

// PurgeDLQ trims by dead-letter time: entries whose stream id timestamp is
// before the cutoff go, the rest stay.
func TestRedisJobQueue_PurgeDLQBefore(t *testing.T) {
	rdb, cleanup := testutil.CreateRDB()
	defer cleanup()

	for _, id := range []string{"1000-0", "2000-0", "2000-1", "3000-0"} {
		if err := rdb.XAdd(context.Background(), &redis.XAddArgs{
			Stream: "jobqueue:dlq",
			ID:     id,
			Values: map[string]interface{}{"payload": `{"id":"` + id + `","type":"test","payload":{},"attempt":8,"createdAt":"2026-01-01T00:00:00Z"}`},
		}).Err(); err != nil {
			t.Fatalf("XAdd failed: %v", err)
		}
	}

	q := jobqueue.NewRedisJobQueue(rdb, jobqueue.WithConsumerName("test-purge"))

	deleted, err := q.PurgeDLQ(context.Background(), time.UnixMilli(2000))
	if err != nil {
		t.Fatalf("PurgeDLQ failed: %v", err)
	}
	if deleted != 1 {
		t.Fatalf("PurgeDLQ deleted %d entries, want 1", deleted)
	}

	var remaining []string
	if err := q.ScanDLQ(context.Background(), func(dl jobqueue.DeadLetter) error {
		remaining = append(remaining, dl.StreamID)
		return nil
	}); err != nil {
		t.Fatalf("ScanDLQ failed: %v", err)
	}
	if len(remaining) != 3 || remaining[0] != "2000-0" {
		t.Fatalf("remaining DLQ entries = %v, want [2000-0 2000-1 3000-0]", remaining)
	}
}

// Every loop of a running queue must keep its liveness heartbeat, and must
// release it when the queue stops, so a shut-down queue does not read as a
// wedged process.
func TestRedisJobQueue_ReportsLoopHeartbeats(t *testing.T) {
	rdb, cleanup := testutil.CreateRDB()
	defer cleanup()

	watchdog := health.NewWatchdog()
	q := jobqueue.NewRedisJobQueue(rdb,
		jobqueue.WithConsumerName("test-heartbeat"),
		jobqueue.WithConcurrency(2),
		jobqueue.WithWatchdog(watchdog),
	)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		q.Run(ctx)
		close(done)
	}()

	want := []string{"jobqueue/consumer-0", "jobqueue/consumer-1", "jobqueue/mover", "jobqueue/reclaimer", "jobqueue/stats"}
	testutil.WaitFor(t, func() bool { return len(watchdog.Loops(time.Now())) == len(want) })
	if got := slices.Sorted(maps.Keys(watchdog.Loops(time.Now()))); !reflect.DeepEqual(got, want) {
		t.Fatalf("registered loops %v, want %v", got, want)
	}

	// each loop has beaten at least once: seen from far in the future, every
	// one of them is overdue, which proves they are all running
	testutil.WaitFor(t, func() bool {
		return len(watchdog.Stalled(time.Now().Add(time.Hour))) == len(want)
	})
	if stalled := watchdog.Stalled(time.Now()); len(stalled) != 0 {
		t.Fatalf("no loop should be stalled while running: %v", stalled)
	}

	cancel()
	<-done
	if stalled := watchdog.Stalled(time.Now().Add(time.Hour)); len(stalled) != 0 {
		t.Fatalf("stopped loops must release their heartbeat: %v", stalled)
	}
}
