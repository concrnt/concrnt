package record

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/stretchr/testify/require"

	"github.com/concrnt/concrnt"
	aggspanner "github.com/concrnt/concrnt/internal/infra/aggregate/spanner"
	"github.com/concrnt/concrnt/internal/infra/repository/repotest"
	"github.com/concrnt/concrnt/internal/testutil"
	"github.com/concrnt/concrnt/internal/usecase/aggregate"
	ucrecord "github.com/concrnt/concrnt/internal/usecase/record"
)

// rpcs counts the Spanner RPCs that ended since the last reset by method
// name, and those among them that ended ABORTED: a transaction the server
// gave up for a lock conflict, which the client then runs again.
func (m *commitMeter) rpcs() (calls, aborted map[string]int) {
	calls, aborted = map[string]int{}, map[string]int{}
	for _, span := range m.recorder.Ended()[m.seen:] {
		method, ok := strings.CutPrefix(span.Name(), "google.spanner.v1.Spanner/")
		if !ok {
			continue
		}
		calls[method]++
		for _, attr := range span.Attributes() {
			if attr.Key == "rpc.response.status_code" && attr.Value.AsString() == "ABORTED" {
				aborted[method]++
			}
		}
	}
	return calls, aborted
}

type latencies struct {
	mu sync.Mutex
	d  []time.Duration
}

func (l *latencies) since(start time.Time) {
	d := time.Since(start)
	l.mu.Lock()
	l.d = append(l.d, d)
	l.mu.Unlock()
}

func (l *latencies) String() string {
	if len(l.d) == 0 {
		return "n=0"
	}
	slices.Sort(l.d)
	at := func(p float64) time.Duration {
		return l.d[min(len(l.d)-1, int(float64(len(l.d))*p))].Round(100 * time.Microsecond)
	}
	return fmt.Sprintf("n=%d p50=%s p95=%s max=%s", len(l.d), at(0.5), at(0.95), at(1))
}

// The load a popular post puts on the database, with the count cache in
// the same database: associations committed on one target while its counts
// and its listing are read, records committed into one timeline, and for
// every new record several callers offering its (missing) count entry at
// once. None of these decide on a row another one writes, so no
// transaction may be aborted and re-run, every write costs the commits its
// method is specified with, and nothing is counted twice or lost.
//
// Spanner Omni runs the lock manager of Cloud Spanner, so an abort here is
// one there; the latencies logged are Omni's on this machine and say
// nothing about Cloud Spanner.
func TestHotTargetCommitsAndReadsDoNotAbort(t *testing.T) {
	ctx := context.Background()
	client, _ := testutil.CreateSpanner(t)
	repo := New(client)
	counts := aggspanner.New(client)
	meter := meterCommits()

	const (
		likers, likesEach   = 8, 50
		posters, postsEach  = 4, 20
		readers             = 8
		offersPerNewEntry   = 4
		likeSchema          = "https://schema.example/like.json"
		timeline            = "cckv://con1owner/posts"
		target              = timeline + "/hot"
		likes, posts        = likers * likesEach, posters * postsEach
		firstLiker          = "con1liker00"
		listedPerLiker      = 11
		expectedFirstListed = min(likesEach, listedPerLiker)
	)
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	putPost(t, ctx, repo, "hot", target, base)
	entry := aggregate.Key(ucrecord.AggregateAssociationCounts, target)
	won, err := counts.PutIfAbsent(ctx, entry, aggregate.Counters{AsOf: time.Now()}, time.Hour)
	require.NoError(t, err)
	require.True(t, won)
	meter.reset()

	var writers, background sync.WaitGroup
	done := make(chan struct{})
	var applyAssociation, applyRecord, add, get, list, offer latencies
	var gets, lists atomic.Int64

	for liker := range likers {
		writers.Add(1)
		go func() {
			defer writers.Done()
			author := fmt.Sprintf("con1liker%02d", liker)
			for n := range likesEach {
				id := fmt.Sprintf("like-%02d-%03d", liker, n)
				createdAt := base.Add(time.Duration(liker*likesEach+n) * time.Second)
				sd := repotest.SignedDocument(t, concrnt.Document[map[string]string]{Kind: "association", Value: map[string]string{"id": id}, CreatedAt: createdAt})
				start := time.Now()
				status, err := repo.ApplyAssociation(ctx, ucrecord.AssociationCommit{
					Commit: ucrecord.Commit{ID: id, IP: "127.0.0.1", Document: sd.Document, Proof: sd.Proof, Owner: "con1owner"},
					Target: target, Author: author, Schema: likeSchema, Unique: id, CreatedAt: createdAt,
				})
				applyAssociation.since(start)
				if err != nil || status != ucrecord.Applied {
					t.Errorf("ApplyAssociation %s: %s, %v", id, status, err)
					return
				}
				start = time.Now()
				ok, err := counts.Add(ctx, entry, aggregate.Delta{Inc: map[string]int64{"like": 1}, AddMembers: []string{author}})
				add.since(start)
				if err != nil || !ok {
					t.Errorf("Add for %s: %v, %v", id, ok, err)
					return
				}
			}
		}()
	}

	for poster := range posters {
		writers.Add(1)
		go func() {
			defer writers.Done()
			for n := range postsEach {
				id := fmt.Sprintf("post-%02d-%03d", poster, n)
				key := timeline + "/" + id
				createdAt := base.Add(time.Duration(poster*postsEach+n) * time.Second)
				sd := repotest.SignedDocument(t, concrnt.Document[map[string]string]{Kind: "record", Key: key, Value: map[string]string{"id": id}, CreatedAt: createdAt})
				start := time.Now()
				outcome, err := repo.ApplyRecord(ctx, ucrecord.RecordCommit{
					Commit: ucrecord.Commit{ID: id, IP: "127.0.0.1", Document: sd.Document, Proof: sd.Proof, Owner: "con1owner"},
					Key:    key, Author: "con1owner", Schema: "https://schema.example/post.json", CreatedAt: createdAt,
				})
				applyRecord.since(start)
				if err != nil || outcome.Status != ucrecord.Applied || !outcome.FreshKey {
					t.Errorf("ApplyRecord %s: %+v, %v", id, outcome, err)
					return
				}

				// the writer seeds the entry of the new key while the
				// first readers, who found none, offer what they computed
				var winners atomic.Int32
				var offers sync.WaitGroup
				for range offersPerNewEntry {
					offers.Add(1)
					go func() {
						defer offers.Done()
						start := time.Now()
						won, err := counts.PutIfAbsent(ctx, aggregate.Key(ucrecord.AggregateAssociationCounts, key), aggregate.Counters{AsOf: time.Now()}, time.Hour)
						offer.since(start)
						if err != nil {
							t.Errorf("PutIfAbsent for %s: %v", id, err)
						}
						if won {
							winners.Add(1)
						}
					}()
				}
				offers.Wait()
				if winners.Load() != 1 {
					t.Errorf("%d of the callers offering the entry of %s stored it", winners.Load(), id)
				}
			}
		}()
	}

	for range readers {
		background.Add(1)
		go func() {
			defer background.Done()
			for {
				select {
				case <-done:
					return
				default:
				}
				start := time.Now()
				_, ok, err := counts.Get(ctx, entry)
				get.since(start)
				gets.Add(1)
				if err != nil || !ok {
					t.Errorf("Get: %v, %v", ok, err)
					return
				}
				start = time.Now()
				_, err = repo.GetAssociatedRecords(ctx, target, "", "", firstLiker, nil, nil, listedPerLiker, "desc")
				list.since(start)
				lists.Add(1)
				if err != nil {
					t.Errorf("GetAssociatedRecords: %v", err)
					return
				}
			}
		}()
	}

	writers.Wait()
	close(done)
	background.Wait()
	calls, aborted := meter.rpcs()

	t.Logf("ApplyAssociation %s", &applyAssociation)
	t.Logf("ApplyRecord      %s", &applyRecord)
	t.Logf("aggregate Add    %s", &add)
	t.Logf("aggregate Get    %s", &get)
	t.Logf("PutIfAbsent      %s", &offer)
	t.Logf("GetAssociatedRecords %s", &list)
	t.Logf("RPCs %v, aborted %v", calls, aborted)

	require.Empty(t, aborted, "a transaction was aborted and re-run")
	// one statement per Apply* and per read, none for a write of the cache
	require.Equal(t, likes+posts+int(gets.Load())+int(lists.Load()), calls["ExecuteStreamingSql"])
	// one commit per Apply*, per Add and per offered entry, and one for
	// every fold of the hot entry's delta rows, of which there are at most
	// a handful more than the Adds require
	writes := likes + posts + likes + posts*offersPerNewEntry
	require.GreaterOrEqual(t, calls["Commit"], writes)
	require.LessOrEqual(t, calls["Commit"], writes+likes/8, "the delta rows are folded once per fold due, not once per reader")
	// an offer that lost reads the entry that beat it, nothing else reads
	// by key
	require.LessOrEqual(t, calls["StreamingRead"], posts*(offersPerNewEntry-1))
	require.Zero(t, calls["BeginTransaction"])

	got, ok, err := counts.Get(ctx, entry)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, map[string]int64{"like": likes}, got.Counts, "every Add is in the entry once")
	require.EqualValues(t, likes, got.Deltas)
	require.Len(t, got.Members, likers)

	summary, err := repo.ComputeAssociationSummary(ctx, target, 1000)
	require.NoError(t, err)
	require.Equal(t, []ucrecord.AssociationVariant{{Schema: likeSchema, Count: likes, Earliest: base}}, summary.Variants)
	require.Len(t, summary.Authors, likers)

	rows, err := repo.GetAssociatedRecords(ctx, target, "", "", firstLiker, nil, nil, listedPerLiker, "desc")
	require.NoError(t, err)
	require.Len(t, rows, expectedFirstListed)
}

// rowsScanned runs stmt and returns how many rows it returned and how many
// the server read for them.
func rowsScanned(t *testing.T, client *spanner.Client, stmt spanner.Statement) (returned, scanned int) {
	t.Helper()
	it := client.Single().QueryWithStats(context.Background(), stmt)
	require.NoError(t, it.Do(func(*spanner.Row) error {
		returned++
		return nil
	}))
	text, ok := it.QueryStats["rows_scanned"].(string)
	require.True(t, ok, "rows_scanned in the query stats, got %v", it.QueryStats)
	scanned, err := strconv.Atoi(text)
	require.NoError(t, err)
	return returned, scanned
}

// The listings a client issues for every message it shows (its own
// associations on the target, the newest associations) read the rows of
// the page and not the target's whole association set, however many
// associations the target has. Ascending order has no index of its own and
// reads the range (associatedRecordsStatement); it must still be right.
func TestAssociationListingsOnALargeTargetSeek(t *testing.T) {
	ctx := context.Background()
	client, _ := testutil.CreateSpanner(t)
	repo := New(client)
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	const (
		target     = "cckv://con1owner/posts/popular"
		total      = 3000
		page       = 11
		likeSchema = "https://schema.example/like.json"
		author     = "con1author000007"
		// a row of the page costs its index entry, its association row
		// and its commit log entry, and the server reads a batch ahead of
		// the limit; the range would be at least total
		scanBudget = total / 10
	)
	putPost(t, ctx, repo, "popular", target, base)
	seedAssociations(t, ctx, client, target, "seek", total)

	for _, c := range []struct {
		name                  string
		schema, author, order string
		returned              int
	}{
		{"newest first", "", "", "desc", page},
		{"one author's", "", author, "desc", 1},
		{"one author's of a schema", likeSchema, author, "desc", 1},
		{"one author's, oldest first", "", author, "asc", 1},
	} {
		t.Run(c.name, func(t *testing.T) {
			returned, scanned := rowsScanned(t, client, associatedRecordsStatement(target, c.schema, "", c.author, nil, nil, page, c.order))
			require.Equal(t, c.returned, returned)
			require.LessOrEqual(t, scanned, scanBudget)
		})
	}

	rows, err := repo.GetAssociatedRecords(ctx, target, "", "", "", nil, nil, 3, "desc")
	require.NoError(t, err)
	require.Len(t, rows, 3)
	for i, row := range rows {
		require.Equal(t, base.Add(time.Duration(total-1-i)*time.Second), row.CreatedAt, "newest first")
	}
	rows, err = repo.GetAssociatedRecords(ctx, target, "", "", "", nil, nil, 3, "asc")
	require.NoError(t, err)
	require.Len(t, rows, 3)
	for i, row := range rows {
		require.Equal(t, base.Add(time.Duration(i)*time.Second), row.CreatedAt, "oldest first")
	}
}
