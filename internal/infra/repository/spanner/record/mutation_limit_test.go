package record

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/internal/infra/repository/repotest"
	"github.com/concrnt/concrnt/internal/testutil"
	ucrecord "github.com/concrnt/concrnt/internal/usecase/record"
)

// mutationLimit is what Cloud Spanner lets one commit hold, index entries
// and cascaded rows included.
const mutationLimit = 80000

// Spanner Omni does not enforce the mutation limit, so a test cannot make a
// commit fail on it. What Omni does report is the mutation count of every
// commit, the number the limit is about, which the repository puts on the
// span of the operation. These tests read it from there and hold it
// against the limit.
type commitMeter struct {
	recorder *tracetest.SpanRecorder
	seen     int
}

var (
	meterOnce sync.Once
	meter     commitMeter
)

// meterCommits returns the meter of this test process. The package's tracer
// binds to the first tracer provider the process installs and stays with
// it, so there is one recorder for all tests.
func meterCommits() *commitMeter {
	meterOnce.Do(func() {
		meter.recorder = tracetest.NewSpanRecorder()
		otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(meter.recorder)))
	})
	meter.reset()
	return &meter
}

// reset forgets the spans ended so far.
func (m *commitMeter) reset() {
	m.seen = len(m.recorder.Ended())
}

// last returns the mutation count of the one span named name that ended
// since the previous call or reset.
func (m *commitMeter) last(t *testing.T, name string) int64 {
	t.Helper()
	spans := m.recorder.Ended()[m.seen:]
	m.reset()
	count, found := int64(0), 0
	for _, span := range spans {
		if span.Name() != name {
			continue
		}
		found++
		for _, attr := range span.Attributes() {
			if attr.Key == mutationCountKey {
				count = attr.Value.AsInt64()
			}
		}
	}
	require.Equal(t, 1, found, "one %s span", name)
	return count
}

// seedAssociations writes n associations on the current incarnation of
// target straight into the tables, a thousand per commit: going through
// ApplyAssociation would be a transaction each.
func seedAssociations(t *testing.T, ctx context.Context, client *spanner.Client, target, prefix string, n int) {
	t.Helper()
	row, err := client.Single().ReadRow(ctx, "record_keys", spanner.Key{target}, []string{"gen"})
	require.NoError(t, err)
	var gen string
	require.NoError(t, row.Column(0, &gen))

	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for start := 0; start < n; start += 1000 {
		var mutations []*spanner.Mutation
		for i := start; i < min(start+1000, n); i++ {
			id := fmt.Sprintf("%s-%06d", prefix, i)
			mutations = append(mutations,
				spanner.Insert("commit_logs",
					[]string{"id", "ip", "document", "proof", "owner", "gc_candidate", "c_date"},
					[]any{id, "127.0.0.1", "{}", `{"type":"none"}`, "con1owner", false, spanner.CommitTimestamp}),
				spanner.Insert("associations",
					[]string{"id", "target_uri", "target_gen", "owner", "author", "schema", "variant", "unique_key", "created_at", "c_date"},
					[]any{id, target, gen, "con1owner", fmt.Sprintf("con1author%06d", i), "https://schema.example/like.json", nil, id, at.Add(time.Duration(i) * time.Second), at}))
		}
		_, err := client.Apply(ctx, mutations)
		require.NoError(t, err)
	}
}

func putPost(t *testing.T, ctx context.Context, repo ucrecord.Repository, id, key string, createdAt time.Time) {
	t.Helper()
	sd := repotest.SignedDocument(t, concrnt.Document[map[string]string]{Kind: "record", Key: key, Value: map[string]string{"id": id}, CreatedAt: createdAt})
	outcome, err := repo.ApplyRecord(ctx, ucrecord.RecordCommit{
		Commit: ucrecord.Commit{ID: id, IP: "127.0.0.1", Document: sd.Document, Proof: sd.Proof, Owner: "con1owner"},
		Key:    key, Author: "con1owner", Schema: "https://schema.example/post.json", CreatedAt: createdAt,
	})
	require.NoError(t, err)
	require.Equal(t, ucrecord.Applied, outcome.Status)
}

// A key with 20,000 associations: cascading them from the key row, as the
// schema did before the gen column, is 100,000 mutations in the commit that
// removes the key (each association row has four index entries), which
// Cloud Spanner refuses. Now the removal costs the same handful of
// mutations as that of a key without any, the associations are gone from
// every read with it, and Cleanup removes their rows a page per commit,
// every one of them far below the limit.
func TestMutationLimitRemoveKeyWithManyAssociations(t *testing.T) {
	ctx := context.Background()
	client, _ := testutil.CreateSpanner(t)
	repo := New(client)
	meter := meterCommits()

	const associations = 20000
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	key := "cckv://con1owner/timeline/popular"
	putPost(t, ctx, repo, "popular-1", key, at)
	seedAssociations(t, ctx, client, key, "like", associations)
	summary, err := repo.ComputeAssociationSummary(ctx, key, 10)
	require.NoError(t, err)
	require.Len(t, summary.Variants, 1)
	require.EqualValues(t, associations, summary.Variants[0].Count)
	require.True(t, summary.AuthorsOverflow)
	require.Greater(t, associations*5, mutationLimit, "the cascade this test is about would not fit one commit")

	meter.reset()
	outcome, err := repo.ApplyDelete(ctx, ucrecord.DeleteCommit{
		Commit:   &ucrecord.Commit{ID: "popular-delete", Document: "{}", Owner: "con1owner"},
		Removals: []ucrecord.Removal{{Kind: ucrecord.RemoveKey, URI: key}},
	})
	require.NoError(t, err)
	require.Equal(t, []bool{true}, outcome.Removed)
	require.Len(t, outcome.Cleanups, 1)
	removal := meter.last(t, "Repository.Record.ApplyDelete")
	t.Logf("removing the key: %d mutations", removal)
	require.Positive(t, removal, "the commit must report its mutation count")
	require.Less(t, removal, int64(20), "the commit log entry, the record, the key row and their index entries")

	summary, err = repo.ComputeAssociationSummary(ctx, key, 10)
	require.NoError(t, err)
	require.Empty(t, summary.Variants, "gone from reads before any cleanup")
	require.EqualValues(t, associations, countRows(t, client, "associations"), "and still stored")

	// the key lives again while the old incarnation is being cleaned up
	putPost(t, ctx, repo, "popular-2", key, at.Add(time.Hour))
	seedAssociations(t, ctx, client, key, "again", 3)
	meter.reset()
	putPost(t, ctx, repo, "popular-3", key, at.Add(2*time.Hour))
	t.Logf("replacing a version that is forgotten (DML): %d mutations", meter.last(t, "Repository.Record.ApplyRecord"))

	var steps []int64
	for token := outcome.Cleanups[0]; token != ""; {
		token, err = repo.Cleanup(ctx, token)
		require.NoError(t, err)
		steps = append(steps, meter.last(t, "Repository.Record.Cleanup"))
	}
	t.Logf("cleanup steps: %v mutations", steps)
	require.Len(t, steps, associations/cleanupPage+1, "full pages, then the step that finds the rest (here nothing)")
	for _, step := range steps[:len(steps)-1] {
		require.Equal(t, int64(cleanupPage*5), step, "a page of association rows with four index entries each")
		require.Less(t, step, int64(mutationLimit))
	}
	require.Zero(t, steps[len(steps)-1])
	require.EqualValues(t, 3, countRows(t, client, "associations"), "only the new incarnation's are left")
	rows, err := repo.GetAssociatedRecords(ctx, key, "", "", "", nil, nil, 0, "asc")
	require.NoError(t, err)
	require.Len(t, rows, 3)
}

// One page of a range delete is rangeDeleteSyncLimit (500) keys in one
// ApplyDelete. Each costs its record, its key row, its gc flag and their
// index entries and nothing for its associations, so the page stays an
// order of magnitude below the limit however popular the keys are; and
// DeleteCommitLogs, which has no one to hand cleanup tokens to, leaves no
// association row of the keys it removed behind.
func TestMutationLimitRangeDeletePage(t *testing.T) {
	ctx := context.Background()
	client, _ := testutil.CreateSpanner(t)
	repo := New(client)
	meter := meterCommits()

	const page = 500
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	removals := make([]ucrecord.Removal, page)
	ids := make([]string, page)
	for i := range page {
		ids[i] = fmt.Sprintf("page-%03d", i)
		removals[i] = ucrecord.Removal{Kind: ucrecord.RemoveKey, URI: fmt.Sprintf("cckv://con1owner/timeline/%03d", i)}
		putPost(t, ctx, repo, ids[i], removals[i].URI, at)
	}
	for i := range 3 {
		seedAssociations(t, ctx, client, removals[i].URI, fmt.Sprintf("like-%d", i), 3000)
	}
	survivor := "cckv://con1owner/kept/post"
	putPost(t, ctx, repo, "kept", survivor, at)
	seedAssociations(t, ctx, client, survivor, "kept-like", 3000)
	meter.reset()

	outcome, err := repo.ApplyDelete(ctx, ucrecord.DeleteCommit{
		Commit:      &ucrecord.Commit{ID: "page-delete", Document: "{}", Owner: "con1owner"},
		Removals:    removals,
		GcCommitIDs: append([]string{"page-delete"}, ids...),
	})
	require.NoError(t, err)
	require.NotContains(t, outcome.Removed, false)
	require.Len(t, outcome.Cleanups, 3, "a token for each key that had associations, none for the rest")
	mutations := meter.last(t, "Repository.Record.ApplyDelete")
	t.Logf("a range delete page of %d keys: %d mutations", page, mutations)
	require.Positive(t, mutations)
	require.Less(t, mutations, int64(mutationLimit/8))

	// gc of a live record takes its key along, and the 3000 associations
	// with it, in commits of their own
	deleted, err := repo.DeleteCommitLogs(ctx, []string{"kept"})
	require.NoError(t, err)
	require.Equal(t, 1, deleted)
	require.EqualValues(t, 9000, countRows(t, client, "associations"), "what is left waits for the three cleanup chains")
	for _, token := range outcome.Cleanups {
		for token != "" {
			token, err = repo.Cleanup(ctx, token)
			require.NoError(t, err)
		}
	}
	require.Zero(t, countRows(t, client, "associations"))
}
