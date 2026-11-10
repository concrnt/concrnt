package record

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/stretchr/testify/require"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/internal/infra/repository/repotest"
	"github.com/concrnt/concrnt/internal/testutil"
	ucrecord "github.com/concrnt/concrnt/internal/usecase/record"
)

func countRows(t *testing.T, client *spanner.Client, table string) int64 {
	t.Helper()
	n, err := scalar[int64](context.Background(), client.Single(), spanner.Statement{SQL: "SELECT COUNT(*) FROM " + table})
	require.NoError(t, err)
	return n
}

// Two transactions that read the same key row, pause, then write it
// conflict: Spanner aborts one of them and the client re-runs its function
// from the read, which is the contention retry apply promises. Both writes
// land. The emulator serialises read-write transactions instead of
// aborting them, so this only means something on Omni or Cloud Spanner.
func TestApplyRetriesOnAbort(t *testing.T) {
	ctx := context.Background()
	client, _ := testutil.CreateSpanner(t)
	repo := New(client).(*Repository)
	key := "cckv://con1owner/items/contended"
	_, err := client.Apply(ctx, []*spanner.Mutation{spanner.Insert("record_keys",
		[]string{"uri", "clean_on_update", "gen"}, []any{key, false, "gen-0"})})
	require.NoError(t, err)

	var runs atomic.Int32
	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i := range errs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, errs[i] = repo.apply(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) (ucrecord.ApplyStatus, error) {
				runs.Add(1)
				if _, err := tx.ReadRow(ctx, "record_keys", spanner.Key{key}, []string{"redirect"}); err != nil {
					return ucrecord.Applied, err
				}
				// keeps both transactions holding their read before either
				// commits its write
				time.Sleep(500 * time.Millisecond)
				return ucrecord.Applied, tx.BufferWrite([]*spanner.Mutation{spanner.Update("record_keys",
					[]string{"uri", "redirect"}, []any{key, fmt.Sprintf("writer-%d", i)})})
			})
		}()
	}
	wg.Wait()

	require.NoError(t, errs[0])
	require.NoError(t, errs[1])
	require.GreaterOrEqual(t, runs.Load(), int32(3), "one of the two functions must have been re-run after an abort")
}

// A write that ends Stale or Duplicate is rolled back whole and reported
// without an error.
func TestApplyRollsBackUnappliedWrites(t *testing.T) {
	ctx := context.Background()
	client, _ := testutil.CreateSpanner(t)
	repo := New(client).(*Repository)

	for _, want := range []ucrecord.ApplyStatus{ucrecord.Stale, ucrecord.Duplicate} {
		status, err := repo.apply(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) (ucrecord.ApplyStatus, error) {
			return want, tx.BufferWrite([]*spanner.Mutation{spanner.Insert("record_keys",
				[]string{"uri", "clean_on_update", "gen"}, []any{"cckv://con1rollback/" + want.String(), false, "gen-0"})})
		})
		require.NoError(t, err)
		require.Equal(t, want, status)
	}
	require.Zero(t, countRows(t, client, "record_keys"))
}

// ApplyRecord under contention: writers race for one key that does not
// exist yet, each with its own createdAt. Whatever transactions Spanner
// aborts along the way, the newest document holds the key, every write
// reported Applied is in the log and no other is, and each Applied write
// replaced the one the log shows before it.
func TestApplyRecordConvergesUnderContention(t *testing.T) {
	ctx := context.Background()
	client, _ := testutil.CreateSpanner(t)
	repo := New(client)
	key := "cckv://con1owner/items/raced"
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	const writers = 12
	outcomes := make([]ucrecord.RecordOutcome, writers)
	errs := make([]error, writers)
	var wg sync.WaitGroup
	for i := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			id := fmt.Sprintf("raced-%02d", i)
			createdAt := base.Add(time.Duration(i) * time.Second)
			sd := repotest.SignedDocument(t, concrnt.Document[map[string]string]{Kind: "record", Key: key, Value: map[string]string{"id": id}, CreatedAt: createdAt})
			outcomes[i], errs[i] = repo.ApplyRecord(ctx, ucrecord.RecordCommit{
				Commit: ucrecord.Commit{ID: id, IP: "127.0.0.1", Document: sd.Document, Proof: sd.Proof, Owner: "con1owner"},
				Key:    key, Author: "con1owner", Schema: "https://schema.example/post.json", CreatedAt: createdAt,
			})
		}()
	}
	wg.Wait()

	applied := map[string]bool{}
	fresh := 0
	for i, outcome := range outcomes {
		require.NoError(t, errs[i])
		id := fmt.Sprintf("raced-%02d", i)
		switch outcome.Status {
		case ucrecord.Applied:
			applied[id] = true
			if outcome.FreshKey {
				fresh++
			} else {
				require.Less(t, outcome.PreviousID, id, "a write only replaces an older document")
			}
		case ucrecord.Stale:
		default:
			t.Fatalf("%s ended %s", id, outcome.Status)
		}
	}
	newest := fmt.Sprintf("raced-%02d", writers-1)
	require.True(t, applied[newest], "the newest document is never stale")
	require.Equal(t, 1, fresh, "exactly one write found the key fresh")

	sd, err := repo.GetSignedDocument(ctx, key)
	require.NoError(t, err)
	require.Equal(t, concrnt.ComposeCCFSURI("con1owner", concrnt.CCFSTypeConcrnt, newest), *sd.CCFS)

	entries, err := repo.ListCommitLogs(ctx, ucrecord.CommitLogPage{})
	require.NoError(t, err)
	logged := map[string]bool{}
	for _, entry := range entries {
		logged[entry.ID] = true
	}
	require.Equal(t, applied, logged, "a stale write leaves no commit, an applied one exactly its own")
	require.EqualValues(t, 1, countRows(t, client, "records"), "every replaced version was forgotten")
}
