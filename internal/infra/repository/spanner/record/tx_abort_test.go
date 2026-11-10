//go:build contractv1

package record

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/internal/infra/repository/repotest"
	"github.com/concrnt/concrnt/internal/testutil"
	ucrecord "github.com/concrnt/concrnt/internal/usecase/record"
)

// Two transactions that read the same key, pause, then write it conflict:
// Spanner aborts one of them and RunInTx re-runs its closure (C4: fn may run
// more than once), so both writes land and the later document holds the
// key. The emulator serialises read-write transactions instead of aborting
// them, so this only means something on Omni or Cloud Spanner.
func TestRunInTxRetriesOnAbort(t *testing.T) {
	ctx := context.Background()
	client, _ := testutil.CreateSpanner(t)
	repo := New(client)
	key := "cckv://con1owner/items/contended"

	var runs atomic.Int32
	var wg sync.WaitGroup
	for i, offset := range []time.Duration{0, time.Second} {
		wg.Add(1)
		go func(i int, offset time.Duration) {
			defer wg.Done()
			id := []string{"abort-older", "abort-newer"}[i]
			sd := repotest.SignedDocument(t, concrnt.Document[map[string]string]{Kind: "record", Key: key, Value: map[string]string{"v": id}})
			err := repo.RunInTx(ctx, func(tx ucrecord.RepositoryTx) error {
				runs.Add(1)
				if err := repo.CreateCommitLog(ctx, tx, id, "127.0.0.1", sd.Document, sd.Proof, "con1owner"); err != nil {
					return err
				}
				// CreateRecord reads the key row first; the sleep keeps both
				// transactions holding their read before either writes
				time.Sleep(500 * time.Millisecond)
				_, err := repo.CreateRecord(ctx, tx, id, key, "con1owner", "con1owner", "https://schema.example/post.json", nil, nil, []string{}, nil, time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).Add(offset))
				return err
			})
			require.NoError(t, err)
		}(i, offset)
	}
	wg.Wait()

	require.GreaterOrEqual(t, runs.Load(), int32(3), "one of the two closures must have been re-run after an abort")
	in := newInspector(client)
	for _, id := range []string{"abort-older", "abort-newer"} {
		commit, err := in.Commit(ctx, id)
		require.NoError(t, err)
		require.NotNil(t, commit, "%s must be committed", id)
	}
	state, err := in.RecordKey(ctx, key)
	require.NoError(t, err)
	require.NotNil(t, state)
	require.Equal(t, "abort-newer", *state.RecordID, "the newer document wins whichever order the transactions commit in")
}
