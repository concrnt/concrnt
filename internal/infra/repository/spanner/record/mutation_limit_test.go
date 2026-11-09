package record

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/internal/infra/repository/repotest"
	"github.com/concrnt/concrnt/internal/testutil"
	ucrecord "github.com/concrnt/concrnt/internal/usecase/record"
)

const (
	bigTimeline = "cckv://con1owner/timelines/big"
	bigChildren = 20000
	// rangeDeletePage is record.rangeDeleteSyncLimit: what one page of a
	// range delete removes per transaction
	rangeDeletePage = 500
)

// seedBigTimeline commits bigChildren records under bigTimeline, 100 per
// transaction, 8 transactions at a time.
func seedBigTimeline(t *testing.T, ctx context.Context, repo ucrecord.Repository) {
	t.Helper()
	const perTx, workers = 100, 8
	batches := make(chan int, bigChildren/perTx)
	for b := 0; b < bigChildren/perTx; b++ {
		batches <- b
	}
	close(batches)
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for b := range batches {
				err := repo.RunInTx(ctx, func(tx ucrecord.RepositoryTx) error {
					for i := b * perTx; i < (b+1)*perTx; i++ {
						id := fmt.Sprintf("big-%05d", i)
						key := fmt.Sprintf("%s/%05d", bigTimeline, i)
						createdAt := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).Add(time.Duration(i) * time.Second)
						sd := repotest.SignedDocument(t, concrnt.Document[map[string]string]{Kind: "record", Key: key, Value: map[string]string{"i": id}, CreatedAt: createdAt})
						if err := repo.CreateCommitLog(ctx, tx, id, "127.0.0.1", sd.Document, sd.Proof, "con1owner"); err != nil {
							return err
						}
						if _, err := repo.CreateRecord(ctx, tx, id, key, "con1owner", "con1owner", "https://schema.example/post.json", nil, nil, []string{}, nil, createdAt); err != nil {
							return err
						}
					}
					return nil
				})
				if err != nil {
					errs <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
}

// A timeline of 20,000 records is more than Cloud Spanner lets one commit
// delete: each record takes its key row and index entries with it, well
// past the 80,000 mutations per commit. Paged the way the range delete
// jobs page (QueryRecordSubtree with a cursor, rangeDeletePage keys per
// transaction) the whole subtree goes, and DeleteCommitLogs takes the
// 20,000 commits because it batches internally. Spanner Omni does not
// enforce the limit (a single transaction over the subtree went through
// when this was written), so the test pins the paging, not the failure.
func TestMutationLimitPagedRangeDelete(t *testing.T) {
	ctx := context.Background()
	client, _ := testutil.CreateSpanner(t)
	repo := New(client)
	start := time.Now()
	seedBigTimeline(t, ctx, repo)
	t.Logf("seeded %d records in %s", bigChildren, time.Since(start))

	all, err := repo.QueryRecordSubtree(ctx, bigTimeline, false, "", 0)
	require.NoError(t, err)
	require.Len(t, all, bigChildren)

	start = time.Now()
	pages := 0
	cursor := ""
	for {
		page, err := repo.QueryRecordSubtree(ctx, bigTimeline, false, cursor, rangeDeletePage)
		require.NoError(t, err)
		if len(page) == 0 {
			break
		}
		pages++
		require.NoError(t, repo.RunInTx(ctx, func(tx ucrecord.RepositoryTx) error {
			for _, sd := range page {
				if err := repo.DeleteRecordByKey(ctx, tx, *sd.CCKV); err != nil {
					return err
				}
			}
			return nil
		}))
		cursor = *page[len(page)-1].CCKV
	}
	require.Equal(t, bigChildren/rangeDeletePage, pages)
	t.Logf("deleted %d records in %d pages in %s", bigChildren, pages, time.Since(start))
	remaining, err := repo.QueryRecordSubtree(ctx, bigTimeline, false, "", 1)
	require.NoError(t, err)
	require.Empty(t, remaining)

	entries, err := repo.ListCommitLogs(ctx, ucrecord.CommitLogPage{Owner: "con1owner"})
	require.NoError(t, err)
	require.Len(t, entries, bigChildren)
	ids := make([]string, len(entries))
	for i, e := range entries {
		ids[i] = e.ID
	}
	deleted, err := repo.DeleteCommitLogs(ctx, ids)
	require.NoError(t, err)
	require.Equal(t, bigChildren, deleted)
	n, err := repo.CountGcCandidates(ctx, "~")
	require.NoError(t, err)
	require.Zero(t, n)
	commit, err := newInspector(client).Commit(ctx, "big-00000")
	require.NoError(t, err)
	require.Nil(t, commit)
}
