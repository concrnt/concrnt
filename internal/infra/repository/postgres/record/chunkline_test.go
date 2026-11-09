package record

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/internal/infra/database/models"
	"github.com/concrnt/concrnt/internal/testutil"
	ucrecord "github.com/concrnt/concrnt/internal/usecase/record"
	"github.com/stretchr/testify/require"
)

func TestChunklineRepositoryUsesRecordCreatedAt(t *testing.T) {
	db, cleanup := testutil.CreateDB()
	t.Cleanup(cleanup)

	ctx := context.Background()
	recordRepo := New(db)
	chunklineRepo := NewChunklineRepository(db)

	parentURI := "cckv://con1owner/timeline"
	createdAt10 := time.Unix(10*600+10, 0).UTC()
	createdAt11 := time.Unix(11*600+20, 0).UTC()
	createdAt12 := time.Unix(12*600+30, 0).UTC()

	createChunklineRecord(t, ctx, recordRepo, "record-10", parentURI+"/record-10", createdAt10)
	createChunklineRecord(t, ctx, recordRepo, "record-11", parentURI+"/record-11", createdAt11)
	createChunklineRecord(t, ctx, recordRepo, "record-12", parentURI+"/record-12", createdAt12)

	var parent models.RecordKey
	require.NoError(t, db.Where("uri = ?", parentURI).Take(&parent).Error)
	require.Nil(t, parent.RecordCreatedAt)

	manifest, err := chunklineRepo.GetChunklineManifest(ctx, parentURI)
	require.NoError(t, err)
	require.NotNil(t, manifest.FirstChunk)
	require.EqualValues(t, 10, *manifest.FirstChunk)

	itrs, err := chunklineRepo.LookupLocalItrs(ctx, []string{parentURI}, 11)
	require.NoError(t, err)
	require.EqualValues(t, 11, itrs[parentURI])

	body, err := chunklineRepo.LoadLocalBody(ctx, parentURI, 11)
	require.NoError(t, err)
	require.Len(t, body, 2)
	require.Equal(t, parentURI+"/record-11", body[0].Href)
	require.True(t, body[0].Timestamp.Equal(createdAt11))
	require.Equal(t, parentURI+"/record-10", body[1].Href)
	require.True(t, body[1].Timestamp.Equal(createdAt10))
}

// LoadLocalBody must judge window coverage and emit timestamps by the
// record_created_at column — the column its queries filter and order by — not
// the joined records.created_at, which can diverge (e.g. backdated records).
func TestChunklineLoadLocalBodySurvivesRecordCreatedAtDivergence(t *testing.T) {
	db, cleanup := testutil.CreateDB()
	t.Cleanup(cleanup)

	ctx := context.Background()
	recordRepo := New(db)
	chunklineRepo := NewChunklineRepository(db)

	parentURI := "cckv://con2owner/timeline"

	// more records than defaultChunkSize inside chunk 20's window, so the
	// limited first query cannot cover the window on its own
	count := defaultChunkSize + 1
	for i := range count {
		createdAt := time.Unix(20*600+int64(i)+1, 0).UTC()
		id := fmt.Sprintf("div-record-%02d", i)
		createChunklineRecord(t, ctx, recordRepo, id, fmt.Sprintf("%s/%s", parentURI, id), createdAt)
	}

	// Backdate the joined records.created_at of the limited query's last
	// member (the oldest of the top defaultChunkSize records) to before the
	// window start. The window-coverage guard must ignore this diverged
	// column, or the oldest record in the window gets dropped.
	backdated := time.Unix(18*600, 0).UTC()
	require.NoError(t, db.Model(&models.Record{}).Where("document_id = ?", "div-record-01").Update("created_at", backdated).Error)

	body, err := chunklineRepo.LoadLocalBody(ctx, parentURI, 20)
	require.NoError(t, err)
	require.Len(t, body, count)

	// emitted timestamps must come from record_created_at as well
	for _, item := range body {
		require.True(t, item.Timestamp.After(time.Unix(20*600, 0)), "timestamp %v must be record_created_at, not the backdated created_at", item.Timestamp)
	}
}

// Chunks are half-open intervals [chunkID*600, (chunkID+1)*600) — a record
// whose record_created_at is exactly the next chunk's start time belongs to
// the next chunk, and one exactly at the chunk's start time belongs to it.
func TestChunklineChunkBoundaryIsHalfOpen(t *testing.T) {
	db, cleanup := testutil.CreateDB()
	t.Cleanup(cleanup)

	ctx := context.Background()
	recordRepo := New(db)
	chunklineRepo := NewChunklineRepository(db)

	parentURI := "cckv://con3owner/timeline"
	chunkStart := time.Unix(30*600, 0).UTC()
	inChunk := time.Unix(30*600+10, 0).UTC()
	nextChunkStart := time.Unix(31*600, 0).UTC()

	createChunklineRecord(t, ctx, recordRepo, "boundary-start", parentURI+"/boundary-start", chunkStart)
	createChunklineRecord(t, ctx, recordRepo, "boundary-mid", parentURI+"/boundary-mid", inChunk)
	createChunklineRecord(t, ctx, recordRepo, "boundary-next", parentURI+"/boundary-next", nextChunkStart)

	itrs, err := chunklineRepo.LookupLocalItrs(ctx, []string{parentURI}, 30)
	require.NoError(t, err)
	require.EqualValues(t, 30, itrs[parentURI])

	body, err := chunklineRepo.LoadLocalBody(ctx, parentURI, 30)
	require.NoError(t, err)
	require.Len(t, body, 2)
	require.Equal(t, parentURI+"/boundary-mid", body[0].Href)
	require.Equal(t, parentURI+"/boundary-start", body[1].Href)
}

func TestChunklineManifestEmptyFeedHasNullFirstChunk(t *testing.T) {
	db, cleanup := testutil.CreateDB()
	t.Cleanup(cleanup)

	ctx := context.Background()
	recordRepo := New(db)
	chunklineRepo := NewChunklineRepository(db)

	feedURI := "cckv://con4owner/timeline"
	createChunklineRecord(t, ctx, recordRepo, "empty-feed", feedURI, time.Unix(40*600, 0).UTC())

	manifest, err := chunklineRepo.GetChunklineManifest(ctx, feedURI)
	require.NoError(t, err)
	require.Nil(t, manifest.FirstChunk)
}

func createChunklineRecord(t *testing.T, ctx context.Context, repo ucrecord.Repository, id string, key string, createdAt time.Time) {
	t.Helper()

	sd := repositorySignedDocument(t, concrnt.Document[map[string]string]{
		Kind:      "record",
		Key:       key,
		Value:     map[string]string{"body": id},
		Author:    "con1author",
		Schema:    "https://schema.example/post.json",
		CreatedAt: createdAt,
	})

	withRepositoryTx(t, ctx, repo, id, "127.0.0.1", sd, "con1owner", func(tx ucrecord.RepositoryTx) error {
		_, err := repo.CreateRecord(ctx, tx, id, key, "con1owner", "con1owner", "https://schema.example/post.json", nil, nil, []string{}, nil, createdAt)
		return err
	})
}
