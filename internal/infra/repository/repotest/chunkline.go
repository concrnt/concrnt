package repotest

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/concrnt/concrnt/internal/domain"
)

// RunChunklineSuite runs the chunkline contract tests of record.Repository. open must
// return a fresh, isolated backend per call; it is called once per top-level
// test.
func RunChunklineSuite(t *testing.T, open func(t *testing.T) Backend) {
	t.Run("ManifestItrAndBody", func(t *testing.T) { testChunklineManifestItrAndBody(t, open(t)) })
	t.Run("ChunkBoundaryIsHalfOpen", func(t *testing.T) { testChunklineChunkBoundaryIsHalfOpen(t, open(t)) })
	t.Run("EmptyAndMissingTimelines", func(t *testing.T) { testChunklineEmptyAndMissingTimelines(t, open(t)) })
	t.Run("WindowLargerThanAPage", func(t *testing.T) { testChunklineWindowLargerThanAPage(t, open(t)) })
	t.Run("SparseWindowIsFilledFromOlderItems", func(t *testing.T) { testChunklineSparseWindow(t, open(t)) })
}

// chunkPageSize is the number of items a body holds at least when the
// timeline has that many at or before the chunk.
const chunkPageSize = 32

func chunkTime(chunk, offset int64) time.Time {
	return time.Unix(chunk*600+offset, 0).UTC()
}

func testChunklineManifestItrAndBody(t *testing.T, be Backend) {
	ctx := context.Background()
	repo := be.Record

	parentURI := "cckv://con1owner/timeline"
	createdAt10 := chunkTime(10, 10)
	createdAt11 := chunkTime(11, 20)
	createdAt12 := chunkTime(12, 30)

	putRecord(t, ctx, repo, "record-10", parentURI+"/record-10", createdAt10)
	putRecord(t, ctx, repo, "record-11", parentURI+"/record-11", createdAt11)
	putRecord(t, ctx, repo, "record-12", parentURI+"/record-12", createdAt12)
	// a grandchild is not an item of the timeline
	putRecord(t, ctx, repo, "record-deep", parentURI+"/record-11/deep", chunkTime(9, 0))

	manifest, err := repo.GetChunklineManifest(ctx, parentURI)
	require.NoError(t, err)
	require.NotNil(t, manifest.FirstChunk)
	require.EqualValues(t, 10, *manifest.FirstChunk)

	other := "cckv://con1owner/other"
	putRecord(t, ctx, repo, "other-20", other+"/x", chunkTime(20, 0))
	itrs, err := repo.LookupLocalItrs(ctx, []string{parentURI, other, "cckv://con1owner/unknown"}, 11)
	require.NoError(t, err)
	require.Equal(t, map[string]int64{parentURI: 11}, itrs, "a timeline without an item at or before the chunk has no entry")

	itrs, err = repo.LookupLocalItrs(ctx, []string{parentURI, other}, 30)
	require.NoError(t, err)
	require.Equal(t, map[string]int64{parentURI: 12, other: 20}, itrs)

	body, err := repo.LoadLocalBody(ctx, parentURI, 11)
	require.NoError(t, err)
	require.Len(t, body, 2)
	require.Equal(t, parentURI+"/record-11", body[0].Href)
	require.True(t, body[0].Timestamp.Equal(createdAt11))
	require.Equal(t, parentURI+"/record-10", body[1].Href)
	require.True(t, body[1].Timestamp.Equal(createdAt10))

	// a removed item leaves the body
	removeKeys(t, ctx, repo, "record-11-delete", "con1owner", parentURI+"/record-11")
	body, err = repo.LoadLocalBody(ctx, parentURI, 11)
	require.NoError(t, err)
	require.Len(t, body, 1)
	require.Equal(t, parentURI+"/record-10", body[0].Href)
}

// Chunks are half-open intervals [chunkID*600, (chunkID+1)*600) — a record
// whose createdAt is exactly the next chunk's start time belongs to the next
// chunk, and one exactly at the chunk's start time belongs to it.
func testChunklineChunkBoundaryIsHalfOpen(t *testing.T, be Backend) {
	ctx := context.Background()
	repo := be.Record

	parentURI := "cckv://con3owner/timeline"
	putRecord(t, ctx, repo, "boundary-start", parentURI+"/boundary-start", chunkTime(30, 0))
	putRecord(t, ctx, repo, "boundary-mid", parentURI+"/boundary-mid", chunkTime(30, 10))
	putRecord(t, ctx, repo, "boundary-next", parentURI+"/boundary-next", chunkTime(31, 0))

	itrs, err := repo.LookupLocalItrs(ctx, []string{parentURI}, 30)
	require.NoError(t, err)
	require.EqualValues(t, 30, itrs[parentURI])

	body, err := repo.LoadLocalBody(ctx, parentURI, 30)
	require.NoError(t, err)
	require.Len(t, body, 2)
	require.Equal(t, parentURI+"/boundary-mid", body[0].Href)
	require.Equal(t, parentURI+"/boundary-start", body[1].Href)
}

// A key without items is an empty feed (no first chunk, an empty body); a
// key that does not exist is not found.
func testChunklineEmptyAndMissingTimelines(t *testing.T, be Backend) {
	ctx := context.Background()
	repo := be.Record

	feedURI := "cckv://con4owner/timeline"
	putRecord(t, ctx, repo, "empty-feed", feedURI, chunkTime(40, 0))

	manifest, err := repo.GetChunklineManifest(ctx, feedURI)
	require.NoError(t, err)
	require.Nil(t, manifest.FirstChunk)

	body, err := repo.LoadLocalBody(ctx, feedURI, 40)
	require.NoError(t, err)
	require.Empty(t, body)

	itrs, err := repo.LookupLocalItrs(ctx, []string{feedURI}, 40)
	require.NoError(t, err)
	require.Empty(t, itrs)

	_, err = repo.GetChunklineManifest(ctx, "cckv://con4owner/missing")
	require.True(t, errors.Is(err, domain.ErrNotFound), "expected not found, got %v", err)
	_, err = repo.LoadLocalBody(ctx, "cckv://con4owner/missing", 40)
	require.True(t, errors.Is(err, domain.ErrNotFound), "expected not found, got %v", err)
}

// A window (the chunk and the one before it) holding more items than a
// page comes back whole, newest first.
func testChunklineWindowLargerThanAPage(t *testing.T, be Backend) {
	ctx := context.Background()
	repo := be.Record
	parentURI := "cckv://con2owner/timeline"

	count := chunkPageSize + 9
	for i := range count {
		id := fmt.Sprintf("window-record-%02d", i)
		// spread over both chunks of the window
		putRecord(t, ctx, repo, id, parentURI+"/"+id, chunkTime(19, 300+int64(i)*20))
	}

	body, err := repo.LoadLocalBody(ctx, parentURI, 20)
	require.NoError(t, err)
	require.Len(t, body, count)
	for i, item := range body {
		require.Equal(t, fmt.Sprintf("%s/window-record-%02d", parentURI, count-1-i), item.Href)
	}
}

// A window with fewer items than a page is filled up with the newest items
// before it, so a reader of a quiet timeline gets a page from one request.
func testChunklineSparseWindow(t *testing.T, be Backend) {
	ctx := context.Background()
	repo := be.Record
	parentURI := "cckv://con5owner/timeline"

	for i := range chunkPageSize + 8 {
		id := fmt.Sprintf("old-record-%02d", i)
		putRecord(t, ctx, repo, id, parentURI+"/"+id, chunkTime(10, int64(i)))
	}
	for i := range 3 {
		id := fmt.Sprintf("recent-record-%d", i)
		putRecord(t, ctx, repo, id, parentURI+"/"+id, chunkTime(50, int64(i)))
	}
	putRecord(t, ctx, repo, "future-record", parentURI+"/future-record", chunkTime(51, 0))

	body, err := repo.LoadLocalBody(ctx, parentURI, 50)
	require.NoError(t, err)
	require.Len(t, body, chunkPageSize)
	require.Equal(t, parentURI+"/recent-record-2", body[0].Href)
	require.Equal(t, parentURI+"/recent-record-0", body[2].Href)
	require.Equal(t, fmt.Sprintf("%s/old-record-%02d", parentURI, chunkPageSize+7), body[3].Href, "then the newest of the older items")
	for i := 1; i < len(body); i++ {
		require.False(t, body[i].Timestamp.After(body[i-1].Timestamp), "newest first")
	}
}
