package repotest

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// RunChunklineSuite runs the chunkline contract tests of record.Repository. open must
// return a fresh, isolated backend per call; it is called once per top-level
// test.
func RunChunklineSuite(t *testing.T, open func(t *testing.T) Backend) {
	t.Run("UsesRecordCreatedAt", func(t *testing.T) { testChunklineUsesRecordCreatedAt(t, open(t)) })
	t.Run("ChunkBoundaryIsHalfOpen", func(t *testing.T) { testChunklineChunkBoundaryIsHalfOpen(t, open(t)) })
	t.Run("ManifestEmptyFeedHasNullFirstChunk", func(t *testing.T) { testChunklineManifestEmptyFeedHasNullFirstChunk(t, open(t)) })
	t.Run("LoadLocalBodySurvivesRecordCreatedAtDivergence", func(t *testing.T) {
		testChunklineLoadLocalBodySurvivesRecordCreatedAtDivergence(t, open(t))
	})
}

// chunkPageSize is the number of items the backends' first LoadLocalBody
// query is limited to (their defaultChunkSize).
const chunkPageSize = 32

// LoadLocalBody must judge window coverage and emit timestamps by the
// key's recordCreatedAt — what its queries filter and order by — not the
// record's own createdAt, which can diverge (a backdated write).
func testChunklineLoadLocalBodySurvivesRecordCreatedAtDivergence(t *testing.T, be Backend) {
	ctx := context.Background()
	parentURI := "cckv://con2owner/timeline"

	// more records than one page inside chunk 20's window, so the limited
	// first query cannot cover the window on its own
	count := chunkPageSize + 1
	for i := range count {
		createdAt := time.Unix(20*600+int64(i)+1, 0).UTC()
		id := fmt.Sprintf("div-record-%02d", i)
		createChunklineRecord(t, ctx, be.Record, id, fmt.Sprintf("%s/%s", parentURI, id), createdAt)
	}

	// backdate the record of the limited query's last member (the oldest of
	// the top chunkPageSize records) to before the window start; the
	// window-coverage guard must ignore the diverged column or the oldest
	// record in the window gets dropped
	require.NoError(t, be.Inspect.SetRecordCreatedAt(ctx, "div-record-01", time.Unix(18*600, 0).UTC()))

	body, err := be.Record.LoadLocalBody(ctx, parentURI, 20)
	require.NoError(t, err)
	require.Len(t, body, count)
	for _, item := range body {
		require.True(t, item.Timestamp.After(time.Unix(20*600, 0)), "timestamp %v must be the key's recordCreatedAt, not the backdated createdAt", item.Timestamp)
	}
}

func testChunklineUsesRecordCreatedAt(t *testing.T, be Backend) {
	ctx := context.Background()
	recordRepo := be.Record

	parentURI := "cckv://con1owner/timeline"
	createdAt10 := time.Unix(10*600+10, 0).UTC()
	createdAt11 := time.Unix(11*600+20, 0).UTC()
	createdAt12 := time.Unix(12*600+30, 0).UTC()

	createChunklineRecord(t, ctx, recordRepo, "record-10", parentURI+"/record-10", createdAt10)
	createChunklineRecord(t, ctx, recordRepo, "record-11", parentURI+"/record-11", createdAt11)
	createChunklineRecord(t, ctx, recordRepo, "record-12", parentURI+"/record-12", createdAt12)

	parent := mustRecordKey(t, ctx, be.Inspect, parentURI)
	require.Nil(t, parent.RecordCreatedAt)

	manifest, err := be.Record.GetChunklineManifest(ctx, parentURI)
	require.NoError(t, err)
	require.NotNil(t, manifest.FirstChunk)
	require.EqualValues(t, 10, *manifest.FirstChunk)

	itrs, err := be.Record.LookupLocalItrs(ctx, []string{parentURI}, 11)
	require.NoError(t, err)
	require.EqualValues(t, 11, itrs[parentURI])

	body, err := be.Record.LoadLocalBody(ctx, parentURI, 11)
	require.NoError(t, err)
	require.Len(t, body, 2)
	require.Equal(t, parentURI+"/record-11", body[0].Href)
	require.True(t, body[0].Timestamp.Equal(createdAt11))
	require.Equal(t, parentURI+"/record-10", body[1].Href)
	require.True(t, body[1].Timestamp.Equal(createdAt10))
}

// Chunks are half-open intervals [chunkID*600, (chunkID+1)*600) — a record
// whose createdAt is exactly the next chunk's start time belongs to the next
// chunk, and one exactly at the chunk's start time belongs to it.
func testChunklineChunkBoundaryIsHalfOpen(t *testing.T, be Backend) {
	ctx := context.Background()
	recordRepo := be.Record

	parentURI := "cckv://con3owner/timeline"
	chunkStart := time.Unix(30*600, 0).UTC()
	inChunk := time.Unix(30*600+10, 0).UTC()
	nextChunkStart := time.Unix(31*600, 0).UTC()

	createChunklineRecord(t, ctx, recordRepo, "boundary-start", parentURI+"/boundary-start", chunkStart)
	createChunklineRecord(t, ctx, recordRepo, "boundary-mid", parentURI+"/boundary-mid", inChunk)
	createChunklineRecord(t, ctx, recordRepo, "boundary-next", parentURI+"/boundary-next", nextChunkStart)

	itrs, err := be.Record.LookupLocalItrs(ctx, []string{parentURI}, 30)
	require.NoError(t, err)
	require.EqualValues(t, 30, itrs[parentURI])

	body, err := be.Record.LoadLocalBody(ctx, parentURI, 30)
	require.NoError(t, err)
	require.Len(t, body, 2)
	require.Equal(t, parentURI+"/boundary-mid", body[0].Href)
	require.Equal(t, parentURI+"/boundary-start", body[1].Href)
}

func testChunklineManifestEmptyFeedHasNullFirstChunk(t *testing.T, be Backend) {
	ctx := context.Background()
	recordRepo := be.Record

	feedURI := "cckv://con4owner/timeline"
	createChunklineRecord(t, ctx, recordRepo, "empty-feed", feedURI, time.Unix(40*600, 0).UTC())

	manifest, err := be.Record.GetChunklineManifest(ctx, feedURI)
	require.NoError(t, err)
	require.Nil(t, manifest.FirstChunk)
}
