package repotest

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// RunChunklineSuite runs the chunkline.Repository contract tests. open must
// return a fresh, isolated backend per call; it is called once per top-level
// test.
func RunChunklineSuite(t *testing.T, open func(t *testing.T) Backend) {
	t.Run("UsesRecordCreatedAt", func(t *testing.T) { testChunklineUsesRecordCreatedAt(t, open(t)) })
	t.Run("ChunkBoundaryIsHalfOpen", func(t *testing.T) { testChunklineChunkBoundaryIsHalfOpen(t, open(t)) })
	t.Run("ManifestEmptyFeedHasNullFirstChunk", func(t *testing.T) { testChunklineManifestEmptyFeedHasNullFirstChunk(t, open(t)) })
}

func testChunklineUsesRecordCreatedAt(t *testing.T, be Backend) {
	ctx := context.Background()
	recordRepo := be.Record
	chunklineRepo := be.Chunkline

	parentURI := "cckv://con1owner/timeline"
	createdAt10 := time.Unix(10*600+10, 0).UTC()
	createdAt11 := time.Unix(11*600+20, 0).UTC()
	createdAt12 := time.Unix(12*600+30, 0).UTC()

	CreateChunklineRecord(t, ctx, recordRepo, "record-10", parentURI+"/record-10", createdAt10)
	CreateChunklineRecord(t, ctx, recordRepo, "record-11", parentURI+"/record-11", createdAt11)
	CreateChunklineRecord(t, ctx, recordRepo, "record-12", parentURI+"/record-12", createdAt12)

	parent := mustRecordKey(t, ctx, be.Inspect, parentURI)
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

// Chunks are half-open intervals [chunkID*600, (chunkID+1)*600) — a record
// whose createdAt is exactly the next chunk's start time belongs to the next
// chunk, and one exactly at the chunk's start time belongs to it.
func testChunklineChunkBoundaryIsHalfOpen(t *testing.T, be Backend) {
	ctx := context.Background()
	recordRepo := be.Record
	chunklineRepo := be.Chunkline

	parentURI := "cckv://con3owner/timeline"
	chunkStart := time.Unix(30*600, 0).UTC()
	inChunk := time.Unix(30*600+10, 0).UTC()
	nextChunkStart := time.Unix(31*600, 0).UTC()

	CreateChunklineRecord(t, ctx, recordRepo, "boundary-start", parentURI+"/boundary-start", chunkStart)
	CreateChunklineRecord(t, ctx, recordRepo, "boundary-mid", parentURI+"/boundary-mid", inChunk)
	CreateChunklineRecord(t, ctx, recordRepo, "boundary-next", parentURI+"/boundary-next", nextChunkStart)

	itrs, err := chunklineRepo.LookupLocalItrs(ctx, []string{parentURI}, 30)
	require.NoError(t, err)
	require.EqualValues(t, 30, itrs[parentURI])

	body, err := chunklineRepo.LoadLocalBody(ctx, parentURI, 30)
	require.NoError(t, err)
	require.Len(t, body, 2)
	require.Equal(t, parentURI+"/boundary-mid", body[0].Href)
	require.Equal(t, parentURI+"/boundary-start", body[1].Href)
}

func testChunklineManifestEmptyFeedHasNullFirstChunk(t *testing.T, be Backend) {
	ctx := context.Background()
	recordRepo := be.Record
	chunklineRepo := be.Chunkline

	feedURI := "cckv://con4owner/timeline"
	CreateChunklineRecord(t, ctx, recordRepo, "empty-feed", feedURI, time.Unix(40*600, 0).UTC())

	manifest, err := chunklineRepo.GetChunklineManifest(ctx, feedURI)
	require.NoError(t, err)
	require.Nil(t, manifest.FirstChunk)
}
