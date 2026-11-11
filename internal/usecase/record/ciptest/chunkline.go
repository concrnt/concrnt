package ciptest

import (
	"time"

	"github.com/stretchr/testify/require"

	"github.com/concrnt/concrnt/chunkline"
	"github.com/concrnt/concrnt/internal/domain"
)

func (w *world) manifest(timeline string) *chunkline.Manifest {
	w.t.Helper()
	manifest, err := w.timelines.GetChunklineManifest(w.ctx, timeline)
	require.NoError(w.t, err)
	require.Positive(w.t, manifest.ChunkSize)
	return manifest
}

// chunkOf is the chunk of timeline that at falls into.
func (w *world) chunkOf(timeline string, at time.Time) int64 {
	w.t.Helper()
	return w.manifest(timeline).Time2Chunk(at)
}

func (w *world) body(timeline string, chunk int64) []chunkline.BodyItem {
	w.t.Helper()
	items, err := w.timelines.LoadLocalBody(w.ctx, timeline, chunk)
	require.NoError(w.t, err)
	return items
}

func (w *world) bodyHrefs(timeline string, chunk int64) []string {
	w.t.Helper()
	hrefs := []string{}
	for _, item := range w.body(timeline, chunk) {
		hrefs = append(hrefs, item.Href)
	}
	return hrefs
}

// feedFixture is a timeline whose items sit on chunk boundaries. chunk is
// a chunk well inside the backdate window; the fields name the items by
// where they fall relative to it.
type feedFixture struct {
	owner    user
	timeline string
	manifest *chunkline.Manifest
	chunk    int64

	// old lies three chunks back, outside the two-chunk window of chunk.
	old string
	// previousFirst and previousLast are the first and the last second of
	// the chunk before.
	previousFirst, previousLast string
	// first and last are the first and the last second of chunk; shared is
	// a record elsewhere distributed into the timeline, in between.
	first, shared, last string
	// next is the first second of the chunk after.
	next string
}

func (w *world) newFeedFixture() feedFixture {
	w.t.Helper()
	alice := w.newUser()
	timeline := alice.key("home")
	w.execute(alice, recordDoc(alice, timeline, w.at(0)))

	f := feedFixture{owner: alice, timeline: timeline, manifest: w.manifest(timeline)}
	f.chunk = f.manifest.Time2Chunk(w.origin)
	start := func(chunk int64) time.Time { return f.manifest.Chunk2Time(chunk) }
	item := func(name string, at time.Time) string {
		key := timeline + "/" + name
		w.execute(alice, recordDoc(alice, key, at))
		return key
	}

	// written out of order: a chunkline is ordered by createdAt, not by
	// arrival
	f.first = item("first", start(f.chunk))
	f.old = item("old", start(f.chunk-3).Add(5*time.Second))
	f.next = item("next", start(f.chunk+1))
	f.previousLast = item("previous-last", start(f.chunk).Add(-time.Second))
	f.last = item("last", start(f.chunk+1).Add(-time.Second))
	f.previousFirst = item("previous-first", start(f.chunk-1))

	f.shared = alice.key("posts/shared")
	w.execute(alice, recordDoc(alice, f.shared, start(f.chunk).Add(10*time.Second), distributedTo(timeline)))
	w.drain()
	return f
}

// CIP-11 manifest: the first chunk is the chunk of the earliest item.
func testChunklineManifest(w *world) {
	alice := w.newUser()
	empty := alice.key("empty")
	w.execute(alice, recordDoc(alice, empty, w.at(0)))
	require.Nil(w.t, w.manifest(empty).FirstChunk, "a timeline without items has no first chunk")

	_, err := w.timelines.GetChunklineManifest(w.ctx, alice.key("nowhere"))
	require.ErrorIs(w.t, err, domain.ErrNotFound)

	f := w.newFeedFixture()
	manifest := w.manifest(f.timeline)
	require.NotNil(w.t, manifest.FirstChunk)
	require.Equal(w.t, f.chunk-3, *manifest.FirstChunk)
}

// CIP-11 iterator: for a chunk, the chunk of the timeline's latest item at
// or before it; a timeline with nothing that early has no entry.
func testChunklineItr(w *world) {
	f := w.newFeedFixture()
	empty := f.owner.key("empty")
	w.execute(f.owner, recordDoc(f.owner, empty, w.at(0)))
	timelines := []string{f.timeline, empty, f.owner.key("nowhere")}

	lookups := []struct {
		name  string
		chunk int64
		want  map[string]int64
	}{
		{"after the last item", f.chunk + 5, map[string]int64{f.timeline: f.chunk + 1}},
		{"the chunk of the last item", f.chunk + 1, map[string]int64{f.timeline: f.chunk + 1}},
		{"a chunk with items", f.chunk, map[string]int64{f.timeline: f.chunk}},
		{"a gap falls back to the chunk before it", f.chunk - 2, map[string]int64{f.timeline: f.chunk - 3}},
		{"before the first item", f.chunk - 4, map[string]int64{}},
	}
	for _, tc := range lookups {
		got, err := w.timelines.LookupLocalItrs(w.ctx, timelines, tc.chunk)
		require.NoError(w.t, err)
		require.Equal(w.t, tc.want, got, tc.name)
	}
}

// CIP-11 body: the items of a chunk and of the one before it, newest
// first. A chunk is the half-open interval [start, start+size): an item on
// a boundary belongs to the chunk that starts there. A reference record is
// listed as what it points at.
func testChunklineBody(w *world) {
	f := w.newFeedFixture()
	window := []string{f.last, f.shared, f.first, f.previousLast, f.previousFirst}

	requireBody := func(chunk int64, window []string, older []string) {
		w.t.Helper()
		items := w.body(f.timeline, chunk)
		end := f.manifest.Chunk2Time(chunk + 1)
		for i, item := range items {
			require.True(w.t, item.Timestamp.Before(end), "chunk %d: %s is past the end of the chunk", chunk, item.Href)
			if i > 0 {
				require.False(w.t, item.Timestamp.After(items[i-1].Timestamp), "chunk %d: the items are newest first", chunk)
			}
		}
		hrefs := w.bodyHrefs(f.timeline, chunk)
		require.GreaterOrEqual(w.t, len(hrefs), len(window), "chunk %d: %v", chunk, hrefs)
		require.Equal(w.t, window, hrefs[:len(window)], "chunk %d: the chunk and the one before it", chunk)
		// a sparse window may be filled up with older items, never with
		// anything else
		require.Subset(w.t, older, hrefs[len(window):], "chunk %d", chunk)
	}

	requireBody(f.chunk, window, []string{f.old})
	requireBody(f.chunk+1, []string{f.next, f.last, f.shared, f.first}, []string{f.previousLast, f.previousFirst, f.old})
	requireBody(f.chunk-1, []string{f.previousLast, f.previousFirst}, []string{f.old})
	requireBody(f.chunk-3, []string{f.old}, nil)
	require.Empty(w.t, w.body(f.timeline, f.chunk-5), "before the first item")

	for _, item := range w.body(f.timeline, f.chunk) {
		switch item.Href {
		case f.shared:
			require.True(w.t, item.Timestamp.Equal(f.manifest.Chunk2Time(f.chunk).Add(10*time.Second)), "a distributed record is listed at its own createdAt")
		case f.first:
			require.True(w.t, item.Timestamp.Equal(f.manifest.Chunk2Time(f.chunk)))
		}
	}

	_, err := w.timelines.LoadLocalBody(w.ctx, f.owner.key("nowhere"), f.chunk)
	require.ErrorIs(w.t, err, domain.ErrNotFound)
}
