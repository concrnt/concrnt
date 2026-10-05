package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bradfitz/gomemcache/memcache"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/chunkline"
	"github.com/concrnt/concrnt/client"
	"github.com/concrnt/concrnt/internal/testutil"
	"github.com/concrnt/concrnt/schemas"
)

const testTimeline = "cckv://alice/home"

func newTestResolver(t *testing.T) (*resolver, chunkline.Manifest) {
	t.Helper()
	mc, cleanup := testutil.CreateMC()
	t.Cleanup(cleanup)

	manifest := chunkline.Manifest{
		Version:    "0.1",
		ChunkSize:  600,
		Descending: &chunkline.Endpoint{Iterator: "itr/{chunk}", Body: "body/{chunk}"},
	}
	seedManifest(t, mc, testTimeline, manifest)

	return &resolver{mc: mc}, manifest
}

func seedManifest(t *testing.T, mc *memcache.Client, timeline string, manifest chunkline.Manifest) {
	t.Helper()
	raw, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	err = mc.Set(&memcache.Item{Key: manifestCacheKey(timeline), Value: raw, Expiration: manifestCacheTTL})
	if err != nil {
		t.Fatal(err)
	}
}

func seedChunk(t *testing.T, mc *memcache.Client, timeline string, chunk int64, items []chunkline.BodyItem) {
	t.Helper()
	raw, err := json.Marshal(items)
	if err != nil {
		t.Fatal(err)
	}
	body := "," + string(raw[1:len(raw)-1])
	err = mc.Set(&memcache.Item{Key: bodyCacheKey(timeline, strconv.FormatInt(chunk, 10)), Value: []byte(body), Expiration: bodyCacheTTL})
	if err != nil {
		t.Fatal(err)
	}
	err = mc.Set(&memcache.Item{Key: itrCacheKey(timeline, chunk), Value: []byte(strconv.FormatInt(chunk, 10)), Expiration: itrCacheTTL})
	if err != nil {
		t.Fatal(err)
	}
}

func cachedBody(t *testing.T, mc *memcache.Client, timeline string, chunk int64) string {
	t.Helper()
	item, err := mc.Get(bodyCacheKey(timeline, strconv.FormatInt(chunk, 10)))
	if err != nil {
		t.Fatalf("failed to get cached body: %v", err)
	}
	return string(item.Value)
}

// testEvent builds a created event exactly as the system publishes it: the
// channel (and therefore Source and URI) is the record's own key under the
// timeline, never the bare timeline URI.
func testEvent(uri string, ts time.Time) concrnt.Event {
	return concrnt.Event{
		Type:      "created",
		Source:    uri,
		URI:       uri,
		Timestamp: ts,
	}
}

// Regression test for the frozen-cache bug: created events are published on
// the record's own key (`<timeline>/<id>`), not on the timeline URI. The
// cache updater must still maintain the cache of the timeline the readers
// actually query — deriving it from the record key — or every cached latest
// chunk silently goes stale until the epoch rolls over or the cache is
// flushed. (r.client is nil here: resolving anything but the seeded timeline
// manifest, e.g. treating the record key itself as a timeline, would panic.)
func TestApplyEventToCache(t *testing.T) {
	r, manifest := newTestResolver(t)

	now := time.Now()
	epoch := manifest.Time2Chunk(now)
	seedChunk(t, r.mc, testTimeline, epoch, []chunkline.BodyItem{
		{Timestamp: now.Add(-time.Minute), Href: "cckv://alice/home/existing"},
	})

	r.applyEventToCache(context.Background(), testEvent(testTimeline+"/rec-new", now))

	body := cachedBody(t, r.mc, testTimeline, epoch)
	if !strings.Contains(body, `"cckv://alice/home/rec-new"`) {
		t.Fatalf("record-keyed event not applied to its timeline's cached chunk: %s", body)
	}
	if !strings.Contains(body, "existing") {
		t.Fatalf("existing record lost from cached body: %s", body)
	}
}

// A distribution (reference) record must enter the cached chunk as its
// redirect target, matching what the origin body endpoint serves — otherwise
// the same logical record appears under two hrefs depending on whether it was
// served from cache or from origin, and readers cannot deduplicate them.
func TestApplyEventToCacheReferenceRecordUsesRedirectHref(t *testing.T) {
	r, manifest := newTestResolver(t)

	now := time.Now()
	epoch := manifest.Time2Chunk(now)
	seedChunk(t, r.mc, testTimeline, epoch, []chunkline.BodyItem{
		{Timestamp: now.Add(-time.Minute), Href: "ccfs://bob/concrnt/existing"},
	})

	recordKey := testTimeline + "/dist1"
	original := "ccfs://bob/concrnt/original-post"
	refDoc, err := json.Marshal(concrnt.Document[schemas.Reference]{
		Kind:      "record",
		Key:       recordKey,
		Value:     schemas.Reference{Href: original},
		Schema:    schemas.ReferenceURL,
		CreatedAt: now,
	})
	if err != nil {
		t.Fatal(err)
	}

	event := testEvent(recordKey, now)
	event.References = map[string]concrnt.SignedDocument{
		recordKey: {Document: string(refDoc)},
	}

	r.applyEventToCache(context.Background(), event)

	body := cachedBody(t, r.mc, testTimeline, epoch)
	if !strings.Contains(body, `"`+original+`"`) {
		t.Fatalf("expected redirect target %s in cached chunk: %s", original, body)
	}
	if strings.Contains(body, `"`+recordKey+`"`) {
		t.Fatalf("reference record cached under its own key instead of its redirect target: %s", body)
	}
}

// Duplicate application (two overlapping cache updaters during a leadership
// handover) is tolerated by design: chunkline readers deduplicate by href, so
// the cache only has to stay parsable.
func TestApplyEventToCacheDuplicatesTolerated(t *testing.T) {
	r, manifest := newTestResolver(t)

	now := time.Now()
	epoch := manifest.Time2Chunk(now)
	seedChunk(t, r.mc, testTimeline, epoch, []chunkline.BodyItem{
		{Timestamp: now.Add(-time.Minute), Href: "cckv://alice/home/existing"},
	})

	event := testEvent(testTimeline+"/rec-new", now)
	r.applyEventToCache(context.Background(), event)
	r.applyEventToCache(context.Background(), event)

	results, err := r.LoadChunkBodies(context.Background(), map[string]string{
		testTimeline: strconv.FormatInt(epoch, 10),
	})
	if err != nil {
		t.Fatal(err)
	}
	items := results[testTimeline].Items
	if len(items) != 3 {
		t.Fatalf("expected 3 items (duplicate kept, readers dedupe by href), got %d: %+v", len(items), items)
	}
}

// Concurrent distinct events must all land without corrupting the cached
// body (memcached prepend is atomic).
func TestApplyEventToCacheConcurrentDistinctEvents(t *testing.T) {
	r, manifest := newTestResolver(t)

	now := time.Now()
	epoch := manifest.Time2Chunk(now)
	seedChunk(t, r.mc, testTimeline, epoch, []chunkline.BodyItem{
		{Timestamp: now.Add(-time.Minute), Href: "cckv://alice/home/existing"},
	})

	const n = 8
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			r.applyEventToCache(context.Background(), testEvent(fmt.Sprintf("%s/rec%d", testTimeline, i), now))
		}(i)
	}
	wg.Wait()

	body := cachedBody(t, r.mc, testTimeline, epoch)
	for i := range n {
		if got := strings.Count(body, fmt.Sprintf(`"cckv://alice/home/rec%d"`, i)); got != 1 {
			t.Fatalf("expected rec%d exactly once, got %d: %s", i, got, body)
		}
	}
}

// With no cached body there is nothing to maintain: no key may be created.
func TestApplyEventToCacheNoBody(t *testing.T) {
	r, manifest := newTestResolver(t)

	now := time.Now()
	epoch := manifest.Time2Chunk(now)

	r.applyEventToCache(context.Background(), testEvent(testTimeline+"/new", now))

	_, err := r.mc.Get(bodyCacheKey(testTimeline, strconv.FormatInt(epoch, 10)))
	if err != memcache.ErrCacheMiss {
		t.Fatalf("expected no body key to be created, got err=%v", err)
	}
}

func TestPurgeLatest(t *testing.T) {
	r, manifest := newTestResolver(t)

	now := time.Now()
	current := manifest.Time2Chunk(now)
	item := []chunkline.BodyItem{{Timestamp: now, Href: "cckv://alice/home/rec"}}

	assertChunk := func(chunk int64, wantCached bool) {
		t.Helper()
		_, bodyErr := r.mc.Get(bodyCacheKey(testTimeline, strconv.FormatInt(chunk, 10)))
		_, itrErr := r.mc.Get(itrCacheKey(testTimeline, chunk))
		if wantCached && (bodyErr != nil || itrErr != nil) {
			t.Fatalf("expected chunk %d to stay cached, got body=%v itr=%v", chunk, bodyErr, itrErr)
		}
		if !wantCached && (bodyErr != memcache.ErrCacheMiss || itrErr != memcache.ErrCacheMiss) {
			t.Fatalf("expected chunk %d to be purged, got body=%v itr=%v", chunk, bodyErr, itrErr)
		}
	}

	for _, chunk := range []int64{current, current - 1, current - 2} {
		seedChunk(t, r.mc, testTimeline, chunk, item)
	}
	r.PurgeLatest([]string{testTimeline}, 1)
	assertChunk(current, false)
	assertChunk(current-1, true)
	assertChunk(current-2, true)

	for _, chunk := range []int64{current, current - 1, current - 2} {
		seedChunk(t, r.mc, testTimeline, chunk, item)
	}
	r.PurgeLatest([]string{testTimeline}, 2)
	assertChunk(current, false)
	assertChunk(current-1, false)
	assertChunk(current-2, true)
}

// A close-edge purge (depth 1) must run a second sweep after the workers'
// aggregate cache TTL: a worker holding a pre-close aggregate can re-cache
// the just-purged latest chunk in between (mc.Add succeeds precisely because
// the first purge deleted the key), and that resurrected copy is maintained
// by nothing.
func TestPurgeLatestReplaysCloseEdge(t *testing.T) {
	prev := purgeReplayDelay
	purgeReplayDelay = 50 * time.Millisecond
	t.Cleanup(func() { purgeReplayDelay = prev })

	r, manifest := newTestResolver(t)

	now := time.Now()
	current := manifest.Time2Chunk(now)
	item := []chunkline.BodyItem{{Timestamp: now, Href: "cckv://alice/home/rec"}}

	seedChunk(t, r.mc, testTimeline, current, item)
	r.PurgeLatest([]string{testTimeline}, 1)

	// a stale worker resurrects the latest chunk right after the purge
	seedChunk(t, r.mc, testTimeline, current, item)

	testutil.WaitFor(t, func() bool {
		_, bodyErr := r.mc.Get(bodyCacheKey(testTimeline, strconv.FormatInt(current, 10)))
		_, itrErr := r.mc.Get(itrCacheKey(testTimeline, current))
		return bodyErr == memcache.ErrCacheMiss && itrErr == memcache.ErrCacheMiss
	})
}

// A record created while the body of its epoch is not cached must still
// repoint a cached iterator to that epoch: an iterator cached during an
// empty epoch points at an older chunk and would hide the record until the
// epoch rolls over.
func TestApplyEventToCacheRepointsItrWithoutBody(t *testing.T) {
	r, manifest := newTestResolver(t)

	now := time.Now()
	epoch := manifest.Time2Chunk(now)

	// a read during the empty epoch cached itr[epoch] -> older chunk
	older := strconv.FormatInt(epoch-3, 10)
	if err := r.mc.Set(&memcache.Item{Key: itrCacheKey(testTimeline, epoch), Value: []byte(older), Expiration: itrCacheTTL}); err != nil {
		t.Fatal(err)
	}

	r.applyEventToCache(context.Background(), testEvent(testTimeline+"/new", now))

	item, err := r.mc.Get(itrCacheKey(testTimeline, epoch))
	if err != nil {
		t.Fatalf("expected iterator to stay cached, got %v", err)
	}
	if got := string(item.Value); got != strconv.FormatInt(epoch, 10) {
		t.Fatalf("expected iterator repointed to %d, got %s", epoch, got)
	}

	// still no body key: Replace and Prepend maintain, never create
	if _, err := r.mc.Get(bodyCacheKey(testTimeline, strconv.FormatInt(epoch, 10))); err != memcache.ErrCacheMiss {
		t.Fatalf("expected no body key to be created, got err=%v", err)
	}
}

// The cache updater blindly prepends, so a delayed event (e.g. a federated
// record whose authored timestamp predates records already cached) leaves the
// cached bytes unordered. Readers binary-search chunk items assuming
// descending timestamps, so LoadChunkBodies must restore the order when
// serving from cache.
func TestLoadChunkBodiesSortsCachedBody(t *testing.T) {
	r, manifest := newTestResolver(t)

	// anchor mid-epoch so the delayed timestamp below stays in the same chunk
	epoch := manifest.Time2Chunk(time.Now())
	base := manifest.Chunk2Time(epoch).Add(5 * time.Minute)
	seedChunk(t, r.mc, testTimeline, epoch, []chunkline.BodyItem{
		{Timestamp: base, Href: "cckv://alice/home/newest"},
		{Timestamp: base.Add(-2 * time.Minute), Href: "cckv://alice/home/oldest"},
	})

	// arrives late: authored between the two cached items, lands at the head
	r.applyEventToCache(context.Background(), testEvent(testTimeline+"/delayed", base.Add(-time.Minute)))

	results, err := r.LoadChunkBodies(context.Background(), map[string]string{
		testTimeline: strconv.FormatInt(epoch, 10),
	})
	if err != nil {
		t.Fatal(err)
	}
	items := results[testTimeline].Items

	wantOrder := []string{"cckv://alice/home/newest", testTimeline + "/delayed", "cckv://alice/home/oldest"}
	if len(items) != len(wantOrder) {
		t.Fatalf("expected %d items, got %d: %+v", len(wantOrder), len(items), items)
	}
	for i, want := range wantOrder {
		if items[i].Href != want {
			t.Fatalf("descending order broken at %d: want %s, got %s (%+v)", i, want, items[i].Href, items)
		}
	}
	for i := 1; i < len(items); i++ {
		if items[i].Timestamp.After(items[i-1].Timestamp) {
			t.Fatalf("timestamps not descending: %+v", items)
		}
	}
}

// A body served from cache must not fall through to an origin fetch: the
// whole point of the cache. (r.client is nil here, so any origin attempt
// would panic.)
func TestLoadChunkBodiesCacheHitSkipsOrigin(t *testing.T) {
	r, manifest := newTestResolver(t)

	now := time.Now()
	chunk := manifest.Time2Chunk(now) - 5 // an old, unconditionally-cacheable chunk
	items := []chunkline.BodyItem{{Timestamp: now.Add(-time.Hour), Href: "cckv://alice/home/old"}}
	seedChunk(t, r.mc, testTimeline, chunk, items)

	results, err := r.LoadChunkBodies(context.Background(), map[string]string{
		testTimeline: strconv.FormatInt(chunk, 10),
	})
	if err != nil {
		t.Fatal(err)
	}

	body, ok := results[testTimeline]
	if !ok {
		t.Fatal("expected the cached chunk in the results")
	}
	if body.ChunkID != chunk || len(body.Items) != 1 || body.Items[0].Href != "cckv://alice/home/old" {
		t.Fatalf("unexpected body from cache: %+v", body)
	}
}

// GetRemovedItems serves whatever the cache holds and never errors; timelines
// without a cached list come back empty. The refresh goroutine must not touch
// the (nil in these tests) HTTP client: the seeded manifest has no removed
// endpoint, so a buggy fetch attempt panics the test binary.
func TestGetRemovedItemsServesCache(t *testing.T) {
	r, _ := newTestResolver(t)

	other := "cckv://bob/home"
	seedManifest(t, r.mc, other, chunkline.Manifest{Version: "0.1", ChunkSize: 600})

	raw, err := json.Marshal([]string{"cckv://alice/home/gone"})
	if err != nil {
		t.Fatal(err)
	}
	err = r.mc.Set(&memcache.Item{Key: removedCacheKey(testTimeline), Value: raw, Expiration: removedCacheTTL})
	if err != nil {
		t.Fatal(err)
	}

	result, err := r.GetRemovedItems(context.Background(), []string{testTimeline, other})
	if err != nil {
		t.Fatal(err)
	}
	if len(result[testTimeline]) != 1 || result[testTimeline][0] != "cckv://alice/home/gone" {
		t.Fatalf("unexpected removed items for seeded timeline: %+v", result[testTimeline])
	}
	if len(result[other]) != 0 {
		t.Fatalf("expected no removed items for unseeded timeline, got %+v", result[other])
	}

	time.Sleep(100 * time.Millisecond) // let the refresh goroutine run (and panic if buggy)
}

// A live fresh marker suppresses the origin refresh entirely, even when the
// manifest advertises a removed endpoint (again: a fetch attempt against the
// nil client would panic).
func TestGetRemovedItemsFreshMarkerSkipsRefresh(t *testing.T) {
	r, _ := newTestResolver(t)

	seedManifest(t, r.mc, testTimeline, chunkline.Manifest{
		Version:   "0.1",
		ChunkSize: 600,
		Removed:   "removed?uri=" + testTimeline,
	})
	err := r.mc.Set(&memcache.Item{Key: removedFreshKey(testTimeline), Value: []byte("1"), Expiration: removedFreshTTL})
	if err != nil {
		t.Fatal(err)
	}

	result, err := r.GetRemovedItems(context.Background(), []string{testTimeline})
	if err != nil {
		t.Fatal(err)
	}
	if len(result[testTimeline]) != 0 {
		t.Fatalf("expected no removed items, got %+v", result[testTimeline])
	}

	time.Sleep(100 * time.Millisecond) // let the refresh goroutine run (and panic if buggy)
}

type noDemand struct{}

func (noDemand) CurrentSubscriptions() []string { return nil }

// A timeline given as an https:// manifest URL on a plain static host (no
// /.well-known/concrnt) is read end to end: manifest, then itr and body
// resolved relative to the manifest URL and fetched with plain GETs.
func TestQueryDescendingStaticManifestHost(t *testing.T) {
	const domain = "archive.test"
	mc, cleanup := testutil.CreateMC()
	t.Cleanup(cleanup)

	newest := time.Date(2023, 5, 23, 14, 31, 45, 0, time.UTC)
	older := newest.Add(-time.Hour)
	manifest := chunkline.Manifest{
		Version:    "1.0",
		ChunkSize:  14400,
		Descending: &chunkline.Endpoint{Iterator: "itr/{chunk}", Body: "body/{chunk}.json"},
	}
	chunk := manifest.Time2Chunk(newest)
	manifest.FirstChunk = &chunk
	manifest.LastChunk = &chunk

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/backrooms/manifest.json":
			json.NewEncoder(w).Encode(manifest)
		case "/backrooms/itr/" + strconv.FormatInt(chunk, 10):
			fmt.Fprint(w, chunk)
		case "/backrooms/body/" + strconv.FormatInt(chunk, 10) + ".json":
			json.NewEncoder(w).Encode([]chunkline.BodyItem{
				{Timestamp: newest, Content: "newest"},
				{Timestamp: older, Content: "older"},
			})
		default:
			http.NotFound(w, r) // no /.well-known/concrnt
		}
	}))
	t.Cleanup(server.Close)

	cl := client.New(domain)
	cl.AddHostRemapping(domain, server.URL)

	r := &resolver{mc: mc, client: cl, demand: noDemand{}}
	timeline := "https://" + domain + "/backrooms/manifest.json"

	items, err := chunkline.NewClient(r).QueryDescending(context.Background(), []string{timeline}, newest.Add(time.Minute), 16)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 2 || items[0].Content != "newest" || items[1].Content != "older" {
		t.Fatalf("unexpected items: %+v", items)
	}
	if items[0].Source != timeline {
		t.Fatalf("source %q, want the manifest URL", items[0].Source)
	}
	if !cl.IsOnline(domain) {
		t.Fatal("static host must stay online")
	}
}
