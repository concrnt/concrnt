package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/patrickmn/go-cache"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/chunkline"
	"github.com/concrnt/concrnt/client"
	ucchunkline "github.com/concrnt/concrnt/internal/usecase/chunkline"
	"github.com/concrnt/concrnt/internal/worker"
	"github.com/concrnt/concrnt/schemas"
)

const (
	manifestCacheTTL = 60 * 60 * 24 * 7 // 7 days
	itrCacheTTL      = 60 * 60 * 24 * 2 // 2 days
	bodyCacheTTL     = 60 * 60 * 24 * 2 // 2 days
	removedCacheTTL  = 60 * 60 * 24 * 2 // 2 days, matches the origin's advertisement window
	removedFreshTTL  = 60               // throttles origin refresh to ~1/min/timeline

	// manifestLocalTTL is the in-process copy of a manifest in front of
	// memcached; a manifest only ever gains a FirstChunk, so a stale copy
	// costs nothing but an extra "before first chunk" miss
	manifestLocalTTL = 5 * time.Second
)

// purgeReplayDelay must exceed the workers' aggregate-subscription cache TTL
// (worker.WorkerSubscriber, 3s): a worker holding a pre-close aggregate can
// re-cache a just-purged latest chunk for up to that long, so close-edge
// purges run a second time after this delay to sweep such resurrections.
// Package variable so tests can shorten it.
var purgeReplayDelay = 8 * time.Second

// SubscriptionDemand reports the realtime subscription demand of this replica,
// used to decide whether a latest chunk will be kept fresh by the cache
// updater and is therefore safe to cache.
type SubscriptionDemand interface {
	CurrentSubscriptions() []string
}

type ChunklineGateway struct {
	resolver *chunkline.Client
	r        *resolver
}

func NewChunklineGateway(
	cl *client.Client,
	mc *memcache.Client,
	demand SubscriptionDemand,
	pubsub worker.PubSub,
) *ChunklineGateway {
	r := newResolver(cl, mc, demand, pubsub)
	return &ChunklineGateway{
		resolver: chunkline.NewClient(r),
		r:        r,
	}
}

func newResolver(cl *client.Client, mc *memcache.Client, demand SubscriptionDemand, pubsub worker.PubSub) *resolver {
	return &resolver{
		client:    cl,
		mc:        mc,
		demand:    demand,
		pubsub:    pubsub,
		manifests: cache.New(manifestLocalTTL, time.Minute),
	}
}

// SetLocal makes the timelines of fqdn (this server) read from local, the
// chunkline usecase behind this server's own chunkline API, instead of
// through that API over HTTP.
func (g *ChunklineGateway) SetLocal(fqdn string, local *ucchunkline.Usecase) {
	g.r.fqdn = fqdn
	g.r.local = local
}

// StartWorker registers the resolver's cache keep-alive demand on the
// subscriber, hooks cache purging into the subscription open/close edges, and
// starts the cache updater. Run this only on the replica that runs the
// singleton workers (the leader) — the cache updater maintains the shared
// memcached.
func (g *ChunklineGateway) StartWorker(ctx context.Context, subscriber *worker.LeaderSubscriber) {
	g.r.open = subscriber
	// keep-alive, not a session client: the resolver's demand exists to keep
	// established caches maintained, and must not feed the served aggregate
	// that gates cache writes (that feedback would keep every once-read
	// timeline subscribed forever)
	subscriber.RegisterKeepAliveClient(g.r)
	subscriber.SetCachePurger(g.r)
	go g.r.cacheUpdater(ctx)
}

func (g *ChunklineGateway) QueryDescending(ctx context.Context, uris []string, until time.Time, limit int) ([]chunkline.BodyItemWithSource, error) {
	return g.resolver.QueryDescending(ctx, uris, until, limit)
}

// OpenSubscriptions reports the prefixes whose peer subscription is
// currently established (as opposed to merely demanded).
type OpenSubscriptions interface {
	OpenPrefixes() []string
}

// resolver implements chunkline resolver callbacks.
type resolver struct {
	client *client.Client
	mc     *memcache.Client
	demand SubscriptionDemand
	pubsub worker.PubSub
	open   OpenSubscriptions // set by StartWorker on the leader; nil elsewhere
	fqdn   string
	local  *ucchunkline.Usecase // nil: every timeline goes over HTTP

	manifests *cache.Cache // in-process copies, in front of mc
}

func (r *resolver) localManifest(timeline string) (chunkline.Manifest, bool) {
	x, found := r.manifests.Get(manifestCacheKey(timeline))
	if !found {
		return chunkline.Manifest{}, false
	}
	return x.(chunkline.Manifest), true
}

func (r *resolver) keepManifest(timeline string, manifest chunkline.Manifest) {
	r.manifests.Set(manifestCacheKey(timeline), manifest, cache.DefaultExpiration)
}

func (r *resolver) fetchManifest(ctx context.Context, timeline string) (chunkline.Manifest, error) {
	if r.local != nil && strings.HasPrefix(timeline, "cckv://") {
		if host, err := r.client.ResolveResourceHost(ctx, timeline); err == nil && host == r.fqdn {
			originFetches.WithLabelValues("manifest", "local").Inc()
			manifest, err := r.local.GetChunklineManifest(ctx, timeline)
			if err != nil {
				return chunkline.Manifest{}, err
			}
			return *manifest, nil
		}
	}
	originFetches.WithLabelValues("manifest", "http").Inc()
	var manifest chunkline.Manifest
	err := r.client.GetResource(ctx, timeline, "application/chunkline+json", nil, &manifest)
	return manifest, err
}

// localCall recognises an endpoint of this server's own chunkline API (as
// GetChunklineManifest renders them), so it is answered without HTTP.
func (r *resolver) localCall(endpoint *url.URL, kind string) (chunkID int64, uri string, ok bool) {
	if r.local == nil || endpoint.Host != r.fqdn {
		return 0, "", false
	}
	uri = endpoint.Query().Get("uri")
	rest, found := strings.CutPrefix(endpoint.Path, "/api/v2/chunkline/"+kind)
	if !found || uri == "" {
		return 0, "", false
	}
	if kind == "removed" {
		return 0, uri, rest == ""
	}
	chunkID, err := strconv.ParseInt(strings.TrimPrefix(rest, "/"), 10, 64)
	return chunkID, uri, err == nil && strings.HasPrefix(rest, "/")
}

// fetchFromOrigins issues requestsByDomain (domain -> timeline -> URL) in
// one batch and returns the body of every 200 response by timeline; every
// other outcome goes to report and leaves the timeline out.
func (r *resolver) fetchFromOrigins(ctx context.Context, kind string, requestsByDomain map[string]map[string]string, report func(error)) map[string][]byte {
	if len(requestsByDomain) == 0 {
		return nil
	}
	responses, err := r.client.BatchGet(ctx, requestsByDomain)
	if err != nil {
		report(fmt.Errorf("batch request for %s failed: %w", kind, err))
	}
	bodies := make(map[string][]byte)
	for _, requests := range requestsByDomain {
		originFetches.WithLabelValues(kind, "http").Add(float64(len(requests)))
		for tl := range requests {
			resp, ok := responses[tl]
			if !ok {
				report(fmt.Errorf("missing response for timeline %s", tl))
				continue
			}
			if resp.StatusCode != http.StatusOK {
				report(fmt.Errorf("non-200 response for timeline %s: %d", tl, resp.StatusCode))
				continue
			}
			body, err := io.ReadAll(resp.Body)
			if err != nil {
				report(fmt.Errorf("failed to read response body for timeline %s: %w", tl, err))
				continue
			}
			bodies[tl] = body
		}
	}
	return bodies
}

func manifestCacheKey(timeline string) string {
	return "chunkline_manifest:" + timeline
}

func itrCacheKey(timeline string, chunkID int64) string {
	return fmt.Sprintf("chunkline_itr:%s:%d", timeline, chunkID)
}

func bodyCacheKey(timeline string, chunkID string) string {
	return fmt.Sprintf("chunkline_body:%s:%s", timeline, chunkID)
}

func removedCacheKey(timeline string) string {
	return "chunkline_removed:" + timeline
}

func removedFreshKey(timeline string) string {
	return "chunkline_removed_fresh:" + timeline
}

func (r *resolver) resolveTimeline(ctx context.Context, timeline string) (chunkline.Manifest, error) {
	ctx, span := tracer.Start(ctx, "ChunklineResolver.resolveTimeline")
	defer span.End()

	if manifest, found := r.localManifest(timeline); found {
		return manifest, nil
	}

	cacheKey := manifestCacheKey(timeline)
	item, err := r.mc.Get(cacheKey)
	if err == nil {
		var manifest chunkline.Manifest
		err := json.Unmarshal(item.Value, &manifest)
		if err != nil {
			span.RecordError(fmt.Errorf("failed to unmarshal cached manifest for %s: %w", timeline, err))
		} else if manifest.ChunkSize > 0 {
			// an invalid cached manifest is a miss, so it never reaches a
			// Time2Chunk division
			r.keepManifest(timeline, manifest)
			return manifest, nil
		}
	} else if !errors.Is(err, memcache.ErrCacheMiss) {
		span.RecordError(fmt.Errorf("failed to get manifest from cache for %s: %w", timeline, err))
	}

	manifest, err := r.fetchManifest(ctx, timeline)
	if err != nil {
		span.RecordError(fmt.Errorf("failed to fetch chunkline manifest for %s: %w", timeline, err))
		return chunkline.Manifest{}, err
	}

	// GetResource decodes whatever the resolve endpoint returns: a resource
	// that is not a chunkline timeline (e.g. a space root resolving to the
	// entity document) or a broken origin decodes into a zero manifest.
	// ChunkSize is the Time2Chunk divisor, so letting one through (or caching
	// it) would panic the process on the next chunk computation.
	if manifest.ChunkSize <= 0 {
		err := fmt.Errorf("resource %s is not a chunkline timeline", timeline)
		span.RecordError(err)
		return chunkline.Manifest{}, err
	}
	r.keepManifest(timeline, manifest)

	bytes, err := json.Marshal(manifest)
	if err != nil {
		span.RecordError(fmt.Errorf("failed to marshal manifest for caching for %s: %w", timeline, err))
	} else {
		cacheItem := &memcache.Item{
			Key:        cacheKey,
			Value:      bytes,
			Expiration: manifestCacheTTL,
		}
		err = r.mc.Set(cacheItem)
		if err != nil {
			span.RecordError(fmt.Errorf("failed to set manifest in cache for %s: %w", timeline, err))
		}
	}

	return manifest, nil
}

func (r *resolver) ResolveTimelines(ctx context.Context, timelines []string) (map[string]chunkline.Manifest, error) {
	ctx, span := tracer.Start(ctx, "ChunklineResolver.ResolveTimelines")
	defer span.End()

	result := make(map[string]chunkline.Manifest)
	keys := make([]string, 0, len(timelines))
	for _, tl := range timelines {
		if manifest, found := r.localManifest(tl); found {
			result[tl] = manifest
			continue
		}
		keys = append(keys, manifestCacheKey(tl))
	}

	var cacheItems map[string]*memcache.Item
	if len(keys) > 0 {
		var err error
		cacheItems, err = r.mc.GetMulti(keys)
		if err != nil && !errors.Is(err, memcache.ErrCacheMiss) {
			span.RecordError(fmt.Errorf("failed to get manifests from cache: %w", err))
		}
	}

	remaining := []string{}

	for _, tl := range timelines {
		if _, done := result[tl]; done {
			continue
		}
		if item, found := cacheItems[manifestCacheKey(tl)]; found {
			var manifest chunkline.Manifest
			err := json.Unmarshal(item.Value, &manifest)
			if err != nil {
				span.RecordError(fmt.Errorf("failed to unmarshal cached manifest for %s: %w", tl, err))
				remaining = append(remaining, tl)
				continue
			}
			// invalid manifest cached before validation existed: treat as a
			// miss (see resolveTimeline)
			if manifest.ChunkSize <= 0 {
				remaining = append(remaining, tl)
				continue
			}
			result[tl] = manifest
			r.keepManifest(tl, manifest)
		} else {
			remaining = append(remaining, tl)
		}
	}

	for _, tl := range remaining {
		manifest, err := r.fetchManifest(ctx, tl)
		if err != nil {
			span.RecordError(fmt.Errorf("failed to fetch chunkline manifest for %s: %w", tl, err))
			continue
		}
		// a non-timeline resource or broken origin decodes into a zero
		// manifest whose ChunkSize would divide by zero in Time2Chunk;
		// never return or cache one (see resolveTimeline)
		if manifest.ChunkSize <= 0 {
			span.RecordError(fmt.Errorf("resource %s is not a chunkline timeline", tl))
			continue
		}
		result[tl] = manifest
		r.keepManifest(tl, manifest)

		bytes, err := json.Marshal(manifest)
		if err != nil {
			span.RecordError(fmt.Errorf("failed to marshal manifest for caching for %s: %w", tl, err))
			continue
		}

		cacheItem := &memcache.Item{
			Key:        manifestCacheKey(tl),
			Value:      bytes,
			Expiration: manifestCacheTTL,
		}
		err = r.mc.Set(cacheItem)
		if err != nil {
			span.RecordError(fmt.Errorf("failed to set manifest in cache for %s: %w", tl, err))
		}
	}

	return result, nil

}

// GetRemovedItems serves QueryDescending's per-timeline lists of recently
// removed item IDs, loosely: the synchronous path only reads the shared
// memcached copy (empty on a miss) so the timeline query is never blocked on
// an origin, and a background goroutine refreshes stale entries from each
// origin's manifest-advertised removed endpoint — deletions apply on the next
// query. Deleted items lingering in results until then is accepted behavior.
func (r *resolver) GetRemovedItems(ctx context.Context, timelines []string) (map[string][]string, error) {
	ctx, span := tracer.Start(ctx, "ChunklineResolver.GetRemovedItems")
	defer span.End()

	keys := make([]string, len(timelines))
	for i, tl := range timelines {
		keys[i] = removedCacheKey(tl)
	}

	cachedItems, err := r.mc.GetMulti(keys)
	if err != nil && !errors.Is(err, memcache.ErrCacheMiss) {
		span.RecordError(fmt.Errorf("failed to get removed items from cache: %w", err))
	}

	result := make(map[string][]string)
	for _, tl := range timelines {
		result[tl] = []string{}
		item, found := cachedItems[removedCacheKey(tl)]
		if !found {
			continue
		}
		var ids []string
		err := json.Unmarshal(item.Value, &ids)
		if err != nil {
			span.RecordError(fmt.Errorf("failed to unmarshal cached removed items for %s: %w", tl, err))
			continue
		}
		result[tl] = ids
	}

	// refresh the cache in the background; the request context may be gone
	// before the origin answers, so the goroutine runs on its own context
	go func(timelines []string) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		freshKeys := make([]string, len(timelines))
		for i, tl := range timelines {
			freshKeys[i] = removedFreshKey(tl)
		}
		freshItems, err := r.mc.GetMulti(freshKeys)
		if err != nil && !errors.Is(err, memcache.ErrCacheMiss) {
			slog.Error("chunkline removed: failed to get fresh markers", slog.String("error", err.Error()))
		}

		stale := make([]string, 0, len(timelines))
		for _, tl := range timelines {
			if _, found := freshItems[removedFreshKey(tl)]; found {
				continue
			}
			stale = append(stale, tl)
		}
		if len(stale) == 0 {
			return
		}

		manifests, err := r.ResolveTimelines(ctx, stale)
		if err != nil {
			slog.Error("chunkline removed: failed to resolve timelines", slog.String("error", err.Error()))
			return
		}

		requestsByDomain := make(map[string]map[string]string) // domain -> timeline -> query url
		fetched := make(map[string][]string)

		for _, tl := range stale {
			manifest, ok := manifests[tl]
			if !ok {
				continue
			}

			if manifest.Removed == "" {
				continue // origin doesn't track removals
			}

			relPath, err := concrnt.RenderURITemplate(manifest.Removed, map[string]string{})
			if err != nil {
				slog.Error("chunkline removed: failed to render removed URI", slog.String("timeline", tl), slog.String("error", err.Error()))
				continue
			}

			rel, err := url.Parse(relPath)
			if err != nil {
				slog.Error("chunkline removed: invalid removed URI template", slog.String("timeline", tl), slog.String("error", err.Error()))
				continue
			}

			base, err := url.Parse(tl)
			if err != nil {
				slog.Error("chunkline removed: invalid timeline URI", slog.String("timeline", tl), slog.String("error", err.Error()))
				continue
			}

			endpoint := base.ResolveReference(rel)
			if endpoint.Scheme != "http" && endpoint.Scheme != "https" {
				host, err := r.client.ResolveResourceHost(ctx, tl)
				if err != nil {
					slog.Error("chunkline removed: failed to resolve host", slog.String("timeline", tl), slog.String("error", err.Error()))
					continue
				}
				endpoint.Scheme = "https"
				endpoint.Host = host
			}

			if _, uri, ok := r.localCall(endpoint, "removed"); ok {
				originFetches.WithLabelValues("removed", "local").Inc()
				ids, err := r.local.GetLocalRemovedItems(ctx, uri)
				if err != nil {
					slog.Error("chunkline removed: local lookup failed", slog.String("timeline", tl), slog.String("error", err.Error()))
					continue
				}
				fetched[tl] = ids
				continue
			}

			domain := endpoint.Host
			if _, exists := requestsByDomain[domain]; !exists {
				requestsByDomain[domain] = make(map[string]string)
			}
			requestsByDomain[domain][tl] = endpoint.String()
		}

		bodies := r.fetchFromOrigins(ctx, "removed", requestsByDomain, func(err error) {
			slog.Error("chunkline removed: origin fetch failed", slog.String("error", err.Error()))
		})
		for tl, body := range bodies {
			var ids []string
			if err := json.Unmarshal(body, &ids); err != nil {
				slog.Error("chunkline removed: failed to unmarshal response", slog.String("timeline", tl), slog.String("error", err.Error()))
				continue
			}
			fetched[tl] = ids
		}

		for tl, ids := range fetched {
			if ids == nil {
				ids = []string{}
			}

			value, err := json.Marshal(ids)
			if err != nil {
				slog.Error("chunkline removed: failed to marshal for caching", slog.String("timeline", tl), slog.String("error", err.Error()))
				continue
			}

			err = r.mc.Set(&memcache.Item{
				Key:        removedCacheKey(tl),
				Value:      value,
				Expiration: removedCacheTTL,
			})
			if err != nil {
				slog.Error("chunkline removed: failed to set cache", slog.String("timeline", tl), slog.String("error", err.Error()))
				continue
			}

			// mark fresh only on success so failed origins are retried on the
			// next query
			err = r.mc.Set(&memcache.Item{
				Key:        removedFreshKey(tl),
				Value:      []byte("1"),
				Expiration: removedFreshTTL,
			})
			if err != nil {
				slog.Error("chunkline removed: failed to set fresh marker", slog.String("timeline", tl), slog.String("error", err.Error()))
			}
		}
	}(slices.Clone(timelines))

	// errors never propagate: an unreachable origin must not fail the
	// timeline query, the deleted items just linger until the next refresh
	return result, nil
}

func (r *resolver) LookupChunkItrs(ctx context.Context, timelines []string, until time.Time) (map[string]string, error) {
	ctx, span := tracer.Start(ctx, "ChunklineResolver.LookupChunkItrs")
	defer span.End()

	manifests, err := r.ResolveTimelines(ctx, timelines)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	queries := make(map[string]int64)
	keys := make([]string, 0, len(timelines))

	for _, tl := range timelines {
		manifest, ok := manifests[tl]
		if !ok {
			err := fmt.Errorf("missing chunkline manifest for timeline %s", tl)
			span.RecordError(err)
			continue
		}

		if manifest.Descending.Iterator == "" {
			err := fmt.Errorf("timeline %s does not support descending iteration", tl)
			span.RecordError(err)
			continue
		}

		queryChunk := manifest.Time2Chunk(until)
		if manifest.LastChunk != nil && queryChunk > *manifest.LastChunk {
			queryChunk = *manifest.LastChunk
		}

		if manifest.FirstChunk != nil && queryChunk < *manifest.FirstChunk {
			err := fmt.Errorf("query chunk %d is before first chunk %d for timeline %s", queryChunk, *manifest.FirstChunk, tl)
			span.RecordError(err)
			continue
		}

		queries[tl] = queryChunk
		keys = append(keys, itrCacheKey(tl, queryChunk))
	}

	cachedItems, err := r.mc.GetMulti(keys)
	if err != nil && !errors.Is(err, memcache.ErrCacheMiss) {
		span.RecordError(fmt.Errorf("failed to get iterators from cache: %w", err))
	}

	results := make(map[string]string)

	requestsByDomain := make(map[string]map[string]string) // domain -> timeline -> query path

	type localLookup struct{ timeline, uri string }
	localLookups := make(map[int64][]localLookup) // chunk -> lookups

	remainings := make(map[string]int64)

	for tl, chunkID := range queries {
		cacheKey := itrCacheKey(tl, chunkID)
		if item, found := cachedItems[cacheKey]; found {
			cacheRequests.WithLabelValues("itr", "hit").Inc()
			results[tl] = string(item.Value)
			continue
		}
		cacheRequests.WithLabelValues("itr", "miss").Inc()

		manifest, ok := manifests[tl]
		if !ok {
			err := fmt.Errorf("missing chunkline manifest for timeline %s", tl)
			span.RecordError(err)
			continue
		}

		if manifest.Descending.Iterator == "" {
			err := fmt.Errorf("timeline %s does not support descending iteration", tl)
			span.RecordError(err)
			continue
		}

		relPath, err := concrnt.RenderURITemplate(manifest.Descending.Iterator, map[string]string{
			"chunk": fmt.Sprintf("%d", chunkID),
		})
		if err != nil {
			span.RecordError(fmt.Errorf("failed to render iterator URI for timeline %s: %w", tl, err))
			continue
		}

		rel, err := url.Parse(relPath)
		if err != nil {
			span.RecordError(fmt.Errorf("invalid iterator URI template for timeline %s: %w", tl, err))
			continue
		}

		base, err := url.Parse(tl)
		if err != nil {
			span.RecordError(fmt.Errorf("invalid timeline URI %s: %w", tl, err))
			continue
		}

		endpoint := base.ResolveReference(rel)
		if endpoint.Scheme != "http" && endpoint.Scheme != "https" {
			host, err := r.client.ResolveResourceHost(ctx, tl)
			if err != nil {
				span.RecordError(fmt.Errorf("failed to resolve host for timeline %s: %w", tl, err))
				continue
			}
			endpoint.Scheme = "https"
			endpoint.Host = host
		}

		remainings[tl] = chunkID
		if localChunk, uri, ok := r.localCall(endpoint, "itr"); ok {
			localLookups[localChunk] = append(localLookups[localChunk], localLookup{timeline: tl, uri: uri})
			continue
		}

		domain := endpoint.Host
		if _, exists := requestsByDomain[domain]; !exists {
			requestsByDomain[domain] = make(map[string]string)
		}
		requestsByDomain[domain][tl] = endpoint.String()
	}

	if len(remainings) == 0 {
		return results, nil // all results were cached
	}

	fetched := make(map[string]string, len(remainings)) // timeline -> iterator as served

	for chunkID, lookups := range localLookups {
		originFetches.WithLabelValues("itr", "local").Add(float64(len(lookups)))
		uris := make([]string, len(lookups))
		for i, lookup := range lookups {
			uris[i] = lookup.uri
		}
		itrs, err := r.local.LookupLocalItrs(ctx, uris, chunkID)
		if err != nil {
			span.RecordError(fmt.Errorf("local iterator lookup failed: %w", err))
			continue
		}
		for _, lookup := range lookups {
			if itr, ok := itrs[lookup.uri]; ok {
				fetched[lookup.timeline] = strconv.FormatInt(itr, 10)
			}
		}
	}

	for tl, body := range r.fetchFromOrigins(ctx, "itr", requestsByDomain, func(err error) { span.RecordError(err) }) {
		fetched[tl] = string(body)
	}

	currentSubscriptions := r.demand.CurrentSubscriptions()

	for tl, chunkID := range remainings {
		raw, ok := fetched[tl]
		if !ok {
			continue
		}

		iterator := strings.TrimSpace(raw)
		// Older servers report "no iterator" as a 200 "0" instead of a 404.
		// Chunk 0 is the 1970 epoch bucket and can never be a real iterator,
		// so drop it (and empty bodies) before use and before caching.
		if iterator == "" || iterator == "0" {
			span.RecordError(fmt.Errorf("invalid iterator %q for timeline %s", iterator, tl))
			continue
		}
		results[tl] = iterator

		// もしキャッシュ対象が最新チャンクであれば、現在の購読状態を確認し、購読中でなければキャッシュを保存しない
		if chunkID == manifests[tl].Time2Chunk(time.Now()) {
			isSubscribed := slices.Contains(currentSubscriptions, tl)
			if !isSubscribed {
				continue // skip caching if not subscribed to the latest chunk
			}
		}

		cacheItem := &memcache.Item{
			Key:        itrCacheKey(tl, chunkID),
			Value:      []byte(iterator),
			Expiration: itrCacheTTL,
		}
		err = r.mc.Set(cacheItem)
		if err != nil {
			span.RecordError(fmt.Errorf("failed to set iterator in cache for %s: %w", tl, err))
		}
	}

	return results, nil
}

func (r *resolver) LoadChunkBodies(ctx context.Context, query map[string]string) (map[string]chunkline.BodyChunk, error) {
	ctx, span := tracer.Start(ctx, "ChunklineResolver.LoadChunkBodies")
	defer span.End()

	uris := []string{}
	for itr := range query {
		uris = append(uris, itr)
	}

	manifests, err := r.ResolveTimelines(ctx, uris)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	keys := make([]string, 0, len(query))

	for tl, itr := range query {
		cacheKey := bodyCacheKey(tl, itr)
		keys = append(keys, cacheKey)
	}

	results := make(map[string]chunkline.BodyChunk)

	cachedItems, err := r.mc.GetMulti(keys)
	if err != nil && !errors.Is(err, memcache.ErrCacheMiss) {
		span.RecordError(fmt.Errorf("failed to get chunk bodies from cache: %w", err))
	}

	requestsByDomain := make(map[string]map[string]string) // domain -> timeline -> query path
	fetched := make(map[string][]chunkline.BodyItem)

	remaining := make(map[string]string)

	for tl, itr := range query {
		cacheKey := bodyCacheKey(tl, itr)
		if item, found := cachedItems[cacheKey]; found {
			// a malformed cached item still counts as a hit: it was served
			// from the cache (dropped), not fetched from origin
			cacheRequests.WithLabelValues("body", "hit").Inc()
			var bodyItems []chunkline.BodyItem
			cacheStr := string(item.Value)
			cacheStr = cacheStr[1:]
			cacheStr = "[" + cacheStr + "]"
			err := json.Unmarshal([]byte(cacheStr), &bodyItems)
			if err != nil {
				span.RecordError(fmt.Errorf("failed to unmarshal cached body chunk for %s: %w", tl, err))
				fmt.Printf("invalid cache format for key %s: %s\n", cacheKey, cacheStr)
				continue
			}
			// the cache updater blindly prepends, so a delayed event (e.g. a
			// federated record whose authored time predates cached items) can
			// leave the cached body unordered; readers binary-search on
			// descending timestamps, so restore the order here
			slices.SortStableFunc(bodyItems, func(a, b chunkline.BodyItem) int {
				return b.Timestamp.Compare(a.Timestamp)
			})
			chunkID, err := strconv.ParseInt(itr, 10, 64)
			if err != nil {
				span.RecordError(fmt.Errorf("invalid chunk ID %s for timeline %s: %w", itr, tl, err))
				continue
			}
			results[tl] = chunkline.BodyChunk{
				URI:     tl,
				ChunkID: chunkID,
				Items:   bodyItems,
			}
			continue // cached: no origin fetch needed (a cached latest chunk is kept fresh by the cache updater)
		}
		cacheRequests.WithLabelValues("body", "miss").Inc()

		manifest, ok := manifests[tl]
		if !ok {
			err := fmt.Errorf("missing chunkline manifest for timeline %s", tl)
			span.RecordError(err)
			continue
		}

		if manifest.Descending.Body == "" {
			err := fmt.Errorf("timeline %s does not support descending body retrieval", tl)
			span.RecordError(err)
			continue
		}

		refPath, err := concrnt.RenderURITemplate(manifest.Descending.Body, map[string]string{
			"chunk": itr,
		})
		if err != nil {
			span.RecordError(fmt.Errorf("invalid body URI template for timeline %s: %w", tl, err))
			continue
		}
		ref, err := url.Parse(refPath)
		if err != nil {
			span.RecordError(fmt.Errorf("invalid body URI template for timeline %s: %w", tl, err))
			continue
		}

		base, err := url.Parse(tl)
		if err != nil {
			span.RecordError(fmt.Errorf("invalid timeline URI %s: %w", tl, err))
			continue
		}

		endpoint := base.ResolveReference(ref)
		if endpoint.Scheme != "http" && endpoint.Scheme != "https" {
			host, err := r.client.ResolveResourceHost(ctx, tl)
			if err != nil {
				span.RecordError(fmt.Errorf("failed to resolve host for timeline %s: %w", tl, err))
				continue
			}
			endpoint.Scheme = "https"
			endpoint.Host = host
		}

		remaining[tl] = itr
		if localChunk, uri, ok := r.localCall(endpoint, "body"); ok {
			originFetches.WithLabelValues("body", "local").Inc()
			items, err := r.local.LoadLocalBody(ctx, uri, localChunk)
			if err != nil {
				span.RecordError(fmt.Errorf("failed to load local chunk body for timeline %s: %w", tl, err))
				continue
			}
			fetched[tl] = items
			continue
		}

		domain := endpoint.Host
		if _, exists := requestsByDomain[domain]; !exists {
			requestsByDomain[domain] = make(map[string]string)
		}
		requestsByDomain[domain][tl] = endpoint.String()
	}

	if len(remaining) == 0 {
		return results, nil // all results were cached
	}

	for tl, body := range r.fetchFromOrigins(ctx, "body", requestsByDomain, func(err error) { span.RecordError(err) }) {
		var items []chunkline.BodyItem
		if err := json.Unmarshal(body, &items); err != nil {
			span.RecordError(fmt.Errorf("failed to decode chunk body for timeline %s: %w", tl, err))
			continue
		}
		fetched[tl] = items
	}

	currentSubscriptions := r.demand.CurrentSubscriptions()

	for tl, itr := range remaining {
		items, ok := fetched[tl]
		if !ok {
			continue
		}
		if items == nil {
			items = []chunkline.BodyItem{} // the cached form is the array's inside; "null" has none
		}

		chunkID, err := strconv.ParseInt(itr, 10, 64)
		if err != nil {
			span.RecordError(fmt.Errorf("invalid chunk ID %s for timeline %s: %w", itr, tl, err))
			continue
		}

		bodyChunk := chunkline.BodyChunk{
			URI:     tl,
			ChunkID: chunkID,
			Items:   items,
		}
		results[tl] = bodyChunk

		isLatestChunk := chunkID == manifests[tl].Time2Chunk(time.Now())

		// もしキャッシュ対象が最新チャンクであれば、現在の購読状態を確認し、購読中でなければキャッシュを保存しない
		if isLatestChunk {
			isSubscribed := slices.Contains(currentSubscriptions, tl)
			if !isSubscribed {
				continue // skip caching if not subscribed to the latest chunk
			}
		}

		serialized, err := json.Marshal(items)
		if err != nil {
			span.RecordError(fmt.Errorf("failed to marshal body chunk for caching for %s: %w", tl, err))
			continue
		}

		cacheStr := "," + string(serialized[1:len(serialized)-1])
		cacheItem := &memcache.Item{
			Key:        bodyCacheKey(tl, itr),
			Value:      []byte(cacheStr),
			Expiration: bodyCacheTTL,
		}
		if isLatestChunk {
			// Add, not Set: a live latest chunk may already be maintained by
			// the cache updater, and clobbering it with this (possibly older)
			// origin fetch would drop the records prepended since
			err = r.mc.Add(cacheItem)
			if errors.Is(err, memcache.ErrNotStored) {
				err = nil
			}
		} else {
			err = r.mc.Set(cacheItem)
		}
		if err != nil {
			span.RecordError(fmt.Errorf("failed to set body chunk in cache for %s: %w", tl, err))
		}

		// もしキャッシュ対象が最新チャンクなのであれば、itrも更新する必要がある
		if chunkID == manifests[tl].Time2Chunk(time.Now()) {
			cacheItem := &memcache.Item{
				Key:        itrCacheKey(tl, chunkID),
				Value:      []byte(itr),
				Expiration: itrCacheTTL,
			}
			err = r.mc.Set(cacheItem)
			if err != nil {
				span.RecordError(fmt.Errorf("failed to set iterator in cache for %s: %w", tl, err))
			}
		}
	}

	return results, nil
}

func (r *resolver) CurrentSubscriptions() []string {
	// start from the prefixes whose peer subscription is actually open:
	// "open + latest chunk cached => keep" is self-sustaining regardless of
	// which replica wrote the cache, whereas demand-based input would drop
	// the timeline in the same tick its demand lapses, orphaning the cache
	var ongoing []string
	if r.open != nil {
		ongoing = r.open.OpenPrefixes()
	} else {
		ongoing = r.demand.CurrentSubscriptions()
	}

	timelines, err := r.ResolveTimelines(context.Background(), ongoing)
	if err != nil {
		return ongoing // fallback to raw URIs if manifest resolution fails
	}

	keep := []string{}
	for _, tl := range ongoing {
		manifest, ok := timelines[tl]
		if !ok {
			continue
		}

		currentChunk := manifest.Time2Chunk(time.Now())

		// bodyの最新チャンクのキャッシュが存在する場合は、残しておかないとキャッシュが古いままになってしまう
		latestChunkKey := bodyCacheKey(tl, fmt.Sprintf("%d", currentChunk))
		_, err := r.mc.Get(latestChunkKey)
		if err == nil {
			keep = append(keep, tl)
			continue
		}

		// 最新チャンク-1がなければ、確定で終了して良い
		prevChunkKey := bodyCacheKey(tl, fmt.Sprintf("%d", currentChunk-1))
		_, err = r.mc.Get(prevChunkKey)
		if err != nil {
			continue
		}

		// 前チャンクがあり最新がまだ作成されてない場合、現在チャンクの80%が経過するまでは待機しておく
		now := time.Now()
		currentChunkStart := manifest.Chunk2Time(currentChunk)
		currentChunkEnd := manifest.Chunk2Time(currentChunk + 1)
		currentChunkDuration := currentChunkEnd.Sub(currentChunkStart)
		if now.Before(currentChunkStart.Add(currentChunkDuration * 8 / 10)) {
			keep = append(keep, tl)
			continue
		}
	}

	return keep
}

const (
	// cacheUpdaterQueue absorbs a burst (an import, a bridge catching up)
	// while the workers run their memcached round trips
	cacheUpdaterQueue   = 4096
	cacheUpdaterWorkers = 8
)

// cacheUpdater mirrors created events into the chunk cache. Events are
// applied concurrently: applyEventToCache is order-independent and tolerates
// duplicates. When the subscription was lost and re-established, the latest
// chunk of every maintained timeline is purged so readers rebuild it from
// the origin with whatever was published in between.
func (r *resolver) cacheUpdater(ctx context.Context) {
	events := make(chan concrnt.Event, cacheUpdaterQueue)

	if gaps, ok := r.pubsub.(worker.PubSubGaps); ok {
		gaps.OnGap(func() {
			if ctx.Err() != nil {
				return
			}
			timelines := r.CurrentSubscriptions()
			slog.Warn("chunkline cache updater: event stream gap, purging the latest chunks", slog.Int("timelines", len(timelines)))
			r.PurgeLatest(timelines, 1)
		})
	}

	if err := r.pubsub.SubscribeAll(ctx, events); err != nil {
		slog.Error("chunkline cache updater: subscribe failed", slog.String("error", err.Error()))
		return
	}

	var wg sync.WaitGroup
	for range cacheUpdaterWorkers {
		wg.Go(func() {
			for {
				var event concrnt.Event
				select {
				case <-ctx.Done():
					return
				case event = <-events:
				}
				cacheUpdaterQueued.Set(float64(len(events)))

				if event.Type != "created" {
					continue
				}
				result := "applied"
				switch err := r.applyEventToCache(ctx, event); {
				case errors.Is(err, errNotATimelineRecord):
					result = "skipped"
				case err != nil:
					result = "error"
					slog.Error("chunkline cache updater: event not applied", slog.String("uri", event.URI), slog.String("error", err.Error()))
				}
				cacheUpdaterEvents.WithLabelValues(result).Inc()
			}
		})
	}
	wg.Wait()
}

// errNotATimelineRecord is a created event on a key with no parent.
var errNotATimelineRecord = errors.New("record key has no timeline")

// applyEventToCache maintains the cached chunk of the timeline that gained a
// record. The event arrives on the record's own key (`<timeline>/<id>`, the
// channel prefix subscribers match on), never on the bare timeline URI, so
// the timeline whose chunk cache readers hit is the record key's parent.
//
// The item is blindly prepended: chunkline tolerates duplicates (readers
// deduplicate by href) and unordered chunks (LoadChunkBodies sorts cached
// bodies on read), so no read-modify-write is needed — a concurrent updater
// during a leadership handover or a racing origin fetch at worst duplicates
// the record. Prepend and Replace never create keys, so an unmaintained
// timeline stays uncached.
func (r *resolver) applyEventToCache(ctx context.Context, event concrnt.Event) error {
	// the parent of the created record is the timeline it entered
	authority := strings.Index(event.URI, "://")
	cut := strings.LastIndex(event.URI, "/")
	if authority == -1 || cut <= authority+2 {
		return errNotATimelineRecord // a root key belongs to no timeline; nothing to maintain
	}
	timeline := event.URI[:cut]

	manifest, err := r.resolveTimeline(ctx, timeline)
	if err != nil {
		return fmt.Errorf("resolve timeline %s: %w", timeline, err)
	}
	epoch := manifest.Time2Chunk(event.Timestamp)

	// repoint the cached iterator to this epoch before touching the body:
	// even when no body is cached, a cached iterator from a read during an
	// empty epoch still steers readers to an older chunk and would hide this
	// record until the epoch rolls over.
	err = r.mc.Replace(&memcache.Item{Key: itrCacheKey(timeline, epoch), Value: []byte(strconv.FormatInt(epoch, 10)), Expiration: itrCacheTTL})
	if err != nil && !errors.Is(err, memcache.ErrNotStored) {
		return fmt.Errorf("replace iterator of %s: %w", timeline, err)
	}

	// the cached entry must mirror what the origin body endpoint serves
	// (LoadLocalBody): a reference record enters the timeline as its redirect
	// target (which is also what readers deduplicate by), everything else as
	// the record key itself
	href := event.URI
	if sd, ok := event.References[event.URI]; ok {
		var doc concrnt.Document[schemas.Reference]
		if err := json.Unmarshal([]byte(sd.Document), &doc); err == nil && doc.Schema == schemas.ReferenceURL && doc.Value.Href != "" {
			href = doc.Value.Href
		}
	}

	serializedItem, err := json.Marshal(chunkline.BodyItem{
		Timestamp:   event.Timestamp,
		Href:        href,
		ContentType: "application/concrnt.document+json",
	})
	if err != nil {
		return err
	}

	bodyKey := bodyCacheKey(timeline, strconv.FormatInt(epoch, 10))
	err = r.mc.Prepend(&memcache.Item{Key: bodyKey, Value: append([]byte(","), serializedItem...)})
	if err != nil && !errors.Is(err, memcache.ErrNotStored) {
		return fmt.Errorf("prepend body item of %s: %w", timeline, err)
	}
	return nil
}

// PurgeLatest drops the cached latest chunk (and, for depth 2, the previous
// one) of each timeline. The subscriber calls this when a peer
// subscription opens or closes: a cached latest chunk is only valid while the
// leader is receiving events for it, so both edges invalidate whatever was
// cached before or during the unsubscribed gap.
func (r *resolver) PurgeLatest(prefixes []string, depth int) {
	if len(prefixes) == 0 {
		return
	}

	// deliberately not the caller's context: purge-on-close also runs while
	// the lead context is already cancelled
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	manifests, err := r.ResolveTimelines(ctx, prefixes)
	if err != nil {
		slog.Error("failed to resolve timelines for cache purge", slog.String("error", err.Error()))
		return
	}

	for _, tl := range prefixes {
		manifest, ok := manifests[tl]
		if !ok {
			continue
		}
		current := manifest.Time2Chunk(time.Now())
		for i := range int64(depth) {
			chunk := current - i
			if err := r.mc.Delete(itrCacheKey(tl, chunk)); err != nil && !errors.Is(err, memcache.ErrCacheMiss) {
				slog.Error("failed to purge iterator cache", slog.String("timeline", tl), slog.String("error", err.Error()))
			}
			if err := r.mc.Delete(bodyCacheKey(tl, strconv.FormatInt(chunk, 10))); err != nil && !errors.Is(err, memcache.ErrCacheMiss) {
				slog.Error("failed to purge body cache", slog.String("timeline", tl), slog.String("error", err.Error()))
			}
		}
	}

	if depth == 1 {
		// close edge: a worker whose cached aggregate predates this close can
		// still consider these prefixes subscribed for a few seconds and
		// re-cache an unmaintained latest chunk (mc.Add succeeds precisely
		// because the delete above just removed the key). Sweep again after
		// the workers' view has expired; a spurious delete only costs one
		// cache miss.
		keys := make([]string, 0, len(prefixes)*2)
		for _, tl := range prefixes {
			manifest, ok := manifests[tl]
			if !ok {
				continue
			}
			chunk := manifest.Time2Chunk(time.Now())
			keys = append(keys, itrCacheKey(tl, chunk), bodyCacheKey(tl, strconv.FormatInt(chunk, 10)))
		}
		time.AfterFunc(purgeReplayDelay, func() {
			for _, key := range keys {
				if err := r.mc.Delete(key); err != nil && !errors.Is(err, memcache.ErrCacheMiss) {
					slog.Error("failed to replay cache purge", slog.String("key", key), slog.String("error", err.Error()))
				}
			}
		})
	}
}
