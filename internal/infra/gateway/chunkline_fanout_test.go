package gateway

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/concrnt/concrnt/chunkline"
	"github.com/concrnt/concrnt/client"
	"github.com/concrnt/concrnt/internal/domain"
	fsrecord "github.com/concrnt/concrnt/internal/infra/repository/firestore/record"
	"github.com/concrnt/concrnt/internal/infra/repository/repoutil"
	"github.com/concrnt/concrnt/internal/testutil"
	ucchunkline "github.com/concrnt/concrnt/internal/usecase/chunkline"
	ucrecord "github.com/concrnt/concrnt/internal/usecase/record"
)

const (
	fanOutSelf = "self.example.test"
	// what one Firestore round trip cost in the load test that exposed the
	// serial reads (GetChunklineManifest p50 13.6 ms, LoadLocalBody 15.8 ms)
	fanOutLatency = 13 * time.Millisecond
	// posts per followed timeline; with the default limit of 16 the merge
	// never has to walk back past a chunk
	fanOutPosts = 4
)

func fanOutTimelines(n int) []string {
	timelines := make([]string, n)
	for i := range timelines {
		timelines[i] = fmt.Sprintf("cckv://%s/tl-%02d", fanOutSelf, i)
	}
	return timelines
}

// slowChunklineRepo serves the chunkline views of many timelines the way
// the record stores do, takes fanOutLatency per call, and counts the calls
// and how many of them overlapped.
type slowChunklineRepo struct {
	ucrecord.Repository
	items map[string][]chunkline.BodyItem // newest first

	manifests, itrs, bodies atomic.Int32

	mu                sync.Mutex
	inFlight, maxSeen int
}

func (s *slowChunklineRepo) roundTrip() {
	s.mu.Lock()
	s.inFlight++
	s.maxSeen = max(s.maxSeen, s.inFlight)
	s.mu.Unlock()
	time.Sleep(fanOutLatency)
	s.mu.Lock()
	s.inFlight--
	s.mu.Unlock()
}

func (s *slowChunklineRepo) reset() {
	s.manifests.Store(0)
	s.itrs.Store(0)
	s.bodies.Store(0)
	s.mu.Lock()
	s.maxSeen = 0
	s.mu.Unlock()
}

func (s *slowChunklineRepo) GetChunklineManifest(ctx context.Context, uri string) (*chunkline.Manifest, error) {
	s.manifests.Add(1)
	s.roundTrip()
	items, ok := s.items[uri]
	if !ok {
		return nil, domain.NotFoundError{Resource: "record key"}
	}
	var firstChunk *int64
	if len(items) > 0 {
		first := items[len(items)-1].Timestamp.Unix() / repoutil.ChunkSize
		firstChunk = &first
	}
	return repoutil.ChunklineManifest(uri, firstChunk), nil
}

func (s *slowChunklineRepo) LookupLocalItrs(ctx context.Context, uris []string, chunkID int64) (map[string]int64, error) {
	s.itrs.Add(1)
	s.roundTrip()
	end := time.Unix((chunkID+1)*repoutil.ChunkSize, 0)
	result := map[string]int64{}
	for _, uri := range uris {
		for _, item := range s.items[uri] {
			if item.Timestamp.Before(end) {
				result[uri] = item.Timestamp.Unix() / repoutil.ChunkSize
				break
			}
		}
	}
	return result, nil
}

func (s *slowChunklineRepo) LoadLocalBody(ctx context.Context, uri string, chunkID int64) ([]chunkline.BodyItem, error) {
	s.bodies.Add(1)
	s.roundTrip()
	items, ok := s.items[uri]
	if !ok {
		return nil, domain.NotFoundError{Resource: "record key"}
	}
	end := time.Unix((chunkID+1)*repoutil.ChunkSize, 0)
	body := []chunkline.BodyItem{}
	for _, item := range items {
		if item.Timestamp.Before(end) {
			body = append(body, item)
		}
	}
	return body, nil
}

// awayFromChunkEdge keeps a measurement inside one chunk: whether a chunk
// is "the latest" (and so cached only while subscribed) is decided against
// the wall clock.
func awayFromChunkEdge() time.Time {
	now := time.Now()
	if into := now.Unix() % 600; into < 5 || into > 590 {
		time.Sleep(15 * time.Second)
		now = time.Now()
	}
	return now
}

// timelineRead is one home timeline request as the REST handler issues it.
func timelineRead(t *testing.T, r *resolver, timelines []string, want int) time.Duration {
	t.Helper()
	start := time.Now()
	items, err := chunkline.NewClient(r).QueryDescending(context.Background(), timelines, time.Now(), 16)
	elapsed := time.Since(start)
	require.NoError(t, err)
	require.Len(t, items, want)
	for i := 1; i < len(items); i++ {
		require.False(t, items[i].Timestamp.After(items[i-1].Timestamp), "items must be newest first")
	}
	// the removed-items refresh runs in the background; let it finish so it
	// is not counted against the next request
	time.Sleep(50 * time.Millisecond)
	return elapsed
}

// TestTimelineReadFanOut pins what one home timeline request costs the
// store for N followed timelines of this server, per cache state, and that
// the per-timeline reads overlap instead of queueing behind each other: a
// request whose cache misses used to pay N manifest reads and N body reads
// strictly one after the other (the 500 ms timeline/recent of the
// Firestore load test).
func TestTimelineReadFanOut(t *testing.T) {
	mc, cleanup := testutil.CreateMC()
	t.Cleanup(cleanup)

	cl := client.New(fanOutSelf)

	for _, n := range []int{1, 5, 20, 50} {
		t.Run(fmt.Sprintf("N=%d", n), func(t *testing.T) {
			now := awayFromChunkEdge()
			timelines := fanOutTimelines(n)
			repo := &slowChunklineRepo{items: map[string][]chunkline.BodyItem{}}
			for i, tl := range timelines {
				for p := range fanOutPosts {
					repo.items[tl] = append(repo.items[tl], chunkline.BodyItem{
						Timestamp: now.Add(-time.Duration(p*n+i+1) * time.Millisecond),
						Href:      fmt.Sprintf("%s/post-%d", tl, p),
					})
				}
			}
			want := min(16, n*fanOutPosts)

			newResolverFor := func(mc *memcache.Client, demand SubscriptionDemand) *resolver {
				r := newResolver(cl, mc, demand, nil)
				r.fqdn, r.local = fanOutSelf, ucchunkline.New(repo, nil, nil, nil)
				return r
			}
			measure := func(state string, r *resolver, manifests, itrs, bodies int) {
				t.Helper()
				repo.reset()
				elapsed := timelineRead(t, r, timelines, want)
				t.Logf("%-34s manifest=%-3d itr=%-2d body=%-3d overlap=%-3d wall=%6.1fms (%.1f round trips deep)",
					state, repo.manifests.Load(), repo.itrs.Load(), repo.bodies.Load(), repo.maxSeen,
					float64(elapsed.Microseconds())/1000, float64(elapsed)/float64(fanOutLatency))
				require.Equal(t, int32(manifests), repo.manifests.Load(), "%s: manifest reads", state)
				require.Equal(t, int32(itrs), repo.itrs.Load(), "%s: iterator lookups", state)
				require.Equal(t, int32(bodies), repo.bodies.Load(), "%s: body reads", state)
				require.LessOrEqual(t, repo.maxSeen, originConcurrency, "%s: fan-out is bounded", state)
				if calls := max(manifests, bodies); calls > 0 {
					require.Equal(t, min(calls, originConcurrency), repo.maxSeen, "%s: per-timeline reads overlap", state)
					// manifests, iterators, bodies: three waves, each as deep
					// as the fan-out bound makes it; the slack absorbs a
					// loaded test machine
					waves := 1
					for _, calls := range []int{manifests, bodies} {
						waves += (calls + originConcurrency - 1) / originConcurrency
					}
					require.Less(t, elapsed, time.Duration(waves)*fanOutLatency+150*time.Millisecond, "%s: wall time", state)
				}
			}

			// nothing cached anywhere: every timeline's manifest, one batched
			// iterator lookup, every timeline's body
			require.NoError(t, mc.FlushAll())
			r := newResolverFor(mc, noDemand{})
			measure("cold", r, n, 1, n)
			// the latest chunk of a timeline nobody is subscribed to is never
			// cached (nothing would keep it fresh), so its iterator and body
			// are read again; the manifests are cached
			measure("warm, timelines not subscribed", r, 0, 1, n)

			// subscribed timelines: the latest chunk is cached and maintained
			// by the cache updater
			require.NoError(t, mc.FlushAll())
			r = newResolverFor(mc, fixedDemand(timelines))
			measure("cold, timelines subscribed", r, n, 1, n)
			measure("warm, timelines subscribed", r, 0, 0, 0)

			// a replica whose memcached client has no server (the state of
			// one of the two replicas in the load test): only the in-process
			// manifest copies help, and only for manifestLocalTTL
			r = newResolverFor(memcache.New(), fixedDemand(timelines))
			measure("no memcached, first request", r, n, 1, n)
			measure("no memcached, within 5 s", r, 0, 1, n)
		})
	}
}

// TestTimelineReadMissingTimeline pins the error semantics of the fan-out:
// a timeline that does not exist (or whose read fails) is left out, the
// others are served.
func TestTimelineReadMissingTimeline(t *testing.T) {
	mc, cleanup := testutil.CreateMC()
	t.Cleanup(cleanup)

	now := awayFromChunkEdge()
	timelines := fanOutTimelines(6)
	repo := &slowChunklineRepo{items: map[string][]chunkline.BodyItem{}}
	for i, tl := range timelines[:4] {
		repo.items[tl] = []chunkline.BodyItem{{Timestamp: now.Add(-time.Duration(i+1) * time.Second), Href: tl + "/post"}}
	}
	repo.items[timelines[4]] = nil // exists, empty

	r := newResolver(client.New(fanOutSelf), mc, noDemand{}, nil)
	r.fqdn, r.local = fanOutSelf, ucchunkline.New(repo, nil, nil, nil)

	items, err := chunkline.NewClient(r).QueryDescending(context.Background(), timelines, now, 16)
	require.NoError(t, err)
	require.Len(t, items, 4)
	for i, item := range items {
		require.Equal(t, timelines[i], item.Source, "newest first")
	}
}

// rpcLatency counts the Firestore RPCs of a client by method and, once
// armed, delays each by a fixed round trip time.
type rpcLatency struct {
	delay  atomic.Int64
	mu     sync.Mutex
	counts map[string]int
}

func (c *rpcLatency) observe(fullMethod string) {
	c.mu.Lock()
	c.counts[fullMethod[strings.LastIndex(fullMethod, "/")+1:]]++
	c.mu.Unlock()
	time.Sleep(time.Duration(c.delay.Load()))
}

func (c *rpcLatency) reset() {
	c.mu.Lock()
	c.counts = map[string]int{}
	c.mu.Unlock()
}

func (c *rpcLatency) dialOptions() []grpc.DialOption {
	return []grpc.DialOption{
		grpc.WithChainUnaryInterceptor(func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
			c.observe(method)
			return invoker(ctx, method, req, reply, cc, opts...)
		}),
		grpc.WithChainStreamInterceptor(func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
			c.observe(method)
			return streamer(ctx, desc, cc, method, opts...)
		}),
	}
}

// TestTimelineReadFanOutFirestore runs the same request against the
// Firestore backend (emulator) with every RPC delayed by one round trip
// time: the RPC count per request is what a home timeline read is billed,
// the wall time shows that those RPCs overlap.
func TestTimelineReadFanOutFirestore(t *testing.T) {
	if testing.Short() {
		t.Skip("seeds 76 timelines into the emulator")
	}
	mc, cleanup := testutil.CreateMC()
	t.Cleanup(cleanup)

	rpcs := &rpcLatency{counts: map[string]int{}}
	repo := fsrecord.New(testutil.CreateFirestore(t, rpcs.dialOptions()...))
	cl := client.New(fanOutSelf)
	ctx := context.Background()

	now := awayFromChunkEdge()
	commit := func(id, key string, at time.Time) {
		_, err := repo.ApplyRecord(ctx, ucrecord.RecordCommit{
			Commit: ucrecord.Commit{ID: id, IP: "127.0.0.1", Document: `{"id":"` + id + `"}`, Owner: fanOutSelf},
			Key:    key, Author: "con1author", Schema: "https://schema.example/post.json", CreatedAt: at,
		})
		require.NoError(t, err)
	}

	for _, n := range []int{1, 5, 20, 50} {
		t.Run(fmt.Sprintf("N=%d", n), func(t *testing.T) {
			timelines := make([]string, n)
			rpcs.delay.Store(0)
			for i := range timelines {
				timelines[i] = fmt.Sprintf("cckv://%s/n%d-tl-%02d", fanOutSelf, n, i)
				commit(fmt.Sprintf("n%d-tl-%d", n, i), timelines[i], now.Add(-time.Hour))
				for p := range fanOutPosts {
					commit(fmt.Sprintf("n%d-tl-%d-post-%d", n, i, p), fmt.Sprintf("%s/post-%d", timelines[i], p),
						now.Add(-time.Duration(p*n+i+1)*time.Millisecond))
				}
			}
			rpcs.delay.Store(int64(fanOutLatency))
			want := min(16, n*fanOutPosts)

			measure := func(state string, r *resolver, queries, gets int) {
				t.Helper()
				rpcs.reset()
				elapsed := timelineRead(t, r, timelines, want)
				rpcs.mu.Lock()
				counts := rpcs.counts
				rpcs.mu.Unlock()
				t.Logf("%-34s RunQuery=%-3d BatchGetDocuments=%-3d wall=%6.1fms (%.1f round trips deep)",
					state, counts["RunQuery"], counts["BatchGetDocuments"],
					float64(elapsed.Microseconds())/1000, float64(elapsed)/float64(fanOutLatency))
				require.Equal(t, queries, counts["RunQuery"], "%s: queries", state)
				require.Equal(t, gets, counts["BatchGetDocuments"], "%s: gets", state)
			}
			newResolverFor := func(demand SubscriptionDemand) *resolver {
				r := newResolver(cl, mc, demand, nil)
				r.fqdn, r.local = fanOutSelf, ucchunkline.New(repo, nil, nil, nil)
				return r
			}

			// manifest: a query and a key get per timeline; iterators: a
			// query per timeline and one batch get; body: a query and a key
			// get per timeline
			require.NoError(t, mc.FlushAll())
			r := newResolverFor(noDemand{})
			measure("cold", r, 3*n, 2*n+1)
			measure("warm, timelines not subscribed", r, 2*n, n+1)

			require.NoError(t, mc.FlushAll())
			r = newResolverFor(fixedDemand(timelines))
			measure("cold, timelines subscribed", r, 3*n, 2*n+1)
			measure("warm, timelines subscribed", r, 0, 0)
		})
	}
}
