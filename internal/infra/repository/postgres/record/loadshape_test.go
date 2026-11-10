package record

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand/v2"
	"net"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ory/dockertest"
	dc "github.com/ory/dockertest/docker"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/concrnt/concrnt"
	aggpg "github.com/concrnt/concrnt/internal/infra/aggregate/postgres"
	"github.com/concrnt/concrnt/internal/infra/database"
	"github.com/concrnt/concrnt/internal/infra/repository/postgres"
	"github.com/concrnt/concrnt/internal/usecase/aggregate"
	ucrecord "github.com/concrnt/concrnt/internal/usecase/record"
)

// TestLoadShape reproduces the read/write shape of the 3000 DAU load test
// against one Postgres limited to 2 CPUs, through a pool of the size a
// replica runs with and a proxy that adds the round trip of a database on
// another host. It is a measurement, not a check: it runs only with
// CONCRNT_LOADSHAPE=1 and prints its figures.
//
//	CONCRNT_LOADSHAPE=1 go test ./internal/infra/repository/postgres/record/ -run TestLoadShape -v -count=1
//
// Knobs (environment): LOADSHAPE_RTT (round trip added, default 2ms),
// LOADSHAPE_POOL (64), LOADSHAPE_LURKERS (120, the sessions of one replica
// that hear an event), LOADSHAPE_ROUNDS (120), LOADSHAPE_INTERVAL (150ms),
// LOADSHAPE_AUTHORS (50, the authors a hot target starts with).
//
// The scenarios:
//   - herd: an association is committed on a hot target and every session
//     subscribed to it re-reads the target's associations and counts at
//     once (world-app does on an "associated" event), while other sessions
//     read cold targets and documents.
//   - hotkey: associations committed on one target from many connections at
//     once, every one of them a delta on the same count cache entry.
//   - writers: more commits at once than the pool has connections (a seed
//     burst), while the same bystanders read.
//   - plans: after 180k records, 120k ack states and 3k entities are loaded
//     in bulk, every statement the repository issues is EXPLAINed and the
//     ones that scan a table sequentially are printed, with the foreign
//     keys no index covers.
func TestLoadShape(t *testing.T) {
	if os.Getenv("CONCRNT_LOADSHAPE") == "" {
		t.Skip("set CONCRNT_LOADSHAPE=1 to run the load shape measurement")
	}

	cfg := loadShapeConfig{
		rtt:      envDuration("LOADSHAPE_RTT", 2*time.Millisecond),
		pool:     envInt("LOADSHAPE_POOL", 64),
		lurkers:  envInt("LOADSHAPE_LURKERS", 120),
		rounds:   envInt("LOADSHAPE_ROUNDS", 120),
		interval: envDuration("LOADSHAPE_INTERVAL", 150*time.Millisecond),
		authors:  envInt("LOADSHAPE_AUTHORS", 50),
	}
	env := openLoadShape(t, cfg)

	t.Run("herd", func(t *testing.T) { env.herd(t) })
	t.Run("hotkey", func(t *testing.T) { env.hotKey(t) })
	t.Run("writers", func(t *testing.T) { env.writers(t) })
	t.Run("plans", func(t *testing.T) { env.plans(t) })
}

type loadShapeConfig struct {
	rtt      time.Duration
	pool     int
	lurkers  int
	rounds   int
	interval time.Duration
	authors  int
}

func envInt(name string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(name)); err == nil {
		return v
	}
	return def
}

func envDuration(name string, def time.Duration) time.Duration {
	if v, err := time.ParseDuration(os.Getenv(name)); err == nil {
		return v
	}
	return def
}

const (
	shapeLikeSchema     = "https://schema.example/like.json"
	shapeReactionSchema = "https://schema.example/reaction.json"
	shapeColdTargets    = 2000
	shapeHotTargets     = 8
	shapeBystanders     = 16
)

type loadShape struct {
	cfg loadShapeConfig
	// direct reaches the database without the added round trip: seeding
	// and statistics.
	direct *gorm.DB
	// db is the pool under measurement.
	db    *gorm.DB
	sqlDB *sql.DB
	repo  ucrecord.Repository
	agg   *aggregate.Usecase
	uc    *ucrecord.Usecase
	def   *aggregate.Definition

	hot, cold []string
	seq       atomic.Int64
}

func shapeCCID(kind string, n int) string {
	return fmt.Sprintf("con1%s%0*d", kind, 38-len(kind), n)
}

func openLoadShape(t *testing.T, cfg loadShapeConfig) *loadShape {
	t.Helper()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "postgres",
		Tag:        "17",
		Env:        []string{"POSTGRES_USER=postgres", "POSTGRES_PASSWORD=secret", "POSTGRES_DB=loadshape"},
		Cmd:        []string{"postgres", "-c", "shared_preload_libraries=pg_stat_statements", "-c", "max_connections=300"},
	}, func(hc *dc.HostConfig) {
		// the load test's Cloud SQL instance has 2 vCPUs
		hc.CPUPeriod, hc.CPUQuota = 100_000, 200_000
		hc.AutoRemove = true
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = pool.Purge(resource) })

	port := resource.GetPort("5432/tcp")
	dsn := func(port string) string {
		return fmt.Sprintf("postgres://postgres:secret@localhost:%s/loadshape?sslmode=disable", port)
	}

	var direct *gorm.DB
	require.NoError(t, pool.Retry(func() error {
		var err error
		direct, err = database.NewPostgres(dsn(port), 32)
		if err != nil {
			return err
		}
		return direct.Exec("SELECT 1").Error
	}))
	require.NoError(t, postgres.Migrate(context.Background(), direct))
	require.NoError(t, direct.Exec("CREATE EXTENSION IF NOT EXISTS pg_stat_statements").Error)

	db, err := database.NewPostgres(dsn(delayProxy(t, "localhost:"+port, cfg.rtt/2)), cfg.pool)
	require.NoError(t, err)
	sqlDB, err := db.DB()
	require.NoError(t, err)

	env := &loadShape{cfg: cfg, direct: direct, db: db, sqlDB: sqlDB, repo: New(db)}
	env.agg = aggregate.New(aggpg.New(db), 0, 0)
	env.uc = ucrecord.New(env.repo, nil, nil, nil, nil, nil, nil, nil, nil)
	env.uc.SetAggregates(env.agg)
	// the write side of the count cache the way the commit path drives it
	// (usecase/record/aggregates.go): Touch with the association's delta,
	// which only falls back to this computation when the entry is missing
	env.def = &aggregate.Definition{
		Name: ucrecord.AggregateAssociationCounts,
		Compute: func(ctx context.Context, target string) (aggregate.Counters, error) {
			summary, err := env.repo.ComputeAssociationSummary(ctx, target, 1000)
			if err != nil {
				return aggregate.Counters{}, err
			}
			out := aggregate.Counters{Counts: map[string]int64{}, Minima: map[string]int64{}, Members: summary.Authors}
			for _, v := range summary.Variants {
				out.Counts[v.Schema+"\x1f"+v.Variant] = v.Count
				out.Minima[v.Schema+"\x1f"+v.Variant] = v.Earliest.UnixNano()
			}
			return out, nil
		},
	}

	env.seed(t)
	return env
}

// delayProxy forwards to target, holding every chunk for delay in each
// direction, and returns the port it listens on.
func delayProxy(t *testing.T, target string, delay time.Duration) string {
	t.Helper()
	ln, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })

	pipe := func(dst, src net.Conn) {
		defer dst.Close()
		buf := make([]byte, 64<<10)
		for {
			n, err := src.Read(buf)
			if n > 0 {
				time.Sleep(delay)
				if _, werr := dst.Write(buf[:n]); werr != nil {
					return
				}
			}
			if err != nil {
				return
			}
		}
	}
	go func() {
		for {
			client, err := ln.Accept()
			if err != nil {
				return
			}
			server, err := net.Dial("tcp", target)
			if err != nil {
				_ = client.Close()
				continue
			}
			go pipe(server, client)
			go pipe(client, server)
		}
	}()
	_, port, err := net.SplitHostPort(ln.Addr().String())
	require.NoError(t, err)
	return port
}

func (e *loadShape) record(repo ucrecord.Repository, owner, key string) error {
	id := fmt.Sprintf("doc%032d", e.seq.Add(1))
	now := time.Now()
	_, err := repo.ApplyRecord(context.Background(), ucrecord.RecordCommit{
		Commit:    ucrecord.Commit{ID: id, IP: "192.0.2.1", Document: fmt.Sprintf(`{"key":%q,"author":%q,"schema":"https://schema.example/post.json","value":{"body":"hello"}}`, key, owner), Proof: concrnt.Proof{Type: "none"}, Owner: owner},
		Key:       key,
		Author:    owner,
		Schema:    "https://schema.example/post.json",
		CreatedAt: now,
	})
	return err
}

// associate commits one association of author on target and folds it into
// the count cache, as Commit does.
func (e *loadShape) associate(repo ucrecord.Repository, agg *aggregate.Usecase, target, author, schema, variant string) error {
	ctx := context.Background()
	parsed, err := concrnt.ParseCCURI(target)
	if err != nil {
		return err
	}
	id := fmt.Sprintf("doc%032d", e.seq.Add(1))
	now := time.Now()
	var v *string
	if variant != "" {
		v = &variant
	}
	status, err := repo.ApplyAssociation(ctx, ucrecord.AssociationCommit{
		Commit:    ucrecord.Commit{ID: id, IP: "192.0.2.1", Document: fmt.Sprintf(`{"associate":%q,"author":%q,"schema":%q}`, target, author, schema), Proof: concrnt.Proof{Type: "none"}, Owner: parsed.Owner},
		Target:    target,
		Author:    author,
		Schema:    schema,
		Variant:   v,
		Unique:    id,
		CreatedAt: now,
	})
	if err != nil || status != ucrecord.Applied {
		return err
	}
	name := schema + "\x1f" + variant
	return agg.Touch(ctx, e.def, target, aggregate.Delta{
		Inc:        map[string]int64{name: 1},
		Min:        map[string]int64{name: now.UnixNano()},
		AddMembers: []string{author},
	}, time.Now())
}

func (e *loadShape) seed(t *testing.T) {
	t.Helper()
	repo := New(e.direct)
	agg := aggregate.New(aggpg.New(e.direct), 0, 0)
	uc := ucrecord.New(repo, nil, nil, nil, nil, nil, nil, nil, nil)
	uc.SetAggregates(agg)

	for i := range shapeHotTargets {
		e.hot = append(e.hot, fmt.Sprintf("cckv://%s/posts/hot%04d", shapeCCID("popular", i), i))
	}
	for i := range shapeColdTargets {
		e.cold = append(e.cold, fmt.Sprintf("cckv://%s/posts/cold%06d", shapeCCID("user", i%300), i))
	}

	parallel(t, 16, len(e.hot)+len(e.cold), func(i int) error {
		key := append(slices.Clone(e.hot), e.cold...)[i]
		parsed, err := concrnt.ParseCCURI(key)
		if err != nil {
			return err
		}
		if err := e.record(repo, parsed.Owner, key); err != nil {
			return err
		}
		// a fresh key gets its empty counts stored by the commit
		// (associationCountsSeededEmpty); reading it once does the same
		_, err = uc.GetAssociatedRecordCountsBySchema(context.Background(), key)
		return err
	})
	// the hot targets start with likes and a few reaction variants; every
	// tenth cold target has one like
	parallel(t, 16, len(e.hot)*e.cfg.authors, func(i int) error {
		target, n := e.hot[i%len(e.hot)], i/len(e.hot)
		if n%5 == 4 {
			return e.associate(repo, agg, target, shapeCCID("seedauthor", n), shapeReactionSchema, fmt.Sprintf("https://example.com/emoji/%d.png", n%7))
		}
		return e.associate(repo, agg, target, shapeCCID("seedauthor", n), shapeLikeSchema, "")
	})
	parallel(t, 16, len(e.cold)/10, func(i int) error {
		return e.associate(repo, agg, e.cold[i*10], shapeCCID("seedauthor", i), shapeLikeSchema, "")
	})
	require.NoError(t, e.direct.Exec("ANALYZE").Error)
}

func parallel(t *testing.T, workers, n int, fn func(i int) error) {
	t.Helper()
	var next atomic.Int64
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				i := int(next.Add(1)) - 1
				if i >= n {
					return
				}
				if err := fn(i); err != nil {
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

// latencies collects durations from many goroutines.
type latencies struct {
	mu sync.Mutex
	d  []time.Duration
}

func (l *latencies) time(fn func() error) error {
	start := time.Now()
	err := fn()
	took := time.Since(start)
	l.mu.Lock()
	l.d = append(l.d, took)
	l.mu.Unlock()
	return err
}

func (l *latencies) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.d) == 0 {
		return "n=0"
	}
	slices.Sort(l.d)
	at := func(p float64) float64 {
		return float64(l.d[min(len(l.d)-1, int(p*float64(len(l.d))))].Microseconds()) / 1000
	}
	return fmt.Sprintf("n=%d p50=%.1fms p95=%.1fms p99=%.1fms max=%.1fms", len(l.d), at(0.50), at(0.95), at(0.99), at(1))
}

// measured runs fn between two snapshots of the pool and of
// pg_stat_statements and logs what the pool waited and the statements cost.
func (e *loadShape) measured(t *testing.T, fn func()) {
	t.Helper()
	require.NoError(t, e.direct.Exec("SELECT pg_stat_statements_reset()").Error)
	before := e.sqlDB.Stats()
	start := time.Now()
	fn()
	elapsed := time.Since(start)
	after := e.sqlDB.Stats()

	waits := after.WaitCount - before.WaitCount
	avg := time.Duration(0)
	if waits > 0 {
		avg = (after.WaitDuration - before.WaitDuration) / time.Duration(waits)
	}
	t.Logf("elapsed %s; pool(%d): waits=%d avg=%.1fms total=%s open=%d", elapsed.Round(time.Millisecond), e.cfg.pool,
		waits, float64(avg.Microseconds())/1000, (after.WaitDuration - before.WaitDuration).Round(time.Millisecond), after.OpenConnections)

	var rows []struct {
		Calls   int64
		TotalMs float64
		MeanMs  float64
		Rows    int64
		Query   string
	}
	require.NoError(t, e.direct.Raw(`SELECT calls, round(total_exec_time::numeric, 0) AS total_ms, round(mean_exec_time::numeric, 3) AS mean_ms, rows, query
		FROM pg_stat_statements WHERE query NOT LIKE '%pg_stat_statements%' ORDER BY total_exec_time DESC LIMIT 12`).Scan(&rows).Error)
	var total int64
	require.NoError(t, e.direct.Raw(`SELECT COALESCE(sum(calls), 0) FROM pg_stat_statements WHERE query NOT LIKE '%pg_stat_statements%'`).Scan(&total).Error)
	t.Logf("statements: %d calls", total)
	for _, r := range rows {
		query := strings.Join(strings.Fields(r.Query), " ")
		t.Logf("  %7d calls %7.0f ms total %7.3f ms mean %7d rows  %.110s", r.Calls, r.TotalMs, r.MeanMs, r.Rows, query)
	}
}

// bystanders read cold targets and documents until stop is closed.
func (e *loadShape) bystanders(stop <-chan struct{}, wg *sync.WaitGroup, lat *latencies, failed *atomic.Int64) {
	ctx := context.Background()
	for range shapeBystanders {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				target := e.cold[rand.IntN(len(e.cold))]
				if err := lat.time(func() error {
					_, err := e.uc.GetAssociatedRecordCountsBySchema(ctx, target)
					return err
				}); err != nil {
					failed.Add(1)
				}
				if err := lat.time(func() error {
					_, err := e.repo.GetSignedDocument(ctx, target)
					return err
				}); err != nil {
					failed.Add(1)
				}
				time.Sleep(5 * time.Millisecond)
			}
		}()
	}
}

func (e *loadShape) herd(t *testing.T) {
	ctx := context.Background()
	var herd, bystander, commit latencies
	var failed atomic.Int64

	e.measured(t, func() {
		stop := make(chan struct{})
		var background, readers sync.WaitGroup
		e.bystanders(stop, &background, &bystander, &failed)

		tick := time.NewTicker(e.cfg.interval)
		defer tick.Stop()
		for round := range e.cfg.rounds {
			target := e.hot[round%len(e.hot)]
			schema, variant := shapeLikeSchema, ""
			if round%4 == 3 {
				schema, variant = shapeReactionSchema, "https://example.com/emoji/thumbsup.png"
			}
			if err := commit.time(func() error {
				return e.associate(e.repo, e.agg, target, shapeCCID("engager", round), schema, variant)
			}); err != nil {
				failed.Add(1)
			}
			// what world-app sends on an "associated" event, from every
			// session that heard it: its own associations on the target and
			// the two counts, as one batch
			for lurker := range e.cfg.lurkers {
				me := shapeCCID("lurker", lurker)
				for _, read := range []func() error{
					func() error {
						_, err := e.uc.GetAssociatedRecords(ctx, target, "", "", me, nil, nil, 100, "")
						return err
					},
					func() error {
						_, err := e.uc.GetAssociatedRecordCountsBySchema(ctx, target)
						return err
					},
					func() error {
						_, err := e.uc.GetAssociatedRecordCountsByVariant(ctx, target, shapeReactionSchema)
						return err
					},
				} {
					readers.Add(1)
					go func() {
						defer readers.Done()
						if err := herd.time(read); err != nil {
							failed.Add(1)
						}
					}()
				}
			}
			<-tick.C
		}
		readers.Wait()
		close(stop)
		background.Wait()
	})

	t.Logf("herd reads      %s", &herd)
	t.Logf("bystander reads %s", &bystander)
	t.Logf("commits         %s", &commit)
	require.Zero(t, failed.Load(), "failed operations")

	// the counts the readers were given are the primary data's
	for _, target := range e.hot {
		summary, err := e.repo.ComputeAssociationSummary(ctx, target, 100000)
		require.NoError(t, err)
		var want int64
		for _, v := range summary.Variants {
			want += v.Count
		}
		counts, err := e.uc.GetAssociatedRecordCountsBySchema(ctx, target)
		require.NoError(t, err)
		var got int64
		for _, n := range counts {
			got += n
		}
		require.Equal(t, want, got, "cached counts of %s", target)
	}
}

func (e *loadShape) writers(t *testing.T) {
	var bystander, commit latencies
	var failed atomic.Int64
	writers := e.cfg.pool * 2
	perWriter := 20

	e.measured(t, func() {
		stop := make(chan struct{})
		var background, wg sync.WaitGroup
		e.bystanders(stop, &background, &bystander, &failed)
		for w := range writers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				owner := shapeCCID("writer", w)
				for i := range perWriter {
					key := fmt.Sprintf("cckv://%s/posts/burst%04d", owner, i)
					if err := commit.time(func() error { return e.record(e.repo, owner, key) }); err != nil {
						failed.Add(1)
					}
				}
			}()
		}
		wg.Wait()
		close(stop)
		background.Wait()
	})

	t.Logf("%d writers x %d commits", writers, perWriter)
	t.Logf("bystander reads %s", &bystander)
	t.Logf("commits         %s", &commit)
	require.Zero(t, failed.Load(), "failed operations")
}

func (e *loadShape) hotKey(t *testing.T) {
	var commit latencies
	var failed atomic.Int64
	target := e.hot[0]
	writers, perWriter := e.cfg.pool/2, 10

	e.measured(t, func() {
		var wg sync.WaitGroup
		for w := range writers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := range perWriter {
					if err := commit.time(func() error {
						return e.associate(e.repo, e.agg, target, shapeCCID("fan", w*perWriter+i), shapeLikeSchema, "")
					}); err != nil {
						failed.Add(1)
					}
				}
			}()
		}
		wg.Wait()
	})

	t.Logf("%d writers x %d associations on one target", writers, perWriter)
	t.Logf("commits         %s", &commit)
	require.Zero(t, failed.Load(), "failed operations")
}

// statementLog records the SQL of every statement gorm runs.
type statementLog struct {
	logger.Interface
	mu         sync.Mutex
	statements []string
}

func (l *statementLog) Trace(_ context.Context, _ time.Time, fc func() (string, int64), _ error) {
	sql, _ := fc()
	l.mu.Lock()
	l.statements = append(l.statements, sql)
	l.mu.Unlock()
}

func (l *statementLog) take() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := l.statements
	l.statements = nil
	return out
}

func (e *loadShape) plans(t *testing.T) {
	ctx := context.Background()
	const owner = `'con1bulk' || lpad(u::text, 34, '0')`
	for _, stmt := range []string{
		`INSERT INTO commit_logs (id, ip, document, proof, owner)
			SELECT kind || u || '_' || k, '', '{"schema":"https://schema.example/post.json"}', '{"type":"none"}', ` + owner + `
			FROM generate_series(0, 2999) u, generate_series(1, 30) k, unnest(ARRAY['bulkp', 'bulkr']) kind`,
		`INSERT INTO commit_logs (id, ip, document, proof, owner)
			SELECT kind || u || '_' || k, '', '{}', '{"type":"none"}', ` + owner + `
			FROM generate_series(0, 2999) u, generate_series(1, 20) k, unnest(ARRAY['bulka', 'bulkd']) kind`,
		`INSERT INTO commit_logs (id, ip, document, proof, owner)
			SELECT 'bulke' || u, '', '{}', '{"type":"none"}', ` + owner + ` FROM generate_series(0, 2999) u`,
		`INSERT INTO records (document_id, owner, author, redirect, schema, distributions, created_at)
			SELECT kind || u || '_' || k, ` + owner + `, ` + owner + `,
				CASE kind WHEN 'bulkr' THEN 'cckv://' || ` + owner + ` || '/posts/p' || lpad(k::text, 4, '0') END,
				'https://schema.example/post.json', '{}', now() - make_interval(mins => k, secs => u % 60)
			FROM generate_series(0, 2999) u, generate_series(1, 30) k, unnest(ARRAY['bulkp', 'bulkr']) kind`,
		`INSERT INTO record_keys (uri, clean_on_update)
			SELECT 'cckv://' || ` + owner + ` || dir, false
			FROM generate_series(0, 2999) u, unnest(ARRAY['/posts', '/timeline/home']) dir`,
		`INSERT INTO record_keys (uri, parent_id, record_id, record_created_at, redirect, clean_on_update)
			SELECT parent.uri || '/' || left(r.document_id, 5) || split_part(r.document_id, '_', 2), parent.id, r.document_id, r.created_at, r.redirect, true
			FROM records r JOIN record_keys parent
				ON parent.uri = 'cckv://' || r.owner || CASE WHEN r.document_id LIKE 'bulkp%' THEN '/posts' ELSE '/timeline/home' END
			WHERE r.document_id LIKE 'bulk%'`,
		`INSERT INTO acks (document_id, "from", "to", schema, valid, created_at)
			SELECT 'bulka' || u || '_' || k, ` + owner + `, 'con1bulk' || lpad(((u + k * 37) % 3000)::text, 34, '0'), 'https://schema.example/follow.json', true, now() - make_interval(mins => k)
			FROM generate_series(0, 2999) u, generate_series(1, 20) k`,
		`INSERT INTO ackeds (document_id, "from", "to", schema, valid, created_at)
			SELECT 'bulkd' || u || '_' || k, 'con1bulk' || lpad(((u + k * 37) % 3000)::text, 34, '0'), ` + owner + `, 'https://schema.example/follow.json', true, now() - make_interval(mins => k)
			FROM generate_series(0, 2999) u, generate_series(1, 20) k`,
		`INSERT INTO entities (id, domain, tag, document_id, created_at, m_date)
			SELECT ` + owner + `, 'loadshape.example', '', 'bulke' || u, now(), now() FROM generate_series(0, 2999) u`,
		`ANALYZE`,
	} {
		require.NoError(t, e.direct.Exec(stmt).Error)
	}

	log := &statementLog{Interface: logger.Discard}
	repo := New(e.direct.Session(&gorm.Session{Logger: log}))
	user := func(u int) string { return fmt.Sprintf("con1bulk%034d", u) }
	post := func(u, k int) string { return fmt.Sprintf("cckv://%s/posts/bulkp%d", user(u), k) }
	home := func(u int) string { return fmt.Sprintf("cckv://%s/timeline/home", user(u)) }
	var posts, homes []string
	for i := range 100 {
		posts = append(posts, post(i*7, 1+i%30))
		homes = append(homes, home(i*11))
	}
	since, until := time.Now().Add(-20*time.Minute), time.Now().Add(-5*time.Minute)
	chunk := time.Now().Unix() / 600
	variant := "https://example.com/emoji/1.png"
	commit := func(id string) ucrecord.Commit {
		return ucrecord.Commit{ID: id, IP: "192.0.2.1", Document: "{}", Proof: concrnt.Proof{Type: "none"}, Owner: user(1)}
	}

	calls := []struct {
		name string
		call func() error
	}{
		{"GetSignedDocument", func() error { _, err := repo.GetSignedDocument(ctx, posts[3]); return err }},
		{"GetSignedDocument(ccfs)", func() error {
			_, err := repo.GetSignedDocument(ctx, concrnt.ComposeCCFSURI(user(5), concrnt.CCFSTypeConcrnt, "bulkp5_3"))
			return err
		}},
		{"GetSignedDocuments(16)", func() error { _, err := repo.GetSignedDocuments(ctx, posts[:16]); return err }},
		{"GetSignedDocuments(100)", func() error { _, err := repo.GetSignedDocuments(ctx, posts); return err }},
		{"GetHierarchicalRecordPolicies(ccfs)", func() error {
			_, err := repo.GetHierarchicalRecordPolicies(ctx, concrnt.ComposeCCFSURI(user(5), concrnt.CCFSTypeConcrnt, "bulkp5_3"))
			return err
		}},
		{"GetHierarchicalRecordPoliciesMulti(1)", func() error { _, err := repo.GetHierarchicalRecordPoliciesMulti(ctx, posts[:1]); return err }},
		{"GetHierarchicalRecordPoliciesMulti(16)", func() error { _, err := repo.GetHierarchicalRecordPoliciesMulti(ctx, posts[:16]); return err }},
		{"GetHierarchicalRecordPoliciesMulti(100)", func() error { _, err := repo.GetHierarchicalRecordPoliciesMulti(ctx, posts); return err }},
		{"QueryByPrefix", func() error { _, err := repo.QueryByPrefix(ctx, home(3)+"/", "", "", nil, nil, 17, "desc"); return err }},
		{"QueryByPrefix(schema)", func() error {
			_, err := repo.QueryByPrefix(ctx, home(3)+"/", "https://schema.example/post.json", "", nil, nil, 101, "desc")
			return err
		}},
		{"QueryByPrefix(schema without rows)", func() error {
			_, err := repo.QueryByPrefix(ctx, home(3)+"/", "https://schema.example/community.json", "", nil, nil, 101, "desc")
			return err
		}},
		{"QueryByPrefix(author, since, until)", func() error {
			_, err := repo.QueryByPrefix(ctx, home(3)+"/", "", user(3), &since, &until, 17, "asc")
			return err
		}},
		{"QueryByPrefix(owner)", func() error {
			_, err := repo.QueryByPrefix(ctx, "cckv://"+user(3)+"/", "", "", nil, &until, 17, "desc")
			return err
		}},
		{"QueryByParent", func() error {
			_, err := repo.QueryByParent(ctx, "cckv://"+user(3)+"/posts", "", "", nil, nil, 101, "asc")
			return err
		}},
		{"QueryByParent(schema, until)", func() error {
			_, err := repo.QueryByParent(ctx, home(3), "https://schema.example/post.json", "", nil, &until, 17, "desc")
			return err
		}},
		{"QueryByParent(author)", func() error {
			_, err := repo.QueryByParent(ctx, home(3), "", user(3), nil, nil, 17, "desc")
			return err
		}},
		{"QueryByParent(owner root)", func() error {
			_, err := repo.QueryByParent(ctx, "cckv://"+user(3), "", "", nil, nil, 17, "desc")
			return err
		}},
		{"QueryByParentOrderByKey", func() error {
			_, err := repo.QueryByParentOrderByKey(ctx, home(3), "", "", nil, nil, 17, "asc")
			return err
		}},
		{"QueryRecordSubtree", func() error {
			_, err := repo.QueryRecordSubtree(ctx, "cckv://"+user(3)+"/posts", true, "", 100)
			return err
		}},
		{"GetTimelineRemoval", func() error { _, _, err := repo.GetTimelineRemoval(ctx, home(3)+"/bulkr7"); return err }},
		{"GetAssociatedRecords", func() error {
			_, err := repo.GetAssociatedRecords(ctx, e.hot[0], "", "", "", nil, nil, 101, "desc")
			return err
		}},
		{"GetAssociatedRecords(author)", func() error {
			_, err := repo.GetAssociatedRecords(ctx, e.hot[0], "", "", shapeCCID("seedauthor", 3), nil, nil, 101, "desc")
			return err
		}},
		{"GetAssociatedRecords(schema, variant)", func() error {
			_, err := repo.GetAssociatedRecords(ctx, e.hot[0], shapeReactionSchema, variant, "", &since, nil, 2, "asc")
			return err
		}},
		{"ComputeAssociationSummary", func() error { _, err := repo.ComputeAssociationSummary(ctx, e.hot[0], 1000); return err }},
		{"GetAcknowledgeRecords(from)", func() error {
			_, err := repo.GetAcknowledgeRecords(ctx, user(3), "", "https://schema.example/follow.json", nil, nil, 101, "desc")
			return err
		}},
		{"GetAcknowledgeRecords(to)", func() error {
			_, err := repo.GetAcknowledgeRecords(ctx, "", user(3), "https://schema.example/follow.json", nil, nil, 101, "desc")
			return err
		}},
		{"ComputeAcknowledgeCounts(from)", func() error { _, err := repo.ComputeAcknowledgeCounts(ctx, user(3), "", ""); return err }},
		{"ComputeAcknowledgeCounts(to)", func() error { _, err := repo.ComputeAcknowledgeCounts(ctx, "", user(3), ""); return err }},
		{"GetEntityByCCID", func() error { _, err := repo.GetEntityByCCID(ctx, user(3)); return err }},
		{"GetEntityByAlias", func() error { _, _ = repo.GetEntityByAlias(ctx, "nobody.example"); return nil }},
		{"GetChunklineManifest", func() error { _, err := repo.GetChunklineManifest(ctx, home(3)); return err }},
		{"LookupLocalItrs(21)", func() error { _, err := repo.LookupLocalItrs(ctx, homes[:21], chunk); return err }},
		{"LoadLocalBody", func() error { _, err := repo.LoadLocalBody(ctx, home(3), chunk); return err }},
		{"QueryCommitLogs", func() error { _, err := repo.QueryCommitLogs(ctx, user(3), nil, nil, 100, "asc"); return err }},
		{"ApplyRecord(new key)", func() error {
			_, err := repo.ApplyRecord(ctx, ucrecord.RecordCommit{Commit: commit("plan-new"), Key: post(1, 9000), Author: user(1), Schema: "s", CreatedAt: time.Now()})
			return err
		}},
		{"ApplyRecord(overwrite)", func() error {
			_, err := repo.ApplyRecord(ctx, ucrecord.RecordCommit{Commit: commit("plan-over"), Key: post(1, 9000), Author: user(1), Schema: "s", CreatedAt: time.Now()})
			return err
		}},
		{"ApplyAssociation", func() error {
			_, err := repo.ApplyAssociation(ctx, ucrecord.AssociationCommit{Commit: commit("plan-assoc"), Target: post(1, 9000), Author: user(2), Schema: shapeLikeSchema, Unique: "plan-assoc", CreatedAt: time.Now()})
			return err
		}},
		{"ApplyAck", func() error {
			_, err := repo.ApplyAck(ctx, ucrecord.AckCommit{Commit: commit("plan-ack"), Side: ucrecord.AckerSide, From: user(1), To: user(2999), Schema: "https://schema.example/follow.json", Valid: true, CreatedAt: time.Now()})
			return err
		}},
		{"ApplyDelete(association)", func() error {
			_, err := repo.ApplyDelete(ctx, ucrecord.DeleteCommit{Commit: new(commit("plan-del-assoc")), Removals: []ucrecord.Removal{{Kind: ucrecord.RemoveAssociation, DocumentID: "plan-assoc"}}})
			return err
		}},
		{"ApplyDelete(key)", func() error {
			_, err := repo.ApplyDelete(ctx, ucrecord.DeleteCommit{Commit: new(commit("plan-del-key")), Removals: []ucrecord.Removal{{Kind: ucrecord.RemoveKey, URI: post(1, 9000)}}, GcCommitIDs: []string{"plan-over"}})
			return err
		}},
	}

	// a statement is suspect when it reads one of the tables that grow
	// with the data from end to end
	scanned := 0
	for _, c := range calls {
		log.take()
		require.NoError(t, c.call(), c.name)
		for _, stmt := range log.take() {
			var plan []string
			if err := e.direct.Raw("EXPLAIN " + stmt).Scan(&plan).Error; err != nil {
				t.Logf("%s: cannot explain %.120s: %v", c.name, stmt, err)
				continue
			}
			text := strings.Join(plan, "\n")
			if strings.Contains(text, "Seq Scan on") {
				scanned++
				t.Logf("%s scans sequentially:\n%s\n%s", c.name, strings.Join(strings.Fields(stmt), " "), text)
			}
		}
	}
	t.Logf("%d repository calls explained, %d statements with a sequential scan", len(calls), scanned)

	// a foreign key no index starts with is a sequential scan of the
	// referencing table on every delete or key update of the referenced row
	var uncovered []string
	require.NoError(t, e.direct.Raw(`SELECT c.conrelid::regclass || '(' || a.attname || ') -> ' || c.confrelid::regclass
		FROM pg_constraint c JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = c.conkey[1]
		WHERE c.contype = 'f' AND NOT EXISTS (
			SELECT 1 FROM pg_index i WHERE i.indrelid = c.conrelid AND i.indkey[0] = c.conkey[1])`).Scan(&uncovered).Error)
	t.Logf("foreign keys without an index: %v", uncovered)
}
