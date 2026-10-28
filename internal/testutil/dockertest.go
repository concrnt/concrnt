package testutil

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/labstack/echo/v4"
	"github.com/ory/dockertest"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"

	pgrepo "github.com/concrnt/concrnt/internal/infra/repository/postgres"
)

var (
	user        = "postgres"
	password    = "secret"
	dbName      = "unittest"
	dialect     = "postgres"
	dsnTemplate = "postgres://%s:%s@localhost:%s/%s?sslmode=disable"
)

var pool *dockertest.Pool
var poolLock = &sync.Mutex{}
var dbLock = &sync.Mutex{}

var tracer = otel.Tracer("auth")

func SetupMockTraceProvider() *tracetest.InMemoryExporter {

	spanChecker := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(spanChecker))
	otel.SetTracerProvider(provider)

	return spanChecker
}

func SetupTraceCtx() (context.Context, string) {
	ctx, span := tracer.Start(context.Background(), "testRoot")
	defer span.End()

	traceID := span.SpanContext().TraceID().String()

	return ctx, traceID
}

func CreateHttpRequest() (echo.Context, *http.Request, *httptest.ResponseRecorder, string) {
	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()

	c := e.NewContext(req, rec)
	ctx, span := tracer.Start(c.Request().Context(), "testRoot")
	defer span.End()
	c.SetRequest(c.Request().WithContext(ctx))
	traceID := span.SpanContext().TraceID().String()

	return c, req, rec, traceID
}

func PrintSpans(spans tracetest.SpanStubs, traceID string) {
	fmt.Print("--------------------------------\n")

	var found bool = false

	for _, span := range spans {
		if !(span.SpanContext.TraceID().String() == traceID) {
			continue
		}

		found = true

		fmt.Printf("Name: %s\n", span.Name)
		fmt.Printf("TraceID: %s\n", span.SpanContext.TraceID().String())
		fmt.Printf("Attributes:\n")
		for _, attr := range span.Attributes {
			fmt.Printf("  %s: %s: %s\n", attr.Key, attr.Value.Type().String(), attr.Value.AsString())
		}
		fmt.Printf("Events:\n")
		for _, event := range span.Events {
			fmt.Printf("  %s\n", event.Name)
			for _, attr := range event.Attributes {
				fmt.Printf("    %s: %s: %s\n", attr.Key, attr.Value.Type().String(), attr.Value.AsString())
			}
		}
		fmt.Print("--------------------------------\n")
	}

	if !found {
		fmt.Print("Span not found. spans:\n")
		for _, span := range spans {
			fmt.Printf("%s(%s)\n", span.Name, span.SpanContext.TraceID().String())
		}
	}
}

func CreateDB() (*gorm.DB, func()) {
	dbLock.Lock()
	defer dbLock.Unlock()

	pool := getPool()

	runOptions := &dockertest.RunOptions{
		Name:       getCallerName() + "_postgres_" + getSuffix(),
		Repository: "postgres",
		Tag:        "latest",
		Env: []string{
			"POSTGRES_USER=" + user,
			"POSTGRES_PASSWORD=" + password,
			"POSTGRES_DB=" + dbName,
		},
		ExposedPorts: []string{"5432/tcp"},
	}

	resource, err := pool.RunWithOptions(runOptions)
	if err != nil {
		log.Fatalf("Could not start resource: %s", err)
	}
	cleanup := func() {
		closeContainer(pool, resource)
	}

	port := resource.GetPort("5432/tcp")
	log.Printf("Postgres running on port %s\n", port)
	dsn := fmt.Sprintf(dsnTemplate, user, password, port, dbName)
	log.Printf("DSN: %s\n", dsn)

	var db *gorm.DB
	if err := pool.Retry(func() error {
		time.Sleep(time.Second * 2)

		var err error

		db, err = gorm.Open(postgres.Open(dsn), &gorm.Config{})
		return err
	}); err != nil {
		log.Fatalf("Could not connect to docker: %s", err)
	}

	if err := pgrepo.Migrate(context.Background(), db); err != nil {
		log.Fatalf("Could not migrate database: %s", err)
	}

	return db, cleanup
}

func CreateMC() (*memcache.Client, func()) {

	pool := getPool()

	runOptions := &dockertest.RunOptions{
		Name:       getCallerName() + "_memcached_" + getSuffix(),
		Repository: "memcached",
		Tag:        "1.6.7",
		Env: []string{
			"MEMCACHED_ENABLE_TLS=false",
		},
		ExposedPorts: []string{"11211/tcp"},
	}

	resource, err := pool.RunWithOptions(runOptions)
	if err != nil {
		log.Fatalf("Could not start resource: %s", err)
	}
	cleanup := func() {
		closeContainer(pool, resource)
	}

	port := resource.GetPort("11211/tcp")
	log.Printf("Memcached running on port %s", port)

	// Memcached(コンテナ)との接続
	var client *memcache.Client
	if err := pool.Retry(func() error {
		time.Sleep(time.Second * 1)

		var err error
		client = memcache.New("localhost:" + port)
		return err
	}); err != nil {
		log.Fatalf("Could not connect to docker: %s", err)
	}
	return client, cleanup
}

func CreateRDB() (*redis.Client, func()) {

	pool := getPool()

	runOptions := &dockertest.RunOptions{
		Name:       getCallerName() + "_redis_" + getSuffix(),
		Repository: "redis",
		Tag:        "latest",
		Env: []string{
			"REDIS_PASSWORD=secret",
		},
		ExposedPorts: []string{"6379/tcp"},
	}

	resource, err := pool.RunWithOptions(runOptions)
	if err != nil {
		log.Fatalf("Could not start resource: %s", err)
	}
	cleanup := func() {
		closeContainer(pool, resource)
	}

	port := resource.GetPort("6379/tcp")
	log.Printf("Redis running on port %s", port)

	// Redis(コンテナ)との接続
	var client *redis.Client
	if err := pool.Retry(func() error {
		time.Sleep(time.Second * 1)

		var err error
		client = redis.NewClient(&redis.Options{
			Addr:     "localhost:" + port,
			Password: "secret",
			DB:       0,
		})
		return err
	}); err != nil {
		log.Fatalf("Could not connect to docker: %s", err)
	}
	return client, cleanup
}

func closeContainer(pool *dockertest.Pool, resource *dockertest.Resource) {
	// コンテナの終了
	if err := pool.Purge(resource); err != nil {
		log.Fatalf("Could not purge resource: %s", err)
	}
}

func getPool() *dockertest.Pool {
	poolLock.Lock()
	defer poolLock.Unlock()
	if pool == nil {
		var err error
		pool, err = dockertest.NewPool("")
		pool.MaxWait = time.Second * 10
		if err != nil {
			log.Fatalf("Could not connect to docker: %s", err)
		}
	}
	return pool
}

func getSuffix() string {
	now := time.Now()
	return now.Format("0102150405")
}

func getCallerName() string {
	pc, file, line, ok := runtime.Caller(2)
	if !ok {
		return "unknown"
	}

	fn := runtime.FuncForPC(pc)
	if fn != nil {
		return strings.Replace(
			fn.Name(),
			"/", "-", -1,
		)
	}

	return fmt.Sprintf("%s-%d", strings.Replace(file, "/", "-", -1), line)
}
