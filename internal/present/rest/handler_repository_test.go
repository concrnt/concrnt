package rest

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/require"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/impl/interop"
	"github.com/concrnt/concrnt/internal/domain"
	"github.com/concrnt/concrnt/internal/usecase/record"
)

// committedRecordRepo answers HasCommitLog true for every id, so Commit
// returns before verification: the handler can be driven with unsigned lines.
type committedRecordRepo struct{ record.Repository }

func (committedRecordRepo) HasCommitLog(ctx context.Context, id string) (bool, error) {
	return true, nil
}

func repositoryImportLine(t *testing.T) string {
	t.Helper()
	docBytes, err := json.Marshal(concrnt.Document[any]{Kind: "record", CreatedAt: time.Now()})
	require.NoError(t, err)
	line, err := json.Marshal(concrnt.SignedDocument{Document: string(docBytes), Proof: concrnt.Proof{Type: concrnt.ProofTypeNone}})
	require.NoError(t, err)
	return string(line)
}

func newRepositoryTestHandler() *Handler {
	cfg := domain.Config{FQDN: "example.com", CSID: "ccs1test"}
	return &Handler{
		config: cfg,
		record: record.New(committedRecordRepo{}, nil, nil, &cfg, nil, nil, nil, nil, nil),
	}
}

func TestImportRepositoryRequiresAuthentication(t *testing.T) {
	h := newRepositoryTestHandler()
	e := echo.New()

	call := func(t *testing.T, ctx context.Context, body io.Reader) *httptest.ResponseRecorder {
		t.Helper()
		req := httptest.NewRequest(http.MethodPost, "/api/v2/repository", body).WithContext(ctx)
		rec := httptest.NewRecorder()
		err := h.handleImportRepository(e.NewContext(req, rec))
		if err != nil {
			e.HTTPErrorHandler(err, e.NewContext(req, rec))
		}
		return rec
	}

	t.Run("anonymous", func(t *testing.T) {
		rec := call(t, context.Background(), strings.NewReader(repositoryImportLine(t)+"\n"))
		require.Equal(t, http.StatusForbidden, rec.Code, "body: %s", rec.Body.String())
	})

	t.Run("user", func(t *testing.T) {
		ctx := context.WithValue(context.Background(), interop.RequesterCtxKey, domain.Entity{ID: "con1user"})
		rec := call(t, ctx, strings.NewReader(repositoryImportLine(t)+"\n"))
		require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
		require.JSONEq(t, "[]", strings.TrimSpace(rec.Body.String()))
	})

	t.Run("service account", func(t *testing.T) {
		ctx := context.WithValue(context.Background(), interop.RequesterCtxKey, h.config.CSID)
		ctx = context.WithValue(ctx, interop.ServiceAccountTypeCtxKey, "system")
		rec := call(t, ctx, strings.NewReader(repositoryImportLine(t)+"\n"))
		require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
	})
}

// The body is consumed line by line: memory stays bounded by one line, a line
// over record.MaxImportLineBytes or a body over maxImportBodyBytes is a 413.
func TestImportRepositoryBodyLimits(t *testing.T) {
	h := newRepositoryTestHandler()
	e := echo.New()
	ctx := context.WithValue(context.Background(), interop.RequesterCtxKey, domain.Entity{ID: "con1user"})

	call := func(t *testing.T, body io.Reader) *httptest.ResponseRecorder {
		t.Helper()
		req := httptest.NewRequest(http.MethodPost, "/api/v2/repository", body).WithContext(ctx)
		req.ContentLength = -1
		rec := httptest.NewRecorder()
		err := h.handleImportRepository(e.NewContext(req, rec))
		if err != nil {
			e.HTTPErrorHandler(err, e.NewContext(req, rec))
		}
		return rec
	}

	t.Run("many lines", func(t *testing.T) {
		line := repositoryImportLine(t) + "\n"
		rec := call(t, strings.NewReader(strings.Repeat(line, 5000)))
		require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
		require.JSONEq(t, "[]", strings.TrimSpace(rec.Body.String()))
	})

	t.Run("over-long line", func(t *testing.T) {
		body := repositoryImportLine(t) + "\n" + strings.Repeat("a", record.MaxImportLineBytes+1) + "\n"
		rec := call(t, strings.NewReader(body))
		require.Equal(t, http.StatusRequestEntityTooLarge, rec.Code, "body: %s", rec.Body.String())
	})

	t.Run("body over the cap", func(t *testing.T) {
		// 2048 lines of 64 KiB each (within the line limit) exceed 64 MiB in total
		chunk := strings.Repeat("a", 64<<10) + "\n"
		var readers []io.Reader
		for range 2048 {
			readers = append(readers, strings.NewReader(chunk))
		}
		rec := call(t, io.MultiReader(readers...))
		require.Equal(t, http.StatusRequestEntityTooLarge, rec.Code, "body: %s", rec.Body.String())
	})
}

// Every /api/v2 route is capped at 1M by the group BodyLimit; /repository
// and /batch opt out and carry their own caps.
func TestApiGroupBodyLimit(t *testing.T) {
	h := newRepositoryTestHandler()
	app := echo.New()
	withUser := func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			ctx := context.WithValue(c.Request().Context(), interop.RequesterCtxKey, domain.Entity{ID: "con1user"})
			c.SetRequest(c.Request().WithContext(ctx))
			return next(c)
		}
	}
	h.RegisterRoutes(app, app.Group("", withUser))

	post := func(path string, body io.Reader) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, path, body)
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		app.ServeHTTP(rec, req)
		return rec
	}

	t.Run("commit over 1M is rejected", func(t *testing.T) {
		rec := post("/api/v2/commit", strings.NewReader(`{"document":"`+strings.Repeat("a", 2<<20)+`"}`))
		require.Equal(t, http.StatusRequestEntityTooLarge, rec.Code)
	})

	t.Run("repository over 1M passes the group limit", func(t *testing.T) {
		line := repositoryImportLine(t) + "\n"
		body := strings.Repeat(line, (2<<20)/len(line)+1)
		rec := post("/api/v2/repository", strings.NewReader(body))
		require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
	})
}
