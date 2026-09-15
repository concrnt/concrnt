package rest

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/concrnt/concrnt/internal/domain"
	"github.com/concrnt/concrnt/internal/usecase/record"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/require"
)

// replicationRecordRepo records the window the handler resolved; it serves an
// empty page so the usecase never reaches the (nil) policy service.
type replicationRecordRepo struct {
	record.Repository
	called bool
	owner  string
	since  *time.Time
	until  *time.Time
	limit  int
	order  string
}

func (r *replicationRecordRepo) QueryCommitLogs(ctx context.Context, owner string, since, until *time.Time, limit int, order string) ([]record.QueryRow, error) {
	r.called = true
	r.owner, r.since, r.until, r.limit, r.order = owner, since, until, limit, order
	return nil, nil
}

// CIP-16 §3.1: /replication shares the query window parameters but iterates
// forward (asc) by default; malformed windows are rejected before the
// repository is reached.
func TestReplicationWindow(t *testing.T) {
	cfg := domain.Config{FQDN: "example.com"}

	call := func(t *testing.T, query string) (*replicationRecordRepo, *httptest.ResponseRecorder) {
		t.Helper()
		repo := &replicationRecordRepo{}
		h := &Handler{
			config: cfg,
			record: record.New(repo, nil, nil, &cfg, nil, nil, nil, nil, nil),
		}
		e := echo.New()
		req := httptest.NewRequest(http.MethodGet, "/replication?"+query, nil)
		rec := httptest.NewRecorder()
		require.NoError(t, h.handleReplication(e.NewContext(req, rec)))
		return repo, rec
	}

	t.Run("defaults", func(t *testing.T) {
		repo, rec := call(t, "")
		require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
		require.True(t, repo.called)
		require.Equal(t, "", repo.owner)
		require.Nil(t, repo.since)
		require.Nil(t, repo.until)
		require.Equal(t, 11, repo.limit, "default limit 10 plus the peeked row")
		require.Equal(t, "asc", repo.order)
		require.JSONEq(t, `{"items":[],"prev":null,"next":null}`, rec.Body.String())
	})

	t.Run("explicit window passes through", func(t *testing.T) {
		repo, rec := call(t, "owner=con1x&since=2026-09-15T00:00:00Z&until=2026-09-15T01:00:00.5Z&limit=5&order=desc")
		require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
		require.Equal(t, "con1x", repo.owner)
		require.Equal(t, time.Date(2026, 9, 15, 0, 0, 0, 0, time.UTC), repo.since.UTC())
		require.Equal(t, time.Date(2026, 9, 15, 1, 0, 0, 500_000_000, time.UTC), repo.until.UTC())
		require.Equal(t, 6, repo.limit)
		require.Equal(t, "desc", repo.order)
	})

	for _, tc := range []struct{ name, query string }{
		{"invalid order", "order=sideways"},
		{"invalid since", "since=notatime"},
		{"invalid until", "until=notatime"},
		{"invalid limit", "limit=many"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			repo, rec := call(t, tc.query)
			require.Equal(t, http.StatusBadRequest, rec.Code, "body: %s", rec.Body.String())
			require.False(t, repo.called, "an invalid window must be rejected before reaching the repository")
		})
	}
}
