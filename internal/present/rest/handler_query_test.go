package rest

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/internal/domain"
	"github.com/concrnt/concrnt/internal/usecase/record"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/require"
)

// queryRecordRepo records which listing the handler reached and the raw
// key cursors it passed along.
type queryRecordRepo struct {
	record.Repository
	called      bool
	gotSinceKey *string
	gotUntilKey *string
}

func (r *queryRecordRepo) QueryByPrefix(ctx context.Context, prefix, schema, author string, since, until *time.Time, limit int, order string) ([]record.QueryRow, error) {
	r.called = true
	return nil, errors.New("repository must not be reached for an invalid filter")
}

func (r *queryRecordRepo) QueryByParent(ctx context.Context, parent, schema, author string, since, until *time.Time, limit int, order string) ([]record.QueryRow, error) {
	r.called = true
	return nil, errors.New("repository must not be reached for an invalid filter")
}

func (r *queryRecordRepo) QueryByParentOrderByKey(ctx context.Context, parent, schema, author string, since, until *string, limit int, order string) ([]record.QueryRow, error) {
	r.called = true
	r.gotSinceKey = since
	r.gotUntilKey = until
	return []record.QueryRow{}, nil
}

func (r *queryRecordRepo) GetHierarchicalRecordPolicies(ctx context.Context, uri string) ([]concrnt.Policy, error) {
	return []concrnt.Policy{}, nil
}

// CIP-5 §3.1: orderby is createdAt or key, and key is only valid with parent.
func TestQueryOrderByValidation(t *testing.T) {
	cfg := domain.Config{FQDN: "example.com"}

	for _, tc := range []struct{ name, query string }{
		{"unknown orderby", "parent=cckv://con1alice&orderby=bogus"},
		{"key with prefix", "prefix=cckv://con1alice/&orderby=key"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			repo := &queryRecordRepo{}
			h := &Handler{
				config: cfg,
				record: record.New(repo, nil, nil, &cfg, nil, nil, nil, nil, nil),
			}

			e := echo.New()
			req := httptest.NewRequest(http.MethodGet, "/query?"+tc.query, nil)
			rec := httptest.NewRecorder()
			require.NoError(t, h.handleQuery(e.NewContext(req, rec)))
			require.Equal(t, http.StatusBadRequest, rec.Code, "body: %s", rec.Body.String())
			require.False(t, repo.called, "an invalid filter must be rejected before reaching the repository")
		})
	}
}

// CIP-5 §3.3: with orderby=key the since/until cursors are keys and reach
// the repository verbatim instead of being parsed as datetimes.
func TestQueryOrderByKeyPassesKeyCursors(t *testing.T) {
	cfg := domain.Config{FQDN: "example.com"}
	repo := &queryRecordRepo{}
	h := &Handler{
		config: cfg,
		record: record.New(repo, nil, nil, &cfg, nil, nil, nil, nil, nil),
	}

	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/query?parent=cckv://con1alice/app&orderby=key&since=cckv://con1alice/app/b&order=asc", nil)
	rec := httptest.NewRecorder()
	require.NoError(t, h.handleQuery(e.NewContext(req, rec)))
	require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
	require.True(t, repo.called)
	require.NotNil(t, repo.gotSinceKey)
	require.Equal(t, "cckv://con1alice/app/b", *repo.gotSinceKey)
	require.Nil(t, repo.gotUntilKey)
	require.JSONEq(t, `{"items":[],"prev":null,"next":null}`, rec.Body.String())
}
