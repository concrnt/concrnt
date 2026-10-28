package rest

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/require"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/chunkline"
	"github.com/concrnt/concrnt/internal/domain"
	chunklineuc "github.com/concrnt/concrnt/internal/usecase/chunkline"
	"github.com/concrnt/concrnt/internal/usecase/record"
)

// countingRecordRepo serves canned data and counts every repository call by
// method, so the request paths' round-trip budgets are pinned.
type countingRecordRepo struct {
	record.Repository
	calls map[string]int
	rows  []record.QueryRow
}

func (r *countingRecordRepo) hit(method string) {
	if r.calls == nil {
		r.calls = map[string]int{}
	}
	r.calls[method]++
}

func (r *countingRecordRepo) GetSignedDocument(ctx context.Context, uri string) (*concrnt.SignedDocument, error) {
	r.hit("GetSignedDocument")
	return &concrnt.SignedDocument{CCKV: &uri, Document: `{"kind":"record","key":"` + uri + `"}`}, nil
}

func (r *countingRecordRepo) GetHierarchicalRecordPolicies(ctx context.Context, uri string) ([]concrnt.Policy, error) {
	r.hit("GetHierarchicalRecordPolicies")
	return []concrnt.Policy{}, nil
}

func (r *countingRecordRepo) GetHierarchicalRecordPoliciesMulti(ctx context.Context, uris []string) (map[string][]concrnt.Policy, error) {
	r.hit("GetHierarchicalRecordPoliciesMulti")
	return map[string][]concrnt.Policy{}, nil
}

func (r *countingRecordRepo) LoadLocalBody(ctx context.Context, uri string, chunkID int64) ([]chunkline.BodyItem, error) {
	r.hit("LoadLocalBody")
	return []chunkline.BodyItem{}, nil
}

func (r *countingRecordRepo) GetAssociatedRecords(ctx context.Context, targetURI, schema, variant, author string, since, until *time.Time, limit int, order string) ([]record.QueryRow, error) {
	r.hit("GetAssociatedRecords")
	return r.rows, nil
}

func (r *countingRecordRepo) QueryByParent(ctx context.Context, parent, schema, author string, since, until *time.Time, limit int, order string) ([]record.QueryRow, error) {
	r.hit("QueryByParent")
	return r.rows, nil
}

// Every representative read is served within its round-trip budget (C1):
// the budget is the number of repository calls, whatever the page size.
func TestRequestRoundTripBudgets(t *testing.T) {
	cfg := domain.Config{FQDN: "example.com"}
	rows := make([]record.QueryRow, 0, 20)
	for i := range 20 {
		key := "cckv://con1alice/app/" + string(rune('a'+i))
		rows = append(rows, record.QueryRow{Row: concrnt.SignedDocument{CCKV: &key, Document: `{"kind":"record","key":"` + key + `"}`}, CreatedAt: time.Unix(int64(i), 0)})
	}

	for _, tc := range []struct {
		name   string
		path   string
		accept string
		handle func(h *Handler, c echo.Context) error
		want   map[string]int
	}{
		{
			name:   "resolve a record",
			path:   "/resolve?uri=" + url.QueryEscape("cckv://con1alice/app/a"),
			handle: (*Handler).handleResolve,
			want:   map[string]int{"GetSignedDocument": 1, "GetHierarchicalRecordPolicies": 1},
		},
		{
			name:   "chunkline body",
			path:   "/chunkline/body/1?uri=" + url.QueryEscape("cckv://con1alice/timeline"),
			handle: (*Handler).handleChunklineBody,
			want:   map[string]int{"LoadLocalBody": 1},
		},
		{
			name:   "associations",
			path:   "/associations?uri=" + url.QueryEscape("cckv://con1alice/app/a"),
			handle: (*Handler).handleAssociations,
			want:   map[string]int{"GetAssociatedRecords": 1},
		},
		{
			name:   "query a page of 20",
			path:   "/query?parent=" + url.QueryEscape("cckv://con1alice/app") + "&limit=20",
			handle: (*Handler).handleQuery,
			want:   map[string]int{"QueryByParent": 1, "GetHierarchicalRecordPoliciesMulti": 1},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			repo := &countingRecordRepo{rows: rows}
			recordUC := record.New(repo, nil, nil, &cfg, nil, nil, allowAllPolicy{}, nil, nil)
			h := &Handler{
				config:    cfg,
				record:    recordUC,
				chunkline: chunklineuc.New(repo, nil, nil, recordUC),
			}

			e := echo.New()
			req := httptest.NewRequest(http.MethodGet, tc.path, nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)
			if tc.name == "chunkline body" {
				c.SetParamNames("chunk")
				c.SetParamValues("1")
			}
			require.NoError(t, tc.handle(h, c))
			require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
			require.Equal(t, tc.want, repo.calls)
		})
	}
}
