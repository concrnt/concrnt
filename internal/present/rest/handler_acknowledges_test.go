package rest

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/concrnt/concrnt/internal/domain"
	"github.com/concrnt/concrnt/internal/usecase/record"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/require"
)

// acknowledgesRecordRepo fails loudly if the handler lets an invalid filter
// through to the repository.
type acknowledgesRecordRepo struct {
	record.Repository
	called bool
}

func (r *acknowledgesRecordRepo) GetAcknowledgeRecords(ctx context.Context, from, to, schema string, since, until *time.Time, limit int, order string) ([]record.QueryRow, error) {
	r.called = true
	return nil, errors.New("repository must not be reached for an invalid filter")
}

func (r *acknowledgesRecordRepo) GetAcknowledgeRecordCounts(ctx context.Context, from, to, schema string) (map[string]int64, error) {
	r.called = true
	return nil, errors.New("repository must not be reached for an invalid filter")
}

// CIP-10 §6: /acknowledges and /acknowledge-counts take exactly one of from /
// to — each names a different held side (the acker's acks vs. the target's
// ackeds), so both at once or neither is a 400.
func TestAcknowledgesRequireExactlyOneSide(t *testing.T) {
	cfg := domain.Config{FQDN: "example.com"}

	for _, tc := range []struct{ name, query string }{
		{"neither", "schema=https://example.com/follow.json"},
		{"both", "from=con1alice&to=con1bob"},
	} {
		for _, ep := range []struct {
			path    string
			handler func(*Handler) echo.HandlerFunc
		}{
			{"/acknowledges", func(h *Handler) echo.HandlerFunc { return h.handleAcknowledges }},
			{"/acknowledge-counts", func(h *Handler) echo.HandlerFunc { return h.handleAcknowledgeCounts }},
		} {
			t.Run(ep.path+"/"+tc.name, func(t *testing.T) {
				repo := &acknowledgesRecordRepo{}
				h := &Handler{
					config: cfg,
					record: record.New(repo, nil, nil, &cfg, nil, nil, nil, nil, nil),
				}

				e := echo.New()
				req := httptest.NewRequest(http.MethodGet, ep.path+"?"+tc.query, nil)
				rec := httptest.NewRecorder()
				require.NoError(t, ep.handler(h)(e.NewContext(req, rec)))
				require.Equal(t, http.StatusBadRequest, rec.Code, "body: %s", rec.Body.String())
				require.False(t, repo.called, "an invalid filter must be rejected before reaching the repository")
			})
		}
	}
}
