package rest

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/internal/domain"
	"github.com/concrnt/concrnt/internal/usecase/ratelimit"
	"github.com/concrnt/concrnt/internal/usecase/record"
)

// strangerRecordRepo holds no commit log and no entity: every author is
// unknown to it.
type strangerRecordRepo struct{ record.Repository }

func (strangerRecordRepo) HasCommitLog(ctx context.Context, id string) (bool, error) {
	return false, nil
}

func (strangerRecordRepo) GetEntityByCCID(ctx context.Context, ccid string) (*domain.Entity, error) {
	return nil, domain.ErrNotFound
}

type fullLimiter struct{}

func (fullLimiter) Allow(ctx context.Context, key string, limit ratelimit.Limit) (ratelimit.Result, error) {
	return ratelimit.Result{RetryAfter: 1500 * time.Millisecond}, nil
}

func TestCommitAnswers429WithRetryAfterWhenRateLimited(t *testing.T) {
	cfg := domain.Config{FQDN: "example.com", IngestLimit: domain.IngestLimit{Unknown: 1}}
	recordUC := record.New(strangerRecordRepo{}, nil, nil, &cfg, nil, nil, nil, nil, nil)
	recordUC.SetIngestLimiter(fullLimiter{})
	h := &Handler{config: cfg, record: recordUC}

	docBytes, err := json.Marshal(concrnt.Document[any]{Kind: "record", Author: "con1stranger", CreatedAt: time.Now()})
	require.NoError(t, err)
	body, err := json.Marshal(concrnt.SignedDocument{Document: string(docBytes)})
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodPost, "/api/v2/commit", strings.NewReader(string(body)))
	req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
	rec := httptest.NewRecorder()
	require.NoError(t, h.handleCommit(echo.New().NewContext(req, rec)))

	require.Equal(t, http.StatusTooManyRequests, rec.Code)
	// rounded up to whole seconds, so the caller never comes back early
	require.Equal(t, "2", rec.Header().Get(echo.HeaderRetryAfter))
	require.Contains(t, rec.Body.String(), "rate limit exceeded")
}
