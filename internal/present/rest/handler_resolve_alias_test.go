package rest

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/internal/domain"
	"github.com/concrnt/concrnt/internal/usecase/record"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/require"
)

// aliasResidenceRepo serves one entity by alias and nothing by CCID.
type aliasResidenceRepo struct {
	record.EntityRepository
	alias  string
	entity *domain.Entity
}

func (r aliasResidenceRepo) GetEntityByAlias(ctx context.Context, alias string) (*domain.Entity, error) {
	if alias == r.alias {
		return r.entity, nil
	}
	return nil, domain.ErrNotFound
}

func (r aliasResidenceRepo) GetEntityByCCID(ctx context.Context, ccid string) (*domain.Entity, error) {
	return nil, domain.ErrNotFound
}

// CIP-0 §7.2/§8.3: cckv://@<alias FQDN> names an entity document, so the resolve
// endpoint must serve it through the entity path instead of treating the owner
// as a server FQDN (h.server is nil here: reaching it would panic).
func TestResolveAliasServesEntityDocument(t *testing.T) {
	cfg := domain.Config{FQDN: "example.com"}
	sd := &concrnt.SignedDocument{Document: `{"kind":"entity"}`}
	repo := aliasResidenceRepo{alias: "alice.example.net", entity: &domain.Entity{ID: "con1alice", Domain: "example.com", SignedDocument: sd}}
	h := &Handler{
		config: cfg,
		record: record.New(nil, repo, nil, &cfg, nil, nil, nil, nil, nil),
	}

	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/resolve?uri="+url.QueryEscape("cckv://@alice.example.net"), nil)
	rec := httptest.NewRecorder()
	require.NoError(t, h.handleResolve(e.NewContext(req, rec)))
	require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
	var body struct {
		Document string `json:"document"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	require.Equal(t, sd.Document, body.Document)
}
