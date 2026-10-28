package rest

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/chunkline"
	"github.com/concrnt/concrnt/internal/domain"
	chunklineuc "github.com/concrnt/concrnt/internal/usecase/chunkline"
	"github.com/concrnt/concrnt/internal/usecase/record"
	"github.com/concrnt/concrnt/policy"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/require"
)

// aliasResidenceRepo serves one entity by alias and nothing by CCID.
type aliasResidenceRepo struct {
	record.Repository
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
		record: record.New(repo, nil, &cfg, nil, nil, nil, nil, nil),
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

// aliasKeyRecordRepo serves one record under its canonical (CCID) key only.
type aliasKeyRecordRepo struct {
	record.Repository
	key string
	sd  *concrnt.SignedDocument
	got []string
}

func (r *aliasKeyRecordRepo) GetSignedDocument(ctx context.Context, uri string) (*concrnt.SignedDocument, error) {
	r.got = append(r.got, uri)
	if uri == r.key {
		return r.sd, nil
	}
	return nil, domain.ErrNotFound
}

func (r *aliasKeyRecordRepo) GetHierarchicalRecordPolicies(ctx context.Context, uri string) ([]concrnt.Policy, error) {
	return []concrnt.Policy{}, nil
}

type allowAllPolicy struct{}

func (allowAllPolicy) Eval(ctx context.Context, req policy.RequestContext, stack []concrnt.Policy, action string, key string) error {
	return nil
}

// aliasKeyChunklineRepo serves one manifest under its canonical (CCID) key only.
type aliasKeyChunklineRepo struct {
	record.Repository
	key string
	got []string
}

func (r *aliasKeyChunklineRepo) GetChunklineManifest(ctx context.Context, uri string) (*chunkline.Manifest, error) {
	r.got = append(r.got, uri)
	if uri == r.key {
		return &chunkline.Manifest{}, nil
	}
	return nil, domain.ErrNotFound
}

// CIP-0 §7.2: cckv://@<alias FQDN>/<key> names the same record as
// cckv://<ccid>/<key>. Keys are stored under the CCID form only, so the
// resolve endpoint must canonicalize the owner before the lookup, for the
// document and for its chunkline manifest alike.
func TestResolveAliasWithKeyLooksUpCanonicalOwner(t *testing.T) {
	cfg := domain.Config{FQDN: "example.com"}
	entity := &domain.Entity{ID: "con1alice", Domain: "example.com", SignedDocument: &concrnt.SignedDocument{Document: `{"kind":"entity"}`}}
	sd := &concrnt.SignedDocument{Document: `{"kind":"record","key":"cckv://con1alice/tipjar"}`}
	repo := &aliasKeyRecordRepo{key: "cckv://con1alice/tipjar", sd: sd}
	chunklineRepo := &aliasKeyChunklineRepo{key: "cckv://con1alice/tipjar"}
	recordUC := record.New(aliasResidenceRepo{Repository: repo, alias: "alice.example.net", entity: entity}, nil, &cfg, nil, nil, allowAllPolicy{}, nil, nil)
	h := &Handler{
		config:    cfg,
		record:    recordUC,
		chunkline: chunklineuc.New(chunklineRepo, nil, nil, recordUC),
	}

	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/resolve?uri="+url.QueryEscape("cckv://@alice.example.net/tipjar"), nil)
	rec := httptest.NewRecorder()
	require.NoError(t, h.handleResolve(e.NewContext(req, rec)))
	require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
	require.Equal(t, []string{"cckv://con1alice/tipjar"}, repo.got, "the repository must only ever see the canonical key")
	var body struct {
		Document string `json:"document"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	require.Equal(t, sd.Document, body.Document)

	req = httptest.NewRequest(http.MethodGet, "/resolve?uri="+url.QueryEscape("cckv://@alice.example.net/tipjar"), nil)
	req.Header.Set("Accept", "application/chunkline+json")
	rec = httptest.NewRecorder()
	require.NoError(t, h.handleResolve(e.NewContext(req, rec)))
	require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
	require.Equal(t, []string{"cckv://con1alice/tipjar"}, chunklineRepo.got, "the chunkline repository must only ever see the canonical key")

	// An alias nobody holds is a missing resource, not a server error.
	req = httptest.NewRequest(http.MethodGet, "/resolve?uri="+url.QueryEscape("cckv://@nobody.example.net/tipjar"), nil)
	rec = httptest.NewRecorder()
	require.NoError(t, h.handleResolve(e.NewContext(req, rec)))
	require.Equal(t, http.StatusNotFound, rec.Code, "body: %s", rec.Body.String())
}
