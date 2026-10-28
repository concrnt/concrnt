package residence

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/concrnt/concrnt/impl/interop"
	"github.com/concrnt/concrnt/internal/domain"
	"github.com/concrnt/concrnt/internal/usecase/record"
)

// encodeMetaInfo turns the client-supplied registration meta (any JSON value,
// typically the register-template form data object) into the jsonb string
// stored in EntityMeta.Info.
func TestEncodeMetaInfo(t *testing.T) {
	t.Run("object is marshaled as-is", func(t *testing.T) {
		info, err := encodeMetaInfo(map[string]any{"name": "alice", "consent": true})
		if err != nil {
			t.Fatalf("encodeMetaInfo returned error: %v", err)
		}
		if info != `{"consent":true,"name":"alice"}` {
			t.Fatalf("info = %s", info)
		}
	})

	t.Run("nil becomes jsonb null", func(t *testing.T) {
		info, err := encodeMetaInfo(nil)
		if err != nil {
			t.Fatalf("encodeMetaInfo returned error: %v", err)
		}
		if info != "null" {
			t.Fatalf("info = %s", info)
		}
	})

	t.Run("raw message passes through compacted", func(t *testing.T) {
		info, err := encodeMetaInfo(json.RawMessage(`{"note": "created by conctl"}`))
		if err != nil {
			t.Fatalf("encodeMetaInfo returned error: %v", err)
		}
		if info != `{"note":"created by conctl"}` {
			t.Fatalf("info = %s", info)
		}
	})
}

// registrationResidenceRepo serves a fixed meta (or an error) and records
// UpdateMetaInfo calls without touching inviter.
type registrationResidenceRepo struct {
	record.Repository
	meta    *domain.EntityMeta
	metaErr error
}

func (r *registrationResidenceRepo) GetMeta(ctx context.Context, ccid string) (*domain.EntityMeta, error) {
	if r.metaErr != nil {
		return nil, r.metaErr
	}
	return r.meta, nil
}

func (r *registrationResidenceRepo) UpdateMetaInfo(ctx context.Context, ccid string, info string) error {
	if r.metaErr != nil {
		return r.metaErr
	}
	r.meta.Info = info
	return nil
}

// GetRegistration/UpdateRegistration are the v1 GET/PUT /entity/meta
// equivalents: requester-only access to their own registration info, with
// inviter fixed at registration time.
func TestRegistrationMeta(t *testing.T) {
	cfg := &domain.Config{FQDN: "example.com"}
	ccid := "con1qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqq"
	inviter := "con1pppppppppppppppppppppppppppppppppppppppp"
	authedCtx := context.WithValue(context.Background(), interop.RequesterCtxKey, domain.Entity{ID: ccid})

	t.Run("unauthenticated get rejected", func(t *testing.T) {
		uc := New(&registrationResidenceRepo{}, nil, cfg)
		_, err := uc.GetRegistration(context.Background())
		if err == nil || !errors.Is(err, domain.ErrPermissionDenied) {
			t.Fatalf("expected permission error, got %v", err)
		}
	})

	t.Run("unauthenticated update rejected", func(t *testing.T) {
		repo := &registrationResidenceRepo{meta: &domain.EntityMeta{ID: ccid, Info: "null"}}
		uc := New(repo, nil, cfg)
		err := uc.UpdateRegistration(context.Background(), map[string]any{"email": "a@example.com"})
		if err == nil || !errors.Is(err, domain.ErrPermissionDenied) {
			t.Fatalf("expected permission error, got %v", err)
		}
		if repo.meta.Info != "null" {
			t.Fatal("UpdateMetaInfo should not be called")
		}
	})

	t.Run("service account (non-entity requester) rejected", func(t *testing.T) {
		uc := New(&registrationResidenceRepo{}, nil, cfg)
		saCtx := context.WithValue(context.Background(), interop.RequesterCtxKey, "con1sssssssssssssssssssssssssssssssssssssss")
		_, err := uc.GetRegistration(saCtx)
		if err == nil || !errors.Is(err, domain.ErrPermissionDenied) {
			t.Fatalf("expected permission error, got %v", err)
		}
	})

	t.Run("unregistered requester gets not found", func(t *testing.T) {
		repo := &registrationResidenceRepo{metaErr: domain.NotFoundError{Resource: "entity meta"}}
		uc := New(repo, nil, cfg)
		_, err := uc.GetRegistration(authedCtx)
		if err == nil || !errors.Is(err, domain.ErrNotFound) {
			t.Fatalf("expected not found, got %v", err)
		}
		// the message and code are API contract: they mark the 404 as
		// concrnt's application-level "not registered", not a proxy/misconfig
		// 404
		assertRegistrationNotFound := func(err error) {
			t.Helper()
			if err.Error() != "Registration Not Found" {
				t.Fatalf("message = %q", err.Error())
			}
			var nf domain.NotFoundError
			if !errors.As(err, &nf) || nf.Code != domain.ErrorCodeRegistrationNotFound {
				t.Fatalf("code = %q", nf.Code)
			}
		}
		assertRegistrationNotFound(err)
		err = uc.UpdateRegistration(authedCtx, nil)
		if err == nil || !errors.Is(err, domain.ErrNotFound) {
			t.Fatalf("expected not found, got %v", err)
		}
		assertRegistrationNotFound(err)
	})

	t.Run("get returns own meta", func(t *testing.T) {
		repo := &registrationResidenceRepo{meta: &domain.EntityMeta{ID: ccid, Inviter: &inviter, Info: `{"email":"a@example.com"}`}}
		uc := New(repo, nil, cfg)
		meta, err := uc.GetRegistration(authedCtx)
		if err != nil {
			t.Fatalf("GetRegistration returned error: %v", err)
		}
		if meta.ID != ccid || meta.Inviter != &inviter || meta.Info != `{"email":"a@example.com"}` {
			t.Fatalf("meta = %+v", meta)
		}
	})

	t.Run("update replaces info and keeps inviter", func(t *testing.T) {
		repo := &registrationResidenceRepo{meta: &domain.EntityMeta{ID: ccid, Inviter: &inviter, Info: `{"email":"a@example.com"}`}}
		uc := New(repo, nil, cfg)
		if err := uc.UpdateRegistration(authedCtx, map[string]any{"email": "b@example.com"}); err != nil {
			t.Fatalf("UpdateRegistration returned error: %v", err)
		}
		if repo.meta.Info != `{"email":"b@example.com"}` {
			t.Fatalf("info = %s", repo.meta.Info)
		}
		if repo.meta.Inviter != &inviter {
			t.Fatal("inviter must be preserved")
		}
	})
}

// unregisterResidenceRepo records the order of gc-flag and meta-delete calls
// and can fail either one.
type unregisterResidenceRepo struct {
	record.Repository
	calls     []string
	markErr   error
	deleteErr error
}

func (r *unregisterResidenceRepo) MarkCommitLogsGcCandidateByOwner(ctx context.Context, owner string) error {
	if r.markErr != nil {
		return r.markErr
	}
	r.calls = append(r.calls, "mark:"+owner)
	return nil
}

func (r *unregisterResidenceRepo) DeleteMeta(ctx context.Context, ccid string) error {
	if r.deleteErr != nil {
		return r.deleteErr
	}
	r.calls = append(r.calls, "delete:"+ccid)
	return nil
}

// Unregister is the unified account-deletion path: for a current resident and
// for post-migration cleanup alike, it flags the requester's solely-owned
// commit logs for GC and then deletes the residence meta. Idempotent, so an
// already-unregistered requester succeeds too.
func TestUnregister(t *testing.T) {
	cfg := &domain.Config{FQDN: "example.com"}
	ccid := "con1qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqq"
	authedCtx := context.WithValue(context.Background(), interop.RequesterCtxKey, domain.Entity{ID: ccid})

	t.Run("unauthenticated rejected", func(t *testing.T) {
		repo := &unregisterResidenceRepo{}
		uc := New(repo, nil, cfg)
		err := uc.Unregister(context.Background())
		if err == nil || !errors.Is(err, domain.ErrPermissionDenied) {
			t.Fatalf("expected permission error, got %v", err)
		}
		if len(repo.calls) != 0 {
			t.Fatalf("no repository call expected, got %v", repo.calls)
		}
	})

	t.Run("service account (non-entity requester) rejected", func(t *testing.T) {
		repo := &unregisterResidenceRepo{}
		uc := New(repo, nil, cfg)
		saCtx := context.WithValue(context.Background(), interop.RequesterCtxKey, "con1sssssssssssssssssssssssssssssssssssssss")
		err := uc.Unregister(saCtx)
		if err == nil || !errors.Is(err, domain.ErrPermissionDenied) {
			t.Fatalf("expected permission error, got %v", err)
		}
		if len(repo.calls) != 0 {
			t.Fatalf("no repository call expected, got %v", repo.calls)
		}
	})

	t.Run("resident account is deleted, gc flag first", func(t *testing.T) {
		repo := &unregisterResidenceRepo{}
		uc := New(repo, nil, cfg)
		if err := uc.Unregister(authedCtx); err != nil {
			t.Fatalf("Unregister returned error: %v", err)
		}
		// gc化が先: 途中失敗時にmetaが残り、リトライで全体をやり直せる順序
		want := []string{"mark:" + ccid, "delete:" + ccid}
		if len(repo.calls) != 2 || repo.calls[0] != want[0] || repo.calls[1] != want[1] {
			t.Fatalf("calls = %v, want %v", repo.calls, want)
		}
	})

	t.Run("gc mark failure propagates and keeps meta", func(t *testing.T) {
		repo := &unregisterResidenceRepo{markErr: errors.New("db down")}
		uc := New(repo, nil, cfg)
		if err := uc.Unregister(authedCtx); err == nil {
			t.Fatal("expected error, got nil")
		}
		if len(repo.calls) != 0 {
			t.Fatalf("DeleteMeta should not be called on gc-mark failure, got %v", repo.calls)
		}
	})

	t.Run("delete meta failure propagates", func(t *testing.T) {
		repo := &unregisterResidenceRepo{deleteErr: errors.New("db down")}
		uc := New(repo, nil, cfg)
		if err := uc.Unregister(authedCtx); err == nil {
			t.Fatal("expected error, got nil")
		}
	})
}
