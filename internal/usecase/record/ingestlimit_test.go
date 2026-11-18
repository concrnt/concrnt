package record

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/internal/domain"
	"github.com/concrnt/concrnt/internal/usecase/ratelimit"
)

// stubLimiter records every bucket it is asked about with the limit passed,
// and rejects the buckets named in full.
type stubLimiter struct {
	calls map[string]ratelimit.Limit
	full  map[string]bool
	err   error
}

func (s *stubLimiter) Allow(ctx context.Context, key string, limit ratelimit.Limit) (ratelimit.Result, error) {
	if s.calls == nil {
		s.calls = map[string]ratelimit.Limit{}
	}
	s.calls[key] = limit
	if s.err != nil {
		return ratelimit.Result{}, s.err
	}
	if s.full[key] {
		return ratelimit.Result{RetryAfter: 7 * time.Second}, nil
	}
	return ratelimit.Result{Allowed: true}, nil
}

const ingestTestRemoteBucket = "ingest:domain:remote.example.net"

// newIngestUsecase wires a usecase with the ingest limit on, whose entity
// lookup is entities.
func newIngestUsecase(entities entityLookup, limiter *stubLimiter) (*Usecase, *recordingRecordRepo) {
	repo := &recordingRecordRepo{}
	cfg := &domain.Config{
		FQDN:        "example.com",
		IngestLimit: domain.IngestLimit{Unknown: 20, Min: 300},
	}
	uc := New(withEntities(repo, entities), nil, newTestServerUsecase(cfg), cfg, nil, nopSignalService{}, nopPolicyService{}, nil, nil)
	uc.SetIngestLimiter(limiter)
	return uc, repo
}

func remoteEntity(ccid string) fixedEntity {
	return fixedEntity{entity: &domain.Entity{ID: ccid, Domain: "remote.example.net"}}
}

func requireRateLimited(t *testing.T, err error) {
	t.Helper()
	var limited domain.RateLimitError
	if !errors.As(err, &limited) {
		t.Fatalf("err = %v, want a RateLimitError", err)
	}
	if limited.RetryAfter != 7*time.Second {
		t.Fatalf("RetryAfter = %v, want the limiter's 7s", limited.RetryAfter)
	}
}

func TestIngestLimitChargesRemoteAuthorsDomain(t *testing.T) {
	ccid, priv := newIdentity(t)
	limiter := &stubLimiter{}
	uc, repo := newIngestUsecase(remoteEntity(ccid), limiter)

	sd := signedRecord(t, ccid, priv, time.Now())
	if _, err := uc.Commit(context.Background(), "127.0.0.1", sd, domain.CommitModeExecute); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if !repo.createRecordCalled {
		t.Fatalf("record was not created")
	}
	if len(limiter.calls) != 1 || limiter.calls[ingestTestRemoteBucket] != ratelimit.PerMinute(300) {
		t.Fatalf("limiter calls = %v, want only %s at 300/min", limiter.calls, ingestTestRemoteBucket)
	}
}

func TestIngestLimitRejectsWhenDomainBucketIsFull(t *testing.T) {
	ccid, priv := newIdentity(t)
	limiter := &stubLimiter{full: map[string]bool{ingestTestRemoteBucket: true}}
	uc, repo := newIngestUsecase(remoteEntity(ccid), limiter)

	sd := signedRecord(t, ccid, priv, time.Now())
	_, err := uc.Commit(context.Background(), "127.0.0.1", sd, domain.CommitModeExecute)
	requireRateLimited(t, err)
	if repo.createRecordCalled {
		t.Fatalf("record was created although the commit was rate limited")
	}
}

// The author field alone is a claim anyone can make: a document that fails
// verification must not use up the claimed author's domain bucket.
func TestIngestLimitDoesNotChargeDomainForUnverifiedDocument(t *testing.T) {
	ccid, _ := newIdentity(t)
	_, otherPriv := newIdentity(t)
	limiter := &stubLimiter{}
	uc, _ := newIngestUsecase(remoteEntity(ccid), limiter)

	forged := signedRecord(t, ccid, otherPriv, time.Now())
	if _, err := uc.Commit(context.Background(), "127.0.0.1", forged, domain.CommitModeExecute); err == nil {
		t.Fatalf("forged commit succeeded")
	}
	if len(limiter.calls) != 0 {
		t.Fatalf("limiter calls = %v, want none", limiter.calls)
	}
}

func TestIngestLimitDoesNotApplyToLocalAuthors(t *testing.T) {
	ccid, priv := newIdentity(t)
	limiter := &stubLimiter{full: map[string]bool{"ingest:unknown": true, "ingest:domain:example.com": true}}
	uc, repo := newIngestUsecase(fixedEntity{entity: &domain.Entity{ID: ccid, Domain: "example.com"}}, limiter)

	sd := signedRecord(t, ccid, priv, time.Now())
	if _, err := uc.Commit(context.Background(), "127.0.0.1", sd, domain.CommitModeExecute); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if !repo.createRecordCalled {
		t.Fatalf("record was not created")
	}
	if len(limiter.calls) != 0 {
		t.Fatalf("limiter calls = %v, want none", limiter.calls)
	}
}

// An author this server holds no entity for is charged to the shared unknown
// bucket before anything is verified or resolved.
func TestIngestLimitChargesUnknownAuthorsToSharedBucket(t *testing.T) {
	ccid, priv := newIdentity(t)
	sd := signedRecord(t, ccid, priv, time.Now())

	t.Run("counted", func(t *testing.T) {
		limiter := &stubLimiter{}
		uc, _ := newIngestUsecase(notFoundEntities{}, limiter)

		// the author stays unresolvable here, so the commit fails afterwards
		_, err := uc.Commit(context.Background(), "127.0.0.1", sd, domain.CommitModeExecute)
		if errors.Is(err, domain.ErrRateLimit) {
			t.Fatalf("commit was rate limited: %v", err)
		}
		if len(limiter.calls) != 1 || limiter.calls["ingest:unknown"] != ratelimit.PerMinute(20) {
			t.Fatalf("limiter calls = %v, want only ingest:unknown at 20/min", limiter.calls)
		}
	})

	t.Run("rejected when full", func(t *testing.T) {
		limiter := &stubLimiter{full: map[string]bool{"ingest:unknown": true}}
		uc, _ := newIngestUsecase(notFoundEntities{}, limiter)

		_, err := uc.Commit(context.Background(), "127.0.0.1", sd, domain.CommitModeExecute)
		requireRateLimited(t, err)
	})
}

// Commits this server makes on its own account, and authenticated local
// operations, are not federation traffic.
func TestIngestLimitSkipsNonIngressCommits(t *testing.T) {
	ccid, priv := newIdentity(t)
	full := map[string]bool{"ingest:unknown": true, ingestTestRemoteBucket: true}

	cases := map[string]func(uc *Usecase, sd concrnt.SignedDocument) error{
		"internal commit": func(uc *Usecase, sd concrnt.SignedDocument) error {
			_, err := uc.commit(context.Background(), "127.0.0.1", sd, domain.CommitModeExecute, false)
			return err
		},
		"local-only commit": func(uc *Usecase, sd concrnt.SignedDocument) error {
			_, err := uc.Commit(context.Background(), "127.0.0.1", sd, domain.CommitModeLocalOnlyExecute)
			return err
		},
	}
	for name, commit := range cases {
		t.Run(name, func(t *testing.T) {
			limiter := &stubLimiter{full: full}
			uc, repo := newIngestUsecase(remoteEntity(ccid), limiter)

			if err := commit(uc, signedRecord(t, ccid, priv, time.Now())); err != nil {
				t.Fatalf("commit: %v", err)
			}
			if !repo.createRecordCalled {
				t.Fatalf("record was not created")
			}
			if len(limiter.calls) != 0 {
				t.Fatalf("limiter calls = %v, want none", limiter.calls)
			}
		})
	}
}

// A broken counter store must not stop federation.
func TestIngestLimitFailsOpen(t *testing.T) {
	ccid, priv := newIdentity(t)
	limiter := &stubLimiter{err: errors.New("store down")}
	uc, repo := newIngestUsecase(remoteEntity(ccid), limiter)

	sd := signedRecord(t, ccid, priv, time.Now())
	if _, err := uc.Commit(context.Background(), "127.0.0.1", sd, domain.CommitModeExecute); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if !repo.createRecordCalled {
		t.Fatalf("record was not created")
	}
}

func TestIngestLimitZeroEntryIsUnlimited(t *testing.T) {
	ccid, priv := newIdentity(t)
	limiter := &stubLimiter{full: map[string]bool{"ingest:unknown": true}}
	uc, _ := newIngestUsecase(notFoundEntities{}, limiter)
	uc.config.IngestLimit.Unknown = 0

	sd := signedRecord(t, ccid, priv, time.Now())
	_, err := uc.Commit(context.Background(), "127.0.0.1", sd, domain.CommitModeExecute)
	if errors.Is(err, domain.ErrRateLimit) {
		t.Fatalf("commit was rate limited with unknown: 0")
	}
	if len(limiter.calls) != 0 {
		t.Fatalf("limiter calls = %v, want none", limiter.calls)
	}
}
