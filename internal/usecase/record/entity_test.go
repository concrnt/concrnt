package record

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/client"
	"github.com/concrnt/concrnt/internal/domain"
	"github.com/concrnt/concrnt/schemas"
)

// A server holds a remote entity's document from first contact and has no
// channel that brings a newer one: the only thing that carries a remote
// user's alias/domain change here is the entity document clients and
// federation delivery inline under References["cckv://<ccid>"] on every
// commit (CIP-0 §8.5). These tests cover that application path.

const remoteEntityHost = "remote.example.net"

// signedEntityDoc builds a signed entity document for ccid at createdAt.
// No alias: saveEntity verifies aliases against live DNS.
func signedEntityDoc(t *testing.T, ccid, priv string, createdAt time.Time) concrnt.SignedDocument {
	t.Helper()
	return signTestDocument(t, concrnt.Document[schemas.Entity]{
		Kind:      "entity",
		Author:    ccid,
		Schema:    schemas.EntityURL,
		Value:     schemas.Entity{Domain: remoteEntityHost},
		CreatedAt: createdAt,
	}, priv)
}

// remoteParty is a remote-domain identity whose stored entity document was
// signed at storedAt.
func remoteParty(t *testing.T, storedAt time.Time) ackParty {
	t.Helper()
	party := newAckParty(t, remoteEntityHost)
	stored := signedEntityDoc(t, party.ccid, party.priv, storedAt)
	party.entity.SignedDocument = &stored
	return party
}

func newReferenceCommitUsecase(cfg *domain.Config, repo Repository, residence EntityRepository) *Usecase {
	return New(
		repo,
		residence,
		newTestServerUsecase(cfg),
		cfg,
		nil,
		nopSignalService{},
		nopPolicyService{},
		nil,
		nil,
	)
}

// A commit inlining an entity document strictly newer than the stored one
// applies it (commit log + accept-if-newer upsert in its own tx) before the
// commit itself is processed.
func TestCommitAppliesNewerReferencedEntity(t *testing.T) {
	cfg := &domain.Config{FQDN: "example.com"}
	storedAt := time.Now().Add(-time.Hour).UTC().Truncate(time.Second)
	author := remoteParty(t, storedAt)
	repo := &recordingRecordRepo{storedCreatedAt: storedAt}
	uc := newReferenceCommitUsecase(cfg, repo, residenceOf(author))

	newer := signedEntityDoc(t, author.ccid, author.priv, storedAt.Add(30*time.Minute))
	sd := signedRecord(t, author.ccid, author.priv, time.Now())
	sd.References = map[string]concrnt.SignedDocument{author.entity.CCKV(): newer}

	if _, err := uc.Commit(context.Background(), "127.0.0.1", sd, domain.CommitModeExecute); err != nil {
		t.Fatalf("Commit returned error: %v", err)
	}
	if !repo.createEntityCalled {
		t.Fatal("the inlined entity document was not applied")
	}
	if !repo.createRecordCalled {
		t.Fatal("the record itself was not created")
	}
	want := []string{documentIDOf(t, newer), documentIDOf(t, sd)}
	if strings.Join(repo.createdCommitLogs, ",") != strings.Join(want, ",") {
		t.Fatalf("commit logs = %v, want entity then record %v", repo.createdCommitLogs, want)
	}
	if len(repo.txs) != 2 || !repo.txs[0].committed || !repo.txs[1].committed {
		t.Fatalf("expected two committed txs (entity, record), got %+v", repo.txs)
	}
}

// The steady state is a client inlining the same (or an older) entity
// document on every commit; that must not even reach the repository, since a
// losing accept-if-newer commit rolls back and would be re-verified forever.
func TestCommitIgnoresOlderOrEqualReferencedEntity(t *testing.T) {
	cfg := &domain.Config{FQDN: "example.com"}
	storedAt := time.Now().Add(-time.Hour).UTC().Truncate(time.Second)

	for _, tc := range []struct {
		name      string
		createdAt time.Time
	}{
		{"older", storedAt.Add(-time.Minute)},
		{"same", storedAt},
	} {
		t.Run(tc.name, func(t *testing.T) {
			author := remoteParty(t, storedAt)
			repo := &recordingRecordRepo{storedCreatedAt: storedAt}
			uc := newReferenceCommitUsecase(cfg, repo, residenceOf(author))

			sd := signedRecord(t, author.ccid, author.priv, time.Now())
			sd.References = map[string]concrnt.SignedDocument{
				author.entity.CCKV(): signedEntityDoc(t, author.ccid, author.priv, tc.createdAt),
			}

			if _, err := uc.Commit(context.Background(), "127.0.0.1", sd, domain.CommitModeExecute); err != nil {
				t.Fatalf("Commit returned error: %v", err)
			}
			if repo.createEntityCalled {
				t.Fatal("a not-newer inlined entity document must not be applied")
			}
			if !repo.createRecordCalled || len(repo.txs) != 1 || !repo.txs[0].committed {
				t.Fatalf("the record must still be committed on its own, got txs %+v", repo.txs)
			}
		})
	}
}

// Only an entity document authored by the referenced ccid counts; anything
// else under that key is ignored and the commit proceeds as before.
func TestCommitIgnoresForeignReferencedEntity(t *testing.T) {
	cfg := &domain.Config{FQDN: "example.com"}
	storedAt := time.Now().Add(-time.Hour).UTC().Truncate(time.Second)
	author := remoteParty(t, storedAt)
	other := remoteParty(t, storedAt)

	for name, ref := range map[string]concrnt.SignedDocument{
		"other author":  signedEntityDoc(t, other.ccid, other.priv, storedAt.Add(time.Minute)),
		"not an entity": signedRecord(t, author.ccid, author.priv, storedAt.Add(time.Minute)),
	} {
		t.Run(name, func(t *testing.T) {
			repo := &recordingRecordRepo{storedCreatedAt: storedAt}
			uc := newReferenceCommitUsecase(cfg, repo, residenceOf(author, other))

			sd := signedRecord(t, author.ccid, author.priv, time.Now())
			sd.References = map[string]concrnt.SignedDocument{author.entity.CCKV(): ref}

			if _, err := uc.Commit(context.Background(), "127.0.0.1", sd, domain.CommitModeExecute); err != nil {
				t.Fatalf("Commit returned error: %v", err)
			}
			if repo.createEntityCalled {
				t.Fatal("a foreign document under the author's key must not be applied as their entity")
			}
			if !repo.createRecordCalled {
				t.Fatal("the record itself was not created")
			}
		})
	}
}

// An inlined entity document that fails to commit (here: a forged
// signature) is dropped; it never fails the commit that carried it.
func TestCommitReferencedEntityFailureDoesNotFailCommit(t *testing.T) {
	cfg := &domain.Config{FQDN: "example.com"}
	storedAt := time.Now().Add(-time.Hour).UTC().Truncate(time.Second)
	author := remoteParty(t, storedAt)
	repo := &recordingRecordRepo{storedCreatedAt: storedAt}
	uc := newReferenceCommitUsecase(cfg, repo, residenceOf(author))

	forged := signedEntityDoc(t, author.ccid, author.priv, storedAt.Add(time.Minute))
	forged.Document = strings.Replace(forged.Document, remoteEntityHost, "evil.example.net", 1)
	sd := signedRecord(t, author.ccid, author.priv, time.Now())
	sd.References = map[string]concrnt.SignedDocument{author.entity.CCKV(): forged}

	if _, err := uc.Commit(context.Background(), "127.0.0.1", sd, domain.CommitModeExecute); err != nil {
		t.Fatalf("Commit returned error: %v", err)
	}
	if repo.createEntityCalled {
		t.Fatal("a forged entity document must not be applied")
	}
	if !repo.createRecordCalled || len(repo.txs) != 1 || !repo.txs[0].committed {
		t.Fatalf("the record must still be committed, got txs %+v", repo.txs)
	}
}

// An ack may inline the target's entity document too, keyed by the target's
// ccid; it is applied when newer and the ack goes through. (The lookup used
// to be keyed by the requester's cckv URI, so it never matched.)
func TestCommitAckAppliesTargetReferencedEntity(t *testing.T) {
	cfg := &domain.Config{FQDN: "example.com"}
	storedAt := time.Now().Add(-time.Hour).UTC().Truncate(time.Second)
	from := newAckParty(t, cfg.FQDN)
	to := remoteParty(t, storedAt)
	repo := &recordingRecordRepo{storedCreatedAt: storedAt}
	delivery := &recordingDeliveryQueue{}
	uc := newAckUsecase(cfg, repo, residenceOf(from, to), delivery)

	newer := signedEntityDoc(t, to.ccid, to.priv, storedAt.Add(time.Minute))
	sd := signedAck(t, "ack", from, to, time.Now().Add(-time.Minute))
	sd.References = map[string]concrnt.SignedDocument{to.entity.CCKV(): newer}

	if _, err := uc.Commit(context.Background(), "127.0.0.1", sd, domain.CommitModeExecute); err != nil {
		t.Fatalf("Commit returned error: %v", err)
	}
	if !repo.createEntityCalled {
		t.Fatal("the target's inlined entity document was not applied")
	}
	if !repo.acknowledgeCalled {
		t.Fatal("the ack itself was not recorded")
	}
	if len(delivery.jobs) != 1 {
		t.Fatalf("expected the acked mirror to be delivered, got %+v", delivery.jobs)
	}
}

// The acked mirror shipped to the target's server carries the acker's
// current entity document, like reference distribution does, so the target's
// server can refresh its copy of the acker.
func TestAckedDeliveryCarriesAckerEntity(t *testing.T) {
	cfg := &domain.Config{FQDN: "example.com"}
	from := newAckParty(t, cfg.FQDN)
	to := newAckParty(t, remoteEntityHost)
	repo := &recordingRecordRepo{}
	delivery := &recordingDeliveryQueue{}
	uc := newAckUsecase(cfg, repo, residenceOf(from, to), delivery)

	sd := signedAck(t, "ack", from, to, time.Now().Add(-time.Minute))
	if _, err := uc.Commit(context.Background(), "127.0.0.1", sd, domain.CommitModeExecute); err != nil {
		t.Fatalf("Commit returned error: %v", err)
	}
	if len(delivery.jobs) != 1 {
		t.Fatalf("expected exactly one delivery job, got %+v", delivery.jobs)
	}
	ref, ok := delivery.jobs[0].Payload.References[from.entity.CCKV()]
	if !ok {
		t.Fatalf("acked payload references = %v, want the acker's entity under %s", delivery.jobs[0].Payload.References, from.entity.CCKV())
	}
	if ref.Document != from.entity.SignedDocument.Document {
		t.Fatalf("inlined acker document = %q, want the stored %q", ref.Document, from.entity.SignedDocument.Document)
	}
}

// Committing an entity document drops the client's cached copy of that
// entity: the client resolves ccid -> domain through this server's own copy
// (cached 10 minutes), so a refreshed row would otherwise keep routing
// deliveries to the old domain.
func TestCommitEntityInvalidatesResourceCache(t *testing.T) {
	cfg := &domain.Config{FQDN: "example.com"}
	ccid, priv := newIdentity(t)
	entityURI := concrnt.CCURI{Scheme: "cckv", Owner: ccid}.String()
	served := signedEntityDoc(t, ccid, priv, time.Now().Add(-time.Hour))

	var resolves atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/.well-known/concrnt":
			json.NewEncoder(w).Encode(concrnt.WellKnownConcrnt{
				Domain:    remoteEntityHost,
				Endpoints: map[string]string{"net.concrnt.core.resolve": "/resolve?uri={uri}"},
			})
		case "/resolve":
			if got, _ := url.QueryUnescape(r.URL.Query().Get("uri")); got != entityURI {
				http.NotFound(w, r)
				return
			}
			resolves.Add(1)
			json.NewEncoder(w).Encode(served)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)
	cl := client.New(cfg.FQDN)
	cl.AddHostRemapping(remoteEntityHost, srv.URL)

	repo := &recordingRecordRepo{}
	uc := New(repo, stubResidenceRepo{}, newTestServerUsecase(cfg), cfg, cl, nopSignalService{}, nopPolicyService{}, nil, nil)

	fetch := func() {
		t.Helper()
		var sd concrnt.SignedDocument
		if err := cl.GetResource(context.Background(), entityURI, "application/json", &client.Options{Resolver: remoteEntityHost}, &sd); err != nil {
			t.Fatalf("GetResource: %v", err)
		}
	}
	fetch()
	fetch()
	if n := resolves.Load(); n != 1 {
		t.Fatalf("resolve hits before commit = %d, want 1 (second fetch served from cache)", n)
	}

	if _, err := uc.Commit(context.Background(), "127.0.0.1", signedEntityDoc(t, ccid, priv, time.Now()), domain.CommitModeExecute); err != nil {
		t.Fatalf("Commit returned error: %v", err)
	}
	fetch()
	if n := resolves.Load(); n != 2 {
		t.Fatalf("resolve hits after commit = %d, want 2 (cache invalidated)", n)
	}
}
