package record

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/impl/interop"
	"github.com/concrnt/concrnt/internal/domain"
	"github.com/concrnt/concrnt/internal/usecase"
)

// Ack / unack / acked / unacked commits (CIP-10 §5 / §5.2, SPEC_DECISIONS
// C-3): the acker's server records the ack/unack commit owned by the acker
// and ships a derived acked/unacked document (document-direct proof
// embedding the original) to the target's server, which records that as its
// own commit owned by the target.

const followSchema = "https://example.com/follow.json"

// mapResidenceRepo serves one entity per ccid, so an ack's author and target
// can live on different domains.
type mapResidenceRepo struct {
	entityLookups
	entities map[string]*domain.Entity
}

func (m mapResidenceRepo) GetEntityByCCID(ctx context.Context, ccid string) (*domain.Entity, error) {
	if entity, ok := m.entities[ccid]; ok {
		return entity, nil
	}
	return nil, domain.ErrNotFound
}

// ackParty is one side of an ack: an identity and its resolved entity.
type ackParty struct {
	ccid   string
	priv   string
	entity *domain.Entity
}

func newAckParty(t *testing.T, host string) ackParty {
	t.Helper()
	ccid, priv := newIdentity(t)
	return ackParty{
		ccid: ccid,
		priv: priv,
		entity: &domain.Entity{
			ID:             ccid,
			Domain:         host,
			SignedDocument: &concrnt.SignedDocument{Document: "{}"},
		},
	}
}

func residenceOf(parties ...ackParty) mapResidenceRepo {
	entities := map[string]*domain.Entity{}
	for _, party := range parties {
		entities[party.ccid] = party.entity
	}
	return mapResidenceRepo{entities: entities}
}

// signedAck builds a signed ack/unack by from targeting to (CIP-10 §3).
func signedAck(t *testing.T, kind string, from, to ackParty, createdAt time.Time) concrnt.SignedDocument {
	t.Helper()
	associate := concrnt.CCURI{Scheme: "cckv", Owner: to.ccid}.String()
	return signTestDocument(t, concrnt.Document[any]{
		Kind:      kind,
		Author:    from.ccid,
		Schema:    followSchema,
		CreatedAt: createdAt,
		Associate: &associate,
	}, from.priv)
}

// derivedAcked derives the acked/unacked document the acker's server ships to
// the target's server: the original with only kind replaced, carrying the
// original signed ack in a document-direct proof.
func derivedAcked(t *testing.T, ack concrnt.SignedDocument) concrnt.SignedDocument {
	t.Helper()
	derived, err := ack.DeriveAcked()
	if err != nil {
		t.Fatalf("derive acked document: %v", err)
	}
	return derived
}

func newAckUsecase(cfg *domain.Config, repo Repository, residence entityLookups, delivery usecase.JobQueue) *Usecase {
	return New(
		withEntities{repo, residence},
		newTestServerUsecase(cfg),
		cfg,
		nil,
		nopSignalService{},
		nopPolicyService{},
		delivery,
		nil,
	)
}

// systemCtx marks the commit as coming from the system service account, which
// skips signature verification and the backdate window. Used to exercise the
// apply path of acked/unacked commits in isolation from proof verification.
func systemCtx() context.Context {
	return context.WithValue(context.Background(), interop.ServiceAccountTypeCtxKey, "system")
}

func kindOf(t *testing.T, sd concrnt.SignedDocument) string {
	t.Helper()
	doc, err := sd.ParsedDocument()
	if err != nil {
		t.Fatalf("parse document: %v", err)
	}
	return doc.Kind
}

// The acker's server records the ack/unack commit exactly once, owned by the
// author alone (CIP-10 §5: "このコミットの所有者は author のみ"), and applies
// the ack state under (author, associate owner, schema).
func TestCommitAckOwnerIsAuthor(t *testing.T) {
	cfg := &domain.Config{FQDN: "example.com"}
	from := newAckParty(t, cfg.FQDN)
	to := newAckParty(t, "remote.example.net")

	for _, kind := range []string{"ack", "unack"} {
		t.Run(kind, func(t *testing.T) {
			repo := &recordingRecordRepo{}
			delivery := &recordingDeliveryQueue{}
			uc := newAckUsecase(cfg, repo, residenceOf(from, to), delivery)

			sd := signedAck(t, kind, from, to, time.Now().Add(-time.Minute))
			result, err := uc.Commit(context.Background(), "127.0.0.1", sd, domain.CommitModeExecute)
			if err != nil {
				t.Fatalf("Commit returned error: %v", err)
			}
			id := documentIDOf(t, sd)

			// CIP-3 §3.4: the ack's ccfs identity is held by the author's server
			wantCCFS := concrnt.ComposeCCFSURI(from.ccid, concrnt.CCFSTypeConcrnt, id)
			if result == nil || result.CCFS == nil || *result.CCFS != wantCCFS {
				t.Fatalf("result ccfs = %v, want %s", result.CCFS, wantCCFS)
			}

			if len(repo.createdCommitLogs) != 1 || repo.createdCommitLogs[0] != id {
				t.Fatalf("CreateCommitLog calls = %v, want exactly [%s]", repo.createdCommitLogs, id)
			}
			if repo.commitLogOwners[id] != from.ccid {
				t.Fatalf("ack commit owner = %q, want author %s", repo.commitLogOwners[id], from.ccid)
			}

			wantMethod := "Acknowledge"
			if kind == "unack" {
				wantMethod = "UnAcknowledge"
			}
			if len(repo.ackCalls) != 1 {
				t.Fatalf("ack transitions = %+v, want exactly one", repo.ackCalls)
			}
			call := repo.ackCalls[0]
			if call.method != wantMethod || call.documentID != id || call.from != from.ccid || call.to != to.ccid || call.schema != followSchema {
				t.Fatalf("ack transition = %+v, want %s(%s, from=%s, to=%s, schema=%s)", call, wantMethod, id, from.ccid, to.ccid, followSchema)
			}
			if doc, _ := sd.ParsedDocument(); !call.createdAt.Equal(doc.CreatedAt) {
				t.Fatalf("transition createdAt = %v, want the document's %v", call.createdAt, doc.CreatedAt)
			}
			if repo.acknowledgedCalled || repo.unacknowledgedCalled {
				t.Fatal("the acker's server must not write the target-side acked state for its own ack")
			}
			if len(repo.txs) != 1 || !repo.txs[0].committed {
				t.Fatalf("expected a committed tx, got %+v", repo.txs)
			}
		})
	}
}

// The acker's server derives the acked/unacked document and delivers it to
// the target's server (CIP-10 §5: the state must reach both sides; the target
// side holds the mirror, never the raw ack). The mirror carries the original
// signed ack in a document-direct proof so the target can verify it
// self-contained, and it must actually be committed remotely when the target
// lives on another server.
func TestCommitAckDeliversAckedToTarget(t *testing.T) {
	cfg := &domain.Config{FQDN: "example.com"}
	from := newAckParty(t, cfg.FQDN)
	to := newAckParty(t, "remote.example.net")

	for _, tc := range []struct{ kind, wantKind string }{{"ack", "acked"}, {"unack", "unacked"}} {
		t.Run(tc.kind, func(t *testing.T) {
			repo := &recordingRecordRepo{}
			delivery := &recordingDeliveryQueue{}
			uc := newAckUsecase(cfg, repo, residenceOf(from, to), delivery)

			sd := signedAck(t, tc.kind, from, to, time.Now().Add(-time.Minute))
			if _, err := uc.Commit(context.Background(), "127.0.0.1", sd, domain.CommitModeExecute); err != nil {
				t.Fatalf("Commit returned error: %v", err)
			}
			if len(delivery.jobs) != 1 {
				t.Fatalf("expected exactly one delivery job (the acked mirror), got %d: %+v", len(delivery.jobs), delivery.jobs)
			}
			job := delivery.jobs[0]

			t.Run("addressed to the target entity", func(t *testing.T) {
				if job.ResolveURI != to.entity.CCKVWithHint() {
					t.Fatalf("job ResolveURI = %q, want %q", job.ResolveURI, to.entity.CCKVWithHint())
				}
			})

			t.Run("committed locally when the target is ours", func(t *testing.T) {
				if job.Local != DeliveryLocalCommit {
					t.Fatalf("job Local = %q, want %q", job.Local, DeliveryLocalCommit)
				}
			})

			// CIP-10 §5: the target's server must receive the state. With
			// Remote=none Deliver drops the job for a remote
			// host, so a cross-server ack never reaches the ackee.
			t.Run("committed remotely when the target is elsewhere", func(t *testing.T) {
				if job.Remote != DeliveryRemoteCommit {
					t.Fatalf("job Remote = %q, want %q", job.Remote, DeliveryRemoteCommit)
				}
			})

			t.Run("payload is the derived mirror", func(t *testing.T) {
				want := derivedAcked(t, sd)
				if job.Payload.Document != want.Document {
					t.Fatalf("payload document = %s, want %s", job.Payload.Document, want.Document)
				}
				if kindOf(t, job.Payload) != tc.wantKind {
					t.Fatalf("payload kind = %q, want %q", kindOf(t, job.Payload), tc.wantKind)
				}
				if job.Payload.Proof.Type != concrnt.ProofTypeDocumentDirect {
					t.Fatalf("payload proof type = %q, want %q", job.Payload.Proof.Type, concrnt.ProofTypeDocumentDirect)
				}
				if job.Payload.Proof.Document == nil || *job.Payload.Proof.Document != sd.Document {
					t.Fatal("payload proof must embed the original ack document verbatim")
				}
				if job.Payload.Proof.Proof == nil || job.Payload.Proof.Proof.Type != sd.Proof.Type ||
					job.Payload.Proof.Proof.Signature == nil || *job.Payload.Proof.Proof.Signature != *sd.Proof.Signature {
					t.Fatal("payload proof must embed the original ack proof verbatim")
				}
			})
		})
	}
}

// A repository replay (LocalOnlyExecute) applies the ack locally but ships no
// acked document — like reference distribution, only an executing commit
// federates; the associate owner's holding is that side's own dump.
func TestCommitAckImportDoesNotDeliverAcked(t *testing.T) {
	cfg := &domain.Config{FQDN: "example.com"}
	from := newAckParty(t, cfg.FQDN)
	to := newAckParty(t, "remote.example.net")

	repo := &recordingRecordRepo{}
	delivery := &recordingDeliveryQueue{}
	uc := newAckUsecase(cfg, repo, residenceOf(from, to), delivery)

	sd := signedAck(t, "ack", from, to, time.Now().Add(-time.Minute))
	if _, err := uc.Commit(context.Background(), "127.0.0.1", sd, domain.CommitModeLocalOnlyExecute); err != nil {
		t.Fatalf("Commit returned error: %v", err)
	}
	if !repo.acknowledgeCalled {
		t.Fatal("the ack state must still be applied on import")
	}
	if len(repo.txs) != 1 || !repo.txs[0].committed {
		t.Fatalf("expected a committed tx, got %+v", repo.txs)
	}
	if len(delivery.jobs) != 0 {
		t.Fatalf("an imported ack must not deliver an acked document, got %+v", delivery.jobs)
	}
}

// A stale ack (accept-if-newer loss, CIP-10 §4) is a side-effect-free no-op:
// no mirror is delivered and the tx (commit_log included) rolls back.
func TestCommitStaleAckDeliversNothing(t *testing.T) {
	cfg := &domain.Config{FQDN: "example.com"}
	from := newAckParty(t, cfg.FQDN)
	to := newAckParty(t, "remote.example.net")

	repo := &recordingRecordRepo{ackStale: true}
	delivery := &recordingDeliveryQueue{}
	uc := newAckUsecase(cfg, repo, residenceOf(from, to), delivery)

	sd := signedAck(t, "ack", from, to, time.Now().Add(-time.Minute))
	if _, err := uc.Commit(context.Background(), "127.0.0.1", sd, domain.CommitModeExecute); err != nil {
		t.Fatalf("Commit returned error: %v", err)
	}
	if len(delivery.jobs) != 0 {
		t.Fatalf("a stale ack must not deliver anything, got %+v", delivery.jobs)
	}
	if len(repo.txs) != 1 || repo.txs[0].committed || !repo.txs[0].rolledBack {
		t.Fatalf("expected a single rolled-back tx, got %+v", repo.txs)
	}
}

// The target's server applies an acked/unacked mirror as its own commit
// (owner = associate owner, CIP-10 §5) and writes the target-side state; it
// never treats it as a raw ack and never re-delivers it. Proof verification
// is bypassed here (system context) to test the apply path alone; see
// TestCommitAckedVerifiesDocumentDirectProof for the real entry.
func TestCommitAckedAppliesOnTargetServer(t *testing.T) {
	cfg := &domain.Config{FQDN: "example.com"}
	from := newAckParty(t, "remote.example.net")
	to := newAckParty(t, cfg.FQDN)

	for _, tc := range []struct{ kind, wantMethod string }{{"ack", "Acknowledged"}, {"unack", "UnAcknowledged"}} {
		t.Run(tc.kind, func(t *testing.T) {
			repo := &recordingRecordRepo{}
			delivery := &recordingDeliveryQueue{}
			uc := newAckUsecase(cfg, repo, residenceOf(from, to), delivery)

			ack := signedAck(t, tc.kind, from, to, time.Now().Add(-time.Minute))
			mirror := derivedAcked(t, ack)
			if _, err := uc.Commit(systemCtx(), "127.0.0.1", mirror, domain.CommitModeExecute); err != nil {
				t.Fatalf("Commit returned error: %v", err)
			}
			mirrorID := documentIDOf(t, mirror)

			if len(repo.createdCommitLogs) != 1 || repo.createdCommitLogs[0] != mirrorID {
				t.Fatalf("CreateCommitLog calls = %v, want exactly [%s]", repo.createdCommitLogs, mirrorID)
			}
			if repo.commitLogOwners[mirrorID] != to.ccid {
				t.Fatalf("mirror commit owner = %q, want associate owner %s", repo.commitLogOwners[mirrorID], to.ccid)
			}
			if len(repo.ackCalls) != 1 {
				t.Fatalf("ack transitions = %+v, want exactly one", repo.ackCalls)
			}
			call := repo.ackCalls[0]
			if call.method != tc.wantMethod || call.from != from.ccid || call.to != to.ccid || call.schema != followSchema {
				t.Fatalf("ack transition = %+v, want %s(from=%s, to=%s, schema=%s)", call, tc.wantMethod, from.ccid, to.ccid, followSchema)
			}
			if repo.acknowledgeCalled || repo.unacknowledgeCalled {
				t.Fatal("a mirror must not be applied as a raw ack on the target's server")
			}
			if len(delivery.jobs) != 0 {
				t.Fatalf("a mirror must not be re-delivered, got %+v", delivery.jobs)
			}
			if len(repo.txs) != 1 || !repo.txs[0].committed {
				t.Fatalf("expected a committed tx, got %+v", repo.txs)
			}
			// CIP-10 §4 / §5.2: the accept-if-newer key is the document's
			// createdAt, which the mirror inherits from the original ack, so
			// both servers compare the same value.
			if doc, _ := ack.ParsedDocument(); !call.createdAt.Equal(doc.CreatedAt) {
				t.Fatalf("transition createdAt = %v, want the original ack's %v", call.createdAt, doc.CreatedAt)
			}
		})
	}
}

// The real entry for a mirror is Deliver re-entering Commit
// without any service-account context, so the document-direct proof must be
// verifiable (CIP-10 §5.2 verification rule): the embedded ack verifies
// against its author's signature and the mirror is byte-equal to the
// canonical derivation of the embedded document.
func TestCommitAckedVerifiesDocumentDirectProof(t *testing.T) {
	cfg := &domain.Config{FQDN: "example.com"}
	from := newAckParty(t, "remote.example.net")
	to := newAckParty(t, cfg.FQDN)

	ack := signedAck(t, "ack", from, to, time.Now().Add(-time.Minute))

	t.Run("canonical mirror is accepted", func(t *testing.T) {
		repo := &recordingRecordRepo{}
		uc := newAckUsecase(cfg, repo, residenceOf(from, to), &recordingDeliveryQueue{})

		if _, err := uc.Commit(context.Background(), "127.0.0.1", derivedAcked(t, ack), domain.CommitModeExecute); err != nil {
			t.Fatalf("Commit returned error: %v", err)
		}
		if !repo.acknowledgedCalled {
			t.Fatal("Acknowledged was not called for a verified mirror")
		}
	})

	t.Run("mirror not derived from the embedded ack is rejected", func(t *testing.T) {
		repo := &recordingRecordRepo{}
		uc := newAckUsecase(cfg, repo, residenceOf(from, to), &recordingDeliveryQueue{})

		forged := derivedAcked(t, ack)
		forged.Document = strings.Replace(forged.Document, `"kind":"acked"`, `"kind":"unacked"`, 1)
		if _, err := uc.Commit(context.Background(), "127.0.0.1", forged, domain.CommitModeExecute); err == nil {
			t.Fatal("a mirror whose kind does not correspond to the embedded ack must be rejected")
		}
		if repo.acknowledgedCalled || repo.unacknowledgedCalled {
			t.Fatal("a forged mirror must not reach the repository")
		}
	})

	t.Run("author-signed acked document is rejected", func(t *testing.T) {
		repo := &recordingRecordRepo{}
		uc := newAckUsecase(cfg, repo, residenceOf(from, to), &recordingDeliveryQueue{})

		// kind=acked signed directly by the author: only a server-derived
		// mirror (document-direct proof) may carry the acked kind
		selfSigned := signedAck(t, "acked", from, to, time.Now().Add(-time.Minute))
		if _, err := uc.Commit(context.Background(), "127.0.0.1", selfSigned, domain.CommitModeExecute); err == nil {
			t.Fatal("an author-signed acked document must be rejected")
		}
		if repo.acknowledgedCalled {
			t.Fatal("an author-signed acked document must not reach the repository")
		}
	})
}

// blockingRecordRepo makes the given author's block list contain the given
// targets (the query Commit issues for cross-user documents).
type blockingRecordRepo struct {
	recordingRecordRepo
	blocker string
	blocked []string
}

func (r *blockingRecordRepo) QueryByParent(ctx context.Context, parent, schema, author string, since, until *time.Time, limit int, order string) ([]QueryRow, error) {
	prefix := concrnt.ComposeCCURI("cckv", r.blocker, ".concrnt/blocking") + "/"
	if parent+"/" != prefix {
		return nil, nil
	}
	rows := make([]QueryRow, len(r.blocked))
	for i, id := range r.blocked {
		uri := prefix + id
		rows[i] = QueryRow{Row: concrnt.SignedDocument{CCKV: &uri}}
	}
	return rows, nil
}

// Mirrors are the target's own holding and must stay replayable from a
// repository dump (CIP-10 §5.2 検査の免除): the backdate window, the block
// check and the author-entity resolution do not apply, while a server that
// does not manage the associate owner simply has nothing to hold.
func TestCommitAckedExemptions(t *testing.T) {
	cfg := &domain.Config{FQDN: "example.com"}
	from := newAckParty(t, "remote.example.net")
	to := newAckParty(t, cfg.FQDN)

	t.Run("backdate window does not apply", func(t *testing.T) {
		repo := &recordingRecordRepo{}
		uc := newAckUsecase(cfg, repo, residenceOf(from, to), &recordingDeliveryQueue{})

		old := signedAck(t, "ack", from, to, time.Now().Add(-domain.MaxBackdate-24*time.Hour))
		if _, err := uc.Commit(context.Background(), "127.0.0.1", derivedAcked(t, old), domain.CommitModeExecute); err != nil {
			t.Fatalf("Commit returned error: %v", err)
		}
		if !repo.acknowledgedCalled {
			t.Fatal("Acknowledged was not called for a backdated mirror")
		}
	})

	t.Run("block relationship does not apply", func(t *testing.T) {
		repo := &blockingRecordRepo{blocker: from.ccid, blocked: []string{to.ccid}}
		uc := newAckUsecase(cfg, repo, residenceOf(from, to), &recordingDeliveryQueue{})

		ack := signedAck(t, "ack", from, to, time.Now().Add(-time.Minute))
		if _, err := uc.Commit(systemCtx(), "127.0.0.1", derivedAcked(t, ack), domain.CommitModeExecute); err != nil {
			t.Fatalf("Commit returned error: %v", err)
		}
		if !repo.acknowledgedCalled {
			t.Fatal("Acknowledged was not called for a mirror across a block")
		}
	})

	t.Run("unresolvable author still applies", func(t *testing.T) {
		// dump replay after the acker's server is gone: only the target is
		// known here
		repo := &recordingRecordRepo{}
		uc := newAckUsecase(cfg, repo, residenceOf(to), &recordingDeliveryQueue{})

		ack := signedAck(t, "ack", from, to, time.Now().Add(-time.Minute))
		if _, err := uc.Commit(systemCtx(), "127.0.0.1", derivedAcked(t, ack), domain.CommitModeExecute); err != nil {
			t.Fatalf("Commit returned error: %v", err)
		}
		if !repo.acknowledgedCalled {
			t.Fatal("Acknowledged was not called for a mirror whose author is unresolvable")
		}
	})

	t.Run("ignored when the associate owner is not local", func(t *testing.T) {
		// a server that manages neither side has nothing to hold: the commit
		// is a no-op success (CIP-3 §3.1), nothing is stored and the tx
		// rolls back
		elsewhere := newAckParty(t, "other.example.net")
		repo := &recordingRecordRepo{}
		uc := newAckUsecase(cfg, repo, residenceOf(from, elsewhere), &recordingDeliveryQueue{})

		ack := signedAck(t, "ack", from, elsewhere, time.Now().Add(-time.Minute))
		if _, err := uc.Commit(systemCtx(), "127.0.0.1", derivedAcked(t, ack), domain.CommitModeExecute); err != nil {
			t.Fatalf("Commit returned error: %v", err)
		}
		if repo.acknowledgedCalled {
			t.Fatal("a mirror for an associate owner this server does not manage must not reach the repository")
		}
		if len(repo.txs) != 1 || repo.txs[0].committed || !repo.txs[0].rolledBack {
			t.Fatalf("expected a single rolled-back tx, got %+v", repo.txs)
		}
	})
}
