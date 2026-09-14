package record

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/impl/interop"
	"github.com/concrnt/concrnt/internal/domain"
	"github.com/concrnt/concrnt/internal/service"
	"github.com/concrnt/concrnt/internal/usecase"
	"github.com/concrnt/concrnt/internal/usecase/server"
	"github.com/concrnt/concrnt/policy"
	"github.com/concrnt/concrnt/schemas"
)

type stubRecordRepo struct{ Repository }

func (stubRecordRepo) HasCommitLog(ctx context.Context, id string) (bool, error) {
	return false, nil
}

type stubResidenceRepo struct{ EntityRepository }

func (stubResidenceRepo) GetEntityByCCID(ctx context.Context, ccid string) (*domain.Entity, error) {
	return nil, domain.ErrNotFound
}

// recordingRecordRepo satisfies the write path of Repository in memory
// and records which mutations were attempted. commitLogs seeds the ids
// HasCommitLog answers true for (already-committed documents); storedSD, when
// set, is what GetSignedDocument serves for every key (a pre-existing record);
// storedCreatedAt, when set, is the createdAt of the document the repository
// already holds for whatever CreateEntity / CreateRecord write — both report
// the accept-if-newer loss (applied=false) for a createdAt that is not
// strictly newer, mirroring the repository's row-lock comparison (CIP-3
// §3.4); ackStale / ackedStale
// make Acknowledge / UnAcknowledge and Acknowledged / UnAcknowledged report
// that loss unconditionally.
type recordingRecordRepo struct {
	Repository
	commitLogs      map[string]bool
	storedSD        *concrnt.SignedDocument
	storedCreatedAt time.Time
	ackStale        bool
	ackedStale      bool

	createEntityCalled   bool
	createRecordCalled   bool
	acknowledgeCalled    bool
	unacknowledgeCalled  bool
	acknowledgedCalled   bool
	unacknowledgedCalled bool
	txs                  []*recordingTx

	// createdCommitLogs records every CreateCommitLog call in order, and
	// commitLogOwners the owner each documentID was recorded under.
	createdCommitLogs []string
	commitLogOwners   map[string]string
	// ackCalls records every ack-state transition attempted, in order.
	ackCalls []ackCall
	// gcFlagged records MarkCommitLogGcCandidate calls in order.
	gcFlagged []string
}

// ackCall is one Acknowledge / UnAcknowledge / Acknowledged / UnAcknowledged
// call as the repository saw it.
type ackCall struct {
	method     string
	documentID string
	from       string
	to         string
	schema     string
	createdAt  time.Time
}

func (r *recordingRecordRepo) RunInTx(ctx context.Context, fn func(tx RepositoryTx) error) error {
	tx := &recordingTx{}
	r.txs = append(r.txs, tx)
	err := fn(tx)
	if err != nil {
		tx.rolledBack = true
		return err
	}
	tx.committed = true
	return nil
}
func (r *recordingRecordRepo) CreateCommitLog(ctx context.Context, tx RepositoryTx, id string, ip string, document string, proof any, owner string) error {
	r.createdCommitLogs = append(r.createdCommitLogs, id)
	if r.commitLogOwners == nil {
		r.commitLogOwners = map[string]string{}
	}
	// same first-write-wins semantics as the postgres implementation
	// (ON CONFLICT DO NOTHING on the primary key)
	if _, exists := r.commitLogOwners[id]; !exists {
		r.commitLogOwners[id] = owner
	}
	return nil
}
func (r *recordingRecordRepo) HasCommitLog(ctx context.Context, id string) (bool, error) {
	return r.commitLogs[id], nil
}
func (r *recordingRecordRepo) MarkCommitLogGcCandidate(ctx context.Context, tx RepositoryTx, documentID string) error {
	r.gcFlagged = append(r.gcFlagged, documentID)
	return nil
}
func (r *recordingRecordRepo) CreateEntity(ctx context.Context, tx RepositoryTx, ccid string, alias *string, domain string, documentID string, createdAt time.Time) (bool, error) {
	r.createEntityCalled = true
	return r.storedCreatedAt.IsZero() || createdAt.After(r.storedCreatedAt), nil
}
func (r *recordingRecordRepo) CreateRecord(ctx context.Context, tx RepositoryTx, documentID string, key string, owner string, author string, schema string, onUpdate *string, policies *string, distributions []string, redirect *string, createdAt time.Time) (bool, error) {
	r.createRecordCalled = true
	return r.storedCreatedAt.IsZero() || createdAt.After(r.storedCreatedAt), nil
}
func (r *recordingRecordRepo) Acknowledge(ctx context.Context, tx RepositoryTx, documentID string, from string, to string, schema string, createdAt time.Time) (bool, error) {
	r.acknowledgeCalled = true
	r.ackCalls = append(r.ackCalls, ackCall{"Acknowledge", documentID, from, to, schema, createdAt})
	return !r.ackStale, nil
}
func (r *recordingRecordRepo) UnAcknowledge(ctx context.Context, tx RepositoryTx, documentID string, from string, to string, schema string, createdAt time.Time) (bool, error) {
	r.unacknowledgeCalled = true
	r.ackCalls = append(r.ackCalls, ackCall{"UnAcknowledge", documentID, from, to, schema, createdAt})
	return !r.ackStale, nil
}
func (r *recordingRecordRepo) Acknowledged(ctx context.Context, tx RepositoryTx, documentID string, from string, to string, schema string, createdAt time.Time) (bool, error) {
	r.acknowledgedCalled = true
	r.ackCalls = append(r.ackCalls, ackCall{"Acknowledged", documentID, from, to, schema, createdAt})
	return !r.ackedStale, nil
}
func (r *recordingRecordRepo) UnAcknowledged(ctx context.Context, tx RepositoryTx, documentID string, from string, to string, schema string, createdAt time.Time) (bool, error) {
	r.unacknowledgedCalled = true
	r.ackCalls = append(r.ackCalls, ackCall{"UnAcknowledged", documentID, from, to, schema, createdAt})
	return !r.ackedStale, nil
}
func (r *recordingRecordRepo) GetHierarchicalRecordPolicies(ctx context.Context, uri string) ([]concrnt.Policy, error) {
	return nil, nil
}
func (r *recordingRecordRepo) GetSignedDocument(ctx context.Context, uri string) (*concrnt.SignedDocument, error) {
	if r.storedSD != nil {
		return r.storedSD, nil
	}
	return nil, domain.ErrNotFound
}

// QueryByParent answers the blocking-list query Commit issues when a
// document's author and target owner differ: nobody blocks anybody.
func (r *recordingRecordRepo) QueryByParent(ctx context.Context, parent, schema, author string, since, until *time.Time, limit int, order string) ([]QueryRow, error) {
	return nil, nil
}

// fixedResidenceRepo serves one pre-existing entity for every lookup.
type fixedResidenceRepo struct {
	EntityRepository
	entity *domain.Entity
}

func (s fixedResidenceRepo) GetEntityByCCID(ctx context.Context, ccid string) (*domain.Entity, error) {
	if s.entity == nil {
		return nil, domain.ErrNotFound
	}
	return s.entity, nil
}

// testServerRepo serves a fixed server descriptor for every lookup.
type testServerRepo struct {
	server *domain.Server
}

func (s testServerRepo) Resolve(ctx context.Context, identifier string, hint *string) (*domain.Server, error) {
	if s.server == nil {
		return nil, domain.ErrNotFound
	}
	return s.server, nil
}

func (s testServerRepo) List(ctx context.Context) ([]*concrnt.WellKnownConcrnt, error) {
	return nil, nil
}

// newTestServerUsecase wires a server.Usecase that resolves every remote domain
// to a green (same-layer, unblocked) server.
func newTestServerUsecase(cfg *domain.Config) *server.Usecase {
	sv := &domain.Server{WellKnown: concrnt.WellKnownConcrnt{Layer: cfg.Layer}}
	return server.New(testServerRepo{server: sv}, cfg, concrnt.SoftwareInfo{}, service.NewModuleManager(map[string]string{}, nil), nil)
}

type nopPolicyService struct{}

func (nopPolicyService) Eval(ctx context.Context, req policy.RequestContext, stack []concrnt.Policy, action string, key string) error {
	return nil
}

type nopSignalService struct{}

func (nopSignalService) Publish(ctx context.Context, channel string, event concrnt.Event) error {
	return nil
}

func signTestDocument[T any](t *testing.T, doc concrnt.Document[T], privKeyHex string) concrnt.SignedDocument {
	t.Helper()

	docBytes, err := json.Marshal(doc)
	if err != nil {
		t.Fatalf("marshal document: %v", err)
	}
	sigBytes, err := concrnt.SignBytes(docBytes, privKeyHex)
	if err != nil {
		t.Fatalf("sign document: %v", err)
	}
	signature := hex.EncodeToString(sigBytes)

	return concrnt.SignedDocument{
		Document: string(docBytes),
		Proof: concrnt.Proof{
			Type:      concrnt.ProofTypeEcrecover,
			Signature: &signature,
		},
	}
}

func signedCommitDocument(t *testing.T, kind string) concrnt.SignedDocument {
	t.Helper()

	priv := make([]byte, 32)
	if _, err := rand.Read(priv); err != nil {
		t.Fatalf("generate private key: %v", err)
	}
	privKeyHex := hex.EncodeToString(priv)
	ccid, err := concrnt.PrivKeyToAddr(privKeyHex, "con")
	if err != nil {
		t.Fatalf("derive ccid: %v", err)
	}

	doc := concrnt.Document[any]{
		Kind:      kind,
		Key:       concrnt.CCURI{Scheme: "cckv", Owner: ccid, Key: "test/1"}.String(),
		Author:    ccid,
		Schema:    "https://example.com/schema.json",
		CreatedAt: time.Now(),
	}
	docBytes, err := json.Marshal(doc)
	if err != nil {
		t.Fatalf("marshal document: %v", err)
	}
	sigBytes, err := concrnt.SignBytes(docBytes, privKeyHex)
	if err != nil {
		t.Fatalf("sign document: %v", err)
	}
	signature := hex.EncodeToString(sigBytes)

	return concrnt.SignedDocument{
		Document: string(docBytes),
		Proof: concrnt.Proof{
			Type:      concrnt.ProofTypeEcrecover,
			Signature: &signature,
		},
	}
}

// A validly signed commit whose author entity cannot be resolved (e.g. its
// entity document has a none proof, so a remote server refuses to import it)
// must fail with an error, not a nil-pointer panic.
func TestCommitUnresolvableRequesterReturnsError(t *testing.T) {
	cfg := &domain.Config{FQDN: "example.com"}
	uc := New(
		stubRecordRepo{},
		stubResidenceRepo{},
		newTestServerUsecase(cfg),
		cfg,
		nil,
		nil,
		nil,
		nil,
		nil,
	)

	for _, kind := range []string{"record", "association"} {
		t.Run(kind, func(t *testing.T) {
			sd := signedCommitDocument(t, kind)

			_, err := uc.Commit(context.Background(), "127.0.0.1", sd, domain.CommitModeExecute)
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if !strings.Contains(err.Error(), "requester entity not found") {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

// Entity commits are accept-if-newer: a document older than (or as old as)
// the stored one succeeds as a no-op instead of clobbering the newer
// affiliation. The ordering is decided by the repository under its row lock
// (the usecase no longer pre-compares CDIDs), so CreateEntity is always
// attempted; what matters is that a reported loss rolls the commit
// transaction back — a rejected-as-older document leaves no commit_log
// behind — while a win commits it.
func TestCommitEntityAcceptIfNewer(t *testing.T) {
	priv := make([]byte, 32)
	if _, err := rand.Read(priv); err != nil {
		t.Fatalf("generate private key: %v", err)
	}
	privKeyHex := hex.EncodeToString(priv)
	ccid, err := concrnt.PrivKeyToAddr(privKeyHex, "con")
	if err != nil {
		t.Fatalf("derive ccid: %v", err)
	}

	newEntityDoc := func(createdAt time.Time) concrnt.SignedDocument {
		return signTestDocument(t, concrnt.Document[schemas.Entity]{
			Kind:      "entity",
			Value:     schemas.Entity{Domain: "remote.example.net"},
			Author:    ccid,
			CreatedAt: createdAt,
		}, privKeyHex)
	}

	// Recent so the always-on backdate window doesn't reject these commits.
	storedAt := time.Now().Add(-time.Hour).UTC().Truncate(time.Second)

	cases := []struct {
		name        string
		storedAt    time.Time
		createdAt   time.Time
		wantApplied bool
	}{
		{"older", storedAt, storedAt.Add(-time.Hour), false},
		{"same", storedAt, storedAt, false},
		{"newer", storedAt, storedAt.Add(time.Hour), true},
		// A stored zero createdAt (a migration import from before the migrator
		// set CreatedAt) must lose to any real commit: its CDID used to wrap
		// around into the far future and win accept-if-newer forever.
		{"stored zero createdAt", time.Time{}, storedAt, true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stored := newEntityDoc(tc.storedAt)
			repo := &recordingRecordRepo{storedCreatedAt: tc.storedAt}
			cfg := &domain.Config{FQDN: "example.com"}
			uc := New(
				repo,
				fixedResidenceRepo{entity: &domain.Entity{
					ID:             ccid,
					Domain:         "remote.example.net",
					SignedDocument: &stored,
				}},
				newTestServerUsecase(cfg),
				cfg,
				nil,
				nil,
				nil,
				nil,
				nil,
			)

			sd := newEntityDoc(tc.createdAt)
			if _, err := uc.Commit(context.Background(), "127.0.0.1", sd, domain.CommitModeExecute); err != nil {
				t.Fatalf("Commit returned error: %v", err)
			}
			if !repo.createEntityCalled {
				t.Fatal("CreateEntity was not attempted")
			}
			if len(repo.txs) != 1 {
				t.Fatalf("expected 1 tx, got %d", len(repo.txs))
			}
			if repo.txs[0].committed != tc.wantApplied || repo.txs[0].rolledBack == tc.wantApplied {
				t.Fatalf("tx state = %+v, want committed = %v", repo.txs[0], tc.wantApplied)
			}
		})
	}
}

// Record commits follow the same accept-if-newer rule (CIP-3 §3.4): a
// document older than (or as old as) the one stored at its key succeeds as a
// no-op. The repository decides the ordering under the RecordKey row lock, so
// CreateRecord is always attempted; a reported loss must roll the tx back (no
// commit_log), so the stored newer version is neither overwritten nor
// tombstoned.
func TestCommitRecordAcceptIfNewer(t *testing.T) {
	ccid, priv := newIdentity(t)
	cfg := &domain.Config{FQDN: "example.com"}

	// Recent so the always-on backdate window doesn't reject these commits.
	storedAt := time.Now().Add(-time.Hour).UTC().Truncate(time.Second)
	stored := signedRecord(t, ccid, priv, storedAt)

	cases := []struct {
		name        string
		createdAt   time.Time
		wantApplied bool
	}{
		{"older", storedAt.Add(-time.Minute), false},
		{"same", storedAt, false},
		{"newer", storedAt.Add(time.Minute), true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			repo := &recordingRecordRepo{storedSD: &stored, storedCreatedAt: storedAt}
			uc := newRecordCommitUsecase(ccid, cfg, repo, nil)

			sd := signedRecord(t, ccid, priv, tc.createdAt)
			if _, err := uc.Commit(context.Background(), "127.0.0.1", sd, domain.CommitModeExecute); err != nil {
				t.Fatalf("Commit returned error: %v", err)
			}
			if !repo.createRecordCalled {
				t.Fatal("CreateRecord was not attempted")
			}
			if len(repo.txs) != 1 {
				t.Fatalf("expected 1 tx, got %d", len(repo.txs))
			}
			if repo.txs[0].committed != tc.wantApplied || repo.txs[0].rolledBack == tc.wantApplied {
				t.Fatalf("tx state = %+v, want committed = %v", repo.txs[0], tc.wantApplied)
			}
		})
	}
}

// Ack/unack transitions follow accept-if-newer too (CIP-10 §4): when the
// repository reports the stored (from, to, schema) state already carries a
// newer document, the commit is a no-op success — the tx rolls back (no
// commit_log) and no side effects run. The same holds for the target-side
// acked/unacked mirror state (CIP-10 §5.2), exercised here with the proof
// check bypassed (system context).
func TestCommitAckAcceptIfNewer(t *testing.T) {
	cfg := &domain.Config{FQDN: "example.com"}
	// Self-ack keeps author == target owner, sidestepping the blocking-list
	// lookup Commit issues for cross-user operations.
	self := newAckParty(t, cfg.FQDN)

	cases := []struct {
		kind   string
		mirror bool
		called func(*recordingRecordRepo) bool
		ctx    context.Context
	}{
		{"ack", false, func(r *recordingRecordRepo) bool { return r.acknowledgeCalled }, context.Background()},
		{"unack", false, func(r *recordingRecordRepo) bool { return r.unacknowledgeCalled }, context.Background()},
		{"acked", true, func(r *recordingRecordRepo) bool { return r.acknowledgedCalled }, systemCtx()},
		{"unacked", true, func(r *recordingRecordRepo) bool { return r.unacknowledgedCalled }, systemCtx()},
	}

	for _, tc := range cases {
		for _, stale := range []bool{false, true} {
			name := tc.kind + "/applied"
			if stale {
				name = tc.kind + "/stale"
			}
			t.Run(name, func(t *testing.T) {
				repo := &recordingRecordRepo{ackStale: stale, ackedStale: stale}
				uc := newAckUsecase(cfg, repo, residenceOf(self), &recordingDeliveryQueue{})

				var sd concrnt.SignedDocument
				if tc.mirror {
					sd = derivedAcked(t, signedAck(t, strings.TrimSuffix(tc.kind, "ed"), self, self, time.Now().Add(-time.Minute)))
				} else {
					sd = signedAck(t, tc.kind, self, self, time.Now().Add(-time.Minute))
				}

				if _, err := uc.Commit(tc.ctx, "127.0.0.1", sd, domain.CommitModeExecute); err != nil {
					t.Fatalf("Commit returned error: %v", err)
				}
				if !tc.called(repo) {
					t.Fatalf("repository transition for %s was not attempted (calls: %+v)", tc.kind, repo.ackCalls)
				}
				if len(repo.txs) != 1 {
					t.Fatalf("expected 1 tx, got %d", len(repo.txs))
				}
				applied := !stale
				if repo.txs[0].committed != applied || repo.txs[0].rolledBack == applied {
					t.Fatalf("tx state = %+v, want committed = %v", repo.txs[0], applied)
				}
			})
		}
	}
}

// A document-reference commit whose target is inlined in References must
// succeed with no client at all (resolver == nil) — this is the migration /
// import scenario, where the referenced record's origin server may not be
// reachable (e.g. it hasn't migrated yet).
func TestCommitDocumentReferenceInlineTargetOffline(t *testing.T) {
	priv := make([]byte, 32)
	if _, err := rand.Read(priv); err != nil {
		t.Fatalf("generate private key: %v", err)
	}
	privKeyHex := hex.EncodeToString(priv)
	ccid, err := concrnt.PrivKeyToAddr(privKeyHex, "con")
	if err != nil {
		t.Fatalf("derive ccid: %v", err)
	}

	// Recent so the always-on backdate window doesn't reject these commits.
	createdAt := time.Now().Add(-time.Minute).UTC().Truncate(time.Second)
	targetURI := concrnt.CCURI{Scheme: "cckv", Owner: ccid, Key: "posts/1"}.String()
	targetSD := signTestDocument(t, concrnt.Document[any]{
		Kind:      "record",
		Key:       targetURI,
		Author:    ccid,
		Schema:    "https://example.com/post.json",
		CreatedAt: createdAt,
	}, privKeyHex)

	refKey := concrnt.CCURI{Scheme: "cckv", Owner: ccid, Key: "timelines/home/entry1"}.String()
	refDoc := concrnt.Document[schemas.Reference]{
		Kind:      "record",
		Key:       refKey,
		Value:     schemas.Reference{Href: targetURI},
		Author:    ccid,
		Schema:    schemas.ReferenceURL,
		CreatedAt: createdAt,
	}
	refDocBytes, err := json.Marshal(refDoc)
	if err != nil {
		t.Fatalf("marshal reference document: %v", err)
	}
	sd := concrnt.SignedDocument{
		Document:   string(refDocBytes),
		Proof:      concrnt.Proof{Type: concrnt.ProofTypeDocumentReference, Href: &targetURI},
		References: map[string]concrnt.SignedDocument{targetURI: targetSD},
	}

	repo := &recordingRecordRepo{}
	cfg := &domain.Config{FQDN: "example.com"}
	uc := New(
		repo,
		fixedResidenceRepo{entity: &domain.Entity{ID: ccid, Domain: "remote.example.net"}},
		newTestServerUsecase(cfg),
		cfg,
		nil, // no client: verification must not need the network
		nopSignalService{},
		nopPolicyService{},
		nil,
		nil,
	)

	if _, err := uc.Commit(context.Background(), "127.0.0.1", sd, domain.CommitModeExecute); err != nil {
		t.Fatalf("Commit returned error: %v", err)
	}
	if !repo.createRecordCalled {
		t.Fatal("CreateRecord was not called")
	}
}

// stubKVS records SetAdd calls.
type stubKVS struct {
	addedSets []string
}

func (s *stubKVS) SetAdd(ctx context.Context, key string, value string, ttl time.Duration) error {
	s.addedSets = append(s.addedSets, key)
	return nil
}
func (s *stubKVS) SetMembers(ctx context.Context, key string) ([]string, error) {
	return nil, nil
}

// signedRecord builds a signed record commit by the given identity at createdAt.
func signedRecord(t *testing.T, ccid, privKeyHex string, createdAt time.Time) concrnt.SignedDocument {
	t.Helper()
	return signTestDocument(t, concrnt.Document[any]{
		Kind:      "record",
		Key:       concrnt.CCURI{Scheme: "cckv", Owner: ccid, Key: "posts/1"}.String(),
		Author:    ccid,
		Schema:    "https://example.com/post.json",
		CreatedAt: createdAt,
	}, privKeyHex)
}

// newRecordCommitUsecase wires a usecase whose author entity is local and
// resolvable, so a plain record commit reaches the repository.
func newRecordCommitUsecase(ccid string, cfg *domain.Config, repo Repository, store usecase.KVS) *Usecase {
	return New(
		repo,
		fixedResidenceRepo{entity: &domain.Entity{ID: ccid, Domain: cfg.FQDN}},
		newTestServerUsecase(cfg),
		cfg,
		nil,
		nopSignalService{},
		nopPolicyService{},
		nil,
		store,
	)
}

func newIdentity(t *testing.T) (ccid, privKeyHex string) {
	t.Helper()
	priv := make([]byte, 32)
	if _, err := rand.Read(priv); err != nil {
		t.Fatalf("generate private key: %v", err)
	}
	privKeyHex = hex.EncodeToString(priv)
	ccid, err := concrnt.PrivKeyToAddr(privKeyHex, "con")
	if err != nil {
		t.Fatalf("derive ccid: %v", err)
	}
	return ccid, privKeyHex
}

// A document stamped too far in the future is rejected for everyone.
func TestCommitRejectsFarFutureCreatedAt(t *testing.T) {
	ccid, priv := newIdentity(t)
	cfg := &domain.Config{FQDN: "example.com"}
	repo := &recordingRecordRepo{}
	uc := newRecordCommitUsecase(ccid, cfg, repo, nil)

	sd := signedRecord(t, ccid, priv, time.Now().Add(24*time.Hour))
	_, err := uc.Commit(context.Background(), "127.0.0.1", sd, domain.CommitModeExecute)
	if err == nil || !strings.Contains(err.Error(), "future") {
		t.Fatalf("expected future-createdAt rejection, got %v", err)
	}
	if repo.createRecordCalled {
		t.Fatal("CreateRecord should not be called for a far-future document")
	}
}

// Documents older than the backdate window are rejected, while documents
// within it commit normally.
func TestCommitBackdateWindow(t *testing.T) {
	ccid, priv := newIdentity(t)
	cfg := &domain.Config{FQDN: "example.com"}

	t.Run("too old", func(t *testing.T) {
		repo := &recordingRecordRepo{}
		uc := newRecordCommitUsecase(ccid, cfg, repo, nil)
		sd := signedRecord(t, ccid, priv, time.Now().Add(-domain.MaxBackdate-time.Hour))
		_, err := uc.Commit(context.Background(), "127.0.0.1", sd, domain.CommitModeExecute)
		if err == nil || !strings.Contains(err.Error(), "backdate") {
			t.Fatalf("expected backdate rejection, got %v", err)
		}
		if repo.createRecordCalled {
			t.Fatal("CreateRecord should not be called for a too-old document")
		}
	})

	t.Run("within window", func(t *testing.T) {
		repo := &recordingRecordRepo{}
		uc := newRecordCommitUsecase(ccid, cfg, repo, nil)
		sd := signedRecord(t, ccid, priv, time.Now().Add(-domain.MaxBackdate+time.Hour))
		if _, err := uc.Commit(context.Background(), "127.0.0.1", sd, domain.CommitModeExecute); err != nil {
			t.Fatalf("Commit returned error: %v", err)
		}
		if !repo.createRecordCalled {
			t.Fatal("CreateRecord was not called for an in-window document")
		}
	})
}

// Entity documents are exempt from the backdate window (CIP-3 §3.4): the
// affiliation signature is long-lived and re-presented indefinitely, and
// accept-if-newer already no-ops old replays. In exchange they must be
// master-key signed (CIP-0 §8.2) — a subkey proof would let a leaked,
// since-revoked subkey forge a backdated affiliation forever.
func TestCommitEntityBackdateExemptRequiresDirectProof(t *testing.T) {
	ccid, priv := newIdentity(t)
	cfg := &domain.Config{FQDN: "example.com"}

	newEntitySD := func(createdAt time.Time) concrnt.SignedDocument {
		return signTestDocument(t, concrnt.Document[schemas.Entity]{
			Kind:      "entity",
			Value:     schemas.Entity{Domain: "remote.example.net"},
			Author:    ccid,
			CreatedAt: createdAt,
		}, priv)
	}
	newEntityUsecase := func(repo Repository) *Usecase {
		return New(
			repo,
			fixedResidenceRepo{},
			newTestServerUsecase(cfg),
			cfg,
			nil,
			nopSignalService{},
			nopPolicyService{},
			nil,
			nil,
		)
	}

	t.Run("old entity document applies", func(t *testing.T) {
		repo := &recordingRecordRepo{}
		uc := newEntityUsecase(repo)
		sd := newEntitySD(time.Now().Add(-domain.MaxBackdate - 30*24*time.Hour))
		if _, err := uc.Commit(context.Background(), "127.0.0.1", sd, domain.CommitModeExecute); err != nil {
			t.Fatalf("Commit returned error: %v", err)
		}
		if !repo.createEntityCalled {
			t.Fatal("CreateEntity was not called for an old entity document")
		}
	})

	t.Run("subkey proof rejected", func(t *testing.T) {
		repo := &recordingRecordRepo{}
		uc := newEntityUsecase(repo)
		sd := newEntitySD(time.Now().Add(-time.Minute))
		kid := concrnt.CCURI{Scheme: "cckv", Owner: ccid, Key: "subkeys/cck1dummy"}.String()
		sd.Proof.Type = concrnt.ProofTypeSubkey
		sd.Proof.Key = &kid
		_, err := uc.Commit(context.Background(), "127.0.0.1", sd, domain.CommitModeExecute)
		if err == nil || !strings.Contains(err.Error(), "must be signed with") {
			t.Fatalf("expected proof-type rejection, got %v", err)
		}
		if repo.createEntityCalled {
			t.Fatal("CreateEntity should not be called for a subkey-signed entity document")
		}
	})
}

// documentIDOf derives the documentID a signed document commits under, the
// same way Commit does: CDID from the document body + createdAt.
func documentIDOf(t *testing.T, sd concrnt.SignedDocument) string {
	t.Helper()
	id, err := sd.CDID()
	if err != nil {
		t.Fatalf("derive document id: %v", err)
	}
	return id
}

// A document already in commit_logs was fully applied once: redelivery is a
// no-op success, nothing reaches the repository again. This is the replay
// guard — deleted and superseded documents stay in commit_logs, so a captured
// copy can't be replayed back in — while a fresh document at the same cckv
// key has a different CDID and commits normally.
func TestCommitSkipsAlreadyCommittedDocument(t *testing.T) {
	ccid, priv := newIdentity(t)
	cfg := &domain.Config{FQDN: "example.com"}

	committed := signedRecord(t, ccid, priv, time.Now().Add(-time.Hour))

	t.Run("redelivery of a committed document", func(t *testing.T) {
		repo := &recordingRecordRepo{commitLogs: map[string]bool{documentIDOf(t, committed): true}}
		uc := newRecordCommitUsecase(ccid, cfg, repo, nil)
		result, err := uc.Commit(context.Background(), "127.0.0.1", committed, domain.CommitModeExecute)
		if err != nil {
			t.Fatalf("redelivery must succeed, got %v", err)
		}
		if result == nil {
			t.Fatal("redelivery must return the document")
		}
		if repo.createRecordCalled {
			t.Fatal("CreateRecord should not be called for an already-committed document")
		}
		if len(repo.txs) != 0 {
			t.Fatal("no transaction may be opened for an already-committed document")
		}
	})

	t.Run("fresh document at the same key", func(t *testing.T) {
		repo := &recordingRecordRepo{commitLogs: map[string]bool{documentIDOf(t, committed): true}}
		uc := newRecordCommitUsecase(ccid, cfg, repo, nil)
		fresh := signedRecord(t, ccid, priv, time.Now())
		if _, err := uc.Commit(context.Background(), "127.0.0.1", fresh, domain.CommitModeExecute); err != nil {
			t.Fatalf("Commit returned error: %v", err)
		}
		if !repo.createRecordCalled {
			t.Fatal("CreateRecord was not called for a fresh document at a committed key")
		}
	})
}

// System service accounts bypass the backdate window (the migration/import
// path replays old documents), but not the already-committed skip: a
// re-imported document is still a no-op.
func TestCommitSystemAccountBypassesBackdateWindow(t *testing.T) {
	ccid, priv := newIdentity(t)
	cfg := &domain.Config{FQDN: "example.com"}
	ctx := context.WithValue(context.Background(), interop.ServiceAccountTypeCtxKey, "system")

	sd := signedRecord(t, ccid, priv, time.Now().Add(-100*24*time.Hour))

	t.Run("old document applies", func(t *testing.T) {
		repo := &recordingRecordRepo{}
		uc := newRecordCommitUsecase(ccid, cfg, repo, nil)
		if _, err := uc.Commit(ctx, "127.0.0.1", sd, domain.CommitModeExecute); err != nil {
			t.Fatalf("system commit returned error: %v", err)
		}
		if !repo.createRecordCalled {
			t.Fatal("CreateRecord was not called for a system-account commit")
		}
	})

	t.Run("already-committed document still no-ops", func(t *testing.T) {
		repo := &recordingRecordRepo{commitLogs: map[string]bool{documentIDOf(t, sd): true}}
		uc := newRecordCommitUsecase(ccid, cfg, repo, nil)
		if _, err := uc.Commit(ctx, "127.0.0.1", sd, domain.CommitModeExecute); err != nil {
			t.Fatalf("system redelivery must succeed, got %v", err)
		}
		if repo.createRecordCalled {
			t.Fatal("CreateRecord should not be called for a re-imported document")
		}
	})
}

// associationRecordingRepo captures the unique key passed to CreateAssociation.
type associationRecordingRepo struct {
	recordingRecordRepo
	uniques []string
}

func (r *associationRecordingRepo) CreateAssociation(ctx context.Context, tx RepositoryTx, documentID string, targetURI string, owner string, author string, schema string, variant *string, unique string, createdAt time.Time) (bool, error) {
	r.uniques = append(r.uniques, unique)
	return true, nil
}

// importRecordingRepo additionally answers the blocking-list query issued when
// a commit's author and target owner differ.
type importRecordingRepo struct {
	associationRecordingRepo
}

func (r *importRecordingRepo) QueryByParent(ctx context.Context, parent, schema, author string, since, until *time.Time, limit int, order string) ([]QueryRow, error) {
	return nil, nil
}

// Self-service migration: an authenticated requester importing a repository
// dump (LocalOnlyExecute) is exempt from the backdate window for their own
// documents and for others' documents targeting their content. The exemption
// requires both the import mode and authentication.
func TestCommitBackdateExemptForAuthenticatedSelfImport(t *testing.T) {
	ccid, priv := newIdentity(t)
	cfg := &domain.Config{FQDN: "example.com"}
	old := time.Now().Add(-domain.MaxBackdate - time.Hour)
	authedCtx := context.WithValue(context.Background(), interop.RequesterCtxKey, domain.Entity{ID: ccid, Domain: cfg.FQDN})

	t.Run("own old document imports", func(t *testing.T) {
		repo := &recordingRecordRepo{}
		uc := newRecordCommitUsecase(ccid, cfg, repo, nil)
		sd := signedRecord(t, ccid, priv, old)
		if _, err := uc.Commit(authedCtx, "127.0.0.1", sd, domain.CommitModeLocalOnlyExecute); err != nil {
			t.Fatalf("Commit returned error: %v", err)
		}
		if !repo.createRecordCalled {
			t.Fatal("CreateRecord was not called for an exempt import")
		}
	})

	t.Run("unauthenticated import rejected", func(t *testing.T) {
		repo := &recordingRecordRepo{}
		uc := newRecordCommitUsecase(ccid, cfg, repo, nil)
		sd := signedRecord(t, ccid, priv, old)
		_, err := uc.Commit(context.Background(), "127.0.0.1", sd, domain.CommitModeLocalOnlyExecute)
		if err == nil || !strings.Contains(err.Error(), "backdate") {
			t.Fatalf("expected backdate rejection, got %v", err)
		}
	})

	t.Run("non-import mode rejected", func(t *testing.T) {
		repo := &recordingRecordRepo{}
		uc := newRecordCommitUsecase(ccid, cfg, repo, nil)
		sd := signedRecord(t, ccid, priv, old)
		_, err := uc.Commit(authedCtx, "127.0.0.1", sd, domain.CommitModeExecute)
		if err == nil || !strings.Contains(err.Error(), "backdate") {
			t.Fatalf("expected backdate rejection, got %v", err)
		}
	})

	t.Run("other author targeting requester imports", func(t *testing.T) {
		otherCCID, otherPriv := newIdentity(t)
		associate := concrnt.CCURI{Scheme: "cckv", Owner: ccid, Key: "posts/1"}.String()
		sd := signTestDocument(t, concrnt.Document[any]{
			Kind:      "association",
			Associate: &associate,
			Author:    otherCCID,
			Schema:    "https://example.com/a/like.json",
			CreatedAt: old,
		}, otherPriv)

		repo := &importRecordingRepo{}
		uc := newRecordCommitUsecase(ccid, cfg, repo, nil)
		if _, err := uc.Commit(authedCtx, "127.0.0.1", sd, domain.CommitModeLocalOnlyExecute); err != nil {
			t.Fatalf("Commit returned error: %v", err)
		}
		if len(repo.uniques) != 1 {
			t.Fatal("CreateAssociation was not called for an inbound association import")
		}
	})

	t.Run("other author unrelated target rejected", func(t *testing.T) {
		otherCCID, otherPriv := newIdentity(t)
		sd := signedRecord(t, otherCCID, otherPriv, old)

		repo := &importRecordingRepo{}
		uc := newRecordCommitUsecase(ccid, cfg, repo, nil)
		_, err := uc.Commit(authedCtx, "127.0.0.1", sd, domain.CommitModeLocalOnlyExecute)
		if err == nil || !strings.Contains(err.Error(), "backdate") {
			t.Fatalf("expected backdate rejection, got %v", err)
		}
	})
}

// The association unique key includes the body: associations that differ only
// in body must coexist (v1 semantics — e.g. two replies by the same author to
// the same message), while a byte-identical body still dedupes.
func TestCommitAssociationUniqueIncludesBody(t *testing.T) {
	ccid, priv := newIdentity(t)
	cfg := &domain.Config{FQDN: "example.com"}
	repo := &associationRecordingRepo{}
	uc := newRecordCommitUsecase(ccid, cfg, repo, nil)

	associate := concrnt.CCURI{Scheme: "cckv", Owner: ccid, Key: "posts/1"}.String()
	commitWithBody := func(body any) {
		t.Helper()
		sd := signTestDocument(t, concrnt.Document[any]{
			Kind:      "association",
			Associate: &associate,
			Value:     body,
			Author:    ccid,
			Schema:    "https://example.com/a/reply.json",
			CreatedAt: time.Now(),
		}, priv)
		if _, err := uc.Commit(context.Background(), "127.0.0.1", sd, domain.CommitModeDryRun); err != nil {
			t.Fatalf("commit returned error: %v", err)
		}
	}

	commitWithBody(map[string]any{"messageId": "m1"})
	commitWithBody(map[string]any{"messageId": "m2"})
	commitWithBody(map[string]any{"messageId": "m1"})

	if len(repo.uniques) != 3 {
		t.Fatalf("expected 3 CreateAssociation calls, got %d", len(repo.uniques))
	}
	if repo.uniques[0] == repo.uniques[1] {
		t.Fatal("associations with different bodies must not share a unique key")
	}
	if repo.uniques[0] != repo.uniques[2] {
		t.Fatal("associations with identical bodies must share a unique key")
	}
}

// CIP-3 §3.1: alias-form owners (@<FQDN>) are rejected in every
// authority-bearing commit field — key, associate, delete target, and
// distributes entries — with a 400, while the resolve path still accepts
// aliases.
func TestCommitRejectsAliasOwners(t *testing.T) {
	ccid, priv := newIdentity(t)
	cfg := &domain.Config{FQDN: "example.com"}

	commit := func(t *testing.T, mutate func(doc *concrnt.Document[any])) error {
		t.Helper()
		doc := concrnt.Document[any]{
			Kind:      "record",
			Key:       concrnt.CCURI{Scheme: "cckv", Owner: ccid, Key: "posts/1"}.String(),
			Author:    ccid,
			Schema:    "https://example.com/post.json",
			CreatedAt: time.Now(),
		}
		mutate(&doc)
		repo := &recordingRecordRepo{}
		uc := newRecordCommitUsecase(ccid, cfg, repo, nil)
		_, err := uc.Commit(context.Background(), "127.0.0.1", signTestDocument(t, doc, priv), domain.CommitModeExecute)
		if err != nil && repo.createRecordCalled {
			t.Fatal("CreateRecord must not be called for a rejected commit")
		}
		return err
	}

	alias := "cckv://@alice.example.net/foo"

	for name, mutate := range map[string]func(doc *concrnt.Document[any]){
		"key": func(doc *concrnt.Document[any]) { doc.Key = alias },
		"associate": func(doc *concrnt.Document[any]) {
			doc.Kind = "association"
			doc.Key = ""
			doc.Associate = &alias
		},
		"delete target": func(doc *concrnt.Document[any]) {
			doc.Kind = "delete"
			doc.Key = ""
			doc.Value = alias + "*"
		},
		"distributes": func(doc *concrnt.Document[any]) {
			doc.Distributes = &[]string{alias}
		},
	} {
		t.Run(name, func(t *testing.T) {
			err := commit(t, mutate)
			if !errors.Is(err, domain.ErrValidation) || !strings.Contains(err.Error(), "alias") {
				t.Fatalf("expected alias rejection, got %v", err)
			}
		})
	}

	t.Run("plain owners still commit", func(t *testing.T) {
		if err := commit(t, func(doc *concrnt.Document[any]) {}); err != nil {
			t.Fatalf("Commit returned error: %v", err)
		}
	})
}

// capturingPolicyService records every evaluation's action and context.
type capturingPolicyService struct {
	actions  []string
	contexts []policy.RequestContext
}

func (p *capturingPolicyService) Eval(ctx context.Context, req policy.RequestContext, stack []concrnt.Policy, action string, key string) error {
	p.actions = append(p.actions, action)
	p.contexts = append(p.contexts, req)
	return nil
}

// CIP-12 §5.3.0: updating a key evaluates the *stored* document as self — the
// submitted document (and any permissive policy field it carries) must not
// influence its own authorization.
func TestCommitUpdateEvaluatesStoredSelf(t *testing.T) {
	ccid, priv := newIdentity(t)
	cfg := &domain.Config{FQDN: "example.com"}

	storedAt := time.Now().Add(-time.Hour).UTC().Truncate(time.Second)
	stored := signedRecord(t, ccid, priv, storedAt)

	repo := &recordingRecordRepo{storedSD: &stored}
	pol := &capturingPolicyService{}
	uc := New(
		repo,
		fixedResidenceRepo{entity: &domain.Entity{ID: ccid, Domain: cfg.FQDN}},
		newTestServerUsecase(cfg),
		cfg,
		nil,
		nopSignalService{},
		pol,
		nil,
		nil,
	)

	fresh := signedRecord(t, ccid, priv, time.Now())
	if _, err := uc.Commit(context.Background(), "127.0.0.1", fresh, domain.CommitModeExecute); err != nil {
		t.Fatalf("Commit returned error: %v", err)
	}

	idx := slices.Index(pol.actions, "record:update")
	if idx < 0 {
		t.Fatalf("expected a record:update evaluation, got %v", pol.actions)
	}
	self, ok := pol.contexts[idx].Self.(concrnt.Document[any])
	if !ok {
		t.Fatalf("unexpected self type %T", pol.contexts[idx].Self)
	}
	if !self.CreatedAt.Equal(storedAt) {
		t.Fatalf("policy self must be the stored document (createdAt %v), got %v", storedAt, self.CreatedAt)
	}
}

// recordingSignalService captures every published realtime event and the
// channel it went out on.
type recordingSignalService struct {
	channels []string
	events   []concrnt.Event
}

func (s *recordingSignalService) Publish(ctx context.Context, channel string, event concrnt.Event) error {
	s.channels = append(s.channels, channel)
	s.events = append(s.events, event)
	return nil
}

// anonymousDenyPolicyService denies read actions for the zero (anonymous)
// entity and allows everything else, recording every evaluated action.
type anonymousDenyPolicyService struct {
	actions []string
}

func (p *anonymousDenyPolicyService) Eval(ctx context.Context, req policy.RequestContext, stack []concrnt.Policy, action string, key string) error {
	p.actions = append(p.actions, action)
	if strings.HasSuffix(action, ":read") {
		if requester, ok := req.Requester.(domain.Entity); ok && requester.ID == "" {
			return domain.PermissionError{Reason: "anonymous read denied"}
		}
	}
	return nil
}

// CIP-11 §3.2: realtime events are flagged against the anonymous baseline at
// publish time — the documents stay on the event for internal consumers, and
// each carries IsPublic so the websocket edge can filter (Event.PublicView).
func TestCreatedEventMarkedForAnonymous(t *testing.T) {
	ccid, priv := newIdentity(t)
	cfg := &domain.Config{FQDN: "example.com"}

	run := func(t *testing.T, pol PolicyService) concrnt.Event {
		t.Helper()
		signal := &recordingSignalService{}
		uc := New(
			&recordingRecordRepo{},
			fixedResidenceRepo{entity: &domain.Entity{ID: ccid, Domain: cfg.FQDN}},
			newTestServerUsecase(cfg),
			cfg,
			nil,
			signal,
			pol,
			nil,
			nil,
		)
		sd := signedRecord(t, ccid, priv, time.Now())
		if _, err := uc.Commit(context.Background(), "127.0.0.1", sd, domain.CommitModeExecute); err != nil {
			t.Fatalf("Commit returned error: %v", err)
		}
		if len(signal.events) != 1 {
			t.Fatalf("expected 1 published event, got %d", len(signal.events))
		}
		return signal.events[0]
	}

	t.Run("protected document is kept but flagged not public", func(t *testing.T) {
		event := run(t, &anonymousDenyPolicyService{})
		if len(event.References) != 1 {
			t.Fatalf("documents must stay on the event for internal consumers, got %v", event.References)
		}
		for _, ref := range event.References {
			if ref.IsPublic == nil || *ref.IsPublic {
				t.Fatalf("protected document must be flagged isPublic=false, got %+v", ref.IsPublic)
			}
		}
		if event.Type != "created" || event.URI == "" || event.Timestamp.IsZero() {
			t.Fatalf("event envelope must be delivered intact: %+v", event)
		}
		if len(event.PublicView().References) != 0 {
			t.Fatal("the public view must drop the protected document")
		}
	})

	t.Run("public document is flagged public", func(t *testing.T) {
		event := run(t, nopPolicyService{})
		if len(event.References) != 1 {
			t.Fatalf("documents must be preserved, got %v", event.References)
		}
		for _, ref := range event.References {
			if ref.IsPublic == nil || !*ref.IsPublic {
				t.Fatalf("public document must be flagged isPublic=true, got %+v", ref.IsPublic)
			}
		}
		if len(event.PublicView().References) != 1 {
			t.Fatal("the public view must keep the public document")
		}
	})
}

// uriDenyPolicyService denies anonymous reads of a single key and allows
// everything else.
type uriDenyPolicyService struct {
	denyKey string
}

func (p *uriDenyPolicyService) Eval(ctx context.Context, req policy.RequestContext, stack []concrnt.Policy, action string, key string) error {
	if strings.HasSuffix(action, ":read") && key == p.denyKey {
		if requester, ok := req.Requester.(domain.Entity); ok && requester.ID == "" {
			return domain.PermissionError{Reason: "anonymous read denied"}
		}
	}
	return nil
}

// Nested references inside an event document (e.g. the distributed original
// embedded in a timeline reference) are flagged by the same anonymous
// baseline; anything nested deeper is removed outright.
func TestEventNestedReferencesMarkedForAnonymous(t *testing.T) {
	ccid, priv := newIdentity(t)
	cfg := &domain.Config{FQDN: "example.com"}

	protectedURI := concrnt.CCURI{Scheme: "cckv", Owner: ccid, Key: "posts/protected"}.String()
	publicURI := concrnt.CCURI{Scheme: "cckv", Owner: ccid, Key: "posts/public"}.String()
	deepURI := concrnt.CCURI{Scheme: "cckv", Owner: ccid, Key: "posts/deep"}.String()
	itemURI := concrnt.CCURI{Scheme: "cckv", Owner: ccid, Key: "timelines/home/items/x"}.String()

	publicSD := signedRecord(t, ccid, priv, time.Now())
	publicSD.References = map[string]concrnt.SignedDocument{deepURI: signedRecord(t, ccid, priv, time.Now())}
	itemSD := signedRecord(t, ccid, priv, time.Now())
	itemSD.References = map[string]concrnt.SignedDocument{
		protectedURI: signedRecord(t, ccid, priv, time.Now()),
		publicURI:    publicSD,
	}

	uc := New(
		&recordingRecordRepo{},
		fixedResidenceRepo{entity: &domain.Entity{ID: ccid, Domain: cfg.FQDN}},
		newTestServerUsecase(cfg),
		cfg,
		nil,
		nopSignalService{},
		&uriDenyPolicyService{denyKey: protectedURI},
		nil,
		nil,
	)

	event := uc.markEventForAnonymous(context.Background(), concrnt.Event{
		Type:       "created",
		URI:        itemURI,
		References: map[string]concrnt.SignedDocument{itemURI: itemSD},
		Timestamp:  time.Now(),
	})

	got, ok := event.References[itemURI]
	if !ok {
		t.Fatalf("item must stay on the event, got %v", event.References)
	}
	if got.IsPublic == nil || !*got.IsPublic {
		t.Fatalf("readable item must be flagged isPublic=true, got %+v", got.IsPublic)
	}
	protected, ok := got.References[protectedURI]
	if !ok {
		t.Fatalf("nested reference must stay on the event, got %v", got.References)
	}
	if protected.IsPublic == nil || *protected.IsPublic {
		t.Fatalf("anonymous-unreadable nested reference must be flagged isPublic=false, got %+v", protected.IsPublic)
	}
	kept, ok := got.References[publicURI]
	if !ok {
		t.Fatalf("readable nested reference must survive, got %v", got.References)
	}
	if kept.IsPublic == nil || !*kept.IsPublic {
		t.Fatalf("readable nested reference must be flagged isPublic=true, got %+v", kept.IsPublic)
	}
	if kept.References != nil {
		t.Fatalf("depth-2 references must be removed outright, got %v", kept.References)
	}
	if len(itemSD.References[publicURI].References) == 0 || itemSD.References[publicURI].IsPublic != nil {
		t.Fatal("marking must not mutate the source event (delivery payloads share it)")
	}

	public := event.PublicView()
	if _, leaked := public.References[itemURI].References[protectedURI]; leaked {
		t.Fatal("the public view must strip the unreadable nested reference")
	}
	if _, ok := public.References[itemURI].References[publicURI]; !ok {
		t.Fatal("the public view must keep the readable nested reference")
	}
}

// The associated event is likewise flagged (action association:read), while
// the delivery payload — the signed document remote servers must verify —
// stays complete and unflagged.
func TestAssociatedEventMarkedForAnonymous(t *testing.T) {
	ccid, priv := newIdentity(t)
	cfg := &domain.Config{FQDN: "example.com"}

	target := concrnt.CCURI{Scheme: "cckv", Owner: ccid, Key: "posts/1"}.String()
	targetSD := signedRecord(t, ccid, priv, time.Now().Add(-time.Minute))

	assocSD := signTestDocument(t, concrnt.Document[any]{
		Kind:      "association",
		Associate: &target,
		Value:     map[string]any{"messageId": "m1"},
		Author:    ccid,
		Schema:    "https://example.com/a/reply.json",
		CreatedAt: time.Now(),
	}, priv)
	assocSD.References = map[string]concrnt.SignedDocument{target: targetSD}

	pol := &anonymousDenyPolicyService{}
	delivery := &recordingDeliveryQueue{}
	uc := New(
		&associationRecordingRepo{},
		fixedResidenceRepo{entity: &domain.Entity{ID: ccid, Domain: cfg.FQDN, SignedDocument: &concrnt.SignedDocument{Document: "{}"}}},
		newTestServerUsecase(cfg),
		cfg,
		nil,
		nopSignalService{},
		pol,
		delivery,
		nil,
	)

	if _, err := uc.Commit(context.Background(), "127.0.0.1", assocSD, domain.CommitModeExecute); err != nil {
		t.Fatalf("Commit returned error: %v", err)
	}
	if len(delivery.jobs) == 0 {
		t.Fatal("expected at least one delivery job")
	}
	for _, job := range delivery.jobs {
		if job.Event == nil {
			continue
		}
		if len(job.Event.References) == 0 {
			t.Fatal("event documents must stay on the event for internal consumers")
		}
		for _, ref := range job.Event.References {
			if ref.IsPublic == nil || *ref.IsPublic {
				t.Fatalf("anonymous-unreadable association must be flagged isPublic=false, got %+v", ref.IsPublic)
			}
		}
		if len(job.Event.PublicView().References) != 0 {
			t.Fatal("the public view must drop the protected association document")
		}
		if len(job.Payload.References) == 0 {
			t.Fatal("delivery payload references must stay complete")
		}
		if job.Payload.IsPublic != nil {
			t.Fatal("delivery payload must not carry internal flags")
		}
		for _, ref := range job.Payload.References {
			if ref.IsPublic != nil {
				t.Fatal("delivery payload references must not carry internal flags")
			}
		}
	}
	if !slices.Contains(pol.actions, "association:read") {
		t.Fatalf("expected an association:read evaluation, got %v", pol.actions)
	}
}

// CIP-1 §4.1: an oversized document is rejected for everyone, including
// system service accounts — nothing larger than 32 KiB may reach the DB.
func TestCommitRejectsOversizedDocument(t *testing.T) {
	ccid, priv := newIdentity(t)
	cfg := &domain.Config{FQDN: "example.com"}

	sd := signTestDocument(t, concrnt.Document[any]{
		Kind:      "record",
		Key:       concrnt.CCURI{Scheme: "cckv", Owner: ccid, Key: "posts/1"}.String(),
		Value:     strings.Repeat("x", domain.MaxDocumentSize),
		Author:    ccid,
		Schema:    "https://example.com/post.json",
		CreatedAt: time.Now(),
	}, priv)

	for _, tc := range []struct {
		name string
		ctx  context.Context
	}{
		{"regular commit", context.Background()},
		{"system service account", context.WithValue(context.Background(), interop.ServiceAccountTypeCtxKey, "system")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			repo := &recordingRecordRepo{}
			uc := newRecordCommitUsecase(ccid, cfg, repo, nil)
			_, err := uc.Commit(tc.ctx, "127.0.0.1", sd, domain.CommitModeExecute)
			if err == nil || !strings.Contains(err.Error(), "maximum size") {
				t.Fatalf("expected size rejection, got %v", err)
			}
			if repo.createRecordCalled {
				t.Fatal("CreateRecord should not be called for an oversized document")
			}
		})
	}
}

// CIP-9: an association document must not carry a key, and its
// associationVariant is capped at 512 bytes.
func TestCommitAssociationConstraints(t *testing.T) {
	ccid, priv := newIdentity(t)
	cfg := &domain.Config{FQDN: "example.com"}
	associate := concrnt.CCURI{Scheme: "cckv", Owner: ccid, Key: "posts/1"}.String()

	commit := func(t *testing.T, mutate func(*concrnt.Document[any])) error {
		t.Helper()
		doc := concrnt.Document[any]{
			Kind:      "association",
			Associate: &associate,
			Value:     map[string]any{"messageId": "m1"},
			Author:    ccid,
			Schema:    "https://example.com/a/reply.json",
			CreatedAt: time.Now(),
		}
		mutate(&doc)
		uc := newRecordCommitUsecase(ccid, cfg, &associationRecordingRepo{}, nil)
		_, err := uc.Commit(context.Background(), "127.0.0.1", signTestDocument(t, doc, priv), domain.CommitModeDryRun)
		return err
	}

	if err := commit(t, func(doc *concrnt.Document[any]) {
		doc.Key = concrnt.CCURI{Scheme: "cckv", Owner: ccid, Key: "assoc/1"}.String()
	}); err == nil || !strings.Contains(err.Error(), "must not have a key") {
		t.Fatalf("expected keyed association rejection, got %v", err)
	}

	long := strings.Repeat("v", domain.MaxAssociationVariantSize+1)
	if err := commit(t, func(doc *concrnt.Document[any]) { doc.AssociationVariant = &long }); err == nil || !strings.Contains(err.Error(), "associationVariant") {
		t.Fatalf("expected oversized variant rejection, got %v", err)
	}

	max := strings.Repeat("v", domain.MaxAssociationVariantSize)
	if err := commit(t, func(doc *concrnt.Document[any]) { doc.AssociationVariant = &max }); err != nil {
		t.Fatalf("512-byte variant should commit, got %v", err)
	}
}

// CIP-0 §7: a record's cckv key component is 1..1024 bytes.
func TestCommitRecordKeyConstraints(t *testing.T) {
	ccid, priv := newIdentity(t)
	cfg := &domain.Config{FQDN: "example.com"}

	commit := func(t *testing.T, key string) (*recordingRecordRepo, error) {
		t.Helper()
		sd := signTestDocument(t, concrnt.Document[any]{
			Kind:      "record",
			Key:       key,
			Author:    ccid,
			Schema:    "https://example.com/post.json",
			CreatedAt: time.Now(),
		}, priv)
		repo := &recordingRecordRepo{}
		uc := newRecordCommitUsecase(ccid, cfg, repo, nil)
		_, err := uc.Commit(context.Background(), "127.0.0.1", sd, domain.CommitModeExecute)
		return repo, err
	}

	if _, err := commit(t, concrnt.CCURI{Scheme: "cckv", Owner: ccid}.String()); err == nil || !strings.Contains(err.Error(), "empty") {
		t.Fatalf("expected empty-key rejection, got %v", err)
	}

	if _, err := commit(t, concrnt.CCURI{Scheme: "cckv", Owner: ccid, Key: strings.Repeat("k", domain.MaxRecordKeySize+1)}.String()); err == nil || !strings.Contains(err.Error(), "maximum size") {
		t.Fatalf("expected oversized-key rejection, got %v", err)
	}

	repo, err := commit(t, concrnt.CCURI{Scheme: "cckv", Owner: ccid, Key: strings.Repeat("k", domain.MaxRecordKeySize)}.String())
	if err != nil {
		t.Fatalf("1024-byte key should commit, got %v", err)
	}
	if !repo.createRecordCalled {
		t.Fatal("CreateRecord was not called for a max-length key")
	}
}

// saveEntity is fail-closed on the entity's home server: the entity is only
// stored when its server resolves, sits on the same layer, and is not tagged
// _blocked. (The green path is covered by TestCommitEntityAcceptIfNewer.)
func TestCommitEntityRequiresGreenServer(t *testing.T) {
	ccid, priv := newIdentity(t)

	cases := []struct {
		name    string
		server  *domain.Server
		wantErr string
	}{
		{"unresolvable server", nil, "failed to resolve"},
		{"layer mismatch", &domain.Server{WellKnown: concrnt.WellKnownConcrnt{Layer: "othernet"}}, "different layer"},
		{"blocked server", &domain.Server{TagString: "_blocked", WellKnown: concrnt.WellKnownConcrnt{Layer: "mainnet"}}, "blocked"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			repo := &recordingRecordRepo{}
			cfg := &domain.Config{FQDN: "example.com", Layer: "mainnet"}
			serverUC := server.New(testServerRepo{server: tc.server}, cfg, concrnt.SoftwareInfo{}, service.NewModuleManager(map[string]string{}, nil), nil)
			uc := New(
				repo,
				fixedResidenceRepo{entity: &domain.Entity{ID: ccid, Domain: "remote.example.net"}},
				serverUC,
				cfg,
				nil,
				nil,
				nil,
				nil,
				nil,
			)

			sd := signTestDocument(t, concrnt.Document[schemas.Entity]{
				Kind:      "entity",
				Value:     schemas.Entity{Domain: "remote.example.net"},
				Author:    ccid,
				CreatedAt: time.Now(),
			}, priv)

			_, err := uc.Commit(context.Background(), "127.0.0.1", sd, domain.CommitModeExecute)
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("expected %q error, got %v", tc.wantErr, err)
			}
			if repo.createEntityCalled {
				t.Fatal("CreateEntity should not be called for a non-green server")
			}
		})
	}
}

// None-proof entity documents are exempt from the green-server check: they
// only reach saveEntity via the system service account (migration/import),
// where the entity's home server may be offline, on another layer, or still
// on v1.
func TestCommitEntityNoneProofSkipsGreenServerCheck(t *testing.T) {
	ccid, _ := newIdentity(t)

	repo := &recordingRecordRepo{}
	cfg := &domain.Config{FQDN: "example.com", Layer: "mainnet"}
	unresolvable := server.New(testServerRepo{server: nil}, cfg, concrnt.SoftwareInfo{}, service.NewModuleManager(map[string]string{}, nil), nil)
	uc := New(
		repo,
		fixedResidenceRepo{entity: &domain.Entity{ID: ccid, Domain: "remote.example.net"}},
		unresolvable,
		cfg,
		nil,
		nil,
		nil,
		nil,
		nil,
	)

	docBytes, err := json.Marshal(concrnt.Document[schemas.Entity]{
		Kind:      "entity",
		Value:     schemas.Entity{Domain: "remote.example.net"},
		Author:    ccid,
		CreatedAt: time.Now(),
	})
	if err != nil {
		t.Fatalf("marshal document: %v", err)
	}
	sd := concrnt.SignedDocument{
		Document: string(docBytes),
		Proof:    concrnt.Proof{Type: concrnt.ProofTypeNone},
	}

	ctx := context.WithValue(context.Background(), interop.ServiceAccountTypeCtxKey, "system")
	if _, err := uc.Commit(ctx, "127.0.0.1", sd, domain.CommitModeLocalOnlyExecute); err != nil {
		t.Fatalf("Commit returned error: %v", err)
	}
	if !repo.createEntityCalled {
		t.Fatal("CreateEntity was not called for a none-proof entity import")
	}
}

// Every commit is owned by exactly one entity and is recorded in commit_logs
// inside the commit transaction (CIP-3 §3.1: record = key owner, association
// = target owner, delete = owner of the deleted target — the namespace whose
// repository must carry the tombstone). Records and associations reference
// the commit log by foreign key, so a missing commit log is a failed commit.
func TestCommitWritesCommitLogWithOwner(t *testing.T) {
	ccid, priv := newIdentity(t)
	cfg := &domain.Config{FQDN: "example.com"}

	t.Run("record is owned by the key owner", func(t *testing.T) {
		repo := &recordingRecordRepo{}
		uc := newRecordCommitUsecase(ccid, cfg, repo, nil)

		sd := signedRecord(t, ccid, priv, time.Now())
		if _, err := uc.Commit(context.Background(), "127.0.0.1", sd, domain.CommitModeExecute); err != nil {
			t.Fatalf("Commit returned error: %v", err)
		}
		id := documentIDOf(t, sd)
		if len(repo.createdCommitLogs) != 1 || repo.createdCommitLogs[0] != id {
			t.Fatalf("CreateCommitLog calls = %v, want exactly [%s]", repo.createdCommitLogs, id)
		}
		if repo.commitLogOwners[id] != ccid {
			t.Fatalf("commit owner = %q, want key owner %s", repo.commitLogOwners[id], ccid)
		}
	})

	t.Run("association is owned by the target owner", func(t *testing.T) {
		repo := &associationRecordingRepo{}
		uc := newRecordCommitUsecase(ccid, cfg, repo, nil)

		associate := concrnt.CCURI{Scheme: "cckv", Owner: ccid, Key: "posts/1"}.String()
		sd := signTestDocument(t, concrnt.Document[any]{
			Kind:      "association",
			Associate: &associate,
			Value:     map[string]any{"messageId": "m1"},
			Author:    ccid,
			Schema:    "https://example.com/a/reply.json",
			CreatedAt: time.Now(),
		}, priv)
		if _, err := uc.Commit(context.Background(), "127.0.0.1", sd, domain.CommitModeDryRun); err != nil {
			t.Fatalf("Commit returned error: %v", err)
		}
		id := documentIDOf(t, sd)
		if len(repo.createdCommitLogs) != 1 || repo.createdCommitLogs[0] != id {
			t.Fatalf("CreateCommitLog calls = %v, want exactly [%s]", repo.createdCommitLogs, id)
		}
		if repo.commitLogOwners[id] != ccid {
			t.Fatalf("commit owner = %q, want target owner %s", repo.commitLogOwners[id], ccid)
		}
	})

	t.Run("delete is owned by the target owner", func(t *testing.T) {
		repo := &rangeDeleteRepo{subtree: []concrnt.SignedDocument{subtreeRecord(t, rangeBaseURI)}}
		uc := newRangeDeleteUsecase(ccid, cfg, repo, &denyKeysPolicyService{}, &recordingDeliveryQueue{}, &stubKVS{})

		sd := signedDelete(t, ccid, priv, rangeBaseURI+"*")
		if _, err := uc.Commit(context.Background(), "127.0.0.1", sd, domain.CommitModeExecute); err != nil {
			t.Fatalf("Commit returned error: %v", err)
		}
		id := documentIDOf(t, sd)
		if repo.commitLogOwners[id] != cfg.FQDN {
			t.Fatalf("commit owner = %q, want target owner %s", repo.commitLogOwners[id], cfg.FQDN)
		}
	})
}

// A server that is not authoritative for an association's target only
// publishes the event locally; it must not re-commit the association to
// every distribution channel host. Without a commit log for non-local
// associations there is no replay guard, so re-fan-out between servers would
// loop.
func TestCommitAssociationRemoteTargetDoesNotRefanout(t *testing.T) {
	cfg := &domain.Config{FQDN: "example.com"}
	author := newAckParty(t, cfg.FQDN)
	owner := newAckParty(t, "remote.example.net")

	target := concrnt.CCURI{Scheme: "cckv", Owner: owner.ccid, Key: "posts/1"}.String()
	targetSD := signedRecord(t, owner.ccid, owner.priv, time.Now().Add(-time.Minute))

	sd := signTestDocument(t, concrnt.Document[any]{
		Kind:      "association",
		Associate: &target,
		Value:     map[string]any{"messageId": "m1"},
		Author:    author.ccid,
		Schema:    "https://example.com/a/reply.json",
		CreatedAt: time.Now(),
	}, author.priv)
	sd.References = map[string]concrnt.SignedDocument{target: targetSD}

	repo := &associationRecordingRepo{}
	delivery := &recordingDeliveryQueue{}
	uc := newAckUsecase(cfg, repo, residenceOf(author, owner), delivery)

	if _, err := uc.Commit(context.Background(), "127.0.0.1", sd, domain.CommitModeExecute); err != nil {
		t.Fatalf("Commit returned error: %v", err)
	}
	if len(repo.uniques) != 0 {
		t.Fatal("a non-authoritative server must not store the association")
	}
	if len(delivery.jobs) == 0 {
		t.Fatal("expected the local publish job")
	}
	for _, job := range delivery.jobs {
		if job.Remote != DeliveryRemoteNone {
			t.Fatalf("non-authoritative association job must not re-commit remotely, got Remote=%q for %s", job.Remote, job.ResolveURI)
		}
	}
}
