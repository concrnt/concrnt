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

// entityLookups is the entity slice of Repository the residence fakes serve.
type entityLookups interface {
	GetEntityByCCID(ctx context.Context, ccid string) (*domain.Entity, error)
	GetEntityByAlias(ctx context.Context, alias string) (*domain.Entity, error)
}

// withEntities serves records from Repository and entity lookups from entities.
type withEntities struct {
	Repository
	entities entityLookups
}

func (w withEntities) GetEntityByCCID(ctx context.Context, ccid string) (*domain.Entity, error) {
	return w.entities.GetEntityByCCID(ctx, ccid)
}
func (w withEntities) GetEntityByAlias(ctx context.Context, alias string) (*domain.Entity, error) {
	return w.entities.GetEntityByAlias(ctx, alias)
}

type stubResidenceRepo struct{ entityLookups }

func (stubResidenceRepo) GetEntityByCCID(ctx context.Context, ccid string) (*domain.Entity, error) {
	return nil, domain.ErrNotFound
}

// recordingRecordRepo satisfies the write path of Repository in memory and
// records every Apply* call it is given. commitLogs seeds the commit ids
// already in the log: an Apply* call carrying one reports Duplicate, as does
// a second call with an id this fake applied. storedSD, when set, is what
// GetSignedDocument serves for every key (a pre-existing record);
// storedCreatedAt, when set, is the createdAt of the document the repository
// already holds for whatever ApplyEntity / ApplyRecord write: both report
// Stale for a createdAt that is not strictly newer (CIP-3 §3.4). ackStale /
// ackedStale make ApplyAck report Stale for the acker / target side
// unconditionally.
type recordingRecordRepo struct {
	Repository
	commitLogs      map[string]bool
	storedSD        *concrnt.SignedDocument
	storedCreatedAt time.Time
	ackStale        bool
	ackedStale      bool

	entityCommits []EntityCommit
	recordCommits []RecordCommit
	ackCommits    []AckCommit

	// commitIDs is the commit id of every Apply* call in order and statuses
	// how each of them ended; commitLogOwners is the owner each applied
	// commit was recorded under.
	commitIDs       []string
	statuses        []ApplyStatus
	commitLogOwners map[string]string
}

// apply records one Apply* call and decides how it ends: Duplicate for a
// commit id already in the log, Stale when the caller lost accept-if-newer
// (nothing is stored then, not even the commit), Applied otherwise.
func (r *recordingRecordRepo) apply(commit Commit, stale bool) ApplyStatus {
	if r.commitLogOwners == nil {
		r.commitLogOwners = map[string]string{}
	}
	status := Applied
	if _, logged := r.commitLogOwners[commit.ID]; logged || r.commitLogs[commit.ID] {
		status = Duplicate
	} else if stale {
		status = Stale
	} else {
		r.commitLogOwners[commit.ID] = commit.Owner
	}
	r.commitIDs = append(r.commitIDs, commit.ID)
	r.statuses = append(r.statuses, status)
	return status
}

// olderThanStored is the accept-if-newer loss against storedCreatedAt.
func (r *recordingRecordRepo) olderThanStored(createdAt time.Time) bool {
	return !r.storedCreatedAt.IsZero() && !createdAt.After(r.storedCreatedAt)
}

// sawAck reports whether ApplyAck was called for the given side and
// validity (AckerSide: ack / unack, TargetSide: acked / unacked).
func (r *recordingRecordRepo) sawAck(side AckSide, valid bool) bool {
	return slices.ContainsFunc(r.ackCommits, func(c AckCommit) bool { return c.Side == side && c.Valid == valid })
}

func (r *recordingRecordRepo) ApplyEntity(ctx context.Context, commit EntityCommit) (ApplyStatus, error) {
	r.entityCommits = append(r.entityCommits, commit)
	return r.apply(commit.Commit, r.olderThanStored(commit.CreatedAt)), nil
}
func (r *recordingRecordRepo) ApplyRecord(ctx context.Context, commit RecordCommit) (RecordOutcome, error) {
	r.recordCommits = append(r.recordCommits, commit)
	status := r.apply(commit.Commit, r.olderThanStored(commit.CreatedAt))
	if status != Applied {
		return RecordOutcome{Status: status}, nil
	}
	if r.storedSD == nil {
		return RecordOutcome{Status: Applied, FreshKey: true}, nil
	}
	previousID, err := r.storedSD.CDID()
	if err != nil {
		return RecordOutcome{}, err
	}
	return RecordOutcome{Status: Applied, PreviousID: previousID}, nil
}
func (r *recordingRecordRepo) ApplyAck(ctx context.Context, commit AckCommit) (AckOutcome, error) {
	r.ackCommits = append(r.ackCommits, commit)
	stale := r.ackStale
	if commit.Side == TargetSide {
		stale = r.ackedStale
	}
	status := r.apply(commit.Commit, stale)
	if status != Applied {
		return AckOutcome{Status: status}, nil
	}
	// there was no state before the call
	return AckOutcome{Status: Applied, IsValid: commit.Valid}, nil
}
func (r *recordingRecordRepo) GetHierarchicalRecordPolicies(ctx context.Context, uri string) ([]concrnt.Policy, error) {
	return nil, nil
}
func (r *recordingRecordRepo) GetHierarchicalRecordPoliciesMulti(ctx context.Context, uris []string) (map[string][]concrnt.Policy, error) {
	return emptyStacks(uris), nil
}

// emptyStacks is the Multi answer of a fake whose records carry no policy.
func emptyStacks(uris []string) map[string][]concrnt.Policy {
	stacks := make(map[string][]concrnt.Policy, len(uris))
	for _, uri := range uris {
		stacks[uri] = []concrnt.Policy{}
	}
	return stacks
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
	entityLookups
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
		withEntities{nil, stubResidenceRepo{}},
		nil,
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
// affiliation. The ordering is decided by the repository (the usecase does
// not pre-compare), so ApplyEntity is always attempted, exactly once; a loss
// comes back as Stale, which stores nothing, and the commit still succeeds.
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
				withEntities{repo, fixedResidenceRepo{entity: &domain.Entity{
					ID:             ccid,
					Domain:         "remote.example.net",
					SignedDocument: &stored,
				}}},
				nil,
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
			if len(repo.entityCommits) != 1 {
				t.Fatalf("expected exactly one ApplyEntity call, got %d", len(repo.entityCommits))
			}
			wantStatus := Stale
			if tc.wantApplied {
				wantStatus = Applied
			}
			if !slices.Equal(repo.statuses, []ApplyStatus{wantStatus}) {
				t.Fatalf("apply statuses = %v, want [%v]", repo.statuses, wantStatus)
			}
		})
	}
}

// Record commits follow the same accept-if-newer rule (CIP-3 §3.4): a
// document older than (or as old as) the one stored at its key succeeds as a
// no-op. The repository decides the ordering, so ApplyRecord is always
// attempted, exactly once; a loss comes back as Stale, which stores nothing,
// so the stored newer version is neither overwritten nor tombstoned.
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
			if len(repo.recordCommits) != 1 {
				t.Fatalf("expected exactly one ApplyRecord call, got %d", len(repo.recordCommits))
			}
			wantStatus := Stale
			if tc.wantApplied {
				wantStatus = Applied
			}
			if !slices.Equal(repo.statuses, []ApplyStatus{wantStatus}) {
				t.Fatalf("apply statuses = %v, want [%v]", repo.statuses, wantStatus)
			}
		})
	}
}

// Ack/unack transitions follow accept-if-newer too (CIP-10 §4): when the
// repository reports the stored (from, to, schema) state already carries a
// newer document (Stale), the commit is a no-op success: nothing is stored
// and no side effects run. The same holds for the target-side
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
		side   AckSide
		valid  bool
		ctx    context.Context
	}{
		{"ack", false, AckerSide, true, context.Background()},
		{"unack", false, AckerSide, false, context.Background()},
		{"acked", true, TargetSide, true, systemCtx()},
		{"unacked", true, TargetSide, false, systemCtx()},
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
				if len(repo.ackCommits) != 1 || !repo.sawAck(tc.side, tc.valid) {
					t.Fatalf("repository transition for %s was not attempted exactly once (calls: %+v)", tc.kind, repo.ackCommits)
				}
				wantStatus := Applied
				if stale {
					wantStatus = Stale
				}
				if !slices.Equal(repo.statuses, []ApplyStatus{wantStatus}) {
					t.Fatalf("apply statuses = %v, want [%v]", repo.statuses, wantStatus)
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
		withEntities{repo, fixedResidenceRepo{entity: &domain.Entity{ID: ccid, Domain: "remote.example.net"}}},
		nil,
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
	if len(repo.recordCommits) != 1 {
		t.Fatal("ApplyRecord was not called")
	}
}

// stubKVS records SetAdd / SetNX calls. SetNX reports setNXDenied's
// inverse, so a test can simulate a throttle key that is already present.
type stubKVS struct {
	addedSets   []string
	setNXKeys   []string
	setNXDenied bool
}

func (s *stubKVS) SetAdd(ctx context.Context, key string, value string, ttl time.Duration) error {
	s.addedSets = append(s.addedSets, key)
	return nil
}
func (s *stubKVS) SetMembers(ctx context.Context, key string) ([]string, error) {
	return nil, nil
}
func (s *stubKVS) SetNX(ctx context.Context, key string, value string, ttl time.Duration) (bool, error) {
	s.setNXKeys = append(s.setNXKeys, key)
	return !s.setNXDenied, nil
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
		withEntities{repo, fixedResidenceRepo{entity: &domain.Entity{ID: ccid, Domain: cfg.FQDN}}},
		nil,
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
	if len(repo.recordCommits) != 0 {
		t.Fatal("ApplyRecord should not be called for a far-future document")
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
		if len(repo.recordCommits) != 0 {
			t.Fatal("ApplyRecord should not be called for a too-old document")
		}
	})

	t.Run("within window", func(t *testing.T) {
		repo := &recordingRecordRepo{}
		uc := newRecordCommitUsecase(ccid, cfg, repo, nil)
		sd := signedRecord(t, ccid, priv, time.Now().Add(-domain.MaxBackdate+time.Hour))
		if _, err := uc.Commit(context.Background(), "127.0.0.1", sd, domain.CommitModeExecute); err != nil {
			t.Fatalf("Commit returned error: %v", err)
		}
		if len(repo.recordCommits) == 0 {
			t.Fatal("ApplyRecord was not called for an in-window document")
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
			withEntities{repo, fixedResidenceRepo{}},
			nil,
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
		if len(repo.entityCommits) == 0 {
			t.Fatal("ApplyEntity was not called for an old entity document")
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
		if len(repo.entityCommits) != 0 {
			t.Fatal("ApplyEntity should not be called for a subkey-signed entity document")
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
// no-op success, the repository reports it as Duplicate and changes nothing.
// This is the replay guard — deleted and superseded documents stay in
// commit_logs, so a captured copy can't be replayed back in — while a fresh
// document at the same cckv key has a different CDID and commits normally.
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
		if !slices.Equal(repo.statuses, []ApplyStatus{Duplicate}) {
			t.Fatalf("apply statuses = %v, want the one write to find the commit log (Duplicate)", repo.statuses)
		}
	})

	t.Run("fresh document at the same key", func(t *testing.T) {
		repo := &recordingRecordRepo{commitLogs: map[string]bool{documentIDOf(t, committed): true}}
		uc := newRecordCommitUsecase(ccid, cfg, repo, nil)
		fresh := signedRecord(t, ccid, priv, time.Now())
		if _, err := uc.Commit(context.Background(), "127.0.0.1", fresh, domain.CommitModeExecute); err != nil {
			t.Fatalf("Commit returned error: %v", err)
		}
		if !slices.Equal(repo.statuses, []ApplyStatus{Applied}) {
			t.Fatalf("apply statuses = %v, want a fresh document at a committed key to be Applied", repo.statuses)
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
		if len(repo.recordCommits) == 0 {
			t.Fatal("ApplyRecord was not called for a system-account commit")
		}
	})

	t.Run("already-committed document still no-ops", func(t *testing.T) {
		repo := &recordingRecordRepo{commitLogs: map[string]bool{documentIDOf(t, sd): true}}
		uc := newRecordCommitUsecase(ccid, cfg, repo, nil)
		if _, err := uc.Commit(ctx, "127.0.0.1", sd, domain.CommitModeExecute); err != nil {
			t.Fatalf("system redelivery must succeed, got %v", err)
		}
		if !slices.Equal(repo.statuses, []ApplyStatus{Duplicate}) {
			t.Fatalf("apply statuses = %v, want a re-imported document to be a Duplicate", repo.statuses)
		}
	})
}

// associationRecordingRepo additionally accepts associations and captures
// the commits ApplyAssociation is given.
type associationRecordingRepo struct {
	recordingRecordRepo
	associationCommits []AssociationCommit
}

func (r *associationRecordingRepo) ApplyAssociation(ctx context.Context, commit AssociationCommit) (ApplyStatus, error) {
	r.associationCommits = append(r.associationCommits, commit)
	return r.apply(commit.Commit, false), nil
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
		if len(repo.recordCommits) == 0 {
			t.Fatal("ApplyRecord was not called for an exempt import")
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
		if len(repo.associationCommits) != 1 {
			t.Fatal("ApplyAssociation was not called for an inbound association import")
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

	if len(repo.associationCommits) != 3 {
		t.Fatalf("expected 3 ApplyAssociation calls, got %d", len(repo.associationCommits))
	}
	uniques := make([]string, len(repo.associationCommits))
	for i, commit := range repo.associationCommits {
		uniques[i] = commit.Unique
	}
	if uniques[0] == uniques[1] {
		t.Fatal("associations with different bodies must not share a unique key")
	}
	if uniques[0] != uniques[2] {
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
		if err != nil && len(repo.recordCommits) != 0 {
			t.Fatal("ApplyRecord must not be called for a rejected commit")
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
		withEntities{repo, fixedResidenceRepo{entity: &domain.Entity{ID: ccid, Domain: cfg.FQDN}}},
		nil,
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
			withEntities{&recordingRecordRepo{}, fixedResidenceRepo{entity: &domain.Entity{ID: ccid, Domain: cfg.FQDN}}},
			nil,
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
		withEntities{&recordingRecordRepo{}, fixedResidenceRepo{entity: &domain.Entity{ID: ccid, Domain: cfg.FQDN}}},
		nil,
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
// stays complete and unflagged. A local channel gets the event straight from
// the signal, a remote one through a delivery job.
func TestAssociatedEventMarkedForAnonymous(t *testing.T) {
	for _, ownerDomain := range []string{"example.com", "remote.example.net"} {
		t.Run(ownerDomain, func(t *testing.T) {
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
			signal := &recordingSignalService{}
			uc := New(
				withEntities{&associationRecordingRepo{}, fixedResidenceRepo{entity: &domain.Entity{ID: ccid, Domain: ownerDomain, SignedDocument: &concrnt.SignedDocument{Document: "{}"}}}},
				nil,
				newTestServerUsecase(cfg),
				cfg,
				nil,
				signal,
				pol,
				delivery,
				nil,
			)

			if _, err := uc.Commit(context.Background(), "127.0.0.1", assocSD, domain.CommitModeExecute); err != nil {
				t.Fatalf("Commit returned error: %v", err)
			}

			events := slices.Clone(signal.events)
			for _, job := range delivery.jobs {
				if job.Event != nil {
					events = append(events, *job.Event)
				}
			}
			local := ownerDomain == cfg.FQDN
			if local && (len(signal.events) == 0 || len(delivery.jobs) != 0) {
				t.Fatalf("a local channel must be published directly: %d published, %d queued", len(signal.events), len(delivery.jobs))
			}
			if !local && (len(signal.events) != 0 || len(delivery.jobs) == 0) {
				t.Fatalf("a remote channel must be queued: %d published, %d queued", len(signal.events), len(delivery.jobs))
			}
			for _, event := range events {
				if len(event.References) == 0 {
					t.Fatal("event documents must stay on the event for internal consumers")
				}
				for _, ref := range event.References {
					if ref.IsPublic == nil || *ref.IsPublic {
						t.Fatalf("anonymous-unreadable association must be flagged isPublic=false, got %+v", ref.IsPublic)
					}
				}
				if len(event.PublicView().References) != 0 {
					t.Fatal("the public view must drop the protected association document")
				}
			}
			for _, job := range delivery.jobs {
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
		})
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
			if len(repo.recordCommits) != 0 {
				t.Fatal("ApplyRecord should not be called for an oversized document")
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
	if len(repo.recordCommits) == 0 {
		t.Fatal("ApplyRecord was not called for a max-length key")
	}
}

// planEntity is fail-closed on the entity's home server: the entity is only
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
				withEntities{repo, fixedResidenceRepo{entity: &domain.Entity{ID: ccid, Domain: "remote.example.net"}}},
				nil,
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
			if len(repo.entityCommits) != 0 {
				t.Fatal("ApplyEntity should not be called for a non-green server")
			}
		})
	}
}

// None-proof entity documents are exempt from the green-server check: they
// only reach planEntity via the system service account (migration/import),
// where the entity's home server may be offline, on another layer, or still
// on v1.
func TestCommitEntityNoneProofSkipsGreenServerCheck(t *testing.T) {
	ccid, _ := newIdentity(t)

	repo := &recordingRecordRepo{}
	cfg := &domain.Config{FQDN: "example.com", Layer: "mainnet"}
	unresolvable := server.New(testServerRepo{server: nil}, cfg, concrnt.SoftwareInfo{}, service.NewModuleManager(map[string]string{}, nil), nil)
	uc := New(
		withEntities{repo, fixedResidenceRepo{entity: &domain.Entity{ID: ccid, Domain: "remote.example.net"}}},
		nil,
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
	if len(repo.entityCommits) == 0 {
		t.Fatal("ApplyEntity was not called for a none-proof entity import")
	}
}

// Every commit is owned by exactly one entity and is recorded in commit_logs
// by the commit's one repository write (CIP-3 §3.1: record = key owner,
// association = target owner, delete = owner of the deleted target — the
// namespace whose repository must carry the tombstone).
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
		if len(repo.commitIDs) != 1 || repo.commitIDs[0] != id {
			t.Fatalf("Apply calls = %v, want exactly [%s]", repo.commitIDs, id)
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
		if len(repo.commitIDs) != 1 || repo.commitIDs[0] != id {
			t.Fatalf("Apply calls = %v, want exactly [%s]", repo.commitIDs, id)
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
	if len(repo.commitIDs) != 0 {
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
