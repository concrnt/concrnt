package record

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/cdid"
	"github.com/concrnt/concrnt/client"
	"github.com/concrnt/concrnt/internal/domain"
	"github.com/concrnt/concrnt/internal/usecase"
	"github.com/concrnt/concrnt/internal/usecase/chunkline"
	"github.com/concrnt/concrnt/policy"
	"github.com/concrnt/concrnt/schemas"
)

func TestParseRangeDeleteTarget(t *testing.T) {
	cases := []struct {
		name      string
		target    string
		wantBase  string
		wantSelf  bool
		wantRange bool
		wantErr   bool
	}{
		{"plain key", "cckv://owner/path/item", "cckv://owner/path/item", false, false, false},
		{"subtree with self", "cckv://owner/path/item*", "cckv://owner/path/item", true, true, false},
		{"children only", "cckv://owner/path/item/*", "cckv://owner/path/item", false, true, false},
		{"inner asterisk", "cckv://owner/pa*th/item*", "", false, true, true},
		{"owner-wide children", "cckv://owner/*", "", false, true, true},
		{"owner-wide subtree", "cckv://owner*", "", false, true, true},
		{"ccfs scheme", "ccfs://owner/concrnt/abcdef*", "", false, true, true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			base, includeSelf, isRange, err := parseRangeDeleteTarget(tc.target)
			if isRange != tc.wantRange {
				t.Fatalf("isRange = %v, want %v", isRange, tc.wantRange)
			}
			if tc.wantErr {
				if err == nil || !errors.Is(err, domain.ErrValidation) {
					t.Fatalf("expected validation error, got %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if base != tc.wantBase {
				t.Fatalf("base = %q, want %q", base, tc.wantBase)
			}
			if includeSelf != tc.wantSelf {
				t.Fatalf("includeSelf = %v, want %v", includeSelf, tc.wantSelf)
			}
		})
	}
}

// recordingTx records whether the commit transaction was committed or rolled
// back, so tests can assert the commitlog's fate.
type recordingTx struct {
	committed  bool
	rolledBack bool
}

func (t *recordingTx) IsRepositoryTx() {}

type subtreeQueryCall struct {
	base        string
	includeSelf bool
}

// rangeDeleteRepo serves a fixed subtree enumeration and records the
// enumeration arguments and every delete attempt. stored is what
// GetSignedDocument serves per URI (delete targets, reference rows);
// removals is what GetTimelineRemoval serves per key URI as (timeline, item).
type rangeDeleteRepo struct {
	Repository
	subtree     []concrnt.SignedDocument
	stored      map[string]concrnt.SignedDocument
	removals    map[string][2]string
	queryCalls  []subtreeQueryCall
	deletedURIs []string
	txs         []*recordingTx
	// commitLogOwners records CreateCommitLog calls: documentID -> owner.
	commitLogOwners map[string]string
	// gcFlagged records MarkCommitLogGcCandidate calls in order.
	gcFlagged []string
}

func (r *rangeDeleteRepo) RunInTx(ctx context.Context, fn func(tx RepositoryTx) error) error {
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
func (r *rangeDeleteRepo) CreateCommitLog(ctx context.Context, tx RepositoryTx, id string, ip string, document string, proof any, owner string) error {
	if r.commitLogOwners == nil {
		r.commitLogOwners = map[string]string{}
	}
	r.commitLogOwners[id] = owner
	return nil
}
func (r *rangeDeleteRepo) HasCommitLog(ctx context.Context, id string) (bool, error) {
	return false, nil
}
func (r *rangeDeleteRepo) MarkCommitLogGcCandidate(ctx context.Context, tx RepositoryTx, documentID string) error {
	r.gcFlagged = append(r.gcFlagged, documentID)
	return nil
}
func (r *rangeDeleteRepo) GetHierarchicalRecordPolicies(ctx context.Context, uri string) ([]concrnt.Policy, error) {
	return nil, nil
}
func (r *rangeDeleteRepo) QueryRecordSubtree(ctx context.Context, base string, includeSelf bool) ([]concrnt.SignedDocument, error) {
	r.queryCalls = append(r.queryCalls, subtreeQueryCall{base: base, includeSelf: includeSelf})
	return r.subtree, nil
}
func (r *rangeDeleteRepo) DeleteRecordByKey(ctx context.Context, tx RepositoryTx, targetURI string) error {
	r.deletedURIs = append(r.deletedURIs, targetURI)
	return nil
}
func (r *rangeDeleteRepo) GetSignedDocument(ctx context.Context, uri string) (*concrnt.SignedDocument, error) {
	if sd, ok := r.stored[uri]; ok {
		return &sd, nil
	}
	return nil, domain.NotFoundError{Resource: uri}
}
func (r *rangeDeleteRepo) GetTimelineRemoval(ctx context.Context, keyURI string) (string, string, error) {
	if rm, ok := r.removals[keyURI]; ok {
		return rm[0], rm[1], nil
	}
	return "", "", nil
}

// denyKeysPolicyService denies exactly the configured keys and records every
// evaluated key in order.
type denyKeysPolicyService struct {
	deny   map[string]bool
	evaled []string
}

func (p *denyKeysPolicyService) Eval(ctx context.Context, req policy.RequestContext, stack []concrnt.Policy, action string, key string) error {
	p.evaled = append(p.evaled, key)
	if p.deny[key] {
		return domain.PermissionError{Reason: "denied by test"}
	}
	return nil
}

// recordingDeliveryQueue captures the DeliveryJobs a usecase enqueues and
// the handlers it registers, keyed by job type.
type recordingDeliveryQueue struct {
	jobs     []DeliveryJob
	handlers map[string]func(ctx context.Context, payload json.RawMessage) error
}

func (d *recordingDeliveryQueue) Enqueue(ctx context.Context, jobType string, payload any) error {
	if jobType != JobTypeRecordDelivery {
		return fmt.Errorf("unexpected job type %q", jobType)
	}
	d.jobs = append(d.jobs, payload.(DeliveryJob))
	return nil
}

func (d *recordingDeliveryQueue) RegisterHandler(jobType string, handler func(ctx context.Context, payload json.RawMessage) error) {
	if d.handlers == nil {
		d.handlers = map[string]func(ctx context.Context, payload json.RawMessage) error{}
	}
	d.handlers[jobType] = handler
}

// subtreeRecord builds a stored record fixture the way QueryRecordSubtree
// serves them: document JSON plus CCKV and CCFS URIs. Optional trailing
// arguments become the record's distribute destinations.
func subtreeRecord(t *testing.T, uri string, distributes ...string) concrnt.SignedDocument {
	t.Helper()
	doc := concrnt.Document[map[string]string]{
		Kind:      "record",
		Key:       uri,
		Value:     map[string]string{"body": "x"},
		Author:    "example.com",
		Schema:    "https://example.com/item.json",
		CreatedAt: time.Now(),
	}
	if len(distributes) > 0 {
		doc.Distributes = &distributes
	}
	docBytes, err := json.Marshal(doc)
	if err != nil {
		t.Fatalf("marshal fixture document: %v", err)
	}
	u := uri
	ccfs := concrnt.ComposeCCFSURI("example.com", concrnt.CCFSTypeConcrnt, fmt.Sprintf("doc-%x", len(uri)))
	return concrnt.SignedDocument{
		CCKV:     &u,
		CCFS:     &ccfs,
		Document: string(docBytes),
		Proof:    concrnt.Proof{Type: concrnt.ProofTypeNone},
	}
}

// signedDelete builds a signed delete commit for the given target URI.
func signedDelete(t *testing.T, ccid, privKeyHex, target string) concrnt.SignedDocument {
	t.Helper()
	return signTestDocument(t, concrnt.Document[schemas.Delete]{
		Kind:      "delete",
		Value:     schemas.Delete(target),
		Author:    ccid,
		Schema:    "https://schema.concrnt.net/delete.json",
		CreatedAt: time.Now(),
	}, privKeyHex)
}

// newRangeDeleteUsecase wires a usecase whose delete author is a local,
// resolvable entity. The subtree base's owner is the server FQDN itself, so
// ResolveResourceHost resolves it locally without any network.
func newRangeDeleteUsecase(ccid string, cfg *domain.Config, repo Repository, pol PolicyService, delivery usecase.JobQueue, kvs usecase.KVS) *Usecase {
	return New(
		repo,
		fixedResidenceRepo{entity: &domain.Entity{ID: ccid, Domain: cfg.FQDN}},
		newTestServerUsecase(cfg),
		cfg,
		client.New(cfg.FQDN),
		nopSignalService{},
		pol,
		delivery,
		kvs,
	)
}

const rangeBaseURI = "cckv://example.com/profiles/main/lists/l1"

// A subtree range delete ("...l1*") enumerates with includeSelf, evaluates the
// policy on every target, deletes them all in one batch, commits, and then
// announces each deleted target.
func TestCommitRangeDeleteSubtree(t *testing.T) {
	ccid, priv := newIdentity(t)
	cfg := &domain.Config{FQDN: "example.com"}

	targets := []concrnt.SignedDocument{
		subtreeRecord(t, rangeBaseURI),
		subtreeRecord(t, rangeBaseURI+"/a"),
		subtreeRecord(t, rangeBaseURI+"/b"),
	}
	repo := &rangeDeleteRepo{subtree: targets}
	pol := &denyKeysPolicyService{}
	delivery := &recordingDeliveryQueue{}
	kvs := &stubKVS{}
	uc := newRangeDeleteUsecase(ccid, cfg, repo, pol, delivery, kvs)

	sd := signedDelete(t, ccid, priv, rangeBaseURI+"*")
	result, err := uc.Commit(context.Background(), "127.0.0.1", sd, domain.CommitModeExecute)
	if err != nil {
		t.Fatalf("Commit returned error: %v", err)
	}

	if len(repo.queryCalls) != 1 || repo.queryCalls[0] != (subtreeQueryCall{base: rangeBaseURI, includeSelf: true}) {
		t.Fatalf("unexpected enumeration calls: %+v", repo.queryCalls)
	}

	wantURIs := []string{rangeBaseURI, rangeBaseURI + "/a", rangeBaseURI + "/b"}
	if !slices.Equal(repo.deletedURIs, wantURIs) {
		t.Fatalf("deleted URIs = %v, want %v", repo.deletedURIs, wantURIs)
	}
	if !slices.Equal(pol.evaled, wantURIs) {
		t.Fatalf("policy evaluated on %v, want %v", pol.evaled, wantURIs)
	}
	if len(repo.txs) != 1 || !repo.txs[0].committed || repo.txs[0].rolledBack {
		t.Fatalf("unexpected tx state: %+v", repo.txs)
	}

	// one "deleted" event per target, addressed to the concrete URI
	deletedEventURIs := []string{}
	for _, job := range delivery.jobs {
		if job.Event != nil && job.Event.Type == "deleted" {
			deletedEventURIs = append(deletedEventURIs, job.Event.URI)
		}
	}
	if !slices.Equal(deletedEventURIs, wantURIs) {
		t.Fatalf("deleted events for %v, want %v", deletedEventURIs, wantURIs)
	}

	// the reported result is the base record itself
	if result == nil || result.CCKV == nil || *result.CCKV != rangeBaseURI {
		t.Fatalf("unexpected result: %+v", result)
	}

	// onUpdate defaults to forget: every deleted target's commit and the
	// delete commit itself are flagged for gc
	wantFlagged := []string{}
	for i := range targets {
		id, _ := targets[i].CDID()
		wantFlagged = append(wantFlagged, id)
	}
	wantFlagged = append(wantFlagged, documentIDOf(t, sd))
	if !slices.Equal(repo.gcFlagged, wantFlagged) {
		t.Fatalf("gc flagged = %v, want %v", repo.gcFlagged, wantFlagged)
	}
}

// A deleted document with onUpdate=retain keeps its history: neither its
// commit nor the delete commit is flagged for gc, so a repository replay
// reproduces the document and its deletion.
func TestCommitDeleteRetainKeepsHistory(t *testing.T) {
	ccid, priv := newIdentity(t)
	cfg := &domain.Config{FQDN: "example.com"}

	retained := subtreeRecord(t, rangeBaseURI)
	retained.Document = strings.Replace(retained.Document, `"kind":"record"`, `"kind":"record","onUpdate":"retain"`, 1)
	forgotten := subtreeRecord(t, rangeBaseURI+"/a")

	t.Run("retained target keeps both commits", func(t *testing.T) {
		repo := &rangeDeleteRepo{subtree: []concrnt.SignedDocument{retained}}
		uc := newRangeDeleteUsecase(ccid, cfg, repo, &denyKeysPolicyService{}, &recordingDeliveryQueue{}, nil)

		sd := signedDelete(t, ccid, priv, rangeBaseURI+"*")
		if _, err := uc.Commit(context.Background(), "127.0.0.1", sd, domain.CommitModeExecute); err != nil {
			t.Fatalf("Commit returned error: %v", err)
		}
		if len(repo.gcFlagged) != 0 {
			t.Fatalf("nothing may be flagged for gc when the target is retained, got %v", repo.gcFlagged)
		}
	})

	t.Run("one retained target keeps the delete for all", func(t *testing.T) {
		repo := &rangeDeleteRepo{subtree: []concrnt.SignedDocument{retained, forgotten}}
		uc := newRangeDeleteUsecase(ccid, cfg, repo, &denyKeysPolicyService{}, &recordingDeliveryQueue{}, nil)

		sd := signedDelete(t, ccid, priv, rangeBaseURI+"*")
		if _, err := uc.Commit(context.Background(), "127.0.0.1", sd, domain.CommitModeExecute); err != nil {
			t.Fatalf("Commit returned error: %v", err)
		}
		forgottenID, _ := forgotten.CDID()
		if !slices.Equal(repo.gcFlagged, []string{forgottenID}) {
			t.Fatalf("gc flagged = %v, want only the forgotten target %s", repo.gcFlagged, forgottenID)
		}
	})
}

// A children-only range delete ("...l1/*") enumerates without includeSelf.
func TestCommitRangeDeleteChildrenOnly(t *testing.T) {
	ccid, priv := newIdentity(t)
	cfg := &domain.Config{FQDN: "example.com"}

	repo := &rangeDeleteRepo{subtree: []concrnt.SignedDocument{subtreeRecord(t, rangeBaseURI+"/a")}}
	uc := newRangeDeleteUsecase(ccid, cfg, repo, &denyKeysPolicyService{}, &recordingDeliveryQueue{}, nil)

	sd := signedDelete(t, ccid, priv, rangeBaseURI+"/*")
	if _, err := uc.Commit(context.Background(), "127.0.0.1", sd, domain.CommitModeExecute); err != nil {
		t.Fatalf("Commit returned error: %v", err)
	}
	if len(repo.queryCalls) != 1 || repo.queryCalls[0] != (subtreeQueryCall{base: rangeBaseURI, includeSelf: false}) {
		t.Fatalf("unexpected enumeration calls: %+v", repo.queryCalls)
	}
}

// A single denied target fails the whole range delete: nothing is deleted, the
// transaction (commitlog included) is rolled back, and no advertisement or
// signal is emitted.
func TestCommitRangeDeleteDenyRollsBackEverything(t *testing.T) {
	ccid, priv := newIdentity(t)
	cfg := &domain.Config{FQDN: "example.com"}

	repo := &rangeDeleteRepo{subtree: []concrnt.SignedDocument{
		subtreeRecord(t, rangeBaseURI),
		subtreeRecord(t, rangeBaseURI+"/a"),
		subtreeRecord(t, rangeBaseURI+"/b"),
	}}
	pol := &denyKeysPolicyService{deny: map[string]bool{rangeBaseURI + "/b": true}}
	delivery := &recordingDeliveryQueue{}
	kvs := &stubKVS{}
	uc := newRangeDeleteUsecase(ccid, cfg, repo, pol, delivery, kvs)

	sd := signedDelete(t, ccid, priv, rangeBaseURI+"*")
	_, err := uc.Commit(context.Background(), "127.0.0.1", sd, domain.CommitModeExecute)
	if !errors.Is(err, domain.ErrPermissionDenied) {
		t.Fatalf("expected permission error, got %v", err)
	}

	if len(repo.deletedURIs) != 0 {
		t.Fatalf("no target may be deleted on denial, got %v", repo.deletedURIs)
	}
	if len(repo.txs) != 1 || repo.txs[0].committed || !repo.txs[0].rolledBack {
		t.Fatalf("transaction must be rolled back, got %+v", repo.txs)
	}
	if len(kvs.addedSets) != 0 {
		t.Fatalf("no removed-item advertisement may be set on denial, got %v", kvs.addedSets)
	}
	if len(delivery.jobs) != 0 {
		t.Fatalf("no delivery may be enqueued on denial, got %d jobs", len(delivery.jobs))
	}
	// fail-fast: evaluation stops at the denied key
	if len(pol.evaled) != 3 || pol.evaled[2] != rangeBaseURI+"/b" {
		t.Fatalf("unexpected evaluation order: %v", pol.evaled)
	}
}

// A range that matches nothing is an error (consistent with deleting a missing
// single key) and rolls back the commitlog.
func TestCommitRangeDeleteEmptyIsNotFound(t *testing.T) {
	ccid, priv := newIdentity(t)
	cfg := &domain.Config{FQDN: "example.com"}

	repo := &rangeDeleteRepo{}
	uc := newRangeDeleteUsecase(ccid, cfg, repo, &denyKeysPolicyService{}, &recordingDeliveryQueue{}, nil)

	sd := signedDelete(t, ccid, priv, rangeBaseURI+"/*")
	_, err := uc.Commit(context.Background(), "127.0.0.1", sd, domain.CommitModeExecute)
	if !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("expected not-found error, got %v", err)
	}
	if len(repo.txs) != 1 || repo.txs[0].committed || !repo.txs[0].rolledBack {
		t.Fatalf("transaction must be rolled back, got %+v", repo.txs)
	}
}

// A '*' anywhere but the trailing sentinel position is rejected before any
// enumeration happens.
func TestCommitRangeDeleteRejectsInnerAsterisk(t *testing.T) {
	ccid, priv := newIdentity(t)
	cfg := &domain.Config{FQDN: "example.com"}

	repo := &rangeDeleteRepo{}
	uc := newRangeDeleteUsecase(ccid, cfg, repo, &denyKeysPolicyService{}, &recordingDeliveryQueue{}, nil)

	sd := signedDelete(t, ccid, priv, "cckv://example.com/pa*th/item*")
	_, err := uc.Commit(context.Background(), "127.0.0.1", sd, domain.CommitModeExecute)
	if !errors.Is(err, domain.ErrValidation) {
		t.Fatalf("expected validation error, got %v", err)
	}
	if len(repo.queryCalls) != 0 {
		t.Fatalf("no enumeration may happen for an invalid target, got %+v", repo.queryCalls)
	}
}

// Record keys may not contain '*': it is reserved as the range-delete
// sentinel, so a literal-asterisk key could never be deleted unambiguously.
func TestCommitRejectsAsteriskInRecordKey(t *testing.T) {
	ccid, priv := newIdentity(t)
	cfg := &domain.Config{FQDN: "example.com"}
	repo := &recordingRecordRepo{}
	uc := newRecordCommitUsecase(ccid, cfg, repo, nil)

	sd := signTestDocument(t, concrnt.Document[any]{
		Kind:      "record",
		Key:       concrnt.CCURI{Scheme: "cckv", Owner: ccid, Key: "posts/it*em"}.String(),
		Author:    ccid,
		Schema:    "https://example.com/post.json",
		CreatedAt: time.Now(),
	}, priv)
	_, err := uc.Commit(context.Background(), "127.0.0.1", sd, domain.CommitModeExecute)
	if !errors.Is(err, domain.ErrValidation) {
		t.Fatalf("expected validation error, got %v", err)
	}
	if repo.createRecordCalled {
		t.Fatal("CreateRecord must not be called for a key containing '*'")
	}
}

// The remote arm of a range delete recovers the concrete targets from
// References by subtree match, sweeps each target's local reference row and
// emits one "deleted" signal set per target. The target owner is not local,
// so deleteRecord acts only on the rows it holds.
func TestDeleteRecordRangeRemoteMatchesReferences(t *testing.T) {
	ccid, _ := newIdentity(t)
	cfg := &domain.Config{FQDN: "example.com"}
	requester := domain.Entity{ID: ccid, Domain: cfg.FQDN}
	delivery := &recordingDeliveryQueue{}

	base := "cckv://otherhost.example.net/lists/l1"
	dest := "cckv://example.com/timelines/t1"
	self := subtreeRecord(t, base, dest)
	child := subtreeRecord(t, base+"/a", dest)
	unrelated := subtreeRecord(t, "cckv://otherhost.example.net/lists/l2/x")
	repo := &rangeDeleteRepo{stored: map[string]concrnt.SignedDocument{
		dest + "/" + refSegment(base):      referenceRecord(t, dest+"/"+refSegment(base), base),
		dest + "/" + refSegment(base+"/a"): referenceRecord(t, dest+"/"+refSegment(base+"/a"), base+"/a"),
	}}
	uc := New(
		repo,
		fixedResidenceRepo{entity: &requester},
		newTestServerUsecase(cfg),
		cfg,
		client.New(cfg.FQDN),
		nopSignalService{},
		&denyKeysPolicyService{},
		delivery,
		nil,
	)

	deleteDoc, err := json.Marshal(concrnt.Document[schemas.Delete]{
		Kind:      "delete",
		Value:     schemas.Delete(base + "*"),
		Author:    ccid,
		CreatedAt: time.Now(),
	})
	if err != nil {
		t.Fatalf("marshal delete document: %v", err)
	}
	sd := concrnt.SignedDocument{
		Document: string(deleteDoc),
		References: map[string]concrnt.SignedDocument{
			base:            self,
			base + "/a":     child,
			*unrelated.CCKV: unrelated,
		},
	}

	result, err := uc.deleteRecord(context.Background(), nil, "127.0.0.1", requester, sd, domain.CommitModeExecute)
	if err != nil {
		t.Fatalf("deleteRecord returned error: %v", err)
	}
	for _, task := range result.postProcesses {
		if err := task(context.Background()); err != nil {
			t.Fatalf("post process returned error: %v", err)
		}
	}

	// one "deleted" event per target and destination (the target itself and
	// its distribute destination), so each target shows up once per channel
	eventURIs := []string{}
	for _, job := range delivery.jobs {
		if job.Event != nil && job.Event.Type == "deleted" {
			eventURIs = append(eventURIs, job.Event.URI)
		}
	}
	want := []string{base, base, base + "/a", base + "/a"}
	if !slices.Equal(eventURIs, want) {
		t.Fatalf("deleted events for %v, want %v", eventURIs, want)
	}

	// no matching reference at all passes through as a no-op success
	sd.References = map[string]concrnt.SignedDocument{*unrelated.CCKV: unrelated}
	res, err := uc.deleteRecord(context.Background(), nil, "127.0.0.1", requester, sd, domain.CommitModeExecute)
	if err != nil {
		t.Fatalf("expected no-op success when no reference matches the range, got %v", err)
	}
	if !res.noop || len(res.postProcesses) != 0 {
		t.Fatalf("expected noop result without post processes, got %+v", res)
	}
}

// refSegment derives the key segment a fixture is distributed under — the
// hash-based CDID of its href, the same derivation deleteRecord's
// reference-record sweep performs.
func refSegment(href string) string {
	return cdid.MakeHash([]byte(href)).String()
}

// referenceRecord builds the stored reference row that
// createReferenceDistributionActions leaves at a distribute destination.
func referenceRecord(t *testing.T, refKey, href string) concrnt.SignedDocument {
	t.Helper()
	doc := concrnt.Document[schemas.Reference]{
		Kind:      "record",
		Key:       refKey,
		Value:     schemas.Reference{Href: href},
		Author:    "example.com",
		Schema:    schemas.ReferenceURL,
		CreatedAt: time.Now(),
	}
	docBytes, err := json.Marshal(doc)
	if err != nil {
		t.Fatalf("marshal reference fixture: %v", err)
	}
	k := refKey
	return concrnt.SignedDocument{
		CCKV:     &k,
		Document: string(docBytes),
		Proof:    concrnt.Proof{Type: concrnt.ProofTypeNone},
	}
}

// Deleting a record whose distribute destination lives on this same server
// sweeps the destination's reference row in the same commit: both rows are
// deleted, the reference row's chunkline slot is advertised as removed, and
// the fan-out re-federates the delete.
func TestCommitDeleteSweepsLocalDistributeReference(t *testing.T) {
	ccid, priv := newIdentity(t)
	cfg := &domain.Config{FQDN: "example.com"}

	targetURI := "cckv://example.com/profiles/main/posts/p1"
	dest := "cckv://example.com/timelines/t1"
	target := subtreeRecord(t, targetURI, dest)
	refKey := dest + "/" + refSegment(targetURI)

	repo := &rangeDeleteRepo{
		stored: map[string]concrnt.SignedDocument{
			targetURI: target,
			refKey:    referenceRecord(t, refKey, targetURI),
		},
		removals: map[string][2]string{refKey: {dest, targetURI}},
	}
	pol := &denyKeysPolicyService{}
	delivery := &recordingDeliveryQueue{}
	kvs := &stubKVS{}
	uc := newRangeDeleteUsecase(ccid, cfg, repo, pol, delivery, kvs)

	sd := signedDelete(t, ccid, priv, targetURI)
	if _, err := uc.Commit(context.Background(), "127.0.0.1", sd, domain.CommitModeExecute); err != nil {
		t.Fatalf("Commit returned error: %v", err)
	}

	if !slices.Equal(repo.deletedURIs, []string{targetURI, refKey}) {
		t.Fatalf("deleted URIs = %v, want [%s %s]", repo.deletedURIs, targetURI, refKey)
	}
	if !slices.Equal(pol.evaled, []string{targetURI, refKey}) {
		t.Fatalf("policy evaluated on %v, want [%s %s]", pol.evaled, targetURI, refKey)
	}
	if !slices.Equal(kvs.addedSets, []string{chunkline.RemovedItemsKey(dest)}) {
		t.Fatalf("removed-item advertisements = %v, want [%s]", kvs.addedSets, chunkline.RemovedItemsKey(dest))
	}
	if len(repo.txs) != 1 || !repo.txs[0].committed {
		t.Fatalf("unexpected tx state: %+v", repo.txs)
	}
	for _, job := range delivery.jobs {
		if job.Remote != DeliveryRemoteCommit {
			t.Fatalf("authoritative fan-out must re-federate, got %+v", job)
		}
	}
}

// A delete arriving for a remote target sweeps the reference rows this server
// holds for the target's distribute destinations: the row is deleted in the
// same commit (commitlog kept), its chunkline slot is advertised as removed,
// and the delete is not re-federated.
func TestCommitDeleteRemoteTargetSweepsLocalReference(t *testing.T) {
	ccid, priv := newIdentity(t)
	cfg := &domain.Config{FQDN: "example.com"}

	base := "cckv://otherhost.example.net/lists/l1"
	dest := "cckv://example.com/timelines/t1"
	target := subtreeRecord(t, base, dest)
	refKey := dest + "/" + refSegment(base)

	repo := &rangeDeleteRepo{
		stored:   map[string]concrnt.SignedDocument{refKey: referenceRecord(t, refKey, base)},
		removals: map[string][2]string{refKey: {dest, base}},
	}
	pol := &denyKeysPolicyService{}
	delivery := &recordingDeliveryQueue{}
	kvs := &stubKVS{}
	uc := newRangeDeleteUsecase(ccid, cfg, repo, pol, delivery, kvs)

	sd := signedDelete(t, ccid, priv, base)
	sd.References = map[string]concrnt.SignedDocument{base: target}
	if _, err := uc.Commit(context.Background(), "127.0.0.1", sd, domain.CommitModeExecute); err != nil {
		t.Fatalf("Commit returned error: %v", err)
	}

	if !slices.Equal(repo.deletedURIs, []string{refKey}) {
		t.Fatalf("deleted URIs = %v, want [%s]", repo.deletedURIs, refKey)
	}
	if !slices.Equal(pol.evaled, []string{refKey}) {
		t.Fatalf("policy evaluated on %v, want [%s]", pol.evaled, refKey)
	}
	if !slices.Equal(kvs.addedSets, []string{chunkline.RemovedItemsKey(dest)}) {
		t.Fatalf("removed-item advertisements = %v, want [%s]", kvs.addedSets, chunkline.RemovedItemsKey(dest))
	}
	if len(repo.txs) != 1 || !repo.txs[0].committed {
		t.Fatalf("commitlog must be kept for a delete that acted locally, got %+v", repo.txs)
	}
	// the forwarded delete is committed like any other, under the deleted
	// namespace's owner (CIP-4 §6.1 / CIP-3 §3.1); the swept reference row and
	// the delete itself are history nobody asked to keep, so both go to gc
	deleteID := documentIDOf(t, sd)
	if repo.commitLogOwners[deleteID] != "otherhost.example.net" {
		t.Fatalf("delete commit owner = %q, want the deleted namespace's owner", repo.commitLogOwners[deleteID])
	}
	refID, _ := (&concrnt.SignedDocument{Document: repo.stored[refKey].Document}).CDID()
	if !slices.Equal(repo.gcFlagged, []string{refID, deleteID}) {
		t.Fatalf("gc flagged = %v, want [%s %s]", repo.gcFlagged, refID, deleteID)
	}
	deletedEvents := 0
	for _, job := range delivery.jobs {
		if job.Remote != DeliveryRemoteNone {
			t.Fatalf("a receiving server must not re-federate the delete, got %+v", job)
		}
		if job.Event != nil && job.Event.Type == "deleted" {
			deletedEvents++
			if job.Event.URI != base {
				t.Fatalf("deleted event URI = %s, want %s", job.Event.URI, base)
			}
		}
	}
	if deletedEvents == 0 {
		t.Fatal("expected deleted events to be enqueued")
	}
}

// A delete that concerns neither the target key nor anything in References is
// passed through: no error, nothing deleted, nothing signalled, and the
// commitlog is rolled back so a later delivery carrying the targets still
// applies.
func TestCommitDeleteUnrelatedIsNoop(t *testing.T) {
	ccid, priv := newIdentity(t)
	cfg := &domain.Config{FQDN: "example.com"}
	base := "cckv://otherhost.example.net/lists/l1"
	unrelated := subtreeRecord(t, "cckv://otherhost.example.net/lists/l2/x")

	for _, target := range []string{base, base + "*"} {
		repo := &rangeDeleteRepo{}
		delivery := &recordingDeliveryQueue{}
		uc := newRangeDeleteUsecase(ccid, cfg, repo, &denyKeysPolicyService{}, delivery, nil)

		sd := signedDelete(t, ccid, priv, target)
		sd.References = map[string]concrnt.SignedDocument{*unrelated.CCKV: unrelated}
		if _, err := uc.Commit(context.Background(), "127.0.0.1", sd, domain.CommitModeExecute); err != nil {
			t.Fatalf("Commit(%s) returned error: %v", target, err)
		}
		if len(repo.deletedURIs) != 0 || len(delivery.jobs) != 0 {
			t.Fatalf("nothing may be deleted or signalled for %s, got %v / %d jobs", target, repo.deletedURIs, len(delivery.jobs))
		}
		if len(repo.txs) != 1 || repo.txs[0].committed || !repo.txs[0].rolledBack {
			t.Fatalf("commitlog must be rolled back for %s, got %+v", target, repo.txs)
		}
	}
}

// A delete for a remote target whose destinations left no rows here removes
// nothing, so it is not recorded (CIP-4 §6.1): the tx rolls back, no
// commitlog, no signals — a later delivery that does find rows still applies.
func TestCommitDeleteRemoteTargetWithoutLocalCopies(t *testing.T) {
	ccid, priv := newIdentity(t)
	cfg := &domain.Config{FQDN: "example.com"}
	base := "cckv://otherhost.example.net/lists/l1"
	target := subtreeRecord(t, base, "cckv://example.com/timelines/t1")

	repo := &rangeDeleteRepo{}
	delivery := &recordingDeliveryQueue{}
	uc := newRangeDeleteUsecase(ccid, cfg, repo, &denyKeysPolicyService{}, delivery, &stubKVS{})

	sd := signedDelete(t, ccid, priv, base)
	sd.References = map[string]concrnt.SignedDocument{base: target}
	if _, err := uc.Commit(context.Background(), "127.0.0.1", sd, domain.CommitModeExecute); err != nil {
		t.Fatalf("Commit returned error: %v", err)
	}
	if len(repo.deletedURIs) != 0 {
		t.Fatalf("nothing may be deleted, got %v", repo.deletedURIs)
	}
	if len(repo.txs) != 1 || repo.txs[0].committed || !repo.txs[0].rolledBack {
		t.Fatalf("a delete that removed nothing must roll back, got %+v", repo.txs)
	}
	if len(delivery.jobs) != 0 {
		t.Fatalf("a delete that removed nothing must not signal, got %+v", delivery.jobs)
	}
}

// A destination policy denying the sweep keeps only that reference row: the
// target itself is still deleted and the commit succeeds.
func TestCommitDeleteReferenceSweepDenyKeepsRow(t *testing.T) {
	ccid, priv := newIdentity(t)
	cfg := &domain.Config{FQDN: "example.com"}

	targetURI := "cckv://example.com/profiles/main/posts/p1"
	dest := "cckv://example.com/timelines/t1"
	target := subtreeRecord(t, targetURI, dest)
	refKey := dest + "/" + refSegment(targetURI)

	repo := &rangeDeleteRepo{
		stored: map[string]concrnt.SignedDocument{
			targetURI: target,
			refKey:    referenceRecord(t, refKey, targetURI),
		},
		removals: map[string][2]string{refKey: {dest, targetURI}},
	}
	pol := &denyKeysPolicyService{deny: map[string]bool{refKey: true}}
	kvs := &stubKVS{}
	uc := newRangeDeleteUsecase(ccid, cfg, repo, pol, &recordingDeliveryQueue{}, kvs)

	sd := signedDelete(t, ccid, priv, targetURI)
	if _, err := uc.Commit(context.Background(), "127.0.0.1", sd, domain.CommitModeExecute); err != nil {
		t.Fatalf("Commit returned error: %v", err)
	}
	if !slices.Equal(repo.deletedURIs, []string{targetURI}) {
		t.Fatalf("only the target may be deleted, got %v", repo.deletedURIs)
	}
	if len(kvs.addedSets) != 0 {
		t.Fatalf("a kept row must not be advertised as removed, got %v", kvs.addedSets)
	}
	if len(repo.txs) != 1 || !repo.txs[0].committed {
		t.Fatalf("unexpected tx state: %+v", repo.txs)
	}
}

// A range delete arriving for a remote base sweeps every locally-held
// reference row of every recovered target.
func TestCommitDeleteRemoteRangeSweepsAllReferences(t *testing.T) {
	ccid, priv := newIdentity(t)
	cfg := &domain.Config{FQDN: "example.com"}

	base := "cckv://otherhost.example.net/lists/l1"
	dest := "cckv://example.com/timelines/t1"
	self := subtreeRecord(t, base, dest)
	child := subtreeRecord(t, base+"/a", dest)
	refKeySelf := dest + "/" + refSegment(base)
	refKeyChild := dest + "/" + refSegment(base+"/a")

	repo := &rangeDeleteRepo{
		stored: map[string]concrnt.SignedDocument{
			refKeySelf:  referenceRecord(t, refKeySelf, base),
			refKeyChild: referenceRecord(t, refKeyChild, base+"/a"),
		},
	}
	uc := newRangeDeleteUsecase(ccid, cfg, repo, &denyKeysPolicyService{}, &recordingDeliveryQueue{}, nil)

	sd := signedDelete(t, ccid, priv, base+"*")
	sd.References = map[string]concrnt.SignedDocument{
		base:        self,
		base + "/a": child,
	}
	if _, err := uc.Commit(context.Background(), "127.0.0.1", sd, domain.CommitModeExecute); err != nil {
		t.Fatalf("Commit returned error: %v", err)
	}
	if !slices.Equal(repo.deletedURIs, []string{refKeySelf, refKeyChild}) {
		t.Fatalf("deleted URIs = %v, want [%s %s]", repo.deletedURIs, refKeySelf, refKeyChild)
	}
}
