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

type subtreeQueryCall struct {
	base        string
	includeSelf bool
	afterURI    string
	limit       int
}

// rangeDeleteRepo serves a fixed subtree enumeration and records the
// enumeration arguments and every ApplyDelete call. stored is what
// GetSignedDocument serves per URI (delete targets, reference rows);
// removals is what GetTimelineRemoval serves per key URI as (timeline, item).
// Every removal finds something to remove. cleanups, when set, are the
// tokens each ApplyDelete call hands back, and cleanupNext the token Cleanup
// answers each token with (none: that chain is done).
type rangeDeleteRepo struct {
	Repository
	subtree     []concrnt.SignedDocument
	stored      map[string]concrnt.SignedDocument
	removals    map[string][2]string
	cleanups    []string
	cleanupNext map[string]string
	queryCalls  []subtreeQueryCall
	// cleanupCalls is the token of every Cleanup call in order.
	cleanupCalls []string
	// deletes is every ApplyDelete call in order: the first page of a delete
	// carries its Commit, a range delete's continuation pages none.
	deletes []DeleteCommit
	// deletedURIs are the keys the RemoveKey removals named, in order.
	deletedURIs []string
	// commitLogOwners is the owner each delete commit was recorded under.
	commitLogOwners map[string]string
}

func (r *rangeDeleteRepo) ApplyDelete(ctx context.Context, commit DeleteCommit) (DeleteOutcome, error) {
	r.deletes = append(r.deletes, commit)
	if commit.Commit != nil {
		if r.commitLogOwners == nil {
			r.commitLogOwners = map[string]string{}
		}
		r.commitLogOwners[commit.Commit.ID] = commit.Commit.Owner
	}
	removed := make([]bool, len(commit.Removals))
	for i, removal := range commit.Removals {
		if removal.Kind == RemoveKey {
			r.deletedURIs = append(r.deletedURIs, removal.URI)
		}
		removed[i] = true
	}
	return DeleteOutcome{Status: Applied, Removed: removed, Cleanups: r.cleanups}, nil
}

func (r *rangeDeleteRepo) Cleanup(ctx context.Context, token string) (string, error) {
	r.cleanupCalls = append(r.cleanupCalls, token)
	return r.cleanupNext[token], nil
}

// gcFlagged lists the commits flagged for gc over every ApplyDelete call, in
// order.
func (r *rangeDeleteRepo) gcFlagged() []string {
	flagged := []string{}
	for _, commit := range r.deletes {
		flagged = append(flagged, commit.GcCommitIDs...)
	}
	return flagged
}
func (r *rangeDeleteRepo) GetHierarchicalRecordPolicies(ctx context.Context, uri string) ([]concrnt.Policy, error) {
	return nil, nil
}
func (r *rangeDeleteRepo) GetHierarchicalRecordPoliciesMulti(ctx context.Context, uris []string) (map[string][]concrnt.Policy, error) {
	return emptyStacks(uris), nil
}
func (r *rangeDeleteRepo) QueryRecordSubtree(ctx context.Context, base string, includeSelf bool, afterURI string, limit int) ([]concrnt.SignedDocument, error) {
	r.queryCalls = append(r.queryCalls, subtreeQueryCall{base: base, includeSelf: includeSelf, afterURI: afterURI, limit: limit})
	out := []concrnt.SignedDocument{}
	for _, sd := range r.subtree {
		if *sd.CCKV <= afterURI || slices.Contains(r.deletedURIs, *sd.CCKV) {
			continue
		}
		out = append(out, sd)
		if limit > 0 && len(out) == limit {
			break
		}
	}
	return out, nil
}
func (r *rangeDeleteRepo) GetSignedDocument(ctx context.Context, uri string) (*concrnt.SignedDocument, error) {
	if sd, ok := r.stored[uri]; ok {
		return &sd, nil
	}
	return nil, domain.NotFoundError{Resource: uri}
}
func (r *rangeDeleteRepo) GetSignedDocuments(ctx context.Context, uris []string) (map[string]*concrnt.SignedDocument, error) {
	out := map[string]*concrnt.SignedDocument{}
	for _, uri := range uris {
		if sd, ok := r.stored[uri]; ok {
			out[uri] = &sd
		}
	}
	return out, nil
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
	jobs        []DeliveryJob
	rangeJobs   []RangeDeleteJob
	cleanupJobs []CleanupJob
	handlers    map[string]func(ctx context.Context, payload json.RawMessage) error
	// published holds the events sent through publishRecorder
	published []concrnt.Event
}

// publishRecorder is the signal of a usecase whose local channels skip the
// queue; it keeps their events next to the queued ones.
type publishRecorder struct{ d *recordingDeliveryQueue }

func (p publishRecorder) Publish(ctx context.Context, channel string, event concrnt.Event) error {
	p.d.published = append(p.d.published, event)
	return nil
}

// deletedEventURIs lists the URIs of the deleted events, published or queued.
func (d *recordingDeliveryQueue) deletedEventURIs() []string {
	uris := []string{}
	for _, event := range d.published {
		if event.Type == "deleted" {
			uris = append(uris, event.URI)
		}
	}
	for _, job := range d.jobs {
		if job.Event != nil && job.Event.Type == "deleted" {
			uris = append(uris, job.Event.URI)
		}
	}
	return uris
}

func (d *recordingDeliveryQueue) Enqueue(ctx context.Context, jobType string, payload any) error {
	switch jobType {
	case JobTypeRecordDelivery:
		d.jobs = append(d.jobs, payload.(DeliveryJob))
	case JobTypeRecordRangeDelete:
		d.rangeJobs = append(d.rangeJobs, payload.(RangeDeleteJob))
	case JobTypeRecordCleanup:
		d.cleanupJobs = append(d.cleanupJobs, payload.(CleanupJob))
	default:
		return fmt.Errorf("unexpected job type %q", jobType)
	}
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
func newRangeDeleteUsecase(ccid string, cfg *domain.Config, repo Repository, pol PolicyService, delivery *recordingDeliveryQueue, kvs usecase.KVS) *Usecase {
	return New(
		withEntities{repo, fixedResidenceRepo{entity: &domain.Entity{ID: ccid, Domain: cfg.FQDN}}},
		nil,
		newTestServerUsecase(cfg),
		cfg,
		client.New(cfg.FQDN),
		publishRecorder{delivery},
		pol,
		delivery,
		kvs,
	)
}

const rangeBaseURI = "cckv://example.com/profiles/main/lists/l1"

// A subtree range delete ("...l1*") enumerates with includeSelf, evaluates the
// policy on every target, deletes them all in one write together with the
// commit, and then announces each deleted target.
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

	if len(repo.queryCalls) != 1 || repo.queryCalls[0] != (subtreeQueryCall{base: rangeBaseURI, includeSelf: true, limit: rangeDeleteSyncLimit + 1}) {
		t.Fatalf("unexpected enumeration calls: %+v", repo.queryCalls)
	}

	wantURIs := []string{rangeBaseURI, rangeBaseURI + "/a", rangeBaseURI + "/b"}
	if !slices.Equal(repo.deletedURIs, wantURIs) {
		t.Fatalf("deleted URIs = %v, want %v", repo.deletedURIs, wantURIs)
	}
	if !slices.Equal(pol.evaled, wantURIs) {
		t.Fatalf("policy evaluated on %v, want %v", pol.evaled, wantURIs)
	}
	if len(repo.deletes) != 1 || repo.deletes[0].Commit == nil || repo.deletes[0].Commit.ID != documentIDOf(t, sd) {
		t.Fatalf("expected one ApplyDelete call carrying the delete commit, got %+v", repo.deletes)
	}
	for i, removal := range repo.deletes[0].Removals {
		if removal != (Removal{Kind: RemoveKey, URI: wantURIs[i]}) {
			t.Fatalf("removal %d = %+v, want RemoveKey of %s", i, removal, wantURIs[i])
		}
	}

	// one "deleted" event per target, addressed to the concrete URI
	if deletedEventURIs := delivery.deletedEventURIs(); !slices.Equal(deletedEventURIs, wantURIs) {
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
	if flagged := repo.gcFlagged(); !slices.Equal(flagged, wantFlagged) {
		t.Fatalf("gc flagged = %v, want %v", flagged, wantFlagged)
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
		if flagged := repo.gcFlagged(); len(flagged) != 0 {
			t.Fatalf("nothing may be flagged for gc when the target is retained, got %v", flagged)
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
		if flagged := repo.gcFlagged(); !slices.Equal(flagged, []string{forgottenID}) {
			t.Fatalf("gc flagged = %v, want only the forgotten target %s", flagged, forgottenID)
		}
	})
}

// A subtree larger than rangeDeleteSyncLimit is removed one page per
// ApplyDelete call: the commit takes the first page and enqueues a
// RecordRangeDelete job for the rest; each job page re-enqueues the next
// until the subtree is exhausted, and only the last page flags the delete
// commit for gc. A retained document in any page keeps the commit.
func TestCommitRangeDeleteLargeSubtreePagesThroughJobs(t *testing.T) {
	ccid, priv := newIdentity(t)
	cfg := &domain.Config{FQDN: "example.com"}

	const extra = 3
	targets := []concrnt.SignedDocument{subtreeRecord(t, rangeBaseURI)}
	for i := 0; i < rangeDeleteSyncLimit+extra-1; i++ {
		targets = append(targets, subtreeRecord(t, fmt.Sprintf("%s/c%05d", rangeBaseURI, i)))
	}
	repo := &rangeDeleteRepo{subtree: targets}
	delivery := &recordingDeliveryQueue{}
	uc := newRangeDeleteUsecase(ccid, cfg, repo, &denyKeysPolicyService{}, delivery, &stubKVS{})

	sd := signedDelete(t, ccid, priv, rangeBaseURI+"*")
	if _, err := uc.Commit(context.Background(), "127.0.0.1", sd, domain.CommitModeExecute); err != nil {
		t.Fatalf("Commit returned error: %v", err)
	}
	if len(repo.deletedURIs) != rangeDeleteSyncLimit {
		t.Fatalf("the commit must delete exactly one page, deleted %d", len(repo.deletedURIs))
	}
	if slices.Contains(repo.gcFlagged(), documentIDOf(t, sd)) {
		t.Fatalf("the delete commit must not be flagged before the last page")
	}
	if len(delivery.rangeJobs) != 1 {
		t.Fatalf("one continuation job must be enqueued, got %d", len(delivery.rangeJobs))
	}
	job := delivery.rangeJobs[0]
	if job.AfterURI != *targets[rangeDeleteSyncLimit-1].CCKV || !job.IncludeSelf || job.Base != rangeBaseURI || job.Requester != ccid {
		t.Fatalf("unexpected job: %+v", job)
	}

	// the queue hands the payload back to the registered handler
	payload, _ := json.Marshal(job)
	if err := delivery.handlers[JobTypeRecordRangeDelete](context.Background(), payload); err != nil {
		t.Fatalf("job returned error: %v", err)
	}
	wantURIs := []string{}
	for _, target := range targets {
		wantURIs = append(wantURIs, *target.CCKV)
	}
	if !slices.Equal(repo.deletedURIs, wantURIs) {
		t.Fatalf("after the job every target must be deleted, got %d of %d", len(repo.deletedURIs), len(wantURIs))
	}
	if len(delivery.rangeJobs) != 1 {
		t.Fatalf("the last page must not enqueue another job, got %d", len(delivery.rangeJobs))
	}
	if len(repo.deletes) != 2 || repo.deletes[0].Commit == nil || repo.deletes[1].Commit != nil {
		t.Fatalf("expected the commit's page and one continuation page without a commit, got %d calls", len(repo.deletes))
	}
	if flagged := repo.deletes[1].GcCommitIDs; len(flagged) == 0 || flagged[len(flagged)-1] != documentIDOf(t, sd) {
		t.Fatalf("the last page must flag the delete commit, flagged %v", flagged)
	}

	t.Run("a retained document in a later page keeps the delete commit", func(t *testing.T) {
		retained := subtreeRecord(t, rangeBaseURI+"/zzz")
		retained.Document = strings.Replace(retained.Document, `"kind":"record"`, `"kind":"record","onUpdate":"retain"`, 1)
		repo := &rangeDeleteRepo{subtree: append(slices.Clone(targets), retained)}
		delivery := &recordingDeliveryQueue{}
		uc := newRangeDeleteUsecase(ccid, cfg, repo, &denyKeysPolicyService{}, delivery, &stubKVS{})

		sd := signedDelete(t, ccid, priv, rangeBaseURI+"*")
		if _, err := uc.Commit(context.Background(), "127.0.0.1", sd, domain.CommitModeExecute); err != nil {
			t.Fatalf("Commit returned error: %v", err)
		}
		payload, _ := json.Marshal(delivery.rangeJobs[0])
		if err := delivery.handlers[JobTypeRecordRangeDelete](context.Background(), payload); err != nil {
			t.Fatalf("job returned error: %v", err)
		}
		if slices.Contains(repo.gcFlagged(), documentIDOf(t, sd)) {
			t.Fatalf("the delete commit must stay when a page retained a document")
		}
	})

	t.Run("a denied target in a later page is skipped", func(t *testing.T) {
		repo := &rangeDeleteRepo{subtree: targets}
		denied := *targets[len(targets)-1].CCKV
		delivery := &recordingDeliveryQueue{}
		uc := newRangeDeleteUsecase(ccid, cfg, repo, &denyKeysPolicyService{deny: map[string]bool{denied: true}}, delivery, &stubKVS{})

		sd := signedDelete(t, ccid, priv, rangeBaseURI+"*")
		if _, err := uc.Commit(context.Background(), "127.0.0.1", sd, domain.CommitModeExecute); err != nil {
			t.Fatalf("Commit returned error: %v", err)
		}
		payload, _ := json.Marshal(delivery.rangeJobs[0])
		if err := delivery.handlers[JobTypeRecordRangeDelete](context.Background(), payload); err != nil {
			t.Fatalf("job returned error: %v", err)
		}
		if slices.Contains(repo.deletedURIs, denied) {
			t.Fatalf("the denied target must survive")
		}
		if len(repo.deletedURIs) != len(targets)-1 {
			t.Fatalf("every other target must be deleted, got %d", len(repo.deletedURIs))
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
	if len(repo.queryCalls) != 1 || repo.queryCalls[0] != (subtreeQueryCall{base: rangeBaseURI, includeSelf: false, limit: rangeDeleteSyncLimit + 1}) {
		t.Fatalf("unexpected enumeration calls: %+v", repo.queryCalls)
	}
}

// A single denied target fails the whole range delete: nothing reaches the
// repository (no removal, no commitlog), and no advertisement or signal is
// emitted.
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
	if len(repo.deletes) != 0 {
		t.Fatalf("a denied delete must not reach the repository, got %+v", repo.deletes)
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
// single key) and records no commitlog.
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
	if len(repo.deletes) != 0 {
		t.Fatalf("an empty range must not reach the repository, got %+v", repo.deletes)
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
	if len(repo.recordCommits) != 0 {
		t.Fatal("ApplyRecord must not be called for a key containing '*'")
	}
}

// The remote arm of a range delete recovers the concrete targets from
// References by subtree match, sweeps each target's local reference row and
// emits one "deleted" signal set per target. The target owner is not local,
// so the delete acts only on the rows this server holds.
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
		withEntities{repo, fixedResidenceRepo{entity: &requester}},
		nil,
		newTestServerUsecase(cfg),
		cfg,
		client.New(cfg.FQDN),
		publishRecorder{delivery},
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

	result, err := uc.planDelete(context.Background(), "127.0.0.1", requester, sd, domain.CommitModeExecute)
	if err != nil {
		t.Fatalf("planDelete returned error: %v", err)
	}
	if status, err := result.apply(context.Background()); err != nil || status != Applied {
		t.Fatalf("apply = %v, %v, want Applied", status, err)
	}
	wantRemovals := []Removal{
		{Kind: RemoveKey, URI: dest + "/" + refSegment(base)},
		{Kind: RemoveKey, URI: dest + "/" + refSegment(base+"/a")},
	}
	if len(repo.deletes) != 1 || !slices.Equal(repo.deletes[0].Removals, wantRemovals) {
		t.Fatalf("ApplyDelete calls = %+v, want one removing the two reference rows %+v", repo.deletes, wantRemovals)
	}
	for _, task := range result.postProcesses {
		if err := task(context.Background()); err != nil {
			t.Fatalf("post process returned error: %v", err)
		}
	}

	// one "deleted" event per target and destination (the target itself and
	// its distribute destination), so each target shows up once per channel
	eventURIs := delivery.deletedEventURIs()
	slices.Sort(eventURIs)
	want := []string{base, base, base + "/a", base + "/a"}
	if !slices.Equal(eventURIs, want) {
		t.Fatalf("deleted events for %v, want %v", eventURIs, want)
	}

	// no matching reference at all passes through as a no-op success
	sd.References = map[string]concrnt.SignedDocument{*unrelated.CCKV: unrelated}
	res, err := uc.planDelete(context.Background(), "127.0.0.1", requester, sd, domain.CommitModeExecute)
	if err != nil {
		t.Fatalf("expected no-op success when no reference matches the range, got %v", err)
	}
	if !res.noop || res.apply != nil || len(res.postProcesses) != 0 {
		t.Fatalf("expected a noop plan without a write or post processes, got %+v", res)
	}
}

// refSegment derives the key segment a fixture is distributed under — the
// hash-based CDID of its href, the same derivation planDeleteTargets'
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
// sweeps the destination's reference row in the same write: both rows are
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
	if !slices.Equal(kvs.addedSets, []string{usecase.RemovedItemsKey(dest)}) {
		t.Fatalf("removed-item advertisements = %v, want [%s]", kvs.addedSets, usecase.RemovedItemsKey(dest))
	}
	if len(repo.deletes) != 1 || repo.deletes[0].Commit == nil {
		t.Fatalf("expected one ApplyDelete call carrying the commit, got %+v", repo.deletes)
	}
	for _, job := range delivery.jobs {
		if job.Remote != DeliveryRemoteCommit {
			t.Fatalf("authoritative fan-out must re-federate, got %+v", job)
		}
	}
}

// A delete arriving for a remote target sweeps the reference rows this server
// holds for the target's distribute destinations: the row is deleted in the
// same write as the commitlog, its chunkline slot is advertised as removed,
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
	if !slices.Equal(kvs.addedSets, []string{usecase.RemovedItemsKey(dest)}) {
		t.Fatalf("removed-item advertisements = %v, want [%s]", kvs.addedSets, usecase.RemovedItemsKey(dest))
	}
	if len(repo.deletes) != 1 || repo.deletes[0].Commit == nil {
		t.Fatalf("a delete that acted locally must record its commitlog, got %+v", repo.deletes)
	}
	// the forwarded delete is committed like any other, under the deleted
	// namespace's owner (CIP-4 §6.1 / CIP-3 §3.1); the swept reference row and
	// the delete itself are history nobody asked to keep, so both go to gc
	deleteID := documentIDOf(t, sd)
	if repo.commitLogOwners[deleteID] != "otherhost.example.net" {
		t.Fatalf("delete commit owner = %q, want the deleted namespace's owner", repo.commitLogOwners[deleteID])
	}
	refID, _ := (&concrnt.SignedDocument{Document: repo.stored[refKey].Document}).CDID()
	if flagged := repo.gcFlagged(); !slices.Equal(flagged, []string{refID, deleteID}) {
		t.Fatalf("gc flagged = %v, want [%s %s]", flagged, refID, deleteID)
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
// passed through: no error, nothing deleted, nothing signalled, and no
// commitlog (the repository is not called at all) so a later delivery
// carrying the targets still applies.
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
		if len(repo.deletes) != 0 {
			t.Fatalf("no commitlog may be recorded for %s, got %+v", target, repo.deletes)
		}
	}
}

// A delete for a remote target whose destinations left no rows here removes
// nothing, so it is not recorded (CIP-4 §6.1): the repository is not called,
// no commitlog, no signals — a later delivery that does find rows still applies.
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
	if len(repo.deletes) != 0 {
		t.Fatalf("a delete that removes nothing must not be recorded, got %+v", repo.deletes)
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
	if len(repo.deletes) != 1 || repo.deletes[0].Commit == nil {
		t.Fatalf("expected one ApplyDelete call carrying the commit, got %+v", repo.deletes)
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
