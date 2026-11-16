package record

import (
	"context"
	"encoding/json"
	"time"

	"github.com/patrickmn/go-cache"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/chunkline"
	"github.com/concrnt/concrnt/client"
	"github.com/concrnt/concrnt/internal/domain"
	"github.com/concrnt/concrnt/internal/usecase"
	"github.com/concrnt/concrnt/internal/usecase/aggregate"
	"github.com/concrnt/concrnt/internal/usecase/server"
	"github.com/concrnt/concrnt/policy"
)

type Repository interface {
	// ApplyEntity stores an entity document: the commit log entry and the
	// entity (ccid, alias, domain) it registers. Accept-if-newer on
	// CreatedAt per ccid (CIP-3 §3.4): only a strictly newer document
	// replaces the stored one, an equal or older one is Stale. The commit
	// of a replaced entity document is kept as it is (CIP-0 §8.4: entities
	// are exempt from onUpdate).
	ApplyEntity(ctx context.Context, commit EntityCommit) (ApplyStatus, error)

	// ApplyRecord stores a record under its key: the commit log entry, the
	// record, the key pointing at it, and a placeholder for every ancestor
	// key that does not exist yet (a placeholder is listed by
	// QueryByParentOrderByKey as a key-only row and resolves to nothing).
	// Accept-if-newer on CreatedAt per key: equal or older is Stale. Unless
	// the version it replaces was written with onUpdate "retain", that
	// version is forgotten and its commit flagged for gc; a retained one
	// stays readable by its ccfs URI. Overwriting does not touch the key's
	// associations (the incarnation continues, see ApplyDelete).
	ApplyRecord(ctx context.Context, commit RecordCommit) (RecordOutcome, error)

	// ApplyAssociation stores an association on Target: the commit log
	// entry and the association. Stale when an association with the same
	// Unique already exists on the target's current incarnation (C6);
	// domain.NotFoundError when the target key does not exist (a
	// placeholder key counts as existing). The association belongs to the
	// incarnation of the target key that exists at the time of the call.
	ApplyAssociation(ctx context.Context, commit AssociationCommit) (ApplyStatus, error)

	// ApplyAck moves one side's state of the (From, To, Schema)
	// relationship (CIP-10 §4): the commit log entry and the state, which
	// from then on is anchored to this commit. Accept-if-newer on CreatedAt
	// alone (never the document id, so both servers of a relationship reach
	// the same decision): equal or older is Stale. The two sides are
	// independent states; a server holds the acker side for its own ackers
	// and the target side for its own targets.
	ApplyAck(ctx context.Context, commit AckCommit) (AckOutcome, error)

	// ApplyDelete performs one page of a delete: it records Commit when
	// one is given (Duplicate when its id is already in the log, and then
	// nothing else happens), removes what Removals name, and flags
	// GcCommitIDs for gc (unknown ids are ignored). Removing something that
	// is already gone is not an error; Removed tells the two apart. A call
	// without a Commit (the continuation pages of a range delete, whose
	// commit the first page recorded) is always Applied and idempotent.
	// There is no Stale. The caller keeps one call to at most
	// rangeDeleteSyncLimit removals.
	//
	// Removing a key ends its incarnation: in this same operation every
	// association on it stops being returned by GetAssociatedRecords and
	// ComputeAssociationSummary and stops counting for uniqueness, and a
	// record written to the key later starts with none. Whatever physical
	// removal the backend left for later is handed back as Cleanups.
	ApplyDelete(ctx context.Context, commit DeleteCommit) (DeleteOutcome, error)

	// Cleanup runs one bounded step of the deferred physical removal a
	// token from DeleteOutcome.Cleanups (or from a previous Cleanup call)
	// names, and returns the token of the next step, "" when nothing is
	// left. It changes nothing a read can observe. Idempotent: running a
	// token again, or one whose work is done, is harmless. Tokens are
	// opaque strings, valid on the backend that issued them, small enough
	// for a job payload, and never expire. A backend that removes
	// everything inside ApplyDelete issues none and treats any token as
	// done.
	Cleanup(ctx context.Context, token string) (next string, err error)

	GetSignedDocument(ctx context.Context, uri string) (*concrnt.SignedDocument, error)
	// GetSignedDocuments is GetSignedDocument for several URIs at once; a URI
	// that resolves to nothing is absent from the map, not an error.
	GetSignedDocuments(ctx context.Context, uris []string) (map[string]*concrnt.SignedDocument, error)

	GetAcknowledgeRecords(ctx context.Context, from, to, schema string, since, until *time.Time, limit int, order string) ([]QueryRow, error)
	GetAssociatedRecords(ctx context.Context, targetURI, schema, variant, author string, since, until *time.Time, limit int, order string) ([]QueryRow, error)

	// Compute* recount from the primary data (C2). They are the aggregate
	// layer's cache miss path and the verification tool's reference; request
	// handlers go through the aggregate layer instead (aggregates.go).

	// ComputeAcknowledgeCounts counts the valid states of one side (from or
	// to, as in GetAcknowledgeRecords) by schema. Round trips: 1.
	ComputeAcknowledgeCounts(ctx context.Context, from, to, schema string) (map[string]int64, error)
	// ComputeAssociationSummary scans the associations on the current
	// incarnation of targetURI once and returns the exact count and the
	// earliest createdAt of every (schema, variant), and the distinct
	// authors, or AuthorsOverflow instead when there are more than
	// maxAuthors. A target without associations (or without a key) yields
	// the zero summary. Round trips: 1 (a backend without joins reads the
	// target key in parallel).
	ComputeAssociationSummary(ctx context.Context, targetURI string, maxAuthors int) (AssociationSummary, error)

	QueryByPrefix(ctx context.Context, prefix, schema, author string, since, until *time.Time, limit int, order string) ([]QueryRow, error)
	QueryByParent(ctx context.Context, parent, schema, author string, since, until *time.Time, limit int, order string) ([]QueryRow, error)
	// QueryByParentOrderByKey lists parent's direct children in cckv order
	// (CIP-5 orderby=key). Intermediate keys without a document are returned
	// as key-only rows (Row.CCKV set, Document empty) unless schema or author
	// filters them out; since/until are inclusive key bounds and QueryRow.CreatedAt
	// is unused.
	QueryByParentOrderByKey(ctx context.Context, parent, schema, author string, since, until *string, limit int, order string) ([]QueryRow, error)

	// QueryRecordSubtree lists the live records under base (base itself too
	// when includeSelf) in key order (byte order), those after afterURI
	// only when it is set, at most limit when it is positive. Matching is
	// by path hierarchy, never by string prefix. Round trips: 1 (a backend
	// without a disjunction reads base itself in parallel with its subtree).
	QueryRecordSubtree(ctx context.Context, base string, includeSelf bool, afterURI string, limit int) ([]concrnt.SignedDocument, error)
	GetTimelineRemoval(ctx context.Context, keyURI string) (timeline string, itemID string, err error)
	GetHierarchicalRecordPolicies(ctx context.Context, uri string) ([]concrnt.Policy, error)
	// GetHierarchicalRecordPoliciesMulti is GetHierarchicalRecordPolicies
	// for several cckv URIs at once; every input URI has an entry.
	GetHierarchicalRecordPoliciesMulti(ctx context.Context, uris []string) (map[string][]concrnt.Policy, error)
	GetAllCommitLogs(ctx context.Context, owner string) ([]concrnt.SignedDocument, error)
	// QueryCommitLogs pages the commit log by server receipt time (c_date,
	// tie-broken by id), the replication sort key; QueryRow.CreatedAt carries
	// that c_date. owner "" means no owner filter.
	QueryCommitLogs(ctx context.Context, owner string, since, until *time.Time, limit int, order string) ([]QueryRow, error)

	// Offline tooling (conctl gc-commitlog / dump-commitlog)
	CountGcCandidates(ctx context.Context, cutoffID string) (int64, error)
	ListGcCandidates(ctx context.Context, cutoffID string, afterID string, limit int) ([]CommitLogEntry, error)
	DeleteCommitLogs(ctx context.Context, ids []string) (int, error)
	ListCommitLogs(ctx context.Context, page CommitLogPage) ([]CommitLogEntry, error)

	// Entities
	GetEntityByCCID(ctx context.Context, ccid string) (*domain.Entity, error)
	GetEntityByAlias(ctx context.Context, alias string) (*domain.Entity, error)
	MarkCommitLogsGcCandidateByOwner(ctx context.Context, owner string) error

	// Chunkline views over record keys
	GetChunklineManifest(ctx context.Context, uri string) (*chunkline.Manifest, error)
	LookupLocalItrs(ctx context.Context, uris []string, chunkID int64) (map[string]int64, error)
	LoadLocalBody(ctx context.Context, uri string, chunkID int64) ([]chunkline.BodyItem, error)
}

// Commit is the commit log entry every write carries.
type Commit struct {
	// ID is the CDID of the signed document.
	ID       string
	IP       string
	Document string
	Proof    concrnt.Proof
	// Owner is the one entity the commit is recorded under (CIP-3 §3.1); it
	// is also the namespace the ccfs URI of what the commit stores is
	// composed in.
	Owner string
}

// ApplyStatus is how an Apply* call ended.
type ApplyStatus int

const (
	// Applied: everything the operation describes is stored.
	Applied ApplyStatus = iota
	// Stale: the write lost accept-if-newer or uniqueness. Nothing is
	// stored, not even the commit log entry, so a later delivery of the
	// same document is judged afresh.
	Stale
	// Duplicate: the commit id is already in the log (a redelivery).
	// Nothing changed.
	Duplicate
)

func (s ApplyStatus) String() string {
	switch s {
	case Applied:
		return "applied"
	case Stale:
		return "stale"
	case Duplicate:
		return "duplicate"
	default:
		return "unknown"
	}
}

// EntityCommit is an entity document to store (ApplyEntity).
type EntityCommit struct {
	Commit
	CCID      string
	Alias     *string
	Domain    string
	CreatedAt time.Time
}

// RecordCommit is a record document to store (ApplyRecord). The record is
// owned by Commit.Owner, the owner of Key.
type RecordCommit struct {
	Commit
	// Key is the cckv URI the record is written to.
	Key    string
	Author string
	// Schema and CreatedAt are what the listings filter and order by: the
	// document's own, or those of the target for a reference record.
	Schema string
	// OnUpdate is the document's onUpdate; only "retain" keeps this version
	// when a later one replaces it.
	OnUpdate *string
	// Policies is the JSON of the document's policy, nil without one.
	Policies      *string
	Distributions []string
	// Redirect is the href of a reference record, nil otherwise.
	Redirect  *string
	CreatedAt time.Time
}

// RecordOutcome is the result of ApplyRecord. FreshKey and PreviousID are
// meaningful when Status is Applied.
type RecordOutcome struct {
	Status ApplyStatus
	// FreshKey is true when the key held no record before (it did not
	// exist or was a placeholder).
	FreshKey bool
	// PreviousID is the id of the document the key pointed at before, ""
	// when FreshKey.
	PreviousID string
}

// AssociationCommit is an association document to store
// (ApplyAssociation). The association is owned by Commit.Owner, the owner
// of Target.
type AssociationCommit struct {
	Commit
	// Target is the cckv key the association is attached to.
	Target  string
	Author  string
	Schema  string
	Variant *string
	// Unique identifies the logical association: a second one with the
	// same value on the same incarnation of the target is Stale.
	Unique    string
	CreatedAt time.Time
}

// AckSide names the two states an ack relationship has.
type AckSide int

const (
	// AckerSide is the state the acker's server holds (ack / unack
	// documents), recorded under From.
	AckerSide AckSide = iota
	// TargetSide is the state the target's server holds (acked / unacked
	// documents), recorded under To.
	TargetSide
)

// AckCommit is an ack-kind document to store (ApplyAck).
type AckCommit struct {
	Commit
	Side   AckSide
	From   string
	To     string
	Schema string
	// Valid is the state the document asks for: true for ack / acked,
	// false for unack / unacked.
	Valid     bool
	CreatedAt time.Time
}

// AckOutcome is the result of ApplyAck. When Status is Applied, WasValid
// is the validity of the state before the call (false when there was none)
// and IsValid the one stored, so a caller can tell a validity flip from a
// state that merely moved to a newer document. When Stale, both are the
// stored state's validity.
type AckOutcome struct {
	Status   ApplyStatus
	WasValid bool
	IsValid  bool
}

// RemovalKind says what a Removal removes.
type RemovalKind int

const (
	// RemoveKey removes the cckv key URI together with the record it
	// points at. The key's incarnation ends: its associations are gone
	// with it. A key that does not exist or holds no record is left as it
	// is. DocumentID is unused.
	RemoveKey RemovalKind = iota
	// RemoveVersion removes the record version with DocumentID; URI is the
	// cckv key that version was written to. When the key still points at
	// that version this is RemoveKey; a version the key has moved on from
	// (kept by onUpdate "retain") is forgotten and the key stays.
	RemoveVersion
	// RemoveAssociation removes the association with DocumentID; URI is
	// the cckv key it is attached to.
	RemoveAssociation
)

// Removal is one thing ApplyDelete removes. The commit of what is removed
// stays in the log (DeleteCommit.GcCommitIDs decides its fate).
type Removal struct {
	Kind       RemovalKind
	URI        string
	DocumentID string
}

// DeleteCommit is one page of a delete (ApplyDelete).
type DeleteCommit struct {
	// Commit is the delete document; nil on the continuation pages of a
	// range delete.
	Commit *Commit
	// Removals are applied in order.
	Removals []Removal
	// GcCommitIDs are the commits to flag for gc: those of the removed
	// documents that did not ask to be retained, and the delete commit
	// itself once nothing retained needs it.
	GcCommitIDs []string
}

// DeleteOutcome is the result of ApplyDelete.
type DeleteOutcome struct {
	Status ApplyStatus
	// Removed is parallel to DeleteCommit.Removals: whether each removal
	// found something to remove, that is, something a read still returned
	// (an association of an incarnation that has ended is not, whatever is
	// left of it physically). Empty unless Applied.
	Removed []bool
	// Cleanups are the tokens of the physical removal left to do, one
	// Cleanup chain each. Empty when the backend removed everything.
	Cleanups []string
}

// AssociationSummary is everything the count cache keeps about a target
// (ComputeAssociationSummary).
type AssociationSummary struct {
	// Variants has one entry per (schema, variant) with associations, in
	// no particular order.
	Variants []AssociationVariant
	// Authors are the distinct authors, in no particular order; nil when
	// AuthorsOverflow.
	Authors []string
	// AuthorsOverflow is set when there are more distinct authors than
	// were asked for.
	AuthorsOverflow bool
}

// AssociationVariant is the count of one (schema, variant) of a target. An
// association without a variant counts under Variant "".
type AssociationVariant struct {
	Schema  string
	Variant string
	Count   int64
	// Earliest is the createdAt of the variant's earliest association.
	Earliest time.Time
}

// CommitLogEntry is one commit log row as offline tooling sees it.
type CommitLogEntry struct {
	ID       string
	IP       string
	Document string
	Proof    concrnt.Proof
	Owner    string
	CDate    time.Time
}

// CommitLogPage selects a page of commit logs ordered by id ascending. Commit
// log ids are time-prefixed CDIDs, so id bounds are time bounds.
type CommitLogPage struct {
	// FromID is an inclusive lower bound; "" means none. Ignored when AfterID is set.
	FromID string
	// AfterID is an exclusive lower bound (the last id of the previous page).
	AfterID string
	// UntilID is an inclusive upper bound; "" means none.
	UntilID string
	// Owner restricts to commits owned by this ccid; "" means all.
	Owner string
	Limit int
}

// QueryRow is a raw list-query result row paired with its effective sort key
// (the DB-side created_at the repository ordered by — or, for QueryCommitLogs,
// the commit log's c_date). Pagination cursors are derived from this key, so
// it must be carried alongside the document rather than re-parsed from it.
type QueryRow struct {
	Row       concrnt.SignedDocument
	CreatedAt time.Time
}

// QueryParams are the CIP-5 §3.1 query parameters. Since/Until carry the
// createdAt cursors; SinceKey/UntilKey carry the key cursors used when
// OrderBy is "key" (only valid with Parent).
type QueryParams struct {
	Prefix   string
	Parent   string
	Schema   string
	Author   string
	OrderBy  string
	Since    *time.Time
	Until    *time.Time
	SinceKey *string
	UntilKey *string
	Limit    int
	Order    string
}

type PostProcessAction func(ctx context.Context) error

type SignalService interface {
	Publish(ctx context.Context, channel string, event concrnt.Event) error
}

type PolicyService interface {
	Eval(ctx context.Context, req policy.RequestContext, stack []concrnt.Policy, action string, key string) error
}

type commitApplyResult struct {
	// result is returned once the write is Applied, unapplied when it was
	// Stale (or the commit a noop).
	result    *concrnt.SignedDocument
	unapplied *concrnt.SignedDocument
	// postProcesses run after an Applied write, in order.
	postProcesses []PostProcessAction
	// noop marks a commit that concerns nothing this server stores or acts
	// on; nothing is written.
	noop bool
	// apply is the commit's single repository write. nil when the commit
	// only has post-commit actions.
	apply func(ctx context.Context) (ApplyStatus, error)
}

type Usecase struct {
	repo   Repository
	meta   usecase.EntityMetaRepository
	server *server.Usecase
	config *domain.Config
	client *client.Client
	signal SignalService
	policy PolicyService
	jobs   usecase.JobQueue
	kvs    usecase.KVS
	cache  *cache.Cache
	// agg is the count cache; nil (offline tooling, tests) reads counts
	// straight from repo.Compute*
	agg  *aggregate.Usecase
	aggs aggregates
}

func New(
	repo Repository,
	meta usecase.EntityMetaRepository,
	server *server.Usecase,
	config *domain.Config,
	client *client.Client,
	signal SignalService,
	policy PolicyService,
	jobs usecase.JobQueue,
	kvs usecase.KVS,
) *Usecase {
	uc := &Usecase{
		repo:   repo,
		meta:   meta,
		server: server,
		config: config,
		client: client,
		signal: signal,
		policy: policy,
		jobs:   jobs,
		kvs:    kvs,
		cache:  cache.New(10*time.Minute, 15*time.Minute),
	}
	uc.aggs = newAggregates(repo)

	// this usecase owns the delivery job type: it both enqueues DeliveryJobs
	// and interprets them, so the queue itself stays agnostic
	if jobs != nil {
		jobs.RegisterHandler(JobTypeRecordDelivery, func(ctx context.Context, payload json.RawMessage) error {
			job, err := usecase.ParseJobPayload[DeliveryJob](payload)
			if err != nil {
				return err
			}
			return uc.Deliver(ctx, job)
		})
		jobs.RegisterHandler(JobTypeRecordRangeDelete, func(ctx context.Context, payload json.RawMessage) error {
			job, err := usecase.ParseJobPayload[RangeDeleteJob](payload)
			if err != nil {
				return err
			}
			return uc.ContinueRangeDelete(ctx, job)
		})
		jobs.RegisterHandler(JobTypeRecordCleanup, func(ctx context.Context, payload json.RawMessage) error {
			job, err := usecase.ParseJobPayload[CleanupJob](payload)
			if err != nil {
				return err
			}
			return uc.ContinueCleanup(ctx, job)
		})
	}

	return uc
}

// SetAggregates routes the count reads and writes through the aggregate
// cache agg.
func (uc *Usecase) SetAggregates(agg *aggregate.Usecase) {
	uc.agg = agg
}

// resolver returns uc.client as a DocumentResolver, or a nil interface when
// the client itself is nil (offline tooling, tests) — assigning a typed nil
// pointer directly would bypass Verify's nil-resolver guard and panic on use.
func (uc *Usecase) resolver() concrnt.DocumentResolver {
	if uc.client == nil {
		return nil
	}
	return uc.client
}
