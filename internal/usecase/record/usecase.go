package record

import (
	"context"
	"encoding/json"
	"time"

	"github.com/patrickmn/go-cache"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/client"
	"github.com/concrnt/concrnt/internal/domain"
	"github.com/concrnt/concrnt/internal/usecase"
	"github.com/concrnt/concrnt/internal/usecase/server"
	"github.com/concrnt/concrnt/internal/utils"
	"github.com/concrnt/concrnt/policy"
)

type Repository interface {
	// Utilities
	// RunInTx runs fn atomically. fn may be invoked more than once (a backend
	// may retry on contention), so it must reach the store only through tx
	// and must not have side effects of its own. A non-nil error from fn
	// rolls the transaction back and is returned unchanged.
	RunInTx(ctx context.Context, fn func(tx RepositoryTx) error) error

	// Create / Update
	CreateCommitLog(ctx context.Context, tx RepositoryTx, id string, ip string, document string, proof any, owner string) error

	CreateEntity(ctx context.Context, tx RepositoryTx, ccid string, alias *string, domain string, documentID string, createdAt time.Time) (bool, error)
	CreateRecord(ctx context.Context, tx RepositoryTx, documentID string, key string, owner string, author string, schema string, onUpdate *string, policies *string, distributions []string, redirect *string, createdAt time.Time) (bool, error)
	CreateAssociation(ctx context.Context, tx RepositoryTx, documentID string, targetURI string, owner string, author string, schema string, variant *string, unique string, createdAt time.Time) (bool, error)
	Acknowledge(ctx context.Context, tx RepositoryTx, documentID string, from string, to string, schema string, createdAt time.Time) (bool, error)
	UnAcknowledge(ctx context.Context, tx RepositoryTx, documentID string, from string, to string, schema string, createdAt time.Time) (bool, error)
	Acknowledged(ctx context.Context, tx RepositoryTx, documentID string, from string, to string, schema string, createdAt time.Time) (bool, error)
	UnAcknowledged(ctx context.Context, tx RepositoryTx, documentID string, from string, to string, schema string, createdAt time.Time) (bool, error)

	// Read
	HasCommitLog(ctx context.Context, id string) (bool, error)
	GetSignedDocument(ctx context.Context, uri string) (*concrnt.SignedDocument, error)

	GetAcknowledgeRecords(ctx context.Context, from, to, schema string, since, until *time.Time, limit int, order string) ([]QueryRow, error)
	GetAcknowledgeRecordCounts(ctx context.Context, from, to, schema string) (map[string]int64, error)
	GetAssociatedRecords(ctx context.Context, targetURI, schema, variant, author string, since, until *time.Time, limit int, order string) ([]QueryRow, error)
	GetAssociatedRecordCountsBySchema(ctx context.Context, targetURI string) (map[string]int64, error)
	GetAssociatedRecordCountsByVariant(ctx context.Context, targetURI, schema string) (*utils.OrderedKVMap[int64], error)

	QueryByPrefix(ctx context.Context, prefix, schema, author string, since, until *time.Time, limit int, order string) ([]QueryRow, error)
	QueryByParent(ctx context.Context, parent, schema, author string, since, until *time.Time, limit int, order string) ([]QueryRow, error)

	QueryRecordSubtree(ctx context.Context, base string, includeSelf bool) ([]concrnt.SignedDocument, error)
	GetTimelineRemoval(ctx context.Context, keyURI string) (timeline string, itemID string, err error)
	GetHierarchicalRecordPolicies(ctx context.Context, uri string) ([]concrnt.Policy, error)
	GetAllCommitLogs(ctx context.Context, owner string) ([]concrnt.SignedDocument, error)

	GetDistributions(ctx context.Context, uri string) ([]string, error)

	// Delete
	// MarkCommitLogGcCandidate flags a commit log for `conctl op gc-commitlog`
	// once the backdate window has passed (CIP-3 §3.4 replay guard).
	MarkCommitLogGcCandidate(ctx context.Context, tx RepositoryTx, documentID string) error
	DeleteRecordByKey(ctx context.Context, tx RepositoryTx, targetURI string) error
	DeleteRecordByDocumentID(ctx context.Context, tx RepositoryTx, documentID string) error
	DeleteAssociation(ctx context.Context, tx RepositoryTx, documentID string) error
}

// QueryRow is a raw list-query result row paired with its effective sort key
// (the DB-side created_at the repository ordered by). Pagination cursors are
// derived from this key, so it must be carried alongside the document rather
// than re-parsed from it.
type QueryRow struct {
	Row       concrnt.SignedDocument
	CreatedAt time.Time
}

// RepositoryTx is the opaque per-backend transaction handle passed back into
// the repository's transactional methods. Implementations mark themselves
// with IsRepositoryTx; the usecase never drives commit/rollback itself.
type RepositoryTx interface {
	IsRepositoryTx()
}

type PostProcessAction func(ctx context.Context) error

type SignalService interface {
	Publish(ctx context.Context, channel string, event concrnt.Event) error
}

type PolicyService interface {
	Eval(ctx context.Context, req policy.RequestContext, stack []concrnt.Policy, action string, key string) error
}

// EntityRepository is the slice of the residence repository this usecase
// needs: residency checks on entity commits and entity lookups. The full
// residence.Repository embeds it.
type EntityRepository interface {
	GetMeta(ctx context.Context, ccid string) (*domain.EntityMeta, error)
	GetEntityByCCID(ctx context.Context, ccid string) (*domain.Entity, error)
	GetEntityByAlias(ctx context.Context, alias string) (*domain.Entity, error)
}

type commitApplyResult struct {
	result        *concrnt.SignedDocument
	postProcesses []PostProcessAction
	noop          bool
}

type Usecase struct {
	repo   Repository
	entity EntityRepository
	server *server.Usecase
	config *domain.Config
	client *client.Client
	signal SignalService
	policy PolicyService
	jobs   usecase.JobQueue
	kvs    usecase.KVS
	cache  *cache.Cache
}

func New(
	repo Repository,
	entity EntityRepository,
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
		entity: entity,
		server: server,
		config: config,
		client: client,
		signal: signal,
		policy: policy,
		jobs:   jobs,
		kvs:    kvs,
		cache:  cache.New(10*time.Minute, 15*time.Minute),
	}

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
	}

	return uc
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
