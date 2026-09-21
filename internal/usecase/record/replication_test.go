package record

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/internal/domain"
	"github.com/concrnt/concrnt/policy"
)

// replicationRepo serves a fixed page for QueryCommitLogs and records the
// arguments the usecase asked for; the policy stack is always empty.
type replicationRepo struct {
	Repository
	rows []QueryRow

	gotOwner string
	gotLimit int
	gotOrder string
}

func (r *replicationRepo) QueryCommitLogs(ctx context.Context, owner string, since, until *time.Time, limit int, order string) ([]QueryRow, error) {
	r.gotOwner = owner
	r.gotLimit = limit
	r.gotOrder = order
	return r.rows, nil
}

func (r *replicationRepo) GetHierarchicalRecordPolicies(ctx context.Context, uri string) ([]concrnt.Policy, error) {
	return []concrnt.Policy{}, nil
}

// recordingPolicyService allows everything and records the (action, key)
// pairs it was asked to evaluate.
type recordingPolicyService struct {
	actions []string
	keys    []string
}

func (p *recordingPolicyService) Eval(ctx context.Context, req policy.RequestContext, stack []concrnt.Policy, action string, key string) error {
	p.actions = append(p.actions, action)
	p.keys = append(p.keys, key)
	return nil
}

type failingPolicyService struct{}

func (failingPolicyService) Eval(ctx context.Context, req policy.RequestContext, stack []concrnt.Policy, action string, key string) error {
	return errors.New("policy backend unavailable")
}

func replicationRow(t *testing.T, doc concrnt.Document[any], cdate time.Time) QueryRow {
	t.Helper()
	docBytes, err := json.Marshal(doc)
	require.NoError(t, err)
	ccfs := "ccfs://" + doc.Author + "/concrnt/" + cdate.Format("150405.000000")
	return QueryRow{
		Row: concrnt.SignedDocument{
			CCFS:     &ccfs,
			Document: string(docBytes),
			Proof:    concrnt.Proof{Type: concrnt.ProofTypeNone},
		},
		CreatedAt: cdate,
	}
}

func replicationKinds(t *testing.T, items []concrnt.SignedDocument) []string {
	t.Helper()
	kinds := make([]string, 0, len(items))
	for _, sd := range items {
		kinds = append(kinds, kindOf(t, sd))
	}
	return kinds
}

// CIP-16 §3.4: record commits are filtered by record:read on their key,
// association commits by association:read rooted at the associate, every
// other kind passes through untouched; the system service account sees all.
func TestReplicateReadAccess(t *testing.T) {
	cfg := &domain.Config{FQDN: "example.com"}
	owner := "con1owner"
	base := time.Date(2026, 9, 15, 12, 0, 0, 0, time.UTC)

	protectedKey := "cckv://" + owner + "/posts/protected"
	publicKey := "cckv://" + owner + "/posts/public"
	target := "cckv://" + owner + "/posts/target"

	rows := []QueryRow{
		replicationRow(t, concrnt.Document[any]{Kind: "entity", Author: owner, CreatedAt: base}, base),
		replicationRow(t, concrnt.Document[any]{Kind: "record", Key: protectedKey, Author: owner, CreatedAt: base}, base.Add(1*time.Second)),
		replicationRow(t, concrnt.Document[any]{Kind: "record", Key: publicKey, Author: owner, CreatedAt: base}, base.Add(2*time.Second)),
		replicationRow(t, concrnt.Document[any]{Kind: "association", Key: "cckv://" + owner + "/assoc", Associate: &target, Author: owner, CreatedAt: base}, base.Add(3*time.Second)),
		replicationRow(t, concrnt.Document[any]{Kind: "ack", Author: owner, CreatedAt: base}, base.Add(4*time.Second)),
		replicationRow(t, concrnt.Document[any]{Kind: "acked", Author: owner, CreatedAt: base}, base.Add(5*time.Second)),
		replicationRow(t, concrnt.Document[any]{Kind: "delete", Key: protectedKey, Author: owner, CreatedAt: base}, base.Add(6*time.Second)),
	}

	newUsecase := func(repo *replicationRepo, pol PolicyService) *Usecase {
		return New(repo, stubResidenceRepo{}, newTestServerUsecase(cfg), cfg, nil, nopSignalService{}, pol, nil, nil)
	}

	t.Run("anonymous requester loses the protected record only", func(t *testing.T) {
		repo := &replicationRepo{rows: rows}
		uc := newUsecase(repo, &uriDenyPolicyService{denyKey: protectedKey})

		result, err := uc.Replicate(context.Background(), owner, nil, nil, 10, "asc")
		require.NoError(t, err)
		require.Equal(t, []string{"entity", "record", "association", "ack", "acked", "delete"}, replicationKinds(t, result.Items))
		require.Contains(t, result.Items[1].Document, publicKey)

		require.Equal(t, owner, repo.gotOwner)
		require.Equal(t, 11, repo.gotLimit, "the usecase peeks one row past the window")
		require.Equal(t, "asc", repo.gotOrder)
	})

	t.Run("anonymous baseline keeps every non-record kind", func(t *testing.T) {
		uc := newUsecase(&replicationRepo{rows: rows}, &anonymousDenyPolicyService{})

		result, err := uc.Replicate(context.Background(), "", nil, nil, 10, "asc")
		require.NoError(t, err)
		require.Equal(t, []string{"entity", "ack", "acked", "delete"}, replicationKinds(t, result.Items))
	})

	t.Run("association is evaluated at its associate", func(t *testing.T) {
		pol := &recordingPolicyService{}
		uc := newUsecase(&replicationRepo{rows: rows}, pol)

		_, err := uc.Replicate(context.Background(), "", nil, nil, 10, "asc")
		require.NoError(t, err)
		require.Equal(t, []string{"record:read", "record:read", "association:read"}, pol.actions)
		require.Equal(t, []string{protectedKey, publicKey, target}, pol.keys)
	})

	t.Run("system service account skips the check", func(t *testing.T) {
		pol := &recordingPolicyService{}
		uc := newUsecase(&replicationRepo{rows: rows}, pol)

		result, err := uc.Replicate(systemCtx(), "", nil, nil, 10, "asc")
		require.NoError(t, err)
		require.Len(t, result.Items, len(rows))
		require.Empty(t, pol.actions, "system replication must not consult the policy")
	})

	t.Run("cursors come from the unfiltered window", func(t *testing.T) {
		denied := []QueryRow{
			replicationRow(t, concrnt.Document[any]{Kind: "record", Key: protectedKey, Author: owner, CreatedAt: base}, base),
			replicationRow(t, concrnt.Document[any]{Kind: "record", Key: protectedKey, Author: owner, CreatedAt: base}, base.Add(time.Second)),
			replicationRow(t, concrnt.Document[any]{Kind: "record", Key: protectedKey, Author: owner, CreatedAt: base}, base.Add(2*time.Second)),
		}
		uc := newUsecase(&replicationRepo{rows: denied}, &uriDenyPolicyService{denyKey: protectedKey})

		result, err := uc.Replicate(context.Background(), "", nil, nil, 2, "asc")
		require.NoError(t, err)
		require.Empty(t, result.Items)
		require.NotNil(t, result.Prev)
		require.Equal(t, denied[0].CreatedAt.Format(time.RFC3339Nano), *result.Prev)
		require.NotNil(t, result.Next)
		require.Equal(t, denied[2].CreatedAt.Format(time.RFC3339Nano), *result.Next)
	})

	t.Run("policy failures other than denial abort", func(t *testing.T) {
		uc := newUsecase(&replicationRepo{rows: rows}, failingPolicyService{})

		_, err := uc.Replicate(context.Background(), "", nil, nil, 10, "asc")
		require.Error(t, err)
	})
}
