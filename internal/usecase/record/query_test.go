package record

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/internal/domain"
)

// queryRepo serves a fixed page for the parent listings and records which
// one the usecase chose and with what cursors.
type queryRepo struct {
	Repository
	rows []QueryRow

	gotMethod   string
	gotSinceKey *string
	gotUntilKey *string
	gotLimit    int
}

func (r *queryRepo) QueryByParent(ctx context.Context, parent, schema, author string, since, until *time.Time, limit int, order string) ([]QueryRow, error) {
	r.gotMethod = "createdAt"
	r.gotLimit = limit
	return r.rows, nil
}

func (r *queryRepo) QueryByParentOrderByKey(ctx context.Context, parent, schema, author string, since, until *string, limit int, order string) ([]QueryRow, error) {
	r.gotMethod = "key"
	r.gotSinceKey = since
	r.gotUntilKey = until
	r.gotLimit = limit
	return r.rows, nil
}

func (r *queryRepo) GetHierarchicalRecordPolicies(ctx context.Context, uri string) ([]concrnt.Policy, error) {
	return []concrnt.Policy{}, nil
}

func keyOnlyRow(key string) QueryRow {
	return QueryRow{Row: concrnt.SignedDocument{CCKV: &key}}
}

func keyedRow(t *testing.T, doc concrnt.Document[any], createdAt time.Time) QueryRow {
	t.Helper()
	row := replicationRow(t, doc, createdAt)
	row.Row.CCKV = &doc.Key
	return row
}

// CIP-5 orderby=key: intermediate keys come back as key-only entries that
// are authorized with record:read on the key itself; cursors are keys and
// come from the unfiltered window.
func TestQueryOrderByKey(t *testing.T) {
	cfg := &domain.Config{FQDN: "example.com"}
	owner := "con1owner"
	parent := "cckv://" + owner + "/app"
	base := time.Date(2026, 9, 21, 12, 0, 0, 0, time.UTC)

	docKey := parent + "/a"
	folderKey := parent + "/posts"
	secretKey := parent + "/secret"

	rows := []QueryRow{
		keyedRow(t, concrnt.Document[any]{Kind: "record", Key: docKey, Author: owner, CreatedAt: base}, base),
		keyOnlyRow(folderKey),
		keyOnlyRow(secretKey),
	}

	newUsecase := func(repo *queryRepo, pol PolicyService) *Usecase {
		return New(repo, stubResidenceRepo{}, newTestServerUsecase(cfg), cfg, nil, nopSignalService{}, pol, nil, nil)
	}

	t.Run("key-only entries are evaluated as record:read on the key", func(t *testing.T) {
		pol := &recordingPolicyService{}
		repo := &queryRepo{rows: rows}
		uc := newUsecase(repo, pol)

		since := parent + "/"
		result, err := uc.Query(context.Background(), QueryParams{Parent: parent, OrderBy: "key", SinceKey: &since, Limit: 10, Order: "asc"})
		require.NoError(t, err)
		require.Equal(t, "key", repo.gotMethod)
		require.Equal(t, 11, repo.gotLimit, "the usecase peeks one row past the window")
		require.Equal(t, &since, repo.gotSinceKey)
		require.Nil(t, repo.gotUntilKey)

		require.Len(t, result.Items, 3)
		require.Equal(t, folderKey, *result.Items[1].CCKV)
		require.Empty(t, result.Items[1].Document)
		require.Equal(t, []string{"record:read", "record:read", "record:read"}, pol.actions)
		require.Equal(t, []string{docKey, folderKey, secretKey}, pol.keys)
	})

	t.Run("denied key-only entries are omitted", func(t *testing.T) {
		uc := newUsecase(&queryRepo{rows: rows}, &uriDenyPolicyService{denyKey: secretKey})

		result, err := uc.Query(context.Background(), QueryParams{Parent: parent, OrderBy: "key", Limit: 10, Order: "asc"})
		require.NoError(t, err)
		require.Len(t, result.Items, 2)
		require.Equal(t, docKey, *result.Items[0].CCKV)
		require.Equal(t, folderKey, *result.Items[1].CCKV)
	})

	t.Run("cursors are keys from the unfiltered window", func(t *testing.T) {
		uc := newUsecase(&queryRepo{rows: rows}, &uriDenyPolicyService{denyKey: docKey})

		result, err := uc.Query(context.Background(), QueryParams{Parent: parent, OrderBy: "key", Limit: 2, Order: "asc"})
		require.NoError(t, err)
		require.Equal(t, []string{folderKey}, urisOf(result.Items))
		require.NotNil(t, result.Prev)
		require.Equal(t, docKey, *result.Prev)
		require.NotNil(t, result.Next)
		require.Equal(t, secretKey, *result.Next)
	})

	t.Run("createdAt cursors keep the RFC3339Nano encoding", func(t *testing.T) {
		repo := &queryRepo{rows: rows[:1]}
		uc := newUsecase(repo, &recordingPolicyService{})

		result, err := uc.Query(context.Background(), QueryParams{Parent: parent, Limit: 10, Order: "desc"})
		require.NoError(t, err)
		require.Equal(t, "createdAt", repo.gotMethod)
		require.NotNil(t, result.Prev)
		require.Equal(t, base.Format(time.RFC3339Nano), *result.Prev)
		require.Nil(t, result.Next)
	})

	t.Run("orderby=key needs parent", func(t *testing.T) {
		uc := newUsecase(&queryRepo{rows: rows}, &recordingPolicyService{})

		_, err := uc.Query(context.Background(), QueryParams{Prefix: parent + "/", OrderBy: "key", Limit: 10})
		require.Error(t, err)
	})

	t.Run("unknown orderby is rejected", func(t *testing.T) {
		uc := newUsecase(&queryRepo{rows: rows}, &recordingPolicyService{})

		_, err := uc.Query(context.Background(), QueryParams{Parent: parent, OrderBy: "bogus", Limit: 10})
		require.Error(t, err)
	})
}

func urisOf(items []concrnt.SignedDocument) []string {
	uris := make([]string, 0, len(items))
	for _, sd := range items {
		uris = append(uris, *sd.CCKV)
	}
	return uris
}
