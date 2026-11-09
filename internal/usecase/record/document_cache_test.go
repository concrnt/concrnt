package record

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/internal/domain"
)

// documentRepo serves one document per URI and counts the reads.
type documentRepo struct {
	Repository
	docs  map[string]string
	reads int
}

func (r *documentRepo) GetSignedDocument(ctx context.Context, uri string) (*concrnt.SignedDocument, error) {
	r.reads++
	doc, ok := r.docs[uri]
	if !ok {
		return nil, domain.NotFoundError{Resource: uri}
	}
	return &concrnt.SignedDocument{CCKV: &uri, Document: doc}, nil
}

func (r *documentRepo) GetHierarchicalRecordPoliciesMulti(ctx context.Context, uris []string) (map[string][]concrnt.Policy, error) {
	return emptyStacks(uris), nil
}

// A resolved document is served from the shared cache until the key is
// written or deleted; a missing document is never cached.
func TestGetSignedUsesDocumentCache(t *testing.T) {
	ctx := context.Background()
	key := "cckv://con1owner/home/post1"
	repo := &documentRepo{docs: map[string]string{key: `{"kind":"record","key":"` + key + `","schema":"https://schema.example/post.json"}`}}
	cache := newFakeSharedCache()
	reader := newCachedUsecase(&domain.Config{FQDN: "example.com"}, repo, cache)
	writer := newCachedUsecase(&domain.Config{FQDN: "example.com"}, repo, cache)

	for range 3 {
		sd, err := reader.GetSigned(ctx, key)
		require.NoError(t, err)
		require.Equal(t, key, *sd.CCKV)
	}
	require.Equal(t, 1, repo.reads, "the second and third resolve come from the cache")

	require.NoError(t, writer.documentCacheInvalidated(&key, nil)(ctx))
	_, err := reader.GetSigned(ctx, key)
	require.NoError(t, err)
	require.Equal(t, 2, repo.reads, "an invalidated document is read again")

	missing := "cckv://con1owner/home/none"
	for range 2 {
		_, err := reader.GetSigned(ctx, missing)
		require.ErrorIs(t, err, domain.ErrNotFound)
	}
	require.Equal(t, 4, repo.reads, "a missing document is read every time")
}
