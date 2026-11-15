package postgres

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/concrnt/concrnt/internal/domain"
	"github.com/concrnt/concrnt/internal/testutil"
)

// UpdateMetaInfo must touch only the info column (SaveMeta's upsert would
// clobber inviter) and report NotFound for unregistered ccids.
func TestResidenceUpdateMetaInfo(t *testing.T) {
	db, cleanup := testutil.CreateDB()
	t.Cleanup(cleanup)

	ctx := context.Background()
	repo := &EntityMetaRepository{db: db}

	ccid := "con1qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqq"
	inviter := "con1pppppppppppppppppppppppppppppppppppppppp"

	err := repo.UpdateMetaInfo(ctx, ccid, `{"email":"b@example.com"}`)
	require.True(t, errors.Is(err, domain.ErrNotFound), "expected not found, got %v", err)

	require.NoError(t, repo.SaveMeta(ctx, domain.EntityMeta{
		ID:      ccid,
		Inviter: &inviter,
		Info:    `{"email": "a@example.com"}`,
	}))

	require.NoError(t, repo.UpdateMetaInfo(ctx, ccid, `{"email":"b@example.com"}`))

	meta, err := repo.GetMeta(ctx, ccid)
	require.NoError(t, err)
	require.Equal(t, `{"email": "b@example.com"}`, meta.Info)
	require.NotNil(t, meta.Inviter)
	require.Equal(t, inviter, *meta.Inviter)
}
