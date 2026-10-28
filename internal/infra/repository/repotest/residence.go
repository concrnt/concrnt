package repotest

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/internal/domain"
	"github.com/concrnt/concrnt/internal/usecase/record"
)

// RunResidenceSuite runs the residence.Repository contract tests. open must
// return a fresh, isolated backend per call; it is called once per top-level
// test.
func RunResidenceSuite(t *testing.T, open func(t *testing.T) Backend) {
	t.Run("UpdateMetaInfo", func(t *testing.T) { testResidenceUpdateMetaInfo(t, open(t)) })
	t.Run("MarkCommitLogsGcCandidateByOwner", func(t *testing.T) { testResidenceMarkCommitLogsGcCandidateByOwner(t, open(t)) })
}

// UpdateMetaInfo must touch only the info field (SaveMeta's upsert would
// clobber inviter) and report NotFound for unregistered ccids.
func testResidenceUpdateMetaInfo(t *testing.T, be Backend) {
	ctx := context.Background()
	repo := be.Residence

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
	// jsonb backends may re-serialize the document; compare as JSON
	require.JSONEq(t, `{"email":"b@example.com"}`, meta.Info)
	require.NotNil(t, meta.Inviter)
	require.Equal(t, inviter, *meta.Inviter)
}

// Unregister flags every commit the departing user owns. Every commit has
// exactly one owner (an ack between two local users is two commits: the ack
// owned by the acker and the acked mirror owned by the target), so the whole
// repository of the departing user is eligible and nobody else's commits are
// touched; commits with no recorded owner are left alone.
func testResidenceMarkCommitLogsGcCandidateByOwner(t *testing.T, be Backend) {
	ctx := context.Background()
	repo := be.Residence
	in := be.Inspect

	userA := "con1qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqq"
	userB := "con1pppppppppppppppppppppppppppppppppppppppp"

	seed := func(id string, owner string) {
		t.Helper()
		InTx(t, ctx, be.Record, func(tx record.RepositoryTx) {
			require.NoError(t, be.Record.CreateCommitLog(ctx, tx, id, "127.0.0.1", "{}", concrnt.Proof{Type: concrnt.ProofTypeNone}, owner))
		})
	}
	seed("log-a-ack", userA)
	seed("log-b-mirror", userB)
	seed("log-unowned", "")

	flagged := func() map[string]bool {
		t.Helper()
		out := map[string]bool{}
		for _, id := range []string{"log-a-ack", "log-b-mirror", "log-unowned"} {
			out[id] = mustCommit(t, ctx, in, id).GcCandidate
		}
		return out
	}

	require.NoError(t, repo.MarkCommitLogsGcCandidateByOwner(ctx, userA))
	want := map[string]bool{"log-a-ack": true, "log-b-mirror": false, "log-unowned": false}
	require.Equal(t, want, flagged())

	// idempotent: a second run changes nothing
	require.NoError(t, repo.MarkCommitLogsGcCandidateByOwner(ctx, userA))
	require.Equal(t, want, flagged())
}
