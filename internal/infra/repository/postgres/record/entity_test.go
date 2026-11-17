package record

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/concrnt/concrnt/internal/testutil"
)

// Unregister flags every commit the departing user owns. Every commit has
// exactly one owner (an ack between two local users is two commits: the ack
// owned by the acker and the acked mirror owned by the target), so the whole
// repository of the departing user is eligible and nobody else's rows are
// touched; commits with no recorded owner are left alone.
func TestMarkCommitLogsGcCandidateByOwner(t *testing.T) {
	db, cleanup := testutil.CreateDB()
	t.Cleanup(cleanup)

	ctx := context.Background()
	repo := New(db)
	require.NoError(t, repo.Migrate())

	userA := "con1qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqq"
	userB := "con1pppppppppppppppppppppppppppppppppppppppp"

	seed := func(id string, owner string) {
		t.Helper()
		require.NoError(t, db.Create(&CommitLog{ID: id, Document: "{}", Proof: "{}", Owner: owner}).Error)
	}
	seed("log-a-ack", userA)
	seed("log-b-mirror", userB)
	seed("log-unowned", "")

	flagged := func() map[string]bool {
		t.Helper()
		var logs []CommitLog
		require.NoError(t, db.Find(&logs).Error)
		out := map[string]bool{}
		for _, log := range logs {
			out[log.ID] = log.GcCandidate
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
