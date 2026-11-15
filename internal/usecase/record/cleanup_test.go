package record

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/client"
	"github.com/concrnt/concrnt/internal/domain"
)

const cleanupTargetURI = "cckv://example.com/profiles/main/posts/p1"

// cleanupRepo holds one deletable record whose removal leaves two cleanup
// chains behind: t1 -> t2 -> done, and u1 -> done.
func cleanupRepo(t *testing.T) *rangeDeleteRepo {
	t.Helper()
	return &rangeDeleteRepo{
		stored:      map[string]concrnt.SignedDocument{cleanupTargetURI: subtreeRecord(t, cleanupTargetURI)},
		cleanups:    []string{"t1", "u1"},
		cleanupNext: map[string]string{"t1": "t2"},
	}
}

// The physical removal a delete leaves for later goes to the job queue, one
// RecordCleanup job per token the repository handed back; nothing of it runs
// inside the commit. Each job runs one step and enqueues the token of the
// next until the repository reports the chain done.
func TestCommitDeleteSchedulesCleanupJobs(t *testing.T) {
	ctx := context.Background()
	ccid, priv := newIdentity(t)
	cfg := &domain.Config{FQDN: "example.com"}
	repo := cleanupRepo(t)
	delivery := &recordingDeliveryQueue{}
	uc := newRangeDeleteUsecase(ccid, cfg, repo, &denyKeysPolicyService{}, delivery, nil)

	_, err := uc.Commit(ctx, "127.0.0.1", signedDelete(t, ccid, priv, cleanupTargetURI), domain.CommitModeExecute)
	require.NoError(t, err)
	require.Equal(t, []CleanupJob{{Token: "t1"}, {Token: "u1"}}, delivery.cleanupJobs)
	require.Empty(t, repo.cleanupCalls, "with a queue the commit itself must not run any cleanup step")

	// the queue hands the payload back to the registered handler
	run := func(job CleanupJob) {
		t.Helper()
		payload, err := json.Marshal(job)
		require.NoError(t, err)
		require.NoError(t, delivery.handlers[JobTypeRecordCleanup](ctx, payload))
	}

	run(delivery.cleanupJobs[0])
	require.Equal(t, []string{"t1"}, repo.cleanupCalls)
	require.Equal(t, []CleanupJob{{Token: "t1"}, {Token: "u1"}, {Token: "t2"}}, delivery.cleanupJobs, "an unfinished chain enqueues its next step")

	run(delivery.cleanupJobs[2])
	run(delivery.cleanupJobs[1])
	require.Equal(t, []string{"t1", "t2", "u1"}, repo.cleanupCalls)
	require.Len(t, delivery.cleanupJobs, 3, "a finished chain must not enqueue anything")
}

// Without a job queue (offline tooling) there is nobody to hand the tokens
// to: the commit runs every chain to its end itself.
func TestCommitDeleteRunsCleanupInlineWithoutQueue(t *testing.T) {
	ccid, priv := newIdentity(t)
	cfg := &domain.Config{FQDN: "example.com"}
	repo := cleanupRepo(t)
	uc := New(
		withEntities{repo, fixedResidenceRepo{entity: &domain.Entity{ID: ccid, Domain: cfg.FQDN}}},
		nil,
		newTestServerUsecase(cfg),
		cfg,
		client.New(cfg.FQDN),
		nopSignalService{},
		&denyKeysPolicyService{},
		nil,
		nil,
	)

	_, err := uc.Commit(context.Background(), "127.0.0.1", signedDelete(t, ccid, priv, cleanupTargetURI), domain.CommitModeExecute)
	require.NoError(t, err)
	require.Len(t, repo.deletes, 1)
	require.Equal(t, []string{"t1", "t2", "u1"}, repo.cleanupCalls)
}

// A delete the repository answers with no cleanup token schedules nothing.
func TestCommitDeleteWithoutCleanupsSchedulesNothing(t *testing.T) {
	ccid, priv := newIdentity(t)
	cfg := &domain.Config{FQDN: "example.com"}
	repo := cleanupRepo(t)
	repo.cleanups = nil
	delivery := &recordingDeliveryQueue{}
	uc := newRangeDeleteUsecase(ccid, cfg, repo, &denyKeysPolicyService{}, delivery, nil)

	_, err := uc.Commit(context.Background(), "127.0.0.1", signedDelete(t, ccid, priv, cleanupTargetURI), domain.CommitModeExecute)
	require.NoError(t, err)
	require.Len(t, repo.deletes, 1)
	require.Empty(t, delivery.cleanupJobs)
	require.Empty(t, repo.cleanupCalls)
}
