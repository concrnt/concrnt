package record

import (
	"context"
	"maps"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/internal/domain"
	"github.com/concrnt/concrnt/internal/infra/aggregate/storetest"
	"github.com/concrnt/concrnt/internal/usecase/aggregate"
)

// outcomeRepo is recordingRecordRepo with the outcome of ApplyRecord and
// ApplyAck fixed by the test, so what the commit path does with an outcome
// can be told apart from what it read beforehand. It also serves the count
// computations, counting them.
type outcomeRepo struct {
	recordingRecordRepo
	record RecordOutcome
	ack    AckOutcome

	ackCounts map[string]int64
	computes  int
}

func (r *outcomeRepo) ApplyRecord(ctx context.Context, commit RecordCommit) (RecordOutcome, error) {
	r.recordCommits = append(r.recordCommits, commit)
	return r.record, nil
}

func (r *outcomeRepo) ApplyAck(ctx context.Context, commit AckCommit) (AckOutcome, error) {
	r.ackCommits = append(r.ackCommits, commit)
	return r.ack, nil
}

func (r *outcomeRepo) ComputeAssociationSummary(ctx context.Context, targetURI string, maxAuthors int) (AssociationSummary, error) {
	r.computes++
	return AssociationSummary{}, nil
}

func (r *outcomeRepo) ComputeAcknowledgeCounts(ctx context.Context, from, to, schema string) (map[string]int64, error) {
	r.computes++
	return maps.Clone(r.ackCounts), nil
}

// A record written to a key that held none has no associations, so its empty
// counts are stored without computing them. Whether the key was fresh is the
// repository's answer (RecordOutcome.FreshKey), decided inside the write:
// the document the usecase read beforehand (for the policy action) may be
// gone, or have appeared, by then.
func TestRecordCommitSeedsCountsOnFreshKey(t *testing.T) {
	ctx := context.Background()
	ccid, priv := newIdentity(t)
	cfg := &domain.Config{FQDN: "example.com"}
	stored := signedRecord(t, ccid, priv, time.Now().Add(-time.Hour))
	key := concrnt.CCURI{Scheme: "cckv", Owner: ccid, Key: "posts/1"}.String()

	cases := []struct {
		name       string
		storedSD   *concrnt.SignedDocument
		outcome    RecordOutcome
		wantSeeded bool
	}{
		{"fresh key", nil, RecordOutcome{Status: Applied, FreshKey: true}, true},
		{"overwrite", &stored, RecordOutcome{Status: Applied, PreviousID: documentIDOf(t, stored)}, false},
		{"key written between the read and the write", nil, RecordOutcome{Status: Applied, PreviousID: documentIDOf(t, stored)}, false},
		{"key removed between the read and the write", &stored, RecordOutcome{Status: Applied, FreshKey: true}, true},
		{"stale", nil, RecordOutcome{Status: Stale}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			repo := &outcomeRepo{record: tc.outcome}
			repo.storedSD = tc.storedSD
			uc := newRecordCommitUsecase(ccid, cfg, repo, nil)
			uc.SetAggregates(aggregate.New(storetest.NewInMemory(), 0, 0))

			_, err := uc.Commit(ctx, "127.0.0.1", signedRecord(t, ccid, priv, time.Now()), domain.CommitModeExecute)
			require.NoError(t, err)
			require.Len(t, repo.recordCommits, 1)

			entry, seeded, err := uc.agg.Cached(ctx, uc.aggs.associations, key)
			require.NoError(t, err)
			require.Equal(t, tc.wantSeeded, seeded)
			require.Zero(t, repo.computes, "seeding must not compute")
			if seeded {
				require.False(t, associationCountsOf(entry).mayMatch("", "", ""), "the seeded entry is an empty one")
			}
		})
	}
}

// An ack-kind commit adjusts the cached count of its side by what the write
// did to the state's validity (AckOutcome): a flip moves the schema's count
// by one, a state that merely moved to a newer document moves nothing, and
// the entry is never dropped for it. A dump replay invalidates instead.
func TestAckCommitAdjustsCountsOnValidityFlip(t *testing.T) {
	ctx := context.Background()
	cfg := &domain.Config{FQDN: "example.com"}
	// Self-ack keeps author == target owner, sidestepping the blocking-list
	// lookup Commit issues for cross-user operations.
	self := newAckParty(t, cfg.FQDN)

	repo := &outcomeRepo{ackCounts: map[string]int64{followSchema: 2}}
	uc := newAckUsecase(cfg, repo, residenceOf(self), &recordingDeliveryQueue{})
	uc.SetAggregates(aggregate.New(storetest.NewInMemory(), 0, 0))

	counts := func() map[string]int64 {
		t.Helper()
		got, err := uc.GetAcknowledgeRecordCounts(ctx, self.ccid, "", "")
		require.NoError(t, err)
		return got
	}
	age := time.Hour
	commit := func(kind string, outcome AckOutcome, mode domain.CommitMode) {
		t.Helper()
		repo.ack = outcome
		// every document is distinct and within the backdate window
		age -= time.Minute
		_, err := uc.Commit(ctx, "127.0.0.1", signedAck(t, kind, self, self, time.Now().Add(-age)), mode)
		require.NoError(t, err)
	}

	require.Equal(t, map[string]int64{followSchema: 2}, counts())

	commit("ack", AckOutcome{Status: Applied, WasValid: false, IsValid: true}, domain.CommitModeExecute)
	require.Equal(t, map[string]int64{followSchema: 3}, counts(), "invalid -> valid counts one more")

	commit("ack", AckOutcome{Status: Applied, WasValid: true, IsValid: true}, domain.CommitModeExecute)
	require.Equal(t, map[string]int64{followSchema: 3}, counts(), "a newer ack of a valid state changes no count")

	commit("unack", AckOutcome{Status: Applied, WasValid: true, IsValid: false}, domain.CommitModeExecute)
	require.Equal(t, map[string]int64{followSchema: 2}, counts(), "valid -> invalid counts one less")

	commit("unack", AckOutcome{Status: Applied, WasValid: false, IsValid: false}, domain.CommitModeExecute)
	require.Equal(t, map[string]int64{followSchema: 2}, counts(), "an unack of a state that was not valid changes no count")

	commit("ack", AckOutcome{Status: Stale, WasValid: false, IsValid: false}, domain.CommitModeExecute)
	require.Equal(t, map[string]int64{followSchema: 2}, counts(), "a stale ack changes no count")

	require.Equal(t, 1, repo.computes, "the deltas must land on the cached entry, not recompute it")

	commit("ack", AckOutcome{Status: Applied, WasValid: false, IsValid: true}, domain.CommitModeLocalOnlyExecute)
	_, cached, err := uc.agg.Cached(ctx, uc.aggs.acks, ackTarget(self.ccid, ""))
	require.NoError(t, err)
	require.False(t, cached, "a dump replay drops the entry instead of adjusting it")
}

// A deleted association is taken out of its target's cached counts only when
// the delete actually found it (DeleteOutcome.Removed): a replayed delete
// must not count twice. The counter it leaves at zero is no longer reported,
// yet the target is still listed from the primary data.
func TestAssociationRemovalDecrementsOnlyWhenRemoved(t *testing.T) {
	ctx := context.Background()
	const target = "cckv://owner/post/1"
	repo := &associationListRepo{summary: AssociationSummary{
		Variants: []AssociationVariant{{Schema: "like", Count: 1, Earliest: time.Now()}},
		Authors:  []string{"con1liker"},
	}}
	uc := New(repo, nil, nil, &domain.Config{}, nil, nil, nil, nil, nil)
	uc.SetAggregates(aggregate.New(storetest.NewInMemory(), 0, 0))

	counts, err := uc.GetAssociatedRecordCountsBySchema(ctx, target)
	require.NoError(t, err)
	require.Equal(t, map[string]int64{"like": 1}, counts)

	// the association is the second removal of its delete; the action reads
	// the outcome only when it runs, after the write
	outcome := &DeleteOutcome{}
	removed := uc.associationCountsRemoved(target, "like", nil, outcome, 1, domain.CommitModeExecute)

	*outcome = DeleteOutcome{Status: Applied, Removed: []bool{true, false}}
	require.NoError(t, removed(ctx))
	counts, err = uc.GetAssociatedRecordCountsBySchema(ctx, target)
	require.NoError(t, err)
	require.Equal(t, map[string]int64{"like": 1}, counts, "a removal that found nothing must not decrement")

	*outcome = DeleteOutcome{Status: Applied, Removed: []bool{true, true}}
	require.NoError(t, removed(ctx))
	counts, err = uc.GetAssociatedRecordCountsBySchema(ctx, target)
	require.NoError(t, err)
	require.Empty(t, counts, "the removed association is no longer counted")
	require.Equal(t, 1, repo.summaries, "the decrement must land on the cached entry, not recompute it")

	_, err = uc.GetAssociatedRecords(ctx, target, "like", "", "", nil, nil, 10, "desc")
	require.NoError(t, err)
	require.Equal(t, 1, repo.lists, "a counter at zero must not answer a listing with no rows")

	replay := uc.associationCountsRemoved(target, "like", nil, outcome, 1, domain.CommitModeLocalOnlyExecute)
	require.NoError(t, replay(ctx))
	_, cached, err := uc.agg.Cached(ctx, uc.aggs.associations, target)
	require.NoError(t, err)
	require.False(t, cached, "a dump replay drops the entry instead of adjusting it")
}
