package repotest

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/internal/usecase/record"
)

// RunAckSuite runs the ack contract tests of record.Repository. Ack state
// at the repository layer: the acker's server keeps the acker side
// (anchored to the ack commit), the target's server the target side
// (anchored to the acked mirror commit), and /acknowledges serves whichever
// side this server holds. open must return a fresh, isolated backend per
// call; it is called once per top-level test.
func RunAckSuite(t *testing.T, open func(t *testing.T) Backend) {
	t.Run("AckerSideAcceptIfNewer", func(t *testing.T) { testAckAcceptIfNewer(t, open(t), record.AckerSide) })
	t.Run("TargetSideAcceptIfNewer", func(t *testing.T) { testAckAcceptIfNewer(t, open(t), record.TargetSide) })
	t.Run("UnackAsFirstState", func(t *testing.T) { testAckUnackAsFirstState(t, open(t)) })
	t.Run("GetAcknowledgeRecordsServesHeldSide", func(t *testing.T) { testGetAcknowledgeRecordsServesHeldSide(t, open(t)) })
}

const followSchema = "https://schema.example/follow.json"

// ackCommit builds the commit of an ack-kind document for one side; the
// kind follows from the side and valid.
func ackCommit(t *testing.T, id string, side record.AckSide, from, to string, valid bool, createdAt time.Time) record.AckCommit {
	t.Helper()
	kind, owner := "ack", from
	switch {
	case side == record.AckerSide && !valid:
		kind = "unack"
	case side == record.TargetSide && valid:
		kind, owner = "acked", to
	case side == record.TargetSide:
		kind, owner = "unacked", to
	}
	associate := "cckv://" + to
	sd := SignedDocument(t, concrnt.Document[map[string]string]{
		Kind:      kind,
		Value:     map[string]string{"id": id},
		Author:    from,
		Schema:    followSchema,
		CreatedAt: createdAt,
		Associate: &associate,
	})
	return record.AckCommit{
		Commit: commitOf(id, sd, owner), Side: side, From: from, To: to, Schema: followSchema, Valid: valid, CreatedAt: createdAt,
	}
}

// The state of a (from, to, schema) relationship follows accept-if-newer
// (CIP-10 §4 / §5.2) on either side: the key is the document's createdAt
// alone — only a strictly newer createdAt moves the state, same-or-older
// replays are Stale whatever their document id and leave nothing — and the
// outcome tells a validity flip from a state that only moved. The other
// side's state stays untouched.
func testAckAcceptIfNewer(t *testing.T, be Backend, side record.AckSide) {
	ctx := context.Background()
	repo := be.Record

	from, to := "con1acker", "con1target"
	owner := from
	listed := func() []record.QueryRow {
		t.Helper()
		rows, err := repo.GetAcknowledgeRecords(ctx, from, "", "", nil, nil, 0, "asc")
		require.NoError(t, err)
		return rows
	}
	otherSide := func() []record.QueryRow {
		t.Helper()
		rows, err := repo.GetAcknowledgeRecords(ctx, "", to, "", nil, nil, 0, "asc")
		require.NoError(t, err)
		return rows
	}
	if side == record.TargetSide {
		owner = to
		listed, otherSide = otherSide, listed
	}
	t1 := time.Date(2026, 4, 5, 6, 7, 8, 0, time.UTC)
	t2 := t1.Add(time.Hour)
	t3 := t1.Add(24 * time.Hour)

	apply := func(id string, valid bool, createdAt time.Time) record.AckOutcome {
		t.Helper()
		outcome, err := repo.ApplyAck(ctx, ackCommit(t, id, side, from, to, valid, createdAt))
		require.NoError(t, err)
		return outcome
	}
	stale := func(id string, valid bool, createdAt time.Time, storedValid bool) {
		t.Helper()
		require.Equal(t, record.AckOutcome{Status: record.Stale, WasValid: storedValid, IsValid: storedValid}, apply(id, valid, createdAt))
		require.False(t, commitExists(t, ctx, repo, id), "a stale write must not leave its commit")
	}

	first := ackCommit(t, "ack-1-on", side, from, to, true, t1)
	outcome, err := repo.ApplyAck(ctx, first)
	require.NoError(t, err)
	require.Equal(t, record.AckOutcome{Status: record.Applied, WasValid: false, IsValid: true}, outcome)

	rows := listed()
	require.Equal(t, []string{"ack-1-on"}, rowIDs(t, rows))
	require.Equal(t, first.Document, rows[0].Row.Document)
	require.Equal(t, ccfsOf(owner, "ack-1-on"), *rows[0].Row.CCFS, "the state's ccfs is composed in the side owner's namespace")
	require.True(t, rows[0].CreatedAt.Equal(t1))
	require.Equal(t, owner, commitEntry(t, ctx, repo, "ack-1-on").Owner)

	// same createdAt is not strictly newer: stale even with a higher id
	stale("ack-9-same-time", false, t1, true)
	require.Equal(t, []string{"ack-1-on"}, rowIDs(t, listed()))

	require.Equal(t, record.AckOutcome{Status: record.Applied, WasValid: true, IsValid: false}, apply("ack-2-off", false, t2))
	require.Empty(t, listed(), "an invalid state is not listed")

	// a replayed ack older than the stored unack must not resurrect it
	stale("ack-0-stale", true, t1, false)
	require.Empty(t, listed())

	// a newer ack wins, and an unack replayed from before it is stale
	require.Equal(t, record.AckOutcome{Status: record.Applied, WasValid: false, IsValid: true}, apply("ack-3-on", true, t3))
	stale("ack-2x-stale", false, t2, true)
	rows = listed()
	require.Equal(t, []string{"ack-3-on"}, rowIDs(t, rows), "the state is anchored to the document that moved it last")
	require.True(t, rows[0].CreatedAt.Equal(t3))

	// a newer document of the same validity moves the state without a flip
	require.Equal(t, record.AckOutcome{Status: record.Applied, WasValid: true, IsValid: true}, apply("ack-4-on", true, t3.Add(time.Hour)))
	require.Equal(t, []string{"ack-4-on"}, rowIDs(t, listed()))

	countFrom, countTo := from, ""
	if side == record.TargetSide {
		countFrom, countTo = "", to
	}
	counts, err := repo.ComputeAcknowledgeCounts(ctx, countFrom, countTo, "")
	require.NoError(t, err)
	require.Equal(t, map[string]int64{followSchema: 1}, counts)

	require.Empty(t, otherSide(), "one side's state must not leak into the other's")
}

// An unack may be the first document a relationship ever sees: the state
// is stored invalid, so an older ack cannot bring it to life afterwards.
func testAckUnackAsFirstState(t *testing.T, be Backend) {
	ctx := context.Background()
	repo := be.Record

	from, to := "con1acker", "con1target"
	t1 := time.Date(2026, 4, 5, 6, 7, 8, 0, time.UTC)
	for _, side := range []record.AckSide{record.AckerSide, record.TargetSide} {
		prefix := map[record.AckSide]string{record.AckerSide: "acker", record.TargetSide: "target"}[side]

		outcome, err := repo.ApplyAck(ctx, ackCommit(t, prefix+"-2-off", side, from, to, false, t1.Add(time.Hour)))
		require.NoError(t, err)
		require.Equal(t, record.AckOutcome{Status: record.Applied}, outcome)

		outcome, err = repo.ApplyAck(ctx, ackCommit(t, prefix+"-1-on", side, from, to, true, t1))
		require.NoError(t, err)
		require.Equal(t, record.AckOutcome{Status: record.Stale}, outcome)
	}

	rows, err := repo.GetAcknowledgeRecords(ctx, from, "", "", nil, nil, 0, "asc")
	require.NoError(t, err)
	require.Empty(t, rows)
	rows, err = repo.GetAcknowledgeRecords(ctx, "", to, "", nil, nil, 0, "asc")
	require.NoError(t, err)
	require.Empty(t, rows)
	counts, err := repo.ComputeAcknowledgeCounts(ctx, from, "", "")
	require.NoError(t, err)
	require.Empty(t, counts)
}

// /acknowledges returns the side of each relationship this server holds
// (CIP-10 §6): a from-filtered query lists the ack commits of a local acker
// (the acker side), a to-filtered query lists the acked documents received
// for a local target (the target side). Since every ack — a same-server one
// included — yields both an ack and an acked commit, each side is complete
// on its own. Exactly one of from / to is given (the handler rejects
// anything else), and the counts follow the same split.
func testGetAcknowledgeRecordsServesHeldSide(t *testing.T, be Backend) {
	ctx := context.Background()
	repo := be.Record

	local, localAcker, remoteAcker, remoteTarget := "con1owner", "con1author", "con1remote", "con1elsewhere"
	at := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	apply := func(id string, side record.AckSide, from, to string, createdAt time.Time) {
		t.Helper()
		outcome, err := repo.ApplyAck(ctx, ackCommit(t, id, side, from, to, true, createdAt))
		require.NoError(t, err)
		require.Equal(t, record.Applied, outcome.Status)
	}

	// local acker → local target: this server holds both the ack and the acked
	apply("ack-local", record.AckerSide, localAcker, local, at)
	apply("acked-local", record.TargetSide, localAcker, local, at)
	// local acker → remote target: only the ack is here
	apply("ack-outbound", record.AckerSide, localAcker, remoteTarget, at.Add(time.Hour))
	// remote acker → local target: only the acked is here
	apply("acked-inbound", record.TargetSide, remoteAcker, local, at.Add(2*time.Hour))

	rows, err := repo.GetAcknowledgeRecords(ctx, localAcker, "", "", nil, nil, 0, "desc")
	require.NoError(t, err)
	require.Equal(t, []string{"ack-outbound", "ack-local"}, rowIDs(t, rows), "from-filtered listing is the acker's ack commits, newest first")

	rows, err = repo.GetAcknowledgeRecords(ctx, "", local, "", nil, nil, 0, "desc")
	require.NoError(t, err)
	require.Equal(t, []string{"acked-inbound", "acked-local"}, rowIDs(t, rows), "to-filtered listing is the target's acked documents, newest first")

	// the window applies to the held side's createdAt
	rows, err = repo.GetAcknowledgeRecords(ctx, "", local, "", nil, ptr(at.Add(time.Minute)), 0, "desc")
	require.NoError(t, err)
	require.Equal(t, []string{"acked-local"}, rowIDs(t, rows), "until must window the to-side listing")

	rows, err = repo.GetAcknowledgeRecords(ctx, localAcker, "", "", ptr(at.Add(time.Minute)), nil, 0, "asc")
	require.NoError(t, err)
	require.Equal(t, []string{"ack-outbound"}, rowIDs(t, rows), "since must window the from-side listing")

	rows, err = repo.GetAcknowledgeRecords(ctx, localAcker, "", "", nil, nil, 1, "asc")
	require.NoError(t, err)
	require.Equal(t, []string{"ack-local"}, rowIDs(t, rows))

	rows, err = repo.GetAcknowledgeRecords(ctx, localAcker, "", "https://schema.example/other.json", nil, nil, 0, "asc")
	require.NoError(t, err)
	require.Empty(t, rows, "the schema filter narrows the listing")

	_, err = repo.GetAcknowledgeRecords(ctx, "", "", "", nil, nil, 0, "desc")
	require.Error(t, err, "one of from / to is required")

	counts, err := repo.ComputeAcknowledgeCounts(ctx, localAcker, "", "")
	require.NoError(t, err)
	require.Equal(t, map[string]int64{followSchema: 2}, counts)

	counts, err = repo.ComputeAcknowledgeCounts(ctx, "", local, "")
	require.NoError(t, err)
	require.Equal(t, map[string]int64{followSchema: 2}, counts)

	counts, err = repo.ComputeAcknowledgeCounts(ctx, "", local, "https://schema.example/other.json")
	require.NoError(t, err)
	require.Empty(t, counts)

	_, err = repo.ComputeAcknowledgeCounts(ctx, "", "", "")
	require.Error(t, err, "one of from / to is required")
}
