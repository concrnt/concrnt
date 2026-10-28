package repotest

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/internal/usecase/record"
)

// RunAckSuite runs the ack / acked contract tests of record.Repository. Ack
// state at the repository layer: the acker's server keeps it as acks
// (anchored to the ack commit), the target's server as ackeds (anchored to
// the acked mirror commit), and /acknowledges serves whichever side this
// server holds. open must return a fresh, isolated backend per call; it is
// called once per top-level test.
func RunAckSuite(t *testing.T, open func(t *testing.T) Backend) {
	t.Run("AckedAcceptIfNewer", func(t *testing.T) { testAckedAcceptIfNewer(t, open(t)) })
	t.Run("GetAcknowledgeRecordsServesHeldSide", func(t *testing.T) { testGetAcknowledgeRecordsServesHeldSide(t, open(t)) })
}

// The target-side acked/unacked state follows the same accept-if-newer
// transitions as the acker-side ack state (CIP-10 §4 / §5.2): the key is the
// document's createdAt alone — only a strictly newer createdAt moves the
// (from, to, schema) state, same-or-older replays are no-ops whatever their
// document id — and the acker-side ack state stays untouched.
func testAckedAcceptIfNewer(t *testing.T, be Backend) {
	ctx := context.Background()
	repo := be.Record
	in := be.Inspect

	from, to := "con1remote", "con1owner"
	schema := "https://schema.example/follow.json"
	associate := "cckv://" + to
	t1 := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	t2 := t1.Add(time.Hour)
	t3 := t1.Add(24 * time.Hour)

	mirror := func(kind string, createdAt time.Time) concrnt.SignedDocument {
		return SignedDocument(t, concrnt.Document[map[string]string]{
			Kind:      kind,
			Value:     map[string]string{"context": "follow"},
			Author:    from,
			Schema:    schema,
			CreatedAt: createdAt,
			Associate: &associate,
		})
	}
	state := func() AckState {
		t.Helper()
		return mustAcked(t, ctx, in, from, to, schema)
	}
	commit := func(id, kind string, createdAt time.Time) {
		t.Helper()
		WithCommit(t, ctx, repo, id, "127.0.0.1", mirror(kind, createdAt), to, func(tx record.RepositoryTx) error {
			transition := repo.Acknowledged
			if kind == "unacked" {
				transition = repo.UnAcknowledged
			}
			applied, err := transition(ctx, tx, id, from, to, schema, createdAt)
			if err != nil {
				return err
			}
			require.True(t, applied, "%s must apply", id)
			return nil
		})
	}
	// attempt runs a transition in a transaction that is always rolled back,
	// so only the reported applied flag matters
	attempt := func(id, kind string, createdAt time.Time) bool {
		t.Helper()
		sd := mirror(kind, createdAt)
		var applied bool
		InTxRollback(t, ctx, repo, func(tx record.RepositoryTx) {
			require.NoError(t, repo.CreateCommitLog(ctx, tx, id, "127.0.0.1", sd.Document, sd.Proof, to))
			transition := repo.Acknowledged
			if kind == "unacked" {
				transition = repo.UnAcknowledged
			}
			var err error
			applied, err = transition(ctx, tx, id, from, to, schema, createdAt)
			require.NoError(t, err)
		})
		return applied
	}

	commit("acked-1-on", "acked", t1)
	row := state()
	require.True(t, row.Valid)
	require.Equal(t, "acked-1-on", row.DocumentID)
	require.True(t, row.CreatedAt.Equal(t1))
	requireCommitOwner(t, ctx, in, "acked-1-on", to)

	// same createdAt is not strictly newer: no-op even with a higher id
	require.False(t, attempt("acked-9-same-time", "unacked", t1), "same createdAt must be a no-op: the transition key is createdAt, not the document id")
	require.Equal(t, "acked-1-on", state().DocumentID)

	commit("acked-2-off", "unacked", t2)
	row = state()
	require.False(t, row.Valid)
	require.Equal(t, "acked-2-off", row.DocumentID)

	// an older acked replayed after the unacked must not resurrect it
	require.False(t, attempt("acked-0-stale", "acked", t1))
	require.Equal(t, "acked-2-off", state().DocumentID)

	// a newer acked wins, and an unacked from before it is a no-op
	commit("acked-3-on", "acked", t3)
	require.False(t, attempt("acked-2x-stale", "unacked", t2))
	row = state()
	require.True(t, row.Valid)
	require.Equal(t, "acked-3-on", row.DocumentID)
	require.True(t, row.CreatedAt.Equal(t3))

	ackCount, err := in.AckCount(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 0, ackCount, "target-side state must not leak into the acker-side ack state")
}

// /acknowledges returns the side of each relationship this server holds
// (CIP-10 §6): a from-filtered query lists the ack commits of a local acker
// (acks), a to-filtered query lists the acked documents received for a local
// target (ackeds). Since every ack — a same-server one included — yields both
// an ack and an acked commit, each side is complete on its own. Exactly one
// of from / to is given (the handler rejects anything else), and the counts
// follow the same split.
func testGetAcknowledgeRecordsServesHeldSide(t *testing.T, be Backend) {
	ctx := context.Background()
	repo := be.Record

	schema := "https://schema.example/follow.json"
	local, localAcker, remoteAcker, remoteTarget := "con1owner", "con1author", "con1remote", "con1elsewhere"
	at := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	doc := func(kind, author, target string) concrnt.SignedDocument {
		associate := "cckv://" + target
		return SignedDocument(t, concrnt.Document[map[string]string]{
			Kind:      kind,
			Value:     map[string]string{"context": "follow"},
			Author:    author,
			Schema:    schema,
			CreatedAt: at,
			Associate: &associate,
		})
	}

	// local acker → local target: this server holds both the ack and the acked
	WithCommit(t, ctx, repo, "ack-local", "127.0.0.1", doc("ack", localAcker, local), localAcker, func(tx record.RepositoryTx) error {
		_, err := repo.Acknowledge(ctx, tx, "ack-local", localAcker, local, schema, at)
		return err
	})
	WithCommit(t, ctx, repo, "acked-local", "127.0.0.1", doc("acked", localAcker, local), local, func(tx record.RepositoryTx) error {
		_, err := repo.Acknowledged(ctx, tx, "acked-local", localAcker, local, schema, at)
		return err
	})
	// local acker → remote target: only the ack is here
	WithCommit(t, ctx, repo, "ack-outbound", "127.0.0.1", doc("ack", localAcker, remoteTarget), localAcker, func(tx record.RepositoryTx) error {
		_, err := repo.Acknowledge(ctx, tx, "ack-outbound", localAcker, remoteTarget, schema, at.Add(time.Hour))
		return err
	})
	// remote acker → local target: only the acked is here
	WithCommit(t, ctx, repo, "acked-inbound", "127.0.0.1", doc("acked", remoteAcker, local), local, func(tx record.RepositoryTx) error {
		_, err := repo.Acknowledged(ctx, tx, "acked-inbound", remoteAcker, local, schema, at.Add(2*time.Hour))
		return err
	})

	ids := func(rows []record.QueryRow) []string {
		t.Helper()
		out := make([]string, len(rows))
		for i, row := range rows {
			parsed, err := row.Row.ParsedDocument()
			require.NoError(t, err)
			associate, err := concrnt.ParseCCURI(*parsed.Associate)
			require.NoError(t, err)
			out[i] = parsed.Kind + ":" + parsed.Author + ">" + associate.Owner
		}
		return out
	}

	rows, err := repo.GetAcknowledgeRecords(ctx, localAcker, "", "", nil, nil, 0, "desc")
	require.NoError(t, err)
	require.Equal(t, []string{"ack:" + localAcker + ">" + remoteTarget, "ack:" + localAcker + ">" + local}, ids(rows), "from-filtered listing is the acker's ack commits, newest first")

	rows, err = repo.GetAcknowledgeRecords(ctx, "", local, "", nil, nil, 0, "desc")
	require.NoError(t, err)
	require.Equal(t, []string{"acked:" + remoteAcker + ">" + local, "acked:" + localAcker + ">" + local}, ids(rows), "to-filtered listing is the target's acked documents, newest first")

	// the until window applies to the held side's createdAt
	rows, err = repo.GetAcknowledgeRecords(ctx, "", local, "", nil, Ptr(at.Add(time.Minute)), 0, "desc")
	require.NoError(t, err)
	require.Equal(t, []string{"acked:" + localAcker + ">" + local}, ids(rows), "until must window the to-side listing")

	_, err = repo.GetAcknowledgeRecords(ctx, "", "", "", nil, nil, 0, "desc")
	require.Error(t, err, "one of from / to is required")

	counts, err := repo.GetAcknowledgeRecordCounts(ctx, localAcker, "", "")
	require.NoError(t, err)
	require.EqualValues(t, 2, counts[schema])

	counts, err = repo.GetAcknowledgeRecordCounts(ctx, "", local, "")
	require.NoError(t, err)
	require.EqualValues(t, 2, counts[schema])
}
