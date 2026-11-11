package ciptest

import (
	"github.com/stretchr/testify/require"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/internal/domain"
	"github.com/concrnt/concrnt/internal/usecase/record"
)

// ackList is one side's listing of valid ack states: from lists what the
// acker's server holds, to what the target's server holds.
func (w *world) ackList(from, to string) []concrnt.SignedDocument {
	w.t.Helper()
	result, err := w.records.GetAcknowledgeRecords(w.ctx, from, to, "", nil, nil, listingLimit, "asc")
	require.NoError(w.t, err)
	require.Nil(w.t, result.Next)
	return result.Items
}

func (w *world) ackCounts(from, to, schema string) map[string]int64 {
	w.t.Helper()
	counts, err := w.records.GetAcknowledgeRecordCounts(w.ctx, from, to, schema)
	require.NoError(w.t, err)
	out := map[string]int64{}
	for schema, n := range counts {
		out[schema] = n
	}
	return out
}

// ackCacheRefs are the count cache entries the document sd contributes to.
func ackCacheRefs(w *world, sd concrnt.SignedDocument) []record.AggregateRef {
	w.t.Helper()
	doc, err := sd.ParsedDocument()
	require.NoError(w.t, err)
	refs := record.AggregateTargets(doc)
	require.NotEmpty(w.t, refs)
	return refs
}

// requireAckSide checks one side of the relationship (the listing and the
// counts by from, or by to): held is the document the valid state is
// anchored to, nil when the side holds no valid state. A count is exact,
// and a schema without a valid state is absent rather than zero.
func (w *world) requireAckSide(from, to string, holder user, held *concrnt.SignedDocument, side string) {
	w.t.Helper()
	items := w.ackList(from, to)
	if held == nil {
		require.Empty(w.t, items, "%s side: no valid state", side)
		require.Equal(w.t, map[string]int64{}, w.ackCounts(from, to, ""), "%s side counts", side)
		require.Equal(w.t, map[string]int64{}, w.ackCounts(from, to, followSchema), "%s side counts of the schema", side)
		return
	}
	require.Equal(w.t, documents(*held), documentsOf(items), "%s side: the state is the newest document", side)
	require.NotNil(w.t, items[0].CCFS)
	require.Equal(w.t, holder.ccfs(idOf(w.t, *held)), *items[0].CCFS, "%s side: the state is addressed in its holder's namespace", side)
	require.Equal(w.t, map[string]int64{followSchema: 1}, w.ackCounts(from, to, ""), "%s side counts", side)
	require.Equal(w.t, map[string]int64{followSchema: 1}, w.ackCounts(from, to, followSchema), "%s side counts of the schema", side)
	require.Equal(w.t, map[string]int64{}, w.ackCounts(from, to, likeSchema), "%s side counts of another schema", side)
}

// requireAcked checks both sides of acker's relationship to target: ack is
// the acker's valid state (nil for none), and the target's is the acked
// document derived from it. last is the newest document the acker sent,
// which names the cache entries to compare with a recount.
func (w *world) requireAcked(acker, target user, ack *concrnt.SignedDocument, last concrnt.SignedDocument) {
	w.t.Helper()
	var acked *concrnt.SignedDocument
	if ack != nil {
		derived, err := ack.DeriveAcked()
		require.NoError(w.t, err)
		acked = &derived
	}
	w.requireAckSide(acker.ccid, "", acker, ack, "acker")
	w.requireAckSide("", target.ccid, target, acked, "target")

	derived, err := last.DeriveAcked()
	require.NoError(w.t, err)
	for _, ref := range append(ackCacheRefs(w, last), ackCacheRefs(w, derived)...) {
		w.requireCacheAgrees(ref)
	}
}

// CIP-10: an ack relationship is a state per (acker, target, schema) moved
// by ack and unack documents, the newest createdAt winning. The acker's
// server holds the acker side; the target's server holds the target side,
// moved by the acked / unacked document the acker's server derives and
// delivers. Both live here, so the delivery is a job of this server.
func testAckStateMachine(w *world) {
	alice, bob := w.newUser(), w.newUser()

	send := func(kind string, second int) concrnt.SignedDocument {
		w.t.Helper()
		sd := w.execute(alice, ackDoc(kind, alice, bob, w.at(second)))
		w.drain()
		return sd
	}

	// the counts are read (and so cached) before every write: each write
	// has to adjust them by the right delta
	w.requireAckSide(alice.ccid, "", alice, nil, "acker")
	w.requireAckSide("", bob.ccid, bob, nil, "target")

	follow := send("ack", 10)
	w.requireAcked(alice, bob, &follow, follow)

	w.commit(follow, domain.CommitModeExecute)
	w.drain()
	w.requireAcked(alice, bob, &follow, follow)

	send("unack", 5)
	w.requireAcked(alice, bob, &follow, follow)
	send("unack", 10)
	w.requireAcked(alice, bob, &follow, follow)

	unfollow := send("unack", 20)
	w.requireAcked(alice, bob, nil, unfollow)

	send("ack", 15)
	w.requireAcked(alice, bob, nil, unfollow)
	send("ack", 20)
	w.requireAcked(alice, bob, nil, unfollow)

	again := send("ack", 30)
	w.requireAcked(alice, bob, &again, again)

	// a newer ack on a valid state moves the state without changing a count
	renewed := send("ack", 40)
	w.requireAcked(alice, bob, &renewed, renewed)
	for _, ref := range ackCacheRefs(w, renewed) {
		w.forgetCounts(ref)
	}
	w.requireAcked(alice, bob, &renewed, renewed)

	// and a second unack on an invalid one must not take a count below zero
	gone := send("unack", 50)
	w.requireAcked(alice, bob, nil, gone)
	final := send("unack", 60)
	w.requireAcked(alice, bob, nil, final)

	derived, err := final.DeriveAcked()
	require.NoError(w.t, err)
	for _, ref := range append(ackCacheRefs(w, final), ackCacheRefs(w, derived)...) {
		w.forgetCounts(ref)
	}
	w.requireAcked(alice, bob, nil, final)
}

// CIP-10 §5.2: each side of a relationship is restored from its own
// holder's dump. Importing the ack restores the acker side and ships
// nothing; importing the derived acked document restores the target side.
func testAckImport(w *world) {
	alice, bob := w.newUser(), w.newUser()

	follow := w.sign(alice, ackDoc("ack", alice, bob, w.at(10)))
	w.commit(follow, domain.CommitModeLocalOnlyExecute)
	require.Empty(w.t, w.jobs.pendingTypes(), "an imported ack is not delivered")
	w.requireAckSide(alice.ccid, "", alice, &follow, "acker")
	w.requireAckSide("", bob.ccid, bob, nil, "target")

	followed, err := follow.DeriveAcked()
	require.NoError(w.t, err)
	w.commit(followed, domain.CommitModeLocalOnlyExecute)
	w.requireAckSide(alice.ccid, "", alice, &follow, "acker")
	w.requireAckSide("", bob.ccid, bob, &followed, "target")
	require.Zero(w.t, w.signal.count())
}
