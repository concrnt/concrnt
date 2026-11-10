package ciptest

import (
	"github.com/stretchr/testify/require"

	"github.com/concrnt/concrnt/internal/domain"
)

// CIP-3 §3.4: only a strictly newer document replaces the one under a key.
// A document that loses is accepted (the sender did nothing wrong) and
// leaves no trace: not under the key, not by its content address, not in
// the commit log.
func testRecordAcceptIfNewer(w *world) {
	alice := w.newUser()
	key := alice.key("posts/1")
	entity := w.feed(alice.ccid)

	first := w.execute(alice, recordDoc(alice, key, w.at(10), withValue("first")))
	require.Equal(w.t, first.Document, w.stored(key))

	second := w.execute(alice, recordDoc(alice, key, w.at(20), withValue("second")))
	require.Equal(w.t, second.Document, w.stored(key), "a newer document replaces the stored one")

	stale := []struct {
		name string
		doc  document
	}{
		{"equal createdAt", recordDoc(alice, key, w.at(20), withValue("same instant"))},
		{"older createdAt", recordDoc(alice, key, w.at(15), withValue("older"))},
	}
	for _, tc := range stale {
		loser := w.sign(alice, tc.doc)
		_, err := w.records.Commit(w.ctx, clientIP, loser, domain.CommitModeExecute)
		require.NoError(w.t, err, "%s: losing accept-if-newer is not an error", tc.name)
		require.Equal(w.t, second.Document, w.stored(key), "%s: the stored document stays", tc.name)
		w.requireMissing(alice.ccfs(idOf(w.t, loser)))
	}

	want := append(entity, idsOf(w.t, first, second)...)
	require.Equal(w.t, want, w.feed(alice.ccid), "the replication feed has the applied commits only")
	require.Equal(w.t, want, w.dump(alice), "the dump has the applied commits only")
	require.Len(w.t, w.signal.channels("created", key), 2, "one created event per applied version")
}

// A document delivered twice (a retried request, a federation retry) is
// accepted both times and takes effect once.
func testRecordRedelivery(w *world) {
	alice := w.newUser()
	home := alice.key("home")
	w.execute(alice, recordDoc(alice, home, w.at(1)))
	key := alice.key("posts/1")
	before := w.feed(alice.ccid)

	post := w.execute(alice, recordDoc(alice, key, w.at(10), distributedTo(home)))
	events, queued := w.signal.count(), w.jobs.pendingTypes()
	require.Len(w.t, queued, 1, "one delivery to the distribution destination")

	w.commit(post, domain.CommitModeExecute)
	require.Equal(w.t, post.Document, w.stored(key))
	require.Equal(w.t, events, w.signal.count(), "a redelivery publishes nothing")
	require.Equal(w.t, queued, w.jobs.pendingTypes(), "a redelivery delivers nothing")
	require.Equal(w.t, append(before, idOf(w.t, post)), w.feed(alice.ccid), "stored once")
	require.Equal(w.t, w.feed(alice.ccid), w.dump(alice))

	w.drain()
	distributed := w.feed(alice.ccid)
	require.Len(w.t, distributed, len(before)+2, "the post and its reference in the destination")

	w.commit(post, domain.CommitModeExecute)
	require.Empty(w.t, w.jobs.pendingTypes(), "a redelivery after the distribution delivers nothing either")
	require.Equal(w.t, distributed, w.feed(alice.ccid))
	require.Len(w.t, w.queryKeys(queryParent(home)), 1, "the destination lists the post once")
}

// CIP-1 onUpdate, default "forget": the version a newer one replaces is
// not history worth keeping, its commit is handed to garbage collection.
func testOnUpdateForget(w *world) {
	alice := w.newUser()
	key := alice.key("profile")

	first := w.execute(alice, recordDoc(alice, key, w.at(10), withValue("first")))
	require.Empty(w.t, w.gcCandidates(), "a live version is not a gc candidate")

	second := w.execute(alice, recordDoc(alice, key, w.at(20), withValue("second")))
	require.Equal(w.t, idsOf(w.t, first), w.gcCandidates(), "the replaced version is a gc candidate, the live one is not")
	require.Equal(w.t, second.Document, w.stored(key))
}

// CIP-1 onUpdate "retain": the version stays readable by its content
// address after a newer one took the key. It is the replaced version's own
// onUpdate that decides.
func testOnUpdateRetain(w *world) {
	alice := w.newUser()
	key := alice.key("profile")

	first := w.execute(alice, recordDoc(alice, key, w.at(10), withValue("first"), retained()))
	second := w.execute(alice, recordDoc(alice, key, w.at(20), withValue("second")))

	require.Empty(w.t, w.gcCandidates(), "a retained version is kept")
	require.Equal(w.t, first.Document, w.stored(alice.ccfs(idOf(w.t, first))), "a retained version is readable by its ccfs URI")
	require.Equal(w.t, second.Document, w.stored(key))

	third := w.execute(alice, recordDoc(alice, key, w.at(30), withValue("third"), retained()))
	require.Equal(w.t, idsOf(w.t, second), w.gcCandidates(), "the second version did not ask to be retained")
	require.Equal(w.t, first.Document, w.stored(alice.ccfs(idOf(w.t, first))))
	require.Equal(w.t, third.Document, w.stored(key))
}

// A commit log import (LocalOnlyExecute) restores state and nothing else:
// no realtime event, no distribution, no federation.
func testImportDoesNotDeliver(w *world) {
	alice := w.newUser()
	home := alice.key("home")
	key := alice.key("posts/1")
	w.commit(w.sign(alice, recordDoc(alice, home, w.at(1))), domain.CommitModeLocalOnlyExecute)

	post := w.sign(alice, recordDoc(alice, key, w.at(10), distributedTo(home)))
	w.commit(post, domain.CommitModeLocalOnlyExecute)
	like := w.sign(alice, associationDoc(alice, key, likeSchema, "", nil, w.at(20)))
	w.commit(like, domain.CommitModeLocalOnlyExecute)

	require.Equal(w.t, post.Document, w.stored(key))
	require.Equal(w.t, documents(like), w.associations(key, associationFilter{}))
	require.Equal(w.t, map[string]int64{likeSchema: 1}, w.countsBySchema(key))
	require.Zero(w.t, w.signal.count(), "an import publishes no event")
	require.Empty(w.t, w.jobs.pendingTypes(), "an import delivers nothing")
	require.Empty(w.t, w.queryKeys(queryParent(home)), "an import distributes nothing")
}

// A commit the policy denies stores nothing at all.
func testPolicyDenialStoresNothing(w *world) {
	alice := w.newUser()
	open, closed := alice.key("open"), alice.key("closed")
	kept := w.execute(alice, recordDoc(alice, open, w.at(10)))
	before, events := w.feed(alice.ccid), w.signal.count()

	w.policy.deny("record:create", closed)
	_, err := w.records.Commit(w.ctx, clientIP, w.sign(alice, recordDoc(alice, closed, w.at(20))), domain.CommitModeExecute)
	require.ErrorIs(w.t, err, domain.ErrPermissionDenied)
	w.requireMissing(closed)

	w.policy.deny("record:update", open)
	_, err = w.records.Commit(w.ctx, clientIP, w.sign(alice, recordDoc(alice, open, w.at(30), withValue("update"))), domain.CommitModeExecute)
	require.ErrorIs(w.t, err, domain.ErrPermissionDenied)

	w.policy.deny("association:create", open)
	_, err = w.records.Commit(w.ctx, clientIP, w.sign(alice, associationDoc(alice, open, likeSchema, "", nil, w.at(40))), domain.CommitModeExecute)
	require.ErrorIs(w.t, err, domain.ErrPermissionDenied)

	w.policy.deny("record:delete", open)
	_, err = w.records.Commit(w.ctx, clientIP, w.sign(alice, deleteDoc(alice, open, w.at(50))), domain.CommitModeExecute)
	require.ErrorIs(w.t, err, domain.ErrPermissionDenied)

	require.Equal(w.t, kept.Document, w.stored(open))
	require.Empty(w.t, w.associations(open, associationFilter{}))
	require.Equal(w.t, before, w.feed(alice.ccid), "a denied commit is not in the commit log")
	require.Equal(w.t, events, w.signal.count(), "a denied commit publishes nothing")
	require.Empty(w.t, w.jobs.pendingTypes())
}

// CIP-3 §3.4 for entity documents: accept-if-newer per ccid, and what the
// newest one says (here, the home domain) is what the entity resolves to.
// Replaced entity documents are history (CIP-0 §8.4), never garbage.
func testEntityAcceptIfNewer(w *world) {
	alice := w.newUser()
	registration := w.feed(alice.ccid)

	entity, err := w.records.GetEntity(w.ctx, alice.root())
	require.NoError(w.t, err)
	require.Equal(w.t, alice.ccid, entity.ID)
	require.Equal(w.t, localDomain, entity.Domain)

	moved := w.execute(alice, entityDoc(alice, "elsewhere.example", w.at(20)))
	entity, err = w.records.GetEntity(w.ctx, alice.root())
	require.NoError(w.t, err)
	require.Equal(w.t, "elsewhere.example", entity.Domain, "a newer entity document moves the entity")
	require.Equal(w.t, moved.Document, entity.SignedDocument.Document)
	require.Equal(w.t, moved.Document, w.stored(alice.root()))

	stale := []struct {
		name string
		doc  document
	}{
		{"equal createdAt", entityDoc(alice, "third.example", w.at(20))},
		{"older createdAt", entityDoc(alice, localDomain, w.at(10))},
	}
	for _, tc := range stale {
		_, err := w.records.Commit(w.ctx, clientIP, w.sign(alice, tc.doc), domain.CommitModeExecute)
		require.NoError(w.t, err, "%s: losing accept-if-newer is not an error", tc.name)
		entity, err = w.records.GetEntity(w.ctx, alice.root())
		require.NoError(w.t, err)
		require.Equal(w.t, "elsewhere.example", entity.Domain, "%s: the stored entity stays", tc.name)
		require.Equal(w.t, moved.Document, entity.SignedDocument.Document, tc.name)
	}

	require.Equal(w.t, append(registration, idOf(w.t, moved)), w.feed(alice.ccid), "the applied entity documents only, each once")
	require.Empty(w.t, w.gcCandidates(), "a replaced entity document is kept")
}
