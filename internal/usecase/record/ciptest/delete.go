package ciptest

import (
	"encoding/json"
	"fmt"

	"github.com/stretchr/testify/require"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/internal/domain"
	"github.com/concrnt/concrnt/internal/usecase/record"
	"github.com/concrnt/concrnt/schemas"
)

// removedItems is what a timeline advertises as recently removed, for
// readers to drop from the chunks they cached.
func (w *world) removedItems(timeline string) []string {
	w.t.Helper()
	items, err := w.timelines.GetLocalRemovedItems(w.ctx, timeline)
	require.NoError(w.t, err)
	return items
}

// CIP-4: a delete removes the record from every read, announces it, tells
// the timeline it was an item of, and (nothing asked to be retained) hands
// both the record's commit and its own to garbage collection.
func testDeleteSingle(w *world) {
	alice := w.newUser()
	home := alice.key("home")
	key := home + "/1"
	w.execute(alice, recordDoc(alice, home, w.at(1)))
	post := w.execute(alice, recordDoc(alice, key, w.at(10)))
	before := w.feed(alice.ccid)

	deletion := w.execute(alice, deleteDoc(alice, key, w.at(20)))

	requireDeleted := func() {
		w.t.Helper()
		w.requireMissing(key)
		require.Empty(w.t, w.queryKeys(queryParent(home)))
		require.Empty(w.t, w.bodyHrefs(home, w.chunkOf(home, w.at(10))))
		require.Equal(w.t, []string{key}, w.signal.channels("deleted", key), "the deletion is announced on the record's channel")
		require.Equal(w.t, []string{key}, w.removedItems(home), "the timeline advertises the removed item")
		require.ElementsMatch(w.t, idsOf(w.t, post, deletion), w.gcCandidates())
		require.Equal(w.t, append(before, idOf(w.t, deletion)), w.feed(alice.ccid), "the delete is a commit of the owner")
	}
	requireDeleted()
	w.drain()
	requireDeleted()
	require.Equal(w.t, []string{home}, w.queryKeys(queryPrefix(alice.root()+"/")), "the parent stays")
}

// A record that asked to be retained keeps its commit when it is deleted,
// and with it the commit of the delete: a replay of the log needs both.
func testDeleteSingleRetained(w *world) {
	alice := w.newUser()
	key := alice.key("posts/1")
	post := w.execute(alice, recordDoc(alice, key, w.at(10), retained()))
	deletion := w.execute(alice, deleteDoc(alice, key, w.at(20)))
	w.drain()

	w.requireMissing(key)
	require.Empty(w.t, w.gcCandidates(), "neither the retained record nor its delete is garbage")
	log := w.feed(alice.ccid)
	require.Equal(w.t, idsOf(w.t, post, deletion), log[len(log)-2:])
}

// A delete may address a record by its content address; for the version
// the key points at that is the same as deleting the key.
func testDeleteByCCFS(w *world) {
	alice := w.newUser()
	key := alice.key("profile")
	first := w.execute(alice, recordDoc(alice, key, w.at(10), withValue("first")))
	second := w.execute(alice, recordDoc(alice, key, w.at(20), withValue("second")))

	current := alice.ccfs(idOf(w.t, second))
	deletion := w.execute(alice, deleteDoc(alice, current, w.at(30)))
	w.drain()

	w.requireMissing(key)
	require.Equal(w.t, []string{current}, w.signal.channels("deleted", current))
	require.ElementsMatch(w.t, idsOf(w.t, first, second, deletion), w.gcCandidates())
}

// A retained version outlives the key moving on, so it has to be deletable
// on its own, by its content address: the key stays with the version that
// took it over.
func testDeleteRetainedVersion(w *world) {
	alice := w.newUser()
	key := alice.key("profile")
	first := w.execute(alice, recordDoc(alice, key, w.at(10), withValue("first"), retained()))
	second := w.execute(alice, recordDoc(alice, key, w.at(20), withValue("second")))
	old := alice.ccfs(idOf(w.t, first))

	w.execute(alice, deleteDoc(alice, old, w.at(30)))
	w.drain()

	require.Equal(w.t, second.Document, w.stored(key), "the key stays with the current version")
	require.Equal(w.t, []string{old}, w.signal.channels("deleted", old))
	require.Empty(w.t, w.gcCandidates(), "the deleted version was retained, so its commit and the delete's are kept")
}

// Deleting what is not there is "not found", and stores nothing.
func testDeleteMissingTarget(w *world) {
	alice := w.newUser()
	key := alice.key("posts/1")
	w.execute(alice, recordDoc(alice, alice.key("dir/leaf"), w.at(1)))
	w.execute(alice, recordDoc(alice, key, w.at(10)))
	w.execute(alice, deleteDoc(alice, key, w.at(20)))
	w.drain()
	log, events := w.feed(alice.ccid), w.signal.count()

	targets := []struct {
		name   string
		target string
	}{
		{"a key never written", alice.key("nowhere")},
		{"a key already deleted", key},
		{"an intermediate key without a record", alice.key("dir")},
		{"an empty subtree", alice.key("nowhere") + "*"},
		{"the children of a leaf", alice.key("dir/leaf") + "/*"},
	}
	for i, tc := range targets {
		_, err := w.records.Commit(w.ctx, clientIP, w.sign(alice, deleteDoc(alice, tc.target, w.at(30+i))), domain.CommitModeExecute)
		require.ErrorIs(w.t, err, domain.ErrNotFound, tc.name)
	}
	require.Equal(w.t, log, w.feed(alice.ccid), "a delete of nothing is not in the commit log")
	require.Equal(w.t, events, w.signal.count(), "a delete of nothing announces nothing")
	require.Empty(w.t, w.jobs.pendingTypes())
}

// An association is deleted by its content address: it leaves the listing
// of its target, which is told so, and the target itself is untouched.
func testDeleteAssociation(w *world) {
	alice, bob := w.newUser(), w.newUser()
	target := alice.key("posts/1")
	post := w.execute(alice, recordDoc(alice, target, w.at(1)))
	like := w.execute(bob, associationDoc(bob, target, likeSchema, "+1", nil, w.at(10)))
	reply := w.execute(bob, associationDoc(bob, target, replySchema, "", "hello", w.at(20)))
	address := alice.ccfs(idOf(w.t, like))

	deletion := w.execute(bob, deleteDoc(bob, address, w.at(30)))
	w.drain()

	require.Equal(w.t, documents(reply), w.associations(target, associationFilter{}))
	require.Equal(w.t, post.Document, w.stored(target))
	require.Equal(w.t, []string{address}, w.signal.channels("deleted", address))
	require.Equal(w.t, []string{target}, w.signal.channels("unassociated", target), "the target's subscribers learn the association is gone")
	require.ElementsMatch(w.t, idsOf(w.t, like, deletion), w.gcCandidates())
	log := w.feed(alice.ccid)
	require.Equal(w.t, idOf(w.t, deletion), log[len(log)-1], "the delete is a commit of the target's owner")
}

// subtree is the fixture of the range delete tests: a list with nested
// members, next to keys that merely share its name as a string prefix.
type subtree struct {
	owner user
	// base is the key a range delete names; inside are the keys below it,
	// outside the ones a string-prefix match would wrongly take.
	base    string
	inside  []string
	outside []string
	docs    map[string]concrnt.SignedDocument
}

func (w *world) newSubtree() subtree {
	w.t.Helper()
	alice := w.newUser()
	tree := subtree{
		owner:   alice,
		base:    alice.key("list"),
		inside:  []string{alice.key("list/a"), alice.key("list/a/x"), alice.key("list/b")},
		outside: []string{alice.key("lis"), alice.key("list-b"), alice.key("list2"), alice.key("list2/a")},
		docs:    map[string]concrnt.SignedDocument{},
	}
	for i, key := range append(append([]string{tree.base}, tree.inside...), tree.outside...) {
		tree.docs[key] = w.execute(alice, recordDoc(alice, key, w.at(i+1)))
	}
	return tree
}

// requireRangeDeleted checks that exactly removed is gone after deletion,
// each key announced, and its commit and the delete's own handed to gc.
func (w *world) requireRangeDeleted(tree subtree, deletion concrnt.SignedDocument, removed, kept []string) {
	w.t.Helper()
	flagged := idsOf(w.t, deletion)
	for _, key := range removed {
		w.requireMissing(key)
		require.Equal(w.t, []string{key}, w.signal.channels("deleted", key))
		flagged = append(flagged, idOf(w.t, tree.docs[key]))
	}
	for _, key := range kept {
		require.Equal(w.t, tree.docs[key].Document, w.stored(key), "%s is not part of the range", key)
	}
	require.ElementsMatch(w.t, kept, w.queryKeys(queryPrefix(tree.owner.root()+"/")))
	require.ElementsMatch(w.t, flagged, w.gcCandidates())
}

// CIP-4 range delete, "<key>*": the key and everything below it in the
// path hierarchy, never a sibling that only shares the string prefix.
func testRangeDeleteWithSelf(w *world) {
	tree := w.newSubtree()
	deletion := w.execute(tree.owner, deleteDoc(tree.owner, tree.base+"*", w.at(100)))
	w.drain()
	w.requireRangeDeleted(tree, deletion, append([]string{tree.base}, tree.inside...), tree.outside)
}

// CIP-4 range delete, "<key>/*": everything below the key, not the key.
func testRangeDeleteChildrenOnly(w *world) {
	tree := w.newSubtree()
	deletion := w.execute(tree.owner, deleteDoc(tree.owner, tree.base+"/*", w.at(100)))
	w.drain()
	w.requireRangeDeleted(tree, deletion, tree.inside, append([]string{tree.base}, tree.outside...))
}

// largeSubtreeSize is more than one write of a range delete removes (the
// usecase's rangeDeleteSyncLimit, 500), so the delete needs a second page.
const (
	largeSubtreeSize = 520
	rangeDeletePage  = 500
)

// newLargeSubtree writes base, largeSubtreeSize records below it and a
// sibling outside, and returns the keys below base in key order with the
// ids of their documents.
func (w *world) newLargeSubtree(owner user, base string) (keys, ids []string) {
	w.t.Helper()
	w.commit(w.sign(owner, recordDoc(owner, base, w.at(1))), domain.CommitModeLocalOnlyExecute)
	w.commit(w.sign(owner, recordDoc(owner, base+"2", w.at(2))), domain.CommitModeLocalOnlyExecute)
	for i := range largeSubtreeSize {
		key := fmt.Sprintf("%s/%03d", base, i)
		sd := w.sign(owner, recordDoc(owner, key, w.at(10+i)))
		w.commit(sd, domain.CommitModeLocalOnlyExecute)
		keys = append(keys, key)
		ids = append(ids, idOf(w.t, sd))
	}
	return keys, ids
}

// A subtree larger than one write removes: the commit takes the first
// page, in key order, and background jobs take the rest. Until the last
// page is done the delete commit must stay out of garbage collection (a
// later page may meet a retained record, which keeps it).
func testRangeDeletePaged(w *world) {
	alice := w.newUser()
	base := alice.key("bulk")
	keys, ids := w.newLargeSubtree(alice, base)

	deletion := w.execute(alice, deleteDoc(alice, base+"/*", w.at(1000)))

	require.Equal(w.t, keys[rangeDeletePage:], w.queryKeys(queryByKey(base)), "the commit removes the first page of the subtree in key order")
	require.Contains(w.t, w.jobs.pendingTypes(), record.JobTypeRecordRangeDelete, "the rest is a background job")
	require.NotContains(w.t, w.gcCandidates(), idOf(w.t, deletion), "the delete commit is kept while pages are outstanding")

	w.drain()

	require.Empty(w.t, w.queryKeys(queryByKey(base)), "the jobs remove the rest")
	require.Equal(w.t, 1, w.jobs.finishedCount(record.JobTypeRecordRangeDelete))
	require.ElementsMatch(w.t, append(ids, idOf(w.t, deletion)), w.gcCandidates(), "after the last page the delete commit is garbage too")
	require.Len(w.t, w.signal.channels("deleted", ""), largeSubtreeSize, "every removed record is announced once")
	require.ElementsMatch(w.t, []string{base, base + "2"}, w.queryKeys(queryPrefix(alice.root()+"/")))
}

// Offline tooling runs the usecase without a job queue: the whole subtree
// goes with the commit, whatever its size, and whatever removal the
// backend defers is run to its end before the commit returns.
func testRangeDeleteOffline(w *world) {
	alice := w.newUser()
	base := alice.key("bulk")
	_, ids := w.newLargeSubtree(alice, base)
	baseDoc, err := w.records.GetSigned(w.ctx, base)
	require.NoError(w.t, err)

	deletion := w.execute(alice, deleteDoc(alice, base+"*", w.at(1000)))

	require.Equal(w.t, []string{base + "2"}, w.queryKeys(queryPrefix(alice.root()+"/")))
	require.ElementsMatch(w.t, append(ids, idOf(w.t, *baseDoc), idOf(w.t, deletion)), w.gcCandidates())
	require.Len(w.t, w.signal.channels("deleted", ""), largeSubtreeSize+1)
}

// P4: whatever a backend leaves for later when a key is removed (cleanup
// tokens, run by RecordCleanup jobs) is invisible. Every read answers the
// same before and after the jobs ran, and the jobs run to their end.
func testCleanupChangesNothing(w *world) {
	alice, bob := w.newUser(), w.newUser()
	single, base := alice.key("posts/1"), alice.key("list")
	members := []string{base, base + "/a", base + "/b"}
	for i, key := range append([]string{single, alice.key("kept")}, members...) {
		w.execute(alice, recordDoc(alice, key, w.at(i+1)))
		w.execute(bob, associationDoc(bob, key, likeSchema, "+1", nil, w.at(20+i)))
		w.execute(alice, associationDoc(alice, key, replySchema, "", "hello", w.at(40+i)))
	}
	w.drain()

	w.execute(alice, deleteDoc(alice, single, w.at(100)))
	w.execute(alice, deleteDoc(alice, base+"*", w.at(101)))

	type observation struct {
		Keys         []string
		Associations map[string][]string
		Counts       map[string]map[string]int64
		Log          []string
		Garbage      []string
	}
	observe := func() observation {
		w.t.Helper()
		seen := observation{
			Keys:         w.queryKeys(queryPrefix(alice.root() + "/")),
			Associations: map[string][]string{},
			Counts:       map[string]map[string]int64{},
			Log:          w.feed(""),
			Garbage:      w.gcCandidates(),
		}
		for _, key := range append([]string{single, alice.key("kept")}, members...) {
			seen.Associations[key] = w.associations(key, associationFilter{})
			w.forgetCounts(associationCountsOf(key))
			seen.Counts[key] = w.countsBySchema(key)
		}
		return seen
	}

	before := observe()
	require.Equal(w.t, []string{alice.key("kept")}, before.Keys)
	for _, key := range append([]string{single}, members...) {
		require.Empty(w.t, before.Associations[key], "the associations of %s went with it", key)
		require.Empty(w.t, before.Counts[key])
	}
	require.Len(w.t, before.Associations[alice.key("kept")], 2)

	cleanups := 0
	for _, jobType := range w.jobs.pendingTypes() {
		if jobType == record.JobTypeRecordCleanup {
			cleanups++
		}
	}
	w.drain()
	require.GreaterOrEqual(w.t, w.jobs.finishedCount(record.JobTypeRecordCleanup), cleanups, "every cleanup chain ran to its end")
	require.Equal(w.t, before, observe(), "the deferred removal changed something a read can see")
}

// CIP-7: a record distributed to a timeline is listed there through a
// reference record. Deleting the record takes its references with it: the
// timeline no longer lists it, in the query or in the chunkline body.
func testReferenceSweep(w *world) {
	alice := w.newUser()
	home := alice.key("home")
	key := alice.key("posts/1")
	w.execute(alice, recordDoc(alice, home, w.at(1)))

	first := w.execute(alice, recordDoc(alice, key, w.at(10), withValue("first"), distributedTo(home)))
	w.drain()
	listed := w.query(queryParent(home)).Items
	require.Len(w.t, listed, 1, "the timeline lists the distributed record")
	firstReference := listed[0]
	var reference concrnt.Document[schemas.Reference]
	require.NoError(w.t, json.Unmarshal([]byte(firstReference.Document), &reference))
	require.Equal(w.t, schemas.ReferenceURL, reference.Schema)
	require.Equal(w.t, key, reference.Value.Href)
	require.Equal(w.t, []string{key}, w.bodyHrefs(home, w.chunkOf(home, w.at(10))))

	// the reference is listed as what it points at
	bySchema := queryParent(home)
	bySchema.Schema = postSchema
	require.Equal(w.t, keysOf(w.t, listed), w.queryKeys(bySchema))

	// an update of the record replaces its reference instead of adding one
	second := w.execute(alice, recordDoc(alice, key, w.at(15), withValue("second"), distributedTo(home)))
	w.drain()
	listed = w.query(queryParent(home)).Items
	require.Len(w.t, listed, 1, "the updated record is still listed once")
	secondReference := listed[0]
	require.Equal(w.t, *firstReference.CCKV, *secondReference.CCKV)
	require.Equal(w.t, []string{key}, w.bodyHrefs(home, w.chunkOf(home, w.at(15))))

	deletion := w.execute(alice, deleteDoc(alice, key, w.at(20)))
	w.drain()

	w.requireMissing(key)
	w.requireMissing(*secondReference.CCKV)
	require.Empty(w.t, w.queryKeys(queryParent(home)), "the reference went with the record")
	require.Empty(w.t, w.bodyHrefs(home, w.chunkOf(home, w.at(15))), "the timeline body no longer lists the record")
	require.Equal(w.t, []string{key}, w.removedItems(home), "the timeline advertises the removed item by its href")
	require.ElementsMatch(w.t, []string{key, home}, w.signal.channels("deleted", key), "the deletion is announced where the record was distributed")
	require.ElementsMatch(w.t, idsOf(w.t, first, firstReference, second, secondReference, deletion), w.gcCandidates())
	require.Equal(w.t, []string{home}, w.queryKeys(queryPrefix(alice.root()+"/")), "the timeline itself stays")
}

// CIP-7 for associations: one distributed to a timeline (a notification)
// is listed there by its content address, and leaves with it.
func testReferenceSweepAssociation(w *world) {
	alice, bob := w.newUser(), w.newUser()
	target, inbox := alice.key("posts/1"), alice.key("notifications")
	w.execute(alice, recordDoc(alice, target, w.at(1)))
	w.execute(alice, recordDoc(alice, inbox, w.at(2)))

	doc := associationDoc(bob, target, likeSchema, "+1", nil, w.at(10))
	doc.Distributes = &[]string{inbox}
	like := w.execute(bob, doc)
	w.drain()
	address := alice.ccfs(idOf(w.t, like))

	listed := w.query(queryParent(inbox)).Items
	require.Len(w.t, listed, 1, "the timeline lists the distributed association")
	require.Equal(w.t, []string{address}, w.bodyHrefs(inbox, w.chunkOf(inbox, w.at(10))))

	deletion := w.execute(bob, deleteDoc(bob, address, w.at(20)))
	w.drain()

	require.Empty(w.t, w.associations(target, associationFilter{}))
	require.Empty(w.t, w.queryKeys(queryParent(inbox)), "the reference went with the association")
	require.Empty(w.t, w.bodyHrefs(inbox, w.chunkOf(inbox, w.at(10))))
	require.Equal(w.t, []string{address}, w.removedItems(inbox))
	require.ElementsMatch(w.t, idsOf(w.t, like, listed[0], deletion), w.gcCandidates())
}
