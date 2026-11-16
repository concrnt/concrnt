package ciptest

import (
	"github.com/stretchr/testify/require"

	"github.com/concrnt/concrnt/internal/domain"
	"github.com/concrnt/concrnt/internal/usecase/record"
)

// The realtime fan-out of an association is part of the protocol a client
// sees: a subscriber of a channel reloads the target's associations and
// counts on every "associated" event, so an event too many is three reads
// too many per subscriber, and an event too few is a stale counter. These
// tests pin the number of events per commit, not just their presence.

// eventsOf lists what one publication per channel looks like in a tally.
func eventsOf(eventType string, channels ...string) map[string]int {
	want := map[string]int{}
	for _, channel := range channels {
		want[eventType+" "+channel]++
	}
	return want
}

// timelinePost commits a post of author distributed to count timelines of
// its own and returns the post's key and the timelines.
func (w *world) timelinePost(author user, count int) (string, []string) {
	w.t.Helper()
	timelines := make([]string, 0, count)
	for i := range count {
		timeline := author.key("timelines/" + string(rune('a'+i)))
		w.execute(author, recordDoc(author, timeline, w.at(1)))
		timelines = append(timelines, timeline)
	}
	target := author.key("posts/1")
	w.execute(author, recordDoc(author, target, w.at(2), distributedTo(timelines...)))
	w.drain()
	return target, timelines
}

// One association is announced exactly once on every channel it concerns:
// its target and each timeline the target is distributed to. Nothing else
// is published and nothing is left to a background job that could announce
// it again.
func testAssociationEvents(w *world) {
	alice, bob := w.newUser(), w.newUser()

	for _, distributions := range []int{0, 1, 3} {
		author := w.newUser()
		target, timelines := w.timelinePost(author, distributions)
		mark := w.signal.count()

		w.execute(bob, associationDoc(bob, target, likeSchema, "+1", nil, w.at(10)))
		w.drain()
		require.Equal(w.t, eventsOf("associated", append([]string{target}, timelines...)...), w.signal.tally(mark),
			"%d distributions: one associated event on the target and on each of its timelines", distributions)

		// a second association on the same target is a second round of
		// events, again one per channel
		mark = w.signal.count()
		w.execute(alice, associationDoc(alice, target, likeSchema, "+1", nil, w.at(11)))
		w.drain()
		require.Equal(w.t, eventsOf("associated", append([]string{target}, timelines...)...), w.signal.tally(mark))
	}
}

// An association that is itself distributed (a like sent to the author's
// activity timeline and the target owner's notifications) adds one
// "created" event per destination, for the reference stored there; the
// "associated" events stay one per channel of the target.
func testAssociationDistributionEvents(w *world) {
	alice, bob := w.newUser(), w.newUser()
	target, timelines := w.timelinePost(alice, 2)
	activity, notify := bob.key("activity"), alice.key("notify")
	w.execute(bob, recordDoc(bob, activity, w.at(3)))
	w.execute(alice, recordDoc(alice, notify, w.at(3)))
	mark := w.signal.count()

	doc := associationDoc(bob, target, likeSchema, "+1", nil, w.at(10))
	doc.Distributes = &[]string{activity, notify}
	w.execute(bob, doc)
	w.drain()

	want := eventsOf("associated", append([]string{target}, timelines...)...)
	for _, destination := range []string{activity, notify} {
		references := w.queryKeys(queryParent(destination))
		require.Len(w.t, references, 1, "one reference per destination")
		want["created "+references[0]] = 1
	}
	require.Equal(w.t, want, w.signal.tally(mark))
}

// A commit that changes nothing announces nothing: not the redelivery of an
// association already stored, not the repetition CIP-9 uniqueness folds
// into the first one, not a version of the target that loses
// accept-if-newer. The delivery jobs of the first commit are not produced
// again either.
func testAssociationEventsNotApplied(w *world) {
	alice, bob := w.newUser(), w.newUser()
	target, timelines := w.timelinePost(alice, 2)
	notify := alice.key("notify")
	w.execute(alice, recordDoc(alice, notify, w.at(3)))

	doc := associationDoc(bob, target, likeSchema, "+1", nil, w.at(10))
	doc.Distributes = &[]string{notify}
	like := w.execute(bob, doc)
	w.drain()
	mark, delivered := w.signal.count(), w.jobs.finishedCount(record.JobTypeRecordDelivery)

	// the same signed document again: a retried request or federation retry
	for range 3 {
		w.commit(like, domain.CommitModeExecute)
		w.drain()
	}
	require.Empty(w.t, w.signal.tally(mark), "a redelivered association is not announced again")

	// the same statement in a new document
	repeated := associationDoc(bob, target, likeSchema, "+1", nil, w.at(20))
	repeated.Distributes = &[]string{notify}
	w.execute(bob, repeated)
	w.drain()
	require.Empty(w.t, w.signal.tally(mark), "a repeated association is not announced")

	// an older version of the target does not replace it
	w.execute(alice, recordDoc(alice, target, w.at(1), distributedTo(timelines...)))
	w.drain()
	require.Empty(w.t, w.signal.tally(mark), "a stale commit is not announced")

	require.Equal(w.t, delivered, w.jobs.finishedCount(record.JobTypeRecordDelivery), "nothing was delivered for commits that changed nothing")
	require.Len(w.t, w.queryKeys(queryParent(notify)), 1, "the association is distributed once")
}

// Deleting an association takes back what its creation announced: one
// "unassociated" event on the target and on each timeline the target is
// distributed to, and one "deleted" event for the association itself. A
// redelivered delete announces nothing.
func testAssociationDeletedEvents(w *world) {
	alice, bob := w.newUser(), w.newUser()
	target, timelines := w.timelinePost(alice, 3)
	like := w.execute(bob, associationDoc(bob, target, likeSchema, "+1", nil, w.at(10)))
	w.drain()
	address := alice.ccfs(idOf(w.t, like))
	mark := w.signal.count()

	deletion := w.execute(bob, deleteDoc(bob, address, w.at(20)))
	w.drain()

	want := eventsOf("unassociated", append([]string{target}, timelines...)...)
	want["deleted "+address] = 1
	require.Equal(w.t, want, w.signal.tally(mark))

	mark = w.signal.count()
	w.commit(deletion, domain.CommitModeExecute)
	w.drain()
	require.Empty(w.t, w.signal.tally(mark), "a redelivered delete is not announced again")
}
