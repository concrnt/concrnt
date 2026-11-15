package ciptest

import (
	"slices"

	"github.com/stretchr/testify/require"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/internal/domain"
)

// CIP-16: the replication feed of an owner is every commit applied under
// that owner, once, in the order the server received them, whatever their
// createdAt. A commit that changed nothing (a redelivery, a document that
// lost accept-if-newer or uniqueness) is not in it. The dump is the same
// list.
func testReplicationFeed(w *world) {
	alice, bob := w.newUser(), w.newUser()
	registration := w.feed(alice.ccid)
	require.Len(w.t, registration, 1, "the entity document")
	first, second := alice.key("posts/1"), alice.key("posts/2")

	// received in an order that is not their createdAt order
	late := w.execute(alice, recordDoc(alice, first, w.at(50)))
	early := w.execute(alice, recordDoc(alice, second, w.at(20)))
	like := w.execute(bob, associationDoc(bob, first, likeSchema, "+1", nil, w.at(30)))
	follow := w.execute(alice, ackDoc("ack", alice, bob, w.at(10)))
	w.drain()

	noops := []concrnt.SignedDocument{
		late,
		w.sign(alice, recordDoc(alice, first, w.at(40), withValue("older"))),
		like,
		w.sign(bob, associationDoc(bob, first, likeSchema, "+1", nil, w.at(31))),
		follow,
		w.sign(alice, ackDoc("unack", alice, bob, w.at(5))),
	}
	for _, sd := range noops {
		w.commit(sd, domain.CommitModeExecute)
		w.drain()
	}

	deletion := w.execute(alice, deleteDoc(alice, second, w.at(60)))
	w.drain()

	want := append(slices.Clone(registration), idsOf(w.t, late, early, like, follow, deletion)...)
	require.Equal(w.t, want, w.feed(alice.ccid), "the applied commits in receipt order")
	require.Equal(w.t, want, w.dump(alice), "the dump is the feed")

	for _, order := range []string{"asc", "desc"} {
		ordered := slices.Clone(want)
		if order == "desc" {
			slices.Reverse(ordered)
		}
		for limit := 1; limit <= len(want)+1; limit++ {
			pages := collectPages(w.t, limit, func(cursor *string) concrnt.QueryResult {
				since, until := cursorTime(w.t, cursor), cursorTime(w.t, cursor)
				if order == "desc" {
					since = nil
				} else {
					until = nil
				}
				page, err := w.records.Replicate(w.systemContext(), alice.ccid, since, until, limit, order)
				require.NoError(w.t, err)
				return page
			})
			require.Equal(w.t, ordered, idsOf(w.t, pages...), "order %s, %d per page", order, limit)
		}
	}

	// the target side of the ack is a commit of bob's
	followed, err := follow.DeriveAcked()
	require.NoError(w.t, err)
	bobs := w.feed(bob.ccid)
	require.Len(w.t, bobs, 2, "the entity document and the acked document")
	require.Equal(w.t, idOf(w.t, followed), bobs[1])

	everything := w.feed("")
	require.ElementsMatch(w.t, append(slices.Clone(want), bobs...), everything, "every owner's commits, each once")
	require.Equal(w.t, want, slices.DeleteFunc(slices.Clone(everything), func(id string) bool { return !slices.Contains(want, id) }), "in receipt order")
}
