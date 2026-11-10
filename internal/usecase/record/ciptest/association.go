package ciptest

import (
	"slices"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/internal/domain"
	"github.com/concrnt/concrnt/internal/usecase/record"
)

// associationFilter is the filter part of an association listing; the zero
// value lists everything, oldest first.
type associationFilter struct {
	schema, variant, author string
	since, until            *time.Time
	order                   string
}

// listingLimit is larger than any listing of the suite, for the reads that
// are not about pagination.
const listingLimit = 1000

func (w *world) associationPage(target string, filter associationFilter, limit int) concrnt.QueryResult {
	w.t.Helper()
	result, err := w.records.GetAssociatedRecords(w.ctx, target, filter.schema, filter.variant, filter.author, filter.since, filter.until, limit, filter.order)
	require.NoError(w.t, err)
	return result
}

// associations are the documents of the associations on target matching
// filter, in listing order.
func (w *world) associations(target string, filter associationFilter) []string {
	w.t.Helper()
	result := w.associationPage(target, filter, listingLimit)
	require.Nil(w.t, result.Next)
	return documentsOf(result.Items)
}

func (w *world) countsBySchema(target string) map[string]int64 {
	w.t.Helper()
	counts, err := w.records.GetAssociatedRecordCountsBySchema(w.ctx, target)
	require.NoError(w.t, err)
	out := map[string]int64{}
	for schema, n := range counts {
		out[schema] = n
	}
	return out
}

// variantCount is one variant of a schema with its number of associations;
// variant "" is the associations without one.
type variantCount struct {
	variant string
	count   int64
}

// countsByVariant are the variants of schema on target in display order.
func (w *world) countsByVariant(target, schema string) []variantCount {
	w.t.Helper()
	counts, err := w.records.GetAssociatedRecordCountsByVariant(w.ctx, target, schema)
	require.NoError(w.t, err)
	require.NotNil(w.t, counts)
	type ranked struct {
		variantCount
		order int64
	}
	rows := make([]ranked, 0, len(*counts))
	for variant, entry := range *counts {
		rows = append(rows, ranked{variantCount{variant, entry.Value}, entry.Order})
	}
	slices.SortFunc(rows, func(x, y ranked) int { return int(x.order - y.order) })
	out := make([]variantCount, 0, len(rows))
	for _, row := range rows {
		out = append(out, row.variantCount)
	}
	return out
}

// requireAssociationCounts checks what a client reads as the counts of
// target: per schema, the variants in display order (earliest association
// first) and their sum. The read goes through the count cache, which the
// commits before it adjusted by deltas; the cache then has to agree with a
// recount of the primary data.
func (w *world) requireAssociationCounts(target string, want map[string][]variantCount) {
	w.t.Helper()
	bySchema := map[string]int64{}
	for schema, variants := range want {
		for _, v := range variants {
			bySchema[schema] += v.count
		}
	}
	require.Equal(w.t, bySchema, w.countsBySchema(target), "counts by schema of %s", target)
	for schema, variants := range want {
		require.Equal(w.t, variants, w.countsByVariant(target, schema), "counts by variant of %s on %s", schema, target)
	}
	w.requireCacheAgrees(associationCountsOf(target))
}

// requireCacheAgrees compares a count cache entry that a read just used
// with the recount of the primary data.
func (w *world) requireCacheAgrees(ref record.AggregateRef) {
	w.t.Helper()
	verification, err := w.records.VerifyAggregate(w.ctx, ref)
	require.NoError(w.t, err)
	require.False(w.t, verification.Cold, "the read before this left no cache entry for %s", ref.Target)
	require.True(w.t, verification.Equal, "count cache of %s: cached %s, recounted %s", ref.Target, verification.Cached, verification.Computed)
}

// forgetCounts drops the count cache entry, so the next read recounts.
func (w *world) forgetCounts(ref record.AggregateRef) {
	w.t.Helper()
	require.NoError(w.t, w.records.InvalidateAggregate(w.ctx, ref))
}

func associationCountsOf(target string) record.AggregateRef {
	return record.AggregateRef{Name: record.AggregateAssociationCounts, Target: target}
}

// CIP-9: associations are listed on their target with schema, variant,
// author and createdAt filters, in either order, a page at a time.
func testAssociationListing(w *world) {
	alice, bob, carol := w.newUser(), w.newUser(), w.newUser()
	target := alice.key("posts/1")
	w.execute(alice, recordDoc(alice, target, w.at(1)))

	likeBob := w.execute(bob, associationDoc(bob, target, likeSchema, "+1", nil, w.at(10)))
	likeCarol := w.execute(carol, associationDoc(carol, target, likeSchema, "+1", nil, w.at(20)))
	partyBob := w.execute(bob, associationDoc(bob, target, likeSchema, "party", nil, w.at(30)))
	replyBob := w.execute(bob, associationDoc(bob, target, replySchema, "", "first", w.at(40)))
	replyCarol := w.execute(carol, associationDoc(carol, target, replySchema, "", "second", w.at(50)))
	all := []concrnt.SignedDocument{likeBob, likeCarol, partyBob, replyBob, replyCarol}

	since, until := w.at(20), w.at(40)
	filters := []struct {
		name   string
		filter associationFilter
		want   []concrnt.SignedDocument
	}{
		{"everything, oldest first", associationFilter{}, all},
		{"everything, newest first", associationFilter{order: "desc"}, []concrnt.SignedDocument{replyCarol, replyBob, partyBob, likeCarol, likeBob}},
		{"by schema", associationFilter{schema: likeSchema}, []concrnt.SignedDocument{likeBob, likeCarol, partyBob}},
		{"by schema and variant", associationFilter{schema: likeSchema, variant: "party"}, []concrnt.SignedDocument{partyBob}},
		{"by author", associationFilter{author: carol.ccid}, []concrnt.SignedDocument{likeCarol, replyCarol}},
		{"by schema and author", associationFilter{schema: replySchema, author: bob.ccid}, []concrnt.SignedDocument{replyBob}},
		{"since is inclusive", associationFilter{since: &since}, []concrnt.SignedDocument{likeCarol, partyBob, replyBob, replyCarol}},
		{"until is inclusive", associationFilter{until: &until}, []concrnt.SignedDocument{likeBob, likeCarol, partyBob, replyBob}},
		{"window", associationFilter{since: &since, until: &until, order: "desc"}, []concrnt.SignedDocument{replyBob, partyBob, likeCarol}},
		{"an unused schema", associationFilter{schema: noteSchema}, nil},
		{"an author without associations", associationFilter{author: alice.ccid}, nil},
	}
	for _, tc := range filters {
		require.Equal(w.t, documentsOf(tc.want), w.associations(target, tc.filter), tc.name)
	}

	for _, item := range w.associationPage(target, associationFilter{}, listingLimit).Items {
		require.NotNil(w.t, item.CCFS)
		require.Equal(w.t, alice.ccfs(idOf(w.t, item)), *item.CCFS, "an association is addressed in the namespace of its target's owner")
		require.Equal(w.t, item.Document, w.stored(*item.CCFS))
	}

	for _, order := range []string{"asc", "desc"} {
		want := slices.Clone(all)
		if order == "desc" {
			slices.Reverse(want)
		}
		for limit := 1; limit <= len(all)+1; limit++ {
			got := collectPages(w.t, limit, func(cursor *string) concrnt.QueryResult {
				filter := associationFilter{order: order}
				if order == "desc" {
					filter.until = cursorTime(w.t, cursor)
				} else {
					filter.since = cursorTime(w.t, cursor)
				}
				return w.associationPage(target, filter, limit)
			})
			require.Equal(w.t, documentsOf(want), documentsOf(got), "order %s, %d per page", order, limit)
		}
	}

	first := w.associationPage(target, associationFilter{order: "desc"}, 2)
	require.Equal(w.t, documents(replyCarol, replyBob), documentsOf(first.Items))
	requireTimeCursor(w.t, w.at(30), first.Next, "next is the createdAt of the first row of the next page")
	requireTimeCursor(w.t, w.at(50), first.Prev, "prev is the createdAt of the first row of this page")
}

// CIP-9 uniqueness: the same author saying the same thing (schema, variant,
// body) about the same target twice is one association. Anything that
// differs makes another one.
func testAssociationUniqueness(w *world) {
	alice, bob, carol := w.newUser(), w.newUser(), w.newUser()
	target := alice.key("posts/1")
	w.execute(alice, recordDoc(alice, target, w.at(1)))
	w.requireAssociationCounts(target, map[string][]variantCount{})

	like := w.execute(bob, associationDoc(bob, target, likeSchema, "+1", nil, w.at(10)))
	log, events := w.feed(alice.ccid), len(w.signal.channels("associated", target))

	again := w.sign(bob, associationDoc(bob, target, likeSchema, "+1", nil, w.at(11)))
	_, err := w.records.Commit(w.ctx, clientIP, again, domain.CommitModeExecute)
	require.NoError(w.t, err, "repeating an association is not an error")
	require.Equal(w.t, documents(like), w.associations(target, associationFilter{}), "the first one stays, the repetition is not stored")
	require.Equal(w.t, log, w.feed(alice.ccid), "the repetition is not in the commit log")
	require.Len(w.t, w.signal.channels("associated", target), events, "the repetition is not announced")
	w.requireAssociationCounts(target, map[string][]variantCount{likeSchema: {{"+1", 1}}})

	otherVariant := w.execute(bob, associationDoc(bob, target, likeSchema, "party", nil, w.at(20)))
	otherAuthor := w.execute(carol, associationDoc(carol, target, likeSchema, "+1", nil, w.at(30)))
	replyOne := w.execute(bob, associationDoc(bob, target, replySchema, "", "one", w.at(40)))
	replyTwo := w.execute(bob, associationDoc(bob, target, replySchema, "", "two", w.at(50)))

	require.Equal(w.t, documents(like, otherVariant, otherAuthor, replyOne, replyTwo), w.associations(target, associationFilter{}))
	w.requireAssociationCounts(target, map[string][]variantCount{
		likeSchema:  {{"+1", 2}, {"party", 1}},
		replySchema: {{"", 2}},
	})
}

// An association needs its target: without one nothing is stored.
func testAssociationMissingTarget(w *world) {
	alice := w.newUser()
	target := alice.key("nowhere")
	log := w.feed(alice.ccid)

	for _, mode := range []domain.CommitMode{domain.CommitModeExecute, domain.CommitModeLocalOnlyExecute} {
		_, err := w.records.Commit(w.ctx, clientIP, w.sign(alice, associationDoc(alice, target, likeSchema, "", nil, w.at(10))), mode)
		require.Error(w.t, err, "mode %d", mode)
	}
	// the import path has no event to build, so what it reports is the
	// store's own answer
	_, err := w.records.Commit(w.ctx, clientIP, w.sign(alice, associationDoc(alice, target, likeSchema, "", nil, w.at(20))), domain.CommitModeLocalOnlyExecute)
	require.ErrorIs(w.t, err, domain.ErrNotFound)

	require.Equal(w.t, log, w.feed(alice.ccid), "a rejected association is not in the commit log")
	require.Empty(w.t, w.associations(target, associationFilter{}))
	require.Empty(w.t, w.countsBySchema(target))
}

// The counts a client reads follow every write: an association added, an
// association deleted, the target deleted. Each count is read before the
// next write, so the write lands on a cached entry as a delta; the cache
// has to stay equal to a recount all along.
func testAssociationCounts(w *world) {
	alice, bob, carol := w.newUser(), w.newUser(), w.newUser()
	target := alice.key("posts/1")
	w.execute(alice, recordDoc(alice, target, w.at(1)))
	w.requireAssociationCounts(target, map[string][]variantCount{})

	// the display order of the variants is the order of their earliest
	// association, not the order of arrival
	w.execute(bob, associationDoc(bob, target, likeSchema, "+1", nil, w.at(30)))
	w.requireAssociationCounts(target, map[string][]variantCount{likeSchema: {{"+1", 1}}})

	party := w.execute(bob, associationDoc(bob, target, likeSchema, "party", nil, w.at(10)))
	w.requireAssociationCounts(target, map[string][]variantCount{likeSchema: {{"party", 1}, {"+1", 1}}})

	w.execute(carol, associationDoc(carol, target, likeSchema, "+1", nil, w.at(20)))
	w.execute(carol, associationDoc(carol, target, replySchema, "", "hello", w.at(40)))
	afterWrites := map[string][]variantCount{
		likeSchema:  {{"party", 1}, {"+1", 2}},
		replySchema: {{"", 1}},
	}
	w.requireAssociationCounts(target, afterWrites)
	w.forgetCounts(associationCountsOf(target))
	w.requireAssociationCounts(target, afterWrites)

	w.execute(bob, deleteDoc(bob, alice.ccfs(idOf(w.t, party)), w.at(50)))
	afterDelete := map[string][]variantCount{
		likeSchema:  {{"+1", 2}},
		replySchema: {{"", 1}},
	}
	w.requireAssociationCounts(target, afterDelete)
	w.forgetCounts(associationCountsOf(target))
	w.requireAssociationCounts(target, afterDelete)

	w.execute(alice, deleteDoc(alice, target, w.at(60)))
	w.requireAssociationCounts(target, map[string][]variantCount{})
	require.Empty(w.t, w.countsByVariant(target, likeSchema))
	w.drain()
	w.requireAssociationCounts(target, map[string][]variantCount{})
	w.forgetCounts(associationCountsOf(target))
	w.requireAssociationCounts(target, map[string][]variantCount{})
}

// The listing consults the count cache to answer "no associations" without
// a query. That shortcut must never hide a row: not after a sibling in the
// same variant was deleted, not after the variant emptied and filled again.
func testAssociationListingSurvivesSiblingDelete(w *world) {
	alice, bob, carol := w.newUser(), w.newUser(), w.newUser()
	target := alice.key("posts/1")
	w.execute(alice, recordDoc(alice, target, w.at(1)))
	require.Empty(w.t, w.associations(target, associationFilter{}))

	likeBob := w.execute(bob, associationDoc(bob, target, likeSchema, "+1", nil, w.at(10)))
	likeCarol := w.execute(carol, associationDoc(carol, target, likeSchema, "+1", nil, w.at(20)))
	replyBob := w.execute(bob, associationDoc(bob, target, replySchema, "", "hello", w.at(30)))
	require.Equal(w.t, documents(likeBob, likeCarol, replyBob), w.associations(target, associationFilter{}))

	w.execute(bob, deleteDoc(bob, alice.ccfs(idOf(w.t, likeBob)), w.at(40)))
	require.Equal(w.t, documents(likeCarol), w.associations(target, associationFilter{schema: likeSchema, variant: "+1"}))
	require.Equal(w.t, documents(likeCarol), w.associations(target, associationFilter{author: carol.ccid}))
	require.Equal(w.t, documents(replyBob), w.associations(target, associationFilter{author: bob.ccid}))
	require.Equal(w.t, documents(likeCarol, replyBob), w.associations(target, associationFilter{}))
	w.requireAssociationCounts(target, map[string][]variantCount{likeSchema: {{"+1", 1}}, replySchema: {{"", 1}}})

	w.execute(carol, deleteDoc(carol, alice.ccfs(idOf(w.t, likeCarol)), w.at(50)))
	require.Empty(w.t, w.associations(target, associationFilter{schema: likeSchema}))
	require.Equal(w.t, documents(replyBob), w.associations(target, associationFilter{}))
	w.requireAssociationCounts(target, map[string][]variantCount{replySchema: {{"", 1}}})

	// the deleted association no longer stands in the way of saying the
	// same thing again
	likeAgain := w.execute(bob, associationDoc(bob, target, likeSchema, "+1", nil, w.at(60)))
	require.Equal(w.t, documents(likeAgain), w.associations(target, associationFilter{schema: likeSchema, variant: "+1"}))
	require.Equal(w.t, documents(replyBob, likeAgain), w.associations(target, associationFilter{author: bob.ccid}))
	w.requireAssociationCounts(target, map[string][]variantCount{likeSchema: {{"+1", 1}}, replySchema: {{"", 1}}})
}

// C11: a key that is deleted and written again is a new incarnation. What
// was attached to the old one never comes back, in listings or in counts,
// and does not block the same association from being made again. The
// backend's deferred removal of the old incarnation runs after the new one
// exists and must leave it alone.
func testDeleteThenRecreate(w *world) {
	alice, bob, carol := w.newUser(), w.newUser(), w.newUser()
	target := alice.key("posts/1")
	w.execute(alice, recordDoc(alice, target, w.at(10), withValue("first incarnation")))
	w.execute(bob, associationDoc(bob, target, likeSchema, "+1", nil, w.at(20)))
	w.execute(carol, associationDoc(carol, target, replySchema, "", "hello", w.at(21)))
	w.requireAssociationCounts(target, map[string][]variantCount{likeSchema: {{"+1", 1}}, replySchema: {{"", 1}}})

	w.execute(alice, deleteDoc(alice, target, w.at(30)))
	w.requireMissing(target)
	require.Empty(w.t, w.associations(target, associationFilter{}))
	w.requireAssociationCounts(target, map[string][]variantCount{})

	reborn := w.execute(alice, recordDoc(alice, target, w.at(40), withValue("second incarnation")))
	require.Equal(w.t, reborn.Document, w.stored(target))
	require.Empty(w.t, w.associations(target, associationFilter{}), "the associations of the deleted record do not reappear")
	w.requireAssociationCounts(target, map[string][]variantCount{})

	likeAgain := w.execute(bob, associationDoc(bob, target, likeSchema, "+1", nil, w.at(50)))
	requireNewIncarnation := func() {
		w.t.Helper()
		require.Equal(w.t, documents(likeAgain), w.associations(target, associationFilter{}))
		require.Equal(w.t, documents(likeAgain), w.associations(target, associationFilter{schema: likeSchema, variant: "+1", author: bob.ccid}))
		require.Empty(w.t, w.associations(target, associationFilter{schema: replySchema}))
		w.requireAssociationCounts(target, map[string][]variantCount{likeSchema: {{"+1", 1}}})
	}
	requireNewIncarnation()

	w.drain()
	requireNewIncarnation()
	w.forgetCounts(associationCountsOf(target))
	requireNewIncarnation()
}
