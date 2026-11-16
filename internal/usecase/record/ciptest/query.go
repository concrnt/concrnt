package ciptest

import (
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/internal/usecase/record"
)

func queryParent(parent string) record.QueryParams {
	return record.QueryParams{Parent: parent, Limit: listingLimit, Order: "asc"}
}

func queryPrefix(prefix string) record.QueryParams {
	return record.QueryParams{Prefix: prefix, Limit: listingLimit, Order: "asc"}
}

// queryByKey lists the children of parent in key order (CIP-5 orderby=key).
func queryByKey(parent string) record.QueryParams {
	return record.QueryParams{Parent: parent, OrderBy: "key", Limit: listingLimit, Order: "asc"}
}

func (w *world) query(params record.QueryParams) concrnt.QueryResult {
	w.t.Helper()
	result, err := w.records.Query(w.ctx, params)
	require.NoError(w.t, err)
	return result
}

// queryKeys are the keys of everything params selects, in listing order.
func (w *world) queryKeys(params record.QueryParams) []string {
	w.t.Helper()
	result := w.query(params)
	require.Nil(w.t, result.Next)
	return keysOf(w.t, result.Items)
}

// maxPages stops a listing whose cursor does not advance from hanging the
// suite.
const maxPages = 1000

// collectPages reads a listing to its end the way a client does: each page
// is fetched with the Next cursor of the one before (nil for the first).
// Cursors are inclusive and point at the first row of the next page, so
// the pages line up without a gap or a repeated row as long as the sort
// keys are distinct.
func collectPages(t *testing.T, limit int, fetch func(cursor *string) concrnt.QueryResult) []concrnt.SignedDocument {
	t.Helper()
	var all []concrnt.SignedDocument
	var cursor *string
	for range maxPages {
		page := fetch(cursor)
		require.LessOrEqual(t, len(page.Items), limit, "a page is at most limit rows")
		all = append(all, page.Items...)
		if page.Next == nil {
			return all
		}
		require.Len(t, page.Items, limit, "a page followed by another one is full")
		cursor = page.Next
	}
	t.Fatalf("the listing did not end within %d pages", maxPages)
	return nil
}

// cursorTime decodes a createdAt cursor; nil stays nil.
func cursorTime(t *testing.T, cursor *string) *time.Time {
	t.Helper()
	if cursor == nil {
		return nil
	}
	at, err := time.Parse(time.RFC3339Nano, *cursor)
	require.NoError(t, err, "a createdAt cursor is an RFC 3339 timestamp")
	return &at
}

func requireTimeCursor(t *testing.T, want time.Time, cursor *string, msg string) {
	t.Helper()
	require.NotNil(t, cursor, msg)
	require.True(t, cursorTime(t, cursor).Equal(want), "%s: cursor %s, want %s", msg, *cursor, want.Format(time.RFC3339Nano))
}

// queryPages reads everything params selects, limit rows at a time, moving
// the cursor that matches the ordering: since / until by createdAt,
// sinceKey / untilKey by key.
func (w *world) queryPages(params record.QueryParams, limit int) []concrnt.SignedDocument {
	w.t.Helper()
	return collectPages(w.t, limit, func(cursor *string) concrnt.QueryResult {
		page := params
		page.Limit = limit
		switch {
		case cursor == nil:
		case params.OrderBy == "key" && params.Order == "desc":
			page.UntilKey = cursor
		case params.OrderBy == "key":
			page.SinceKey = cursor
		case params.Order == "desc":
			page.Until = cursorTime(w.t, cursor)
		default:
			page.Since = cursorTime(w.t, cursor)
		}
		return w.query(page)
	})
}

// library is the fixture of the query tests: the children of one parent,
// written in an order that is neither their key order nor their createdAt
// order, with names whose byte order differs from every locale's.
type library struct {
	owner, guest user
	parent       string
	// byTime are the keys of the parent's children holding a record,
	// oldest first; byKey every child in byte order of the key, including
	// folder, which holds no record of its own (only a record below it).
	byTime []string
	byKey  []string
	folder string
	// nested is the record below folder; outside is a record that shares
	// the parent's key as a string prefix without being its child.
	nested  string
	outside string
}

func (w *world) newLibrary() library {
	w.t.Helper()
	alice, bob := w.newUser(), w.newUser()
	parent := alice.key("shelf")
	child := func(name string) string { return parent + "/" + name }
	lib := library{
		owner:   alice,
		guest:   bob,
		parent:  parent,
		byTime:  []string{child("a"), child("a_b"), child("B"), child("a-b")},
		byKey:   []string{child("B"), child("a"), child("a-b"), child("a_b"), child("folder")},
		folder:  child("folder"),
		nested:  child("folder/leaf"),
		outside: alice.key("shelf2"),
	}

	w.execute(alice, recordDoc(alice, child("B"), w.at(30)))
	w.execute(alice, recordDoc(alice, lib.nested, w.at(50)))
	w.execute(alice, recordDoc(alice, child("a"), w.at(10), withSchema(noteSchema)))
	w.execute(alice, recordDoc(alice, lib.outside, w.at(60)))
	w.execute(alice, recordDoc(alice, child("a-b"), w.at(40)))
	// a record in alice's namespace written by somebody else
	w.execute(bob, recordDoc(bob, child("a_b"), w.at(20)))
	return lib
}

func reversed(keys []string) []string {
	out := slices.Clone(keys)
	slices.Reverse(out)
	return out
}

// requirePaged reads params a page at a time in both orders and for every
// page size up to one past the result, and requires want (given in
// ascending order) each time.
func (w *world) requirePaged(params record.QueryParams, want []string, name string) {
	w.t.Helper()
	for _, order := range []string{"asc", "desc"} {
		ordered := want
		if order == "desc" {
			ordered = reversed(want)
		}
		params.Order = order
		for limit := 1; limit <= len(want)+1; limit++ {
			require.Equal(w.t, ordered, keysOf(w.t, w.queryPages(params, limit)), "%s, order %s, %d per page", name, order, limit)
		}
	}
}

// CIP-5 parent query: the direct children of a key that hold a record,
// ordered by createdAt, with filters and inclusive createdAt cursors.
func testQueryByParent(w *world) {
	lib := w.newLibrary()
	w.requirePaged(queryParent(lib.parent), lib.byTime, "children")

	since, until := w.at(20), w.at(30)
	filtered := []struct {
		name   string
		adjust func(*record.QueryParams)
		want   []string
	}{
		{"by schema", func(p *record.QueryParams) { p.Schema = postSchema }, lib.byTime[1:]},
		{"by another schema", func(p *record.QueryParams) { p.Schema = noteSchema }, lib.byTime[:1]},
		{"by author", func(p *record.QueryParams) { p.Author = lib.guest.ccid }, lib.byTime[1:2]},
		{"by schema and author", func(p *record.QueryParams) { p.Schema, p.Author = postSchema, lib.owner.ccid }, lib.byTime[2:]},
		{"since is inclusive", func(p *record.QueryParams) { p.Since = &since }, lib.byTime[1:]},
		{"until is inclusive", func(p *record.QueryParams) { p.Until = &until }, lib.byTime[:3]},
		{"window", func(p *record.QueryParams) { p.Since, p.Until = &since, &until }, lib.byTime[1:3]},
		{"window, newest first", func(p *record.QueryParams) { p.Since, p.Until, p.Order = &since, &until, "desc" }, reversed(lib.byTime[1:3])},
	}
	for _, tc := range filtered {
		params := queryParent(lib.parent)
		tc.adjust(&params)
		require.Equal(w.t, tc.want, w.queryKeys(params), tc.name)
	}

	params := queryParent(lib.parent)
	params.Limit, params.Order = 2, "desc"
	page := w.query(params)
	require.Equal(w.t, reversed(lib.byTime)[:2], keysOf(w.t, page.Items))
	requireTimeCursor(w.t, w.at(20), page.Next, "next is the createdAt of the first row of the next page")
	requireTimeCursor(w.t, w.at(40), page.Prev, "prev is the createdAt of the first row of this page")

	require.Empty(w.t, w.queryKeys(queryParent(lib.nested)), "a leaf has no children")
	require.Equal(w.t, []string{lib.nested}, w.queryKeys(queryParent(lib.folder)), "a key without a record is still a parent")

	for _, item := range w.query(queryParent(lib.parent)).Items {
		require.Equal(w.t, item.Document, w.stored(*item.CCKV), "a listed row carries the document stored under its key")
	}
}

// CIP-5 prefix query: every record whose key starts with the prefix as a
// string, at any depth, ordered by createdAt.
func testQueryByPrefix(w *world) {
	lib := w.newLibrary()
	below := append(slices.Clone(lib.byTime), lib.nested)

	w.requirePaged(queryPrefix(lib.parent+"/"), below, "below the parent")
	w.requirePaged(queryPrefix(lib.parent), append(slices.Clone(below), lib.outside), "the prefix is a string, not a path")
	w.requirePaged(queryPrefix(lib.parent+"/a"), []string{lib.byTime[0], lib.byTime[1], lib.byTime[3]}, "a prefix inside a key segment")
	require.Equal(w.t, []string{lib.parent + "/a-b"}, w.queryKeys(queryPrefix(lib.parent+"/a-")))
	require.Equal(w.t, []string{lib.parent + "/a_b"}, w.queryKeys(queryPrefix(lib.parent+"/a_")), "the prefix is literal: '_' matches itself only")
	require.Empty(w.t, w.queryKeys(queryPrefix(lib.parent+"/b")), "the prefix is case sensitive")

	params := queryPrefix(lib.parent + "/")
	params.Schema, params.Author = postSchema, lib.owner.ccid
	require.Equal(w.t, below[2:], w.queryKeys(params), "by schema and author")

	since, until := w.at(20), w.at(40)
	params = queryPrefix(lib.parent + "/")
	params.Since, params.Until = &since, &until
	require.Equal(w.t, below[1:4], w.queryKeys(params), "an inclusive window")
}

// CIP-5 orderby=key: the children of a key in byte order of their keys,
// intermediate keys included as key-only rows, with inclusive key cursors.
func testQueryByKey(w *world) {
	lib := w.newLibrary()
	w.requirePaged(queryByKey(lib.parent), lib.byKey, "children in key order")

	for _, item := range w.query(queryByKey(lib.parent)).Items {
		if *item.CCKV == lib.folder {
			require.Empty(w.t, item.Document, "an intermediate key is a key-only row")
			continue
		}
		require.Equal(w.t, w.stored(*item.CCKV), item.Document)
	}

	params := queryByKey(lib.parent)
	params.Limit = 2
	page := w.query(params)
	require.Equal(w.t, lib.byKey[:2], keysOf(w.t, page.Items))
	require.NotNil(w.t, page.Next)
	require.Equal(w.t, lib.byKey[2], *page.Next, "next is the key of the first row of the next page")
	require.NotNil(w.t, page.Prev)
	require.Equal(w.t, lib.byKey[0], *page.Prev, "prev is the key of the first row of this page")

	since, until := lib.byKey[1], lib.byKey[3]
	params = queryByKey(lib.parent)
	params.SinceKey, params.UntilKey = &since, &until
	require.Equal(w.t, lib.byKey[1:4], w.queryKeys(params), "the key bounds are inclusive")

	// a filter needs a document to match, so it drops the key-only rows
	params = queryByKey(lib.parent)
	params.Schema = postSchema
	w.requirePaged(params, []string{lib.byKey[0], lib.byKey[2], lib.byKey[3]}, "by schema")
	params = queryByKey(lib.parent)
	params.Author = lib.guest.ccid
	require.Equal(w.t, lib.byKey[3:4], w.queryKeys(params), "by author")

	_, err := w.records.Query(w.ctx, record.QueryParams{Prefix: lib.parent, OrderBy: "key", Limit: 10})
	require.Error(w.t, err, "orderby=key is a parent listing")
}

// The owner root (cckv://<owner>) is a parent like any key: its children
// are the top-level keys.
func testQueryOwnerRoot(w *world) {
	lib := w.newLibrary()
	root := lib.owner.root()

	require.Equal(w.t, []string{lib.outside}, w.queryKeys(queryParent(root)), "only the top-level keys holding a record")
	w.requirePaged(queryByKey(root), []string{lib.parent, lib.outside}, "top-level keys in key order")
	require.Empty(w.t, w.queryKeys(queryParent(lib.guest.root())), "another owner's root")
}
