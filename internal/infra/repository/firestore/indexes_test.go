package firestore

import (
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"cloud.google.com/go/firestore"
	"github.com/stretchr/testify/require"
)

var updateIndexes = flag.Bool("update-indexes", false, "rewrite deploy/firestore/firestore.indexes.json from the query shapes")

const indexesPath = "../../../../deploy/firestore/firestore.indexes.json"

// TestIndexesFileMatchesShapes keeps the deployable index file in lock-step
// with the queries: the emulator does not enforce composite indexes, so a
// query shape without one would only fail in production.
func TestIndexesFileMatchesShapes(t *testing.T) {
	want := requiredIndexFile()
	wantJSON, err := json.MarshalIndent(want, "", "  ")
	require.NoError(t, err)
	wantJSON = append(wantJSON, '\n')

	if *updateIndexes {
		require.NoError(t, os.MkdirAll(filepath.Dir(indexesPath), 0o755))
		require.NoError(t, os.WriteFile(indexesPath, wantJSON, 0o644))
	}

	got, err := os.ReadFile(indexesPath)
	require.NoError(t, err, "run: go test ./internal/infra/repository/firestore -run TestIndexesFileMatchesShapes -args -update-indexes")
	require.JSONEq(t, string(wantJSON), string(got), "deploy/firestore/firestore.indexes.json is stale; regenerate with -args -update-indexes")
}

// TestShapesDoNotUseExemptFields guards against querying a field whose
// single-field index was disabled.
func TestShapesDoNotUseExemptFields(t *testing.T) {
	for _, s := range shapes {
		exempt := map[string]bool{}
		for _, f := range exemptFields[s.Collection] {
			exempt[f] = true
		}
		fields := append(append([]string{}, s.Equalities...), s.Optional...)
		fields = append(fields, s.ArrayContains, s.Range)
		for _, o := range s.OrderBy {
			fields = append(fields, o.Field)
		}
		for _, f := range fields {
			require.False(t, exempt[f], "shape %s queries exempt field %s", s.Name, f)
		}
	}
}

// needsCompositeIndex reports whether a shape (with a given set of optional
// filters present) is beyond what automatic single-field indexes serve: an
// array-contains with an ordering, an equality with an ordering on another
// field, or an ordering over two fields.
func (s queryShape) needsCompositeIndex(optional []string) bool {
	if s.ArrayContains != "" && len(s.OrderBy) > 0 {
		return true
	}
	fields := 0
	for _, o := range s.OrderBy {
		if o.Field != firestore.DocumentID {
			fields++
		}
	}
	if fields == 0 {
		return false
	}
	if len(s.Equalities)+len(optional) == 0 {
		return fields > 1
	}
	return true
}

// indexFile mirrors the Firebase CLI's firestore.indexes.json.
type indexFile struct {
	Indexes        []compositeIndex `json:"indexes"`
	FieldOverrides []fieldOverride  `json:"fieldOverrides"`
}

type compositeIndex struct {
	CollectionGroup string       `json:"collectionGroup"`
	QueryScope      string       `json:"queryScope"`
	Fields          []indexField `json:"fields"`
}

type indexField struct {
	FieldPath   string `json:"fieldPath"`
	Order       string `json:"order,omitempty"`
	ArrayConfig string `json:"arrayConfig,omitempty"`
}

type fieldOverride struct {
	CollectionGroup string  `json:"collectionGroup"`
	FieldPath       string  `json:"fieldPath"`
	Indexes         []index `json:"indexes"`
}

type index struct {
	Order       string `json:"order,omitempty"`
	ArrayConfig string `json:"arrayConfig,omitempty"`
	QueryScope  string `json:"queryScope,omitempty"`
}

// exemptFields are large or never-queried fields whose automatic single-field
// indexes are disabled: they would only add index writes and storage.
var exemptFields = map[string][]string{
	ColCommits:          {"document", "proof", "ip"},
	ColRecords:          {"policies", "distributions", "redirect", "owner", "author", "schema"},
	ColRecordKeys:       {"policies", "distributions", "redirect", "owner", "cleanOnUpdate"},
	ColAssociations:     {"owner"},
	ColEntityMetas:      {"info", "inviter"},
	ColServers:          {"wellKnown", "tag", "layer"},
	ColSubscriptions:    {"subscription", "schemas", "prefixes"},
	ColAbuseReports:     {"body", "ip", "reporter", "targetURI"},
	ColAggregateEntries: {"value", "asOf", "deltas"},
}

// requiredIndexes derives the composite indexes the registered shapes need.
// A shape with optional equality filters gets one index per optional filter
// on top of the mandatory ones; Firestore merges those at query time for any
// combination. __name__ is implicit in every composite index and is omitted.
func requiredIndexes() []compositeIndex {
	seen := map[string]bool{}
	var out []compositeIndex
	add := func(collection string, fields []indexField) {
		raw, _ := json.Marshal(fields)
		key := collection + string(raw)
		if seen[key] {
			return
		}
		seen[key] = true
		out = append(out, compositeIndex{CollectionGroup: collection, QueryScope: "COLLECTION", Fields: fields})
	}

	for _, s := range shapes {
		dirs := []bool{false}
		if s.BothDirections {
			dirs = []bool{false, true}
		}
		for _, desc := range dirs {
			orderFields := func() []indexField {
				var fs []indexField
				for _, o := range s.OrderBy {
					if o.Field == firestore.DocumentID {
						continue
					}
					wantDesc := o.Desc
					if s.BothDirections {
						wantDesc = desc
					}
					order := "ASCENDING"
					if wantDesc {
						order = "DESCENDING"
					}
					fs = append(fs, indexField{FieldPath: o.Field, Order: order})
				}
				return fs
			}
			prefix := func(extra string) []indexField {
				var fs []indexField
				if s.ArrayContains != "" {
					fs = append(fs, indexField{FieldPath: s.ArrayContains, ArrayConfig: "CONTAINS"})
				}
				for _, e := range s.Equalities {
					fs = append(fs, indexField{FieldPath: e, Order: "ASCENDING"})
				}
				if extra != "" {
					fs = append(fs, indexField{FieldPath: extra, Order: "ASCENDING"})
				}
				return fs
			}
			if s.needsCompositeIndex(nil) {
				add(s.Collection, append(prefix(""), orderFields()...))
			}
			for _, o := range s.Optional {
				if s.needsCompositeIndex([]string{o}) {
					add(s.Collection, append(prefix(o), orderFields()...))
				}
			}
		}
	}
	return out
}

// requiredIndexFile is the full indexes.json content derived from the shapes
// and exemptions.
func requiredIndexFile() indexFile {
	f := indexFile{Indexes: requiredIndexes(), FieldOverrides: []fieldOverride{}}
	collections := make([]string, 0, len(exemptFields))
	for c := range exemptFields {
		collections = append(collections, c)
	}
	sort.Strings(collections)
	for _, c := range collections {
		for _, field := range exemptFields[c] {
			f.FieldOverrides = append(f.FieldOverrides, fieldOverride{CollectionGroup: c, FieldPath: field, Indexes: []index{}})
		}
	}
	return f
}
