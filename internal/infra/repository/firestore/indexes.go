package firestore

import (
	"encoding/json"
	"sort"

	"cloud.google.com/go/firestore"
)

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
	colCommits:       {"document", "proof", "ip"},
	colRecords:       {"policies", "distributions", "redirect", "owner", "author", "schema"},
	colRecordKeys:    {"policies", "distributions", "redirect", "owner", "gen", "cleanOnUpdate"},
	colAssociations:  {"owner", "targetKeyID"},
	colEntityMetas:   {"info", "inviter"},
	colServers:       {"wellKnown", "tag", "layer"},
	colSubscriptions: {"subscription", "schemas", "prefixes"},
	colAbuseReports:  {"body", "ip", "reporter", "targetURI"},
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
		if !s.needsCompositeIndex(nil) && !s.needsCompositeIndex(s.Optional) {
			continue
		}
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
			add(s.Collection, append(prefix(""), orderFields()...))
			for _, o := range s.Optional {
				add(s.Collection, append(prefix(o), orderFields()...))
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
