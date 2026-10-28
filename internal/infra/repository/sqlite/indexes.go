package sqlite

import (
	"fmt"
	"strings"
	"time"

	"github.com/concrnt/concrnt/internal/infra/repository/shapes"
)

func nowMicros() int64 {
	return Micros(time.Now())
}

// Tables maps each logical collection to its table.
var Tables = map[shapes.Collection]string{
	shapes.CommitLogs:   "commit_logs",
	shapes.Records:      "records",
	shapes.RecordKeys:   "record_keys",
	shapes.Associations: "associations",
	shapes.Acks:         "acks",
	shapes.Ackeds:       "ackeds",
	shapes.Entities:     "entities",
	shapes.Servers:      "servers",
}

// Columns maps each logical field to its column, per collection.
var Columns = map[shapes.Collection]map[shapes.Field]string{
	shapes.CommitLogs: {
		shapes.ID: "id", shapes.Owner: "owner", shapes.CDate: "c_date", shapes.GcCandidate: "gc_candidate",
	},
	shapes.Records: {
		shapes.ID: "document_id", shapes.Schema: "schema", shapes.Author: "author", shapes.CreatedAt: "created_at",
	},
	shapes.RecordKeys: {
		shapes.Parent: "parent_id", shapes.Key: "uri", shapes.RecordID: "record_id", shapes.RecordCreatedAt: "record_created_at",
	},
	shapes.Associations: {
		shapes.Target: "target_id", shapes.Schema: "schema", shapes.Variant: "variant", shapes.Author: "author", shapes.CreatedAt: "created_at",
	},
	shapes.Acks: {
		shapes.From: "from", shapes.To: "to", shapes.Schema: "schema", shapes.Valid: "valid", shapes.CreatedAt: "created_at",
	},
	shapes.Ackeds: {
		shapes.From: "from", shapes.To: "to", shapes.Schema: "schema", shapes.Valid: "valid", shapes.CreatedAt: "created_at",
	},
	shapes.Entities: {shapes.Alias: "alias"},
	shapes.Servers:  {shapes.CSID: "cs_id"},
}

// Partials renders each shape predicate as a partial-index WHERE clause.
var Partials = map[shapes.Partial]string{
	shapes.PartialMemberKeys: "parent_id IS NOT NULL AND record_created_at IS NOT NULL",
}

// IndexColumns renders a shape's indexed columns in order. SQLite's default
// BINARY collation already is byte order, so Binary needs nothing.
func IndexColumns(s shapes.Shape) ([]string, error) {
	cols := Columns[s.Collection]
	if cols == nil {
		return nil, fmt.Errorf("shape %s: collection %s has no column map", s.Name, s.Collection)
	}
	out := make([]string, 0, len(s.Fields()))
	for _, f := range s.Fields() {
		col, ok := cols[f]
		if !ok {
			return nil, fmt.Errorf("shape %s: field %s of %s has no column", s.Name, f, s.Collection)
		}
		rendered := `"` + col + `"`
		if f == s.Range && s.RangeDesc {
			rendered += " DESC"
		}
		out = append(out, rendered)
	}
	return out, nil
}

// IndexDDL renders one CREATE INDEX IF NOT EXISTS statement per shape.
func IndexDDL() ([]string, error) {
	ddl := make([]string, 0, len(shapes.All))
	for _, s := range shapes.All {
		table, ok := Tables[s.Collection]
		if !ok {
			return nil, fmt.Errorf("shape %s: collection %s has no table", s.Name, s.Collection)
		}
		cols, err := IndexColumns(s)
		if err != nil {
			return nil, err
		}
		stmt := fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s ON %s (%s)`, s.Name, table, strings.Join(cols, ", "))
		if s.Partial != "" {
			where, ok := Partials[s.Partial]
			if !ok {
				return nil, fmt.Errorf("shape %s: partial %s is not rendered", s.Name, s.Partial)
			}
			stmt += " WHERE " + where
		}
		ddl = append(ddl, stmt)
	}
	return ddl, nil
}
