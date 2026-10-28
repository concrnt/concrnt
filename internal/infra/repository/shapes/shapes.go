// Package shapes is the registry of query shapes the repository contract
// needs (C5): each entry is one "equality filters + at most one range +
// ordering" access path over one collection, named by logical field so it
// reads the same for every backend. A backend maps the logical names to its
// own columns, derives its index definitions from the registry, and has a
// test that proves every shape is covered; adding a shape without teaching
// a backend its fields fails that backend's test.
package shapes

// Collection is a logical collection of the record store.
type Collection string

const (
	CommitLogs   Collection = "commitLogs"
	Records      Collection = "records"
	RecordKeys   Collection = "recordKeys"
	Associations Collection = "associations"
	Acks         Collection = "acks"
	Ackeds       Collection = "ackeds"
	Entities     Collection = "entities"
	Servers      Collection = "servers"
)

// Field is a logical field name; a backend maps it per collection.
type Field string

const (
	ID              Field = "id"
	Owner           Field = "owner"
	CDate           Field = "cDate"
	GcCandidate     Field = "gcCandidate"
	Parent          Field = "parent"
	Key             Field = "key"
	RecordID        Field = "recordID"
	RecordCreatedAt Field = "recordCreatedAt"
	Schema          Field = "schema"
	Author          Field = "author"
	CreatedAt       Field = "createdAt"
	Target          Field = "target"
	Variant         Field = "variant"
	From            Field = "from"
	To              Field = "to"
	Valid           Field = "valid"
	CSID            Field = "csid"
	Alias           Field = "alias"
)

// Partial names a row predicate a shape is restricted to; a backend renders
// it as a partial index when it supports one.
type Partial string

// PartialMemberKeys restricts a record-key shape to keys that hang under a
// parent and hold a record (placeholder keys and top-level keys are out).
const PartialMemberKeys Partial = "memberKeys"

// Shape is one access path. The index that serves it is Equal columns in
// order, then Range (in its direction), then Trailing tie-break columns.
type Shape struct {
	// Name is the index name on every backend; keep it stable, existing
	// databases hold indexes under it.
	Name       string
	Collection Collection
	// Equal are the equality-filtered fields.
	Equal []Field
	// Range is the single range / ordering field, "" when the shape only
	// has equality filters.
	Range     Field
	RangeDesc bool
	// Trailing are tie-break (or covering) fields after Range.
	Trailing []Field
	Partial  Partial
	// Binary marks a key-ordered shape: the order is byte order of the UTF-8
	// key (CONTRACT.md "Key order"), so a backend whose default collation is
	// not byte order orders by an explicit binary collation.
	Binary bool
	// UsedBy lists the contract methods the shape serves (documentation).
	UsedBy []string
}

// Fields returns every field the shape references, in index order.
func (s Shape) Fields() []Field {
	fields := append([]Field{}, s.Equal...)
	if s.Range != "" {
		fields = append(fields, s.Range)
	}
	return append(fields, s.Trailing...)
}

// All is the registry.
var All = []Shape{
	{
		Name: "idx_commit_logs_owner_c_date", Collection: CommitLogs,
		Equal: []Field{Owner}, Range: CDate,
		UsedBy: []string{"GetAllCommitLogs", "QueryCommitLogs(owner)", "MarkCommitLogsGcCandidateByOwner", "ListCommitLogs(owner)"},
	},
	{
		Name: "idx_commit_logs_c_date_id", Collection: CommitLogs,
		Range: CDate, Trailing: []Field{ID},
		UsedBy: []string{"QueryCommitLogs"},
	},
	{
		Name: "idx_commit_logs_gc_candidate", Collection: CommitLogs,
		Equal:  []Field{GcCandidate},
		UsedBy: []string{"CountGcCandidates", "ListGcCandidates"},
	},
	{
		Name: "idx_record_keys_parent_id_record_id", Collection: RecordKeys,
		Equal: []Field{Parent}, Trailing: []Field{RecordID},
		UsedBy: []string{"QueryByParent"},
	},
	{
		Name: "idx_record_keys_parent_created_at_record_id", Collection: RecordKeys,
		Equal: []Field{Parent}, Range: RecordCreatedAt, RangeDesc: true, Trailing: []Field{RecordID},
		Partial: PartialMemberKeys,
		UsedBy:  []string{"GetChunklineManifest", "LookupLocalItrs", "LoadLocalBody"},
	},
	{
		Name: "idx_record_keys_parent_id_uri", Collection: RecordKeys,
		Equal: []Field{Parent}, Range: Key, Binary: true,
		UsedBy: []string{"QueryByParentOrderByKey"},
	},
	{
		Name: "idx_records_schema_created_at_document_id", Collection: Records,
		Equal: []Field{Schema}, Range: CreatedAt, Trailing: []Field{ID},
		UsedBy: []string{"QueryByPrefix(schema)", "QueryByParent(schema)"},
	},
	{
		Name: "idx_records_author_created_at_document_id", Collection: Records,
		Equal: []Field{Author}, Range: CreatedAt, Trailing: []Field{ID},
		UsedBy: []string{"QueryByPrefix(author)", "QueryByParent(author)"},
	},
	{
		Name: "idx_records_document_id_created_at", Collection: Records,
		Equal: []Field{ID}, Range: CreatedAt,
		UsedBy: []string{"QueryByPrefix", "QueryByParent"},
	},
	{
		Name: "idx_acks_from_valid_created_at", Collection: Acks,
		Equal: []Field{From, Valid}, Range: CreatedAt,
		UsedBy: []string{"GetAcknowledgeRecords(from)", "ComputeAcknowledgeCounts(from)"},
	},
	{
		Name: "idx_acks_to_valid_created_at", Collection: Acks,
		Equal: []Field{To, Valid}, Range: CreatedAt,
		UsedBy: []string{"GetAcknowledgeRecords(from, to)"},
	},
	{
		Name: "idx_acks_schema_valid_created_at", Collection: Acks,
		Equal: []Field{Schema, Valid}, Range: CreatedAt,
		UsedBy: []string{"GetAcknowledgeRecords(from, schema)"},
	},
	{
		Name: "idx_acked_from_valid_created_at", Collection: Ackeds,
		Equal: []Field{From, Valid}, Range: CreatedAt,
		UsedBy: []string{"GetAcknowledgeRecords(to, from)"},
	},
	{
		Name: "idx_acked_to_valid_created_at", Collection: Ackeds,
		Equal: []Field{To, Valid}, Range: CreatedAt,
		UsedBy: []string{"GetAcknowledgeRecords(to)", "ComputeAcknowledgeCounts(to)"},
	},
	{
		Name: "idx_acked_schema_valid_created_at", Collection: Ackeds,
		Equal: []Field{Schema, Valid}, Range: CreatedAt,
		UsedBy: []string{"GetAcknowledgeRecords(to, schema)"},
	},
	{
		Name: "idx_associations_target_id", Collection: Associations,
		Equal:  []Field{Target},
		UsedBy: []string{"GetAssociatedRecords", "ComputeAssociationCountsBySchema"},
	},
	{
		Name: "idx_associations_target_schema_variant_author", Collection: Associations,
		Equal:  []Field{Target, Schema, Variant, Author},
		UsedBy: []string{"GetAssociatedRecords(filters)"},
	},
	{
		Name: "idx_associations_target_schema_variant_created_at", Collection: Associations,
		Equal: []Field{Target, Schema, Variant}, Range: CreatedAt,
		UsedBy: []string{"GetAssociatedRecords", "ComputeAssociationCountsByVariant"},
	},
	{
		Name: "idx_servers_cs_id", Collection: Servers,
		Equal:  []Field{CSID},
		UsedBy: []string{"server.Repository.Resolve(csid)"},
	},
	{
		Name: "idx_entities_alias", Collection: Entities,
		Equal:  []Field{Alias},
		UsedBy: []string{"GetEntityByAlias"},
	},
}

// Partials lists every predicate a backend has to know.
var Partials = []Partial{PartialMemberKeys}
