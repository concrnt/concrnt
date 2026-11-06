# Postgres backend notes

## Prefix scans on record_keys.uri

`record_keys.uri` has the database's default collation (en_US.utf8 on the
official image and on Cloud SQL). A btree only serves `LIKE 'x%'` when the
index collation is "C", so the unique index on `uri` cannot, and a prefix
scan through it reads the whole table.

The prefix scans therefore match on `record_keys.uri COLLATE "C"` and
`idx_record_keys_uri_c (uri COLLATE "C")` serves them
(`QueryByPrefix`, `QueryRecordSubtree`);
`idx_record_keys_parent_id_uri (parent_id, uri COLLATE "C")` serves the
owner-root branch of `QueryByParent*`. The unique `uri` index stays for the
equality lookups. `QueryRecordSubtree` with `includeSelf` adds an explicit
`[base, base+"0")` range, since the `uri = base OR uri LIKE ...` disjunction
hides the prefix from the planner.

The remaining cost of a prefix listing is joining every matching key to
`records` and `commit_logs` before the sort (top-N heapsort, in memory).
Ordering by the denormalised `record_keys.record_created_at` would let the
limit apply before the joins.

Spanner (`STARTS_WITH` on the uri key) and Firestore (an `ancestors`
array-contains filter) do not use LIKE. SQLite's `BINARY` collation is byte
order already and `case_sensitive_like` is on (`sqlite/db.go`).

## Indexes with callers the load tests do not exercise

`idx_records_author_created_at_document_id`, `idx_commit_logs_owner_c_date`
and `idx_commit_logs_c_date_id` serve the `author` filter of
`QueryByPrefix`/`QueryByParent*`, `GetAllCommitLogs` and `QueryCommitLogs`
(replication); a load test without those paths shows 0 scans on them.
