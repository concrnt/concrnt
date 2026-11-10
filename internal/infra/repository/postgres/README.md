# Postgres backend notes

## Writes and connections (repository contract v2)

Every `Apply*` method of the record repository is one transaction on one
pooled connection (`record/repository.go`, `apply`): it is re-run from
scratch on `serialization_failure` (40001), `deadlock_detected` (40P01) and
when a concurrent writer overtook a row before it could be locked, up to 8
attempts with a 10 ms to 500 ms backoff, and it is rolled back when the
outcome is `Stale` or `Duplicate`, which is how those leave no trace. The
usecase issues its reads before the call, never inside the transaction.

What that removed: the rule that a transaction needs a second connection
for its reads, the transaction slots that enforced it, the setting
`backends.postgres.maxConcurrentTx` (a config that still has it loads; the
key is ignored) and the `concrnt_record_tx_slot*` metrics. Size the pool
with `backends.postgres.maxOpenConns` alone.

Dependents go by `ON DELETE CASCADE` inside the same transaction
(`record_keys.record_id` → `records`, `associations.target_id` →
`record_keys`, everything anchored to a commit → `commit_logs`), so
`ApplyDelete` issues no cleanup tokens and `Cleanup` is a no-op.

Two things this schema does that the contract does not ask for
(`CONTRACT.md`, backend notes):

- `associations."unique"` is unique table-wide, not per target. The usecase
  derives the value from the target, so the difference is not reachable
  through it.
- `record_keys.parent_id` is the parent row's serial id, without a foreign
  key. Removing a key that has children deletes the parent row; the
  children keep the dangling id and are missing from `QueryByParent*` and
  the chunkline views of that parent until they are written again (an
  overwrite sets `parent_id` anew).

Migration step 6 (`aggregate_entries_counters`) drops and recreates the
count cache table in its counter shape (`internal/infra/aggregate/postgres`).

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
