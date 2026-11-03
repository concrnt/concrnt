# Postgres backend notes

## Prefix scans on record_keys.uri

`record_keys.uri` has the database's default collation (en_US.utf8 on the
official image and on Cloud SQL). A btree only serves `LIKE 'x%'` when the
index collation is "C", so the unique index on `uri` cannot, and every
prefix scan read the whole table. The 3000 DAU load test (2026-11-03)
showed it as 51k sequential scans of `record_keys` and 45% of the database
time in `QueryByPrefix`.

The prefix scans now match on `record_keys.uri COLLATE "C"` and
`idx_record_keys_uri_c (uri COLLATE "C")` serves them;
`idx_record_keys_parent_id_uri (parent_id, uri COLLATE "C")` already served
the owner-root branch of `QueryByParent*`. The unique `uri` index stays for
the equality lookups. `QueryRecordSubtree` with `includeSelf` adds an
explicit `[base, base+"0")` range, since the `uri = base OR uri LIKE ...`
disjunction hides the prefix from the planner (it then walked the whole new
index in order).

Measured on 3000 seeded xk6-concrnt users (152k record_keys, 140k records,
220k commit_logs; Postgres 18), the busiest owner's notify-timeline (≈1600
keys):

```
### 1. QueryByPrefix  uri LIKE 'cckv://<owner>/…/notify-timeline/%' ORDER BY r.created_at DESC LIMIT 20
before  Parallel Seq Scan on record_keys (rows=411.67 loops=3)  Rows Removed by Filter: 63168
        Execution Time: 92.809 ms
after   Bitmap Index Scan on idx_record_keys_uri_c (rows=1634)
          Index Cond: ((uri)::text >= '…/notify-timeline/' AND (uri)::text < '…/notify-timeline0')
        Execution Time: 14.619 ms

### 2. QueryByParent owner root  parent_id IS NULL AND uri LIKE 'cckv://<owner>/%'
before  Index Scan using idx_record_keys_parent_id_uri (rows=2)
          Index Cond: ((parent_id IS NULL) AND (uri >= 'cckv://<owner>/') AND (uri < 'cckv://<owner>0'))
        Execution Time: 0.110 ms
after   Bitmap Index Scan on idx_record_keys_parent_id_uri (rows=2), same Index Cond
        Execution Time: 0.060 ms

### 3. QueryRecordSubtree  uri = base OR uri LIKE base/% ORDER BY uri COLLATE "C" LIMIT 100
before  Parallel Seq Scan on record_keys (rows=412.67 loops=3)  Rows Removed by Filter: 63199
        Execution Time: 76.726 ms
after   Bitmap Index Scan on idx_record_keys_uri_c (rows=1635)
          Index Cond: ((uri)::text >= base AND (uri)::text < base||'0')
        Execution Time: 36.919 ms (most of it reading cold commit_logs pages for the joins)
```

The remaining cost of 1 and 3 is joining every matching key to `records`
and `commit_logs` before the sort (top-N heapsort, in memory). Ordering by
the denormalised `record_keys.record_created_at` would let the limit apply
before the joins; not done here.

The seed itself (before the change) ran 32,750 sequential scans of
`record_keys` (2.17 billion tuples) and 8,439 of `records`. With the change,
a 2 minute load (xk6-concrnt load.js, 3000 users) ran none on either table
and no statement exceeded 10 ms (auto_explain). The `records` scans of the
seed were not tied to a statement; they did not recur under load.

Spanner (`STARTS_WITH` on the uri key) and Firestore (an `ancestors`
array-contains filter) do not use LIKE and are unaffected. SQLite's LIKE is
case-insensitive and needs its own treatment; not changed.

## Indexes the load test never used

`idx_records_author_created_at_document_id`, `idx_commit_logs_owner_c_date`
and `idx_commit_logs_c_date_id` showed 0 scans in the load test. All three
have callers the scenario does not exercise: the `author` filter of
`QueryByPrefix`/`QueryByParent*`, `GetAllCommitLogs` and
`QueryCommitLogs` (replication). None is a removal candidate.
