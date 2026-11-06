# Repository contract

The usecase packages (`internal/usecase/*`) declare the `Repository`
interfaces; every backend under `internal/infra/repository/<name>`
implements exactly those; `cmd/concrnt/main.go` and `cmd/conctl/operation.go`
pick the backend with a `switch` on `backends.database` and call each
backend package's constructors by hand (there is no bundling `Set` type). The
contract is the interfaces plus the invariants below. What a backend does
internally (SQL joins, cascades, serial ids) is its own business as long as
the contract's meaning never depends on it.

## Principles

| # | Principle | Meaning |
|---|---|---|
| C1 | No joins assumed | Every read method must be satisfiable as "one collection, equality filters, at most one range condition, an ordering" or as a get by id(s). Fields a query needs live on the key side, denormalised. |
| C2 | Aggregates are caches | Counts are derived values recomputable from the primary data. Request paths read them through the aggregate layer (`internal/usecase/aggregate`); the repository only exposes the expensive `Compute*` recomputation, which nothing but the aggregate layer and `conctl op aggregate-verify` may call. A write adjusts an existing cache entry or recomputes; it never trusts an increment for correctness. |
| C3 | No auto-increment in the contract | Contract identifiers are CDIDs, URI hashes and client-generated random ids. A backend's serial ids never leak out. |
| C4 | Transactions are `RunInTx(ctx, fn)` | `fn` may run more than once (contention retry), reaches the store only through the `tx` it is handed, and has no side effects of its own. The read methods take no `tx` and return **committed** state: inside `fn` they do not see what `fn` has written so far (on Postgres and SQLite they run on another connection, on Spanner as a single-use read), so a usecase never relies on reading its own uncommitted writes. Whether a backend's own write methods see their earlier statements in the same transaction is the backend's business. Keep one transaction's writes bounded (Spanner caps at 80,000 mutations including index entries and cascaded rows, Firestore at 10 MiB per commit; Firestore's former 500-write cap is gone); a range delete removes 500 targets in the commit and the rest page by page in `RecordRangeDelete` jobs. |
| C5 | Indexes are the backend's own | The method table below names the access path (the C1 shape) each method issues; that table is the only cross-backend description. Each backend declares its indexes in its own schema (the gorm tags in `postgres/models.go` and `sqlite/models.go`, the DDL in `spanner/schema.go`, the shape registry in `firestore/indexes.go` that generates `deploy/firestore/firestore.indexes.json`) and pins the resulting index set in a test (`pg_indexes` / `sqlite_master` / `INFORMATION_SCHEMA.INDEXES` / the generated file), so a path added here lands in every backend's schema and test. Models, tables and index definitions are never shared between backends. |
| C6 | Uniqueness is explicit | Contract-level uniqueness (association `unique`, ack `(from, to, schema)`) is "a second write of the same key is a no-op / rejected", verified by repotest, however the backend enforces it. |
| C7 | repotest defines the contract | A backend that does not pass `repotest` does not implement the contract. Change repotest before changing the contract. |
| C8 | Migration is log replay | `conctl op dump-commitlog` / `import-commitlog` work on every backend; no backend copies another's tables. |
| C9 | Split interfaces only along backend boundaries | A separate `Repository` interface exists only where the data behind it could live on a different backend from the rest (e.g. subscriptions on one store, records on another). Methods that read the same tables as an existing interface belong on that interface, even when only offline tooling calls them; `conctl` therefore uses `record.Repository` for gc / dump, and the record, entity and chunkline methods are one interface implemented by one package (`<backend>/record`). Conversely `entity_metas` is keyed by ccid and never joined, so it is its own `usecase.EntityMetaRepository`. |
| C10 | Test-only code stays out of production packages | Code only tests use lives in `_test.go`. When it has to be shared across packages it goes in a package whose name ends in `test` (`repotest`, `storetest`, `testutil`), and no non-test file imports those (`go list -deps ./cmd/...` must not list them). Interface types cannot live in `_test.go`, so the `Inspector` interface is in `repotest` and each backend's implementation is in its own `inspect_test.go`. |

## Key order

Wherever the contract orders or bounds by key (`QueryByParentOrderByKey`
and its `since`/`until` cursors, `QueryRecordSubtree`), the order is **byte
order of the UTF-8 key** (CIP-5 `orderby=key` has to be the same on every
server). A backend whose default collation is not byte order (Postgres with
a locale collation) must force it (`COLLATE "C"`); SQLite's `BINARY`,
Spanner's code-point order of `STRING` and Firestore's string order already
are byte order.

Commit log ids (time-kind CDIDs: fixed-length lowercase base32) and the
`c_date, id` replication cursor sort the same under every collation, so
`ListCommitLogs` / `QueryCommitLogs` need no such care.

## Method inventory

Classification of every method against the principles. "shape" is the C1
reading of the method: `get(x)` is a lookup by identifier, `eq` an equality
filter, `range` the single range condition, `order` the ordering; `tx` marks
a method that only runs inside `RunInTx`.

Verdicts: **keep** — as is; **rename** — same semantics, new name
(phase 2); **add** — new in phase 2; **bug** — contract semantics fixed in
phase 2 (`behavior:` in the PR).

### `record.Repository` (`internal/usecase/record/usecase.go`)

#### Transactions

| Method | Shape / semantics | Verdict |
|---|---|---|
| `RunInTx(ctx, fn)` | Runs `fn` atomically; retries on backend contention (Postgres 40001/40P01, SQLite BUSY, Spanner ABORTED); `fn` may run more than once (C4). | keep |

#### Commit log

| Method | Shape / semantics | Verdict |
|---|---|---|
| `CreateCommitLog(tx, id, ip, document, proof, owner)` | tx; insert by id, a duplicate id leaves the row as first written and returns `ErrAlreadyCommitted` (the commit path's redelivery check: no read precedes the insert). `c_date` (receipt time) is set by the backend. | keep |
| `HasCommitLog(id)` | get(id) → exists. Tooling and tests. | keep |
| `GetAllCommitLogs(owner)` | eq(owner), order(c_date asc). Dump of one owner. | keep |
| `QueryCommitLogs(owner?, since, until, limit, order)` | eq(owner)?, range(c_date), order(c_date, id). Replication cursor; `QueryRow.CreatedAt` is `c_date`. Ownerless rows carry no CCFS. | keep |
| `MarkCommitLogGcCandidate(tx, id)` | tx; set flag by id. | keep |
| `MarkCommitLogsGcCandidateByOwner(owner)` | eq(owner) → set flag; idempotent. Not transactional (unregister). | keep |
| `CountGcCandidates(cutoffID)` | eq(gc_candidate), range(id < cutoff) → count. Offline only. | keep |
| `ListGcCandidates(cutoffID, afterID, limit)` | eq(gc_candidate), range(afterID < id < cutoff), order(id). Offline only. | keep |
| `DeleteCommitLogs(ids)` | tx-free multi-delete by id; everything hanging off a commit (record, key, ack, acked, association, entity) goes with it; returns the number removed. Not atomic over the whole list: a backend with a per-commit write limit (Spanner) deletes in batches. Offline only; callers invalidate aggregates afterwards. | keep |
| `ListCommitLogs(page)` | eq(owner)?, range(id), order(id asc), limit. Offline paging. | keep |

#### Records and keys

| Method | Shape / semantics | Verdict |
|---|---|---|
| `CreateRecord(tx, documentID, key, owner, author, schema, onUpdate, policies, distributions, redirect, createdAt)` | tx; accept-if-newer on `createdAt` per key (equal/older → `false`); creates placeholder parent keys up the chain; `onUpdate != "retain"` forgets the previous generation (record deleted, its commit flagged). The key row carries everything list queries and chunkline need (`recordCreatedAt`, `redirect`, …) so no read has to join the record (C1, T2.4). | keep |
| `GetSignedDocument(uri)` | get(cckv key) or get(ccfs id); fills both `CCKV` and `CCFS` when known; placeholder key → not found. | keep |
| `GetSignedDocuments(uris)` | multi-get over a mixed batch of cckv / ccfs URIs; a URI that resolves to nothing is absent from the map, not an error. Replaces the per-destination loop of the delete sweep (C1 round-trip budget). | add (T2.4) |
| `GetHierarchicalRecordPolicies(uri)` | get(every ancestor key of uri, root first); emits a layer for each level that has a policy or distributes; ccfs input is mapped to its key first. The ancestor set is computed from the URI, not discovered by a join. | keep |
| `GetHierarchicalRecordPoliciesMulti(uris)` | the same for several cckv URIs with one fetch of the union of their ancestor chains; every input has an entry. A listing checks a whole page with one call (C1 round-trip budget). | add (T2.4) |
| `GetDistributions(uri)` | — | removed (T2.4): the stored distributions are the document's own `distributes`, which the caller already holds. |
| `GetTimelineRemoval(keyURI)` | get(key) + get(parent key) → `(parent uri, chunkline item id)`; empty for keys that never appear in a chunkline body. | keep |
| `DeleteRecordByKey(tx, uri)` | tx; delete the record the key points at (associations targeting the key go with it). | keep |
| `DeleteRecordByDocumentID(tx, id)` | tx; delete by id. | keep |

#### List queries (CIP-5)

| Method | Shape / semantics | Verdict |
|---|---|---|
| `QueryByPrefix(prefix, schema?, author?, since, until, limit, order)` | range(key starts with prefix) ∧ eq(schema)? ∧ eq(author)? ∧ range(createdAt), order(createdAt, id). Only keys holding a record. `prefix` is a literal: a backend using `LIKE` escapes `%`/`_`/`\` (bug: Postgres did not). | bug (T2.6) |
| `QueryByParent(parent, schema?, author?, since, until, limit, order)` | eq(parent) ∧ … as above; the owner root (`cckv://<owner>`) is a valid parent and lists top-level keys that hold a record. | keep |
| `QueryByParentOrderByKey(parent, schema?, author?, sinceKey, untilKey, limit, order)` | eq(parent), range(key, inclusive bounds), order(key, **byte order**). Placeholder (intermediate) keys come back as key-only rows unless a record filter (schema/author) drops them. | bug (byte order, T2.3) |
| `QueryRecordSubtree(base, includeSelf, afterKey, limit)` | range(key = base ∨ key starts with base + "/") ∧ range(key > afterKey)?, order(key, byte order), limit. The exact set a range delete removes; the cursor and limit page it one transaction at a time (phase 3: `RecordRangeDelete` jobs). | add (cursor, limit) |

#### Associations

| Method | Shape / semantics | Verdict |
|---|---|---|
| `CreateAssociation(tx, documentID, targetURI, owner, author, schema, variant, unique, createdAt)` | tx; `false` when `documentID` or `unique` already exists (C6). The target key must exist. | keep |
| `DeleteAssociation(tx, documentID)` | tx; delete by id. | keep |
| `GetAssociatedRecords(targetURI, schema?, variant?, author?, since, until, limit, order)` | eq(target) ∧ eq(schema)? ∧ eq(variant)? ∧ eq(author)? ∧ range(createdAt), order(createdAt, id). CCFS is composed from the association's owner. | keep |
| `GetAssociatedRecordCountsBySchema(targetURI)` / `GetAssociatedRecordCountsByVariant(targetURI, schema)` → `ComputeAssociationCounts` | eq(target) → `map[schema]map[variant]count`, each variant carrying the earliest association `createdAt` (its order). One computation serves both reads; the per-schema counts are the sums. Expensive scan; aggregate layer only (C2). | rename + merge (T2.2) |

#### Acks (CIP-10)

| Method | Shape / semantics | Verdict |
|---|---|---|
| `Acknowledge` / `UnAcknowledge(tx, documentID, from, to, schema, createdAt)` | tx; acker-side state keyed by `(from, to, schema)`, accept-if-newer on `createdAt` (equal/older → `false`). `true` means the state row moved, **not** that validity flipped. | keep |
| `Acknowledged` / `UnAcknowledged(…)` | tx; the same on the target-side state. | keep |
| `GetAcknowledgeRecords(from \| to, schema?, since, until, limit, order)` | exactly one of from/to: eq(from ∨ to) ∧ eq(valid) ∧ eq(schema)? ∧ range(createdAt), order(createdAt, id). Serves the side this server holds. | keep |
| `GetAcknowledgeRecordCounts(from \| to, schema?)` → `ComputeAcknowledgeCounts` | eq(from ∨ to) ∧ eq(valid) ∧ eq(schema)? → `map[schema]count`. Aggregate layer only (C2). | rename (T2.2) |

#### Entities

| Method | Shape / semantics | Verdict |
|---|---|---|
| `CreateEntity(tx, ccid, alias, domain, documentID, createdAt)` | tx; accept-if-newer on `createdAt` per ccid. | keep |
| `GetEntityByCCID(ccid)` | get(ccid) + its signed document. | keep |
| `GetEntityByAlias(alias)` | eq(alias) → one entity + its signed document. | keep |

#### Chunkline views (CIP-11)

| Method | Shape / semantics | Verdict |
|---|---|---|
| `GetChunklineManifest(uri)` | get(key) + eq(parent) order(recordCreatedAt asc) limit 1 → first chunk. | keep |
| `LookupLocalItrs(uris, chunkID)` | per uri: eq(parent) ∧ range(recordCreatedAt < cutoff), order desc, limit 1 → chunk id. One statement on SQL backends, N bounded point queries elsewhere. | keep |
| `LoadLocalBody(uri, chunkID)` | eq(parent) ∧ range(recordCreatedAt), order desc; reads only key-side fields (`recordCreatedAt`, `redirect`), never the record. | keep |

### `usecase.EntityMetaRepository` (`internal/usecase/entitymeta.go`)

| Method | Shape / semantics | Verdict |
|---|---|---|
| `SaveMeta(meta)` | upsert by ccid (inviter and info). | keep |
| `GetMeta(ccid)` | get(ccid); `domain.NotFoundError` when absent. | keep |
| `UpdateMetaInfo(ccid, info)` | update info only; `domain.NotFoundError` when absent. | keep |
| `DeleteMeta(ccid)` | delete by ccid; idempotent. | keep |
| `ListMetas(owner?)` | all, or eq(ccid). Offline dump. | keep |

### `server.Repository` (`internal/usecase/server/server.go`)

| Method | Shape / semantics | Verdict |
|---|---|---|
| `Resolve(identifier, hint)` | get(fqdn) or eq(csid) in the local cache; on a miss fetches the well-known through the client (csid needs `hint`) and upserts it by fqdn. | keep |
| `List()` | all cached well-knowns (rows with an empty well-known are skipped). | keep |

### `notification.Repository` (`internal/usecase/notification/notification.go`)

| Method | Shape / semantics | Verdict |
|---|---|---|
| `Subscribe(subscription)` | upsert by `(vendorID, owner)`. | keep |
| `List()` | all. | keep |
| `Get(vendorID, owner)` | get; `domain.NotFoundError` when absent. | keep |
| `Delete(vendorID, owner)` | delete; idempotent. | keep |

### `abuse.Repository` (`internal/usecase/abuse/abuse.go`)

| Method | Shape / semantics | Verdict |
|---|---|---|
| `CreateAbuseReport(report, reporter, ip)` | append-only insert; the report's id is backend-internal (C3: it never leaves the repository). | keep |

### Identifiers crossing the contract (C3)

Nothing returns a backend-generated id. The `int64` values that do cross are
chunk ids (`unix / 600` time buckets), counts, and the number of commit logs
deleted.

## Backend notes

What each backend does with the contract that the other backends do not
have to think about. None of it changes the contract's meaning.

### Spanner (`internal/infra/repository/spanner`)

- GoogleSQL through the native client; writes are DML statements inside
  `ReadWriteTransaction`, so a write method sees the rows earlier statements
  of the same transaction wrote (the conditional upserts and placeholder
  parents rely on it). Reads outside a transaction are single-use strong
  reads. `ABORTED` is retried by the client: that is the `RunInTx` retry.
- `commit_logs.c_date` is a commit timestamp (`PENDING_COMMIT_TIMESTAMP()`),
  monotonic within the database, so `QueryCommitLogs`'s `(c_date, id)`
  order is exact. The column cannot be read in the transaction that writes
  it; nothing in the contract does. `Inspector.SetCommitCDate` writes an
  explicit past timestamp, which the column allows.
- 80,000 mutations per commit, index entries and cascaded rows included.
  `DeleteCommitLogs` deletes 100 commits per transaction; a range delete
  pages (C4). What still exceeds it: deleting one record whose key has more
  than roughly 15,000 associations (every association row and its four
  index entries cascade in that one transaction). Spanner Omni does not
  enforce the limit; Cloud Spanner does.
- No serial ids: `record_keys` is keyed by `uri` with a `parent_uri`
  column, associations reference the target by `uri`. `records`, `acks`,
  `ackeds` and `associations` are interleaved in `commit_logs` (`ON DELETE
  CASCADE`); `entities.document_id`, `record_keys.record_id` and
  `associations.target_uri` cascade through foreign keys.
- Time-ordered primary keys (CDIDs) are written to monotonically increasing
  key ranges; no sharding at this scale. Adding a shard column later is a
  DDL batch plus the key predicates, nothing in the contract.
- Spanner Omni accepts only `projects/default/instances/default` as the
  instance path; the evaluation license runs 90 days from the first start
  of a data directory.

### Firestore (`internal/infra/repository/firestore`)

- A Firestore transaction must issue every read before its first write and
  never sees its own writes, so `RunInTx` hands the closure a handle that
  buffers the writes (coalesced per document) and lays them over the reads
  it serves; the writes are flushed when the closure returns. Point reads
  and batch reads in the closure therefore see the closure's earlier
  writes, queries see the committed state minus the pending deletes
  (no method needs more). The SDK re-runs the closure on ABORTED: that is
  the `RunInTx` retry. Reads outside a transaction are plain strong reads
  (C4).
- One top-level collection per table; URI-keyed documents use a hash of
  the URI as id (ids may not contain `/`) and carry the URI as a field.
  `record_keys` link to their parent by `parentURI` (the owner root
  `cckv://<owner>` for top-level keys, which is how the owner-root listings
  of `QueryByParent*` work) and carry every strict ancestor in
  `ancestors`, the field `QueryByPrefix` matches with `array-contains`
  (the last path segment of a prefix is filtered client-side). A prefix
  without an owner and a path has no indexed form and is rejected.
- Placeholder keys hold none of the record fields: a query that orders by
  `recordCreatedAt` or filters on `schema` / `author` leaves them out by
  itself, which is the `IS NOT NULL` of the SQL backends.
  `QueryRecordSubtree` orders by `uri` then `recordCreatedAt` only for that
  reason.
- No cascading deletes: a record delete removes the record, its key and the
  associations targeting the key in the transaction; `DeleteCommitLogs`
  fans out through a BulkWriter outside any transaction (idempotent, so a
  partial run is finished by re-running). A key with tens of thousands of
  associations hits the 10 MiB commit cap, the same ceiling Spanner has.
- `cDate` is a server timestamp set at commit; it is receipt time, not a
  strict commit sequence (as on Postgres). Timestamps have microsecond
  precision.
- Composite indexes are not inferred: `firestore/indexes.go` registers
  every query shape, generates `deploy/firestore/firestore.indexes.json`
  (pinned by `indexes_test.go`, `-args -update-indexes` regenerates) and
  probes each shape at server start. The emulator enforces none of them,
  so a shape added without an index only fails in production. A query with
  two optional equalities (`GetAssociatedRecords` with schema and variant)
  relies on Firestore merging two single-equality indexes.
- Time-ordered document ids (CDIDs) cap sustained writes to `commits` /
  `records` / `associations` around 500 per second.

## Changing the contract

1. Change (or add) the suite in `repotest` first; the expected behaviour is
   whatever the current Postgres implementation does unless the change is
   deliberate (then say so in the PR as `behavior:`).
2. Implement it in every backend in the same PR. Do not merge a state
   where only one backend passes.
3. If a method issues a new query shape, record it in the method table and
   add the index to every backend's own schema and pinned index test.
4. Offline tooling (`conctl`) builds the repositories it needs over the
   selected backend; a command that speaks SQL directly is documented as
   Postgres-only and refuses to run on any other backend.

## Running the suites

```sh
go test ./internal/infra/repository/postgres/... -run Suite -count=1   # Postgres via dockertest
go test ./internal/infra/repository/sqlite/... -run Suite -count=1     # SQLite on a temp file, no Docker
go test ./internal/infra/repository/spanner/... -run Suite -count=1    # Spanner Omni via dockertest (CONCRNT_TEST_SPANNER_ENDPOINT reuses a running one)
go test ./internal/infra/repository/firestore/... -run Suite -count=1  # Firestore emulator via dockertest (CONCRNT_TEST_FIRESTORE_HOST reuses a running one)
```

`repotest` must not import a backend package:
`go list -deps ./internal/infra/repository/repotest | grep repository/postgres` stays empty.
