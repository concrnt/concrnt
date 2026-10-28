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
| C4 | Transactions are `RunInTx(ctx, fn)` | `fn` may run more than once (contention retry), reaches the store only through the `tx` it is handed, and has no side effects of its own. Keep one transaction's writes bounded (Firestore caps at 500); bulk deletes go to the job queue. |
| C5 | Query shapes are declared | A backend lists the query shapes it issues in the shape registry (`internal/infra/repository/shapes`) and a test checks its indexes cover them. |
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
a locale collation) must force it (`COLLATE "C"`); SQLite's `BINARY` and
Firestore's string order already are byte order.

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
| `RunInTx(ctx, fn)` | Runs `fn` atomically; retries on backend contention (Postgres 40001/40P01, SQLite BUSY); `fn` may run more than once (C4). | keep |

#### Commit log

| Method | Shape / semantics | Verdict |
|---|---|---|
| `CreateCommitLog(tx, id, ip, document, proof, owner)` | tx; insert by id, duplicate id is a silent no-op. `c_date` (receipt time) is set by the backend. | keep |
| `HasCommitLog(id)` | get(id) → exists. | keep |
| `GetAllCommitLogs(owner)` | eq(owner), order(c_date asc). Dump of one owner. | keep |
| `QueryCommitLogs(owner?, since, until, limit, order)` | eq(owner)?, range(c_date), order(c_date, id). Replication cursor; `QueryRow.CreatedAt` is `c_date`. Ownerless rows carry no CCFS. | keep |
| `MarkCommitLogGcCandidate(tx, id)` | tx; set flag by id. | keep |
| `MarkCommitLogsGcCandidateByOwner(owner)` | eq(owner) → set flag; idempotent. Not transactional (unregister). | keep |
| `CountGcCandidates(cutoffID)` | eq(gc_candidate), range(id < cutoff) → count. Offline only. | keep |
| `ListGcCandidates(cutoffID, afterID, limit)` | eq(gc_candidate), range(afterID < id < cutoff), order(id). Offline only. | keep |
| `DeleteCommitLogs(ids)` | tx-free multi-delete by id; everything hanging off a commit (record, key, ack, acked, association, entity) goes with it; returns the number removed. Offline only; callers invalidate aggregates afterwards. | keep |
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
| `QueryRecordSubtree(base, includeSelf)` | range(key = base ∨ key starts with base + "/"), order(key, byte order). The exact set a range delete removes. | keep |

#### Associations

| Method | Shape / semantics | Verdict |
|---|---|---|
| `CreateAssociation(tx, documentID, targetURI, owner, author, schema, variant, unique, createdAt)` | tx; `false` when `documentID` or `unique` already exists (C6). The target key must exist. | keep |
| `DeleteAssociation(tx, documentID)` | tx; delete by id. | keep |
| `GetAssociatedRecords(targetURI, schema?, variant?, author?, since, until, limit, order)` | eq(target) ∧ eq(schema)? ∧ eq(variant)? ∧ eq(author)? ∧ range(createdAt), order(createdAt, id). CCFS is composed from the association's owner. | keep |
| `GetAssociatedRecordCountsBySchema(targetURI)` → `ComputeAssociationCountsBySchema` | eq(target) → `map[schema]count`. Expensive scan; aggregate layer only (C2). | rename (T2.2) |
| `GetAssociatedRecordCountsByVariant(targetURI, schema)` → `ComputeAssociationCountsByVariant` | eq(target) ∧ eq(schema) → `map[variant]count` ordered by the earliest association `createdAt` of each variant (bug: Postgres scanned the wrong column and the order was effectively random). | rename + bug (T2.2) |

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

## Changing the contract

1. Change (or add) the suite in `repotest` first; the expected behaviour is
   whatever the current Postgres implementation does unless the change is
   deliberate (then say so in the PR as `behavior:`).
2. Implement it in every backend in the same PR. Do not merge a state
   where only one backend passes.
3. If a backend issues a new query shape, add it to the shape registry and
   update the backend's index definitions; the shape test fails on drift.
4. Offline tooling (`conctl`) builds the repositories it needs over the
   selected backend; a command that speaks SQL directly is documented as
   Postgres-only and refuses to run on any other backend.

## Running the suites

```sh
go test ./internal/infra/repository/postgres/... -run Suite -count=1   # Postgres via dockertest
```

`repotest` must not import a backend package:
`go list -deps ./internal/infra/repository/repotest | grep repository/postgres` stays empty.
