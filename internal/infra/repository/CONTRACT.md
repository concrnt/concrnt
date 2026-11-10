# Repository contract

The usecase packages (`internal/usecase/*`) declare the `Repository`
interfaces; every backend under `internal/infra/repository/<name>`
implements exactly those; `cmd/concrnt/main.go` and `cmd/conctl/operation.go`
pick the backend with a `switch` on `backends.database` and call each
backend package's constructors by hand (there is no bundling `Set` type). The
contract is the interfaces plus the invariants below. What a backend does
internally (SQL joins, cascades, serial ids, which collections exist) is its
own business as long as the contract's meaning never depends on it.

This is the second version of the contract ("v2"). The first one was the
method surface of the Postgres implementation turned into an interface, and
building and load-testing four backends on it showed where that fails:

- A write was a set of table operations composed inside a closure
  transaction (`RunInTx` with `CreateCommitLog`, `CreateRecord`, ...). That
  asks every backend for interactive serializable transactions with
  read-your-writes. Firestore had to build a write-buffer overlay to fake
  it, and Postgres needed a second connection per transaction for the
  closure's reads, which is where the pool exhaustion and the commit
  deadlock came from.
- "No joins" only constrained single queries. The signed document every
  read returns lived in the commit log and was fetched again for every
  listed row: two round trips and 2N document reads per page.
- The aggregate store was an opaque byte blob with compare-and-swap, so
  every backend serialised the writers of one key. `aggregate_entries` was
  the top lock wait on Spanner and the point where Firestore collapsed in
  the 1000 DAU load test.
- Cascades were synchronous and unbounded: removing a key removed its
  associations in the same transaction (a range query inside the
  transaction on Firestore, the per-commit mutation cap on Spanner).
- `repotest` pinned Postgres behaviour method by method through a white-box
  `Inspector`, which fixed the internal model of every backend.

v2 answers these with C4 (one commit-level write, no transaction handle),
C1 (complete rows from one access path, with a round-trip budget), C2
(commutative counters), C11 (dependents leave reads at once and storage
later) and C7 (conformance at the protocol level, black-box below it).

## Principles

| # | Principle | Meaning |
|---|---|---|
| C1 | A read returns complete rows from one access path | Every read returns committed state, and whatever it returns (the signed document included) is stored with the entity its access path lands on: "one collection, equality filters, at most one range condition, an ordering" or a get by id(s). No read follows a pointer per row, so a row whose commit has been garbage-collected never shortens a page. Every read has a round-trip budget in the method tables below: a list read is one query, a point read one get. The commit log is written once and read only by id (ccfs resolution, redelivery check), by the replication cursor and by offline tooling. |
| C2 | Aggregates are caches of commutative counters | Counts are derived values recomputable from the primary data. A cache entry is a set of named integer counters, per-counter minima and a member set; a write folds in as a blind, commutative delta (`Add`), with no read-modify-write and no retry by the caller. Request paths read counts through the aggregate layer (`internal/usecase/aggregate`); the repository only exposes the expensive `Compute*` recomputation, which nothing but the aggregate layer and `conctl op aggregate-verify` may call (`internal/usecase/aggregate/guard_test.go` enforces it). The cache is never trusted for correctness: dropping all of it is always safe. |
| C3 | No auto-increment in the contract | Contract identifiers are CDIDs, URI hashes and client-generated random ids. A backend's serial ids never leak out. |
| C4 | A write is one commit-level operation | The usecase reads and evaluates policy first (committed state, no transaction), then calls exactly one `Apply*` method with a complete description of the write. That call is atomic (it stores everything it describes or nothing), idempotent on the commit id, retries backend contention internally and returns only a final result. Its read set and write set follow from its arguments alone: no closure, no transaction handle, no data-dependent fan-out. The caller keeps one call bounded (`rangeDeleteSyncLimit` = 500 removals per `ApplyDelete`; the rest of a range delete goes page by page in `RecordRangeDelete` jobs). |
| C5 | Indexes are the backend's own | The method tables name the access path (the C1 shape) each method issues; those tables are the only cross-backend description. Each backend declares its indexes in its own schema (the gorm tags in `postgres/models.go` and `sqlite/models.go`, the DDL in `spanner/schema.go`, the shape registry in `firestore/indexes.go` that generates `deploy/firestore/firestore.indexes.json`) and pins the resulting index set in a test (`pg_indexes` / `sqlite_master` / `INFORMATION_SCHEMA.INDEXES` / the generated file), so a path added here lands in every backend's schema and test. Models, tables and index definitions are never shared between backends. |
| C6 | Uniqueness is explicit | Contract-level uniqueness is stated per method and verified by `repotest`, however the backend enforces it: a second association with the same `Unique` on the current incarnation of its target (C11) is `Stale`; there is one ack state per `(side, from, to, schema)` and one entity per ccid, which a newer document moves and an equal or older one leaves (`Stale`); a commit id occurs once in the log (`Duplicate`). |
| C7 | Conformance is defined at two heights | `ciptest` (`internal/usecase/record/ciptest`) drives the real usecases over the backend and asserts what a client or a peer server can observe: passing it is what "implements concrnt" means. `repotest` checks each contract method black-box, writing and reading only through contract methods. Neither asserts backend internals (tables, collections, columns). A backend that does not pass both does not implement the contract; change the suites before changing the contract. |
| C8 | Migration is log replay | `conctl op dump-commitlog` / `import-commitlog` work on every backend; no backend copies another's tables, and a backend whose stored shape changes incompatibly is migrated the same way (`MIGRATION.md`). |
| C9 | Split interfaces only along backend boundaries | A separate `Repository` interface exists only where the data behind it could live on a different backend from the rest (e.g. subscriptions on one store, records on another). Methods that read the same tables as an existing interface belong on that interface, even when only offline tooling calls them; `conctl` therefore uses `record.Repository` for gc / dump, and the record, entity and chunkline methods are one interface implemented by one package (`<backend>/record`). Conversely `entity_metas` is keyed by ccid and never joined, so it is its own `usecase.EntityMetaRepository`. |
| C10 | Test-only code stays out of production packages | Code only tests use lives in `_test.go`. When it has to be shared across packages it goes in a package whose name ends in `test` (`repotest`, `ciptest`, `storetest`, `testutil`), and no non-test file imports those (`go list -deps ./cmd/...` must not list them). Interface types cannot live in `_test.go`, so the `Inspector` interface is in `repotest` and each backend's implementation is in its own `inspect_test.go`. |
| C11 | Dependents disappear atomically from reads and physically later | A key has an *incarnation*: it starts when the key comes to exist (as a record or as a placeholder) and ends when the key is removed; overwrites do not change it. Associations belong to the incarnation they were created on. When a key is removed, every association on it stops being returned by every read and stops counting for uniqueness in that same operation; the physical removal is bounded follow-up work the backend hands back as opaque cleanup tokens. A backend that removes everything synchronously issues no tokens. |

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

Every method against the principles. "shape" is the C1 reading of the
method: `get(x)` is a lookup by identifier, `eq` an equality filter,
`range` the single range condition, `order` the ordering.

"RT" is the round-trip budget: the number of sequential steps the method
may take against the store, whatever the page size. Requests a backend
issues in parallel count as one step (Firestore has no joins, so where a
read needs a query and the key that qualifies it, it issues both at once).
The budget is what every backend guarantees; where one cannot hold it in
every case, the exception is stated in the row. Firestore pins its numbers
per RPC in `firestore/record/budget_test.go`; the request-level budget (the
number of repository calls per handler) is pinned in
`internal/present/rest/handler_roundtrip_test.go`. The doc comments in
`internal/usecase/record/usecase.go` carry the same budgets and are the
authoritative wording of each method's semantics.

### `record.Repository` (`internal/usecase/record/usecase.go`)

#### Writes (C4)

Every write carries a `Commit{ID, IP, Document, Proof, Owner}`, the commit
log entry (`c_date`, the receipt time, is set by the backend), and ends in
an `ApplyStatus`:

| Status | Meaning |
|---|---|
| `Applied` | Everything the operation describes is stored. |
| `Stale` | The write lost accept-if-newer or uniqueness. **Nothing** is stored, not even the commit log entry or an ancestor placeholder, so a later delivery of the same document is judged afresh. |
| `Duplicate` | The commit id is already in the log (a redelivery). Nothing changed and the log entry stays as first written. Checked before anything else: a duplicate id is `Duplicate` whatever the rest of the call would have been. |

A call that returns an error stored nothing either. Contention
(Postgres 40001 / 40P01, SQLite BUSY / LOCKED, Spanner and Firestore
ABORTED) is retried inside the backend.

| Method | Semantics |
|---|---|
| `ApplyEntity(EntityCommit{Commit, CCID, Alias, Domain, CreatedAt})` | The commit and the entity it registers. Accept-if-newer on `CreatedAt` per ccid (CIP-3 §3.4): equal or older is `Stale`. The commit of a replaced entity document is kept as it is (entities are exempt from onUpdate). |
| `ApplyRecord(RecordCommit{Commit, Key, Author, Schema, OnUpdate, Policies, Distributions, Redirect, CreatedAt})` → `RecordOutcome{Status, FreshKey, PreviousID}` | The commit, the record, the key pointing at it, and a placeholder for every ancestor key that does not exist yet. Accept-if-newer on `CreatedAt` per key: equal or older is `Stale`. Unless the version it replaces was written with onUpdate `retain`, that version is forgotten and its commit flagged for gc; a retained one stays readable by its ccfs URI. `FreshKey` is true when the key held no record before (it did not exist or was a placeholder), `PreviousID` the document id it replaced (`""` when fresh). After a `RemoveKey` the next record on the key is a fresh key even when it is older than the removed one. An overwrite does not touch the key's associations. Two commits racing on a key that does not exist yet converge on the newer document. |
| `ApplyAssociation(AssociationCommit{Commit, Target, Author, Schema, Variant, Unique, CreatedAt})` | The commit and the association, bound to the incarnation of `Target` that exists at the time of the call. `Stale` when an association with the same `Unique` already exists on that incarnation (C6); `domain.NotFoundError` (and no trace) when the target key does not exist. A placeholder key counts as existing and keeps its associations when a record arrives on it. The usecase derives `Unique` from the target, author, schema, variant and body, so one value never occurs on two targets (see the Postgres / SQLite note). |
| `ApplyAck(AckCommit{Commit, Side, From, To, Schema, Valid, CreatedAt})` → `AckOutcome{Status, WasValid, IsValid}` | The commit and one side's state of the `(From, To, Schema)` relationship (CIP-10 §4), from then on anchored to this commit. `Side` is `AckerSide` (ack / unack, recorded under `From`) or `TargetSide` (acked / unacked, under `To`); the two are independent states. Accept-if-newer on `CreatedAt` alone, never the document id, so both servers of a relationship reach the same decision: equal or older is `Stale`. When `Applied`, `WasValid` is the validity before the call (false when there was no state) and `IsValid` the one stored, so the caller can tell a validity flip from a state that merely moved to a newer document. When `Stale`, both are the stored state's validity. An unack that is the first document of a relationship is stored as an invalid state, so an older ack cannot bring it to life afterwards. |
| `ApplyDelete(DeleteCommit{Commit, Removals, GcCommitIDs})` → `DeleteOutcome{Status, Removed, Cleanups}` | One page of a delete; see below. |
| `Cleanup(token)` → `next` | One bounded step of deferred physical removal (C11); see below. |

`ApplyDelete` records `Commit` when one is given, applies `Removals` in
order (a later removal sees what an earlier one of the same call removed)
and flags `GcCommitIDs` for gc (unknown ids are ignored; the delete commit
of the same call may be among them).

- `Commit` is nil on the continuation pages of a range delete, whose commit
  the first page recorded. Such a call is always `Applied`, writes nothing
  to the log and is idempotent (a retried job replays it and removes
  nothing). With a `Commit` whose id is already in the log the call is
  `Duplicate` and removes nothing. There is no `Stale`. Zero removals are
  legal: a commit that stores only its log entry, or an empty page.
- `Removal{Kind, URI, DocumentID}`. `URI` is filled for every kind (the
  cckv key the removal concerns); `DocumentID` for `RemoveVersion` and
  `RemoveAssociation`.

  | Kind | Removes |
  |---|---|
  | `RemoveKey` | The key `URI` and the record it points at. The key's incarnation ends. A key that does not exist or holds no record (a placeholder) is left as it is. |
  | `RemoveVersion` | The record version `DocumentID`, written to key `URI`. When the key still points at that version this is `RemoveKey` (the incarnation ends); a version the key has moved on from (kept by onUpdate `retain`) is forgotten and the key stays. |
  | `RemoveAssociation` | The association `DocumentID` on key `URI`. |

- `Removed[i]` tells whether removal `i` found something to remove, that
  is, something a read still returned. Removing what is already gone is not
  an error; an association of an incarnation that has ended is not there to
  remove, whatever is left of it physically. `Removed` is empty unless
  `Applied`.
- The commits of what is removed stay in the log; `GcCommitIDs` decides
  their fate (those of the removed documents that did not ask to be
  retained, and the delete commit itself once nothing retained needs it).
- `Cleanups` are the tokens of the physical removal left to do, one
  `Cleanup` chain each; empty when the backend removed everything.

`Cleanup(token)` runs one bounded step of the removal a token names and
returns the token of the next step, `""` when nothing is left. It changes
nothing a read can observe, whether it runs before or after the keys
concerned are written again. Idempotent: running a token again, or one
whose work is done, is harmless. Tokens are opaque strings, valid only on
the backend that issued them, small enough for a job payload, and never
expire. A backend that issues none treats any token as done. The usecase
enqueues one `RecordCleanup` job per token and runs the chains inline when
there is no job queue (offline tooling).

#### Commit log

| Method | Shape / semantics | RT |
|---|---|---|
| `GetAllCommitLogs(owner)` | eq(owner), order(c_date asc). Dump of one owner. | 1 |
| `QueryCommitLogs(owner?, since, until, limit, order)` | eq(owner)?, range(c_date), order(c_date, id). Replication cursor; `QueryRow.CreatedAt` is `c_date`. Commits flagged for gc are included; ownerless rows carry no CCFS. | 1 |
| `MarkCommitLogsGcCandidateByOwner(owner)` | eq(owner) → set flag; idempotent. Not atomic over the owner's commits (unregister). | - |
| `CountGcCandidates(cutoffID)` | eq(gc_candidate), range(id < cutoff) → count. Offline only. | - |
| `ListGcCandidates(cutoffID, afterID, limit)` | eq(gc_candidate), range(afterID < id < cutoff), order(id). Offline only. | - |
| `DeleteCommitLogs(ids)` | Multi-delete by id; everything anchored to a commit (record and its key, ack state, association, entity) goes with it, and a key removed this way ends its incarnation like a `RemoveKey` (the backend finishes that cleanup itself: there is no one to hand tokens to). Missing ids are ignored; returns the number of commit logs removed. Not atomic over the list (a backend may work in batches) and idempotent, so a partial run is finished by running it again. Offline only; callers invalidate aggregates afterwards. | - |
| `ListCommitLogs(page)` | eq(owner)?, range(id), order(id asc), limit. Offline paging. | - |

#### Records and keys

| Method | Shape / semantics | RT |
|---|---|---|
| `GetSignedDocument(uri)` | get(cckv key), or get(commit by ccfs id) then get(the key it was written to). A ccfs URI resolves to the commit of that id whatever its kind, flagged for gc or not; both `CCKV` and `CCFS` are filled when a key points at the document. `domain.NotFoundError` for a key holding no record (a placeholder included), an unknown id and a ccfs URI of another type. | 1 by cckv, 2 by ccfs |
| `GetSignedDocuments(uris)` | The same over a mixed batch of cckv / ccfs URIs; a URI that resolves to nothing is absent from the map, not an error. The delete sweep checks every destination with one call. | 1 for the cckv URIs plus 2 for the ccfs ones, whatever their number |
| `GetHierarchicalRecordPolicies(uri)` | get(every key on the ancestor chain of uri), root first, uri itself last; one layer for each whose record carries a policy or distributions (CIP-12 §5.3). The ancestor set is computed from the URI. A ccfs URI is mapped to the key pointing at it first (`domain.NotFoundError` when none does). | 1 by cckv, 2 by ccfs |
| `GetHierarchicalRecordPoliciesMulti(uris)` | The same for several cckv URIs with one fetch of the union of their ancestor chains; every input has an entry. A listing checks a whole page with one call. | 1 |
| `GetTimelineRemoval(keyURI)` | get(key) → `(parent key, chunkline item id)`: the item id is the redirect of a reference record, the key otherwise. Both empty for a key that appears in no chunkline body (a placeholder, a top-level key); `domain.NotFoundError` for a key that does not exist. The delete plan reads it before the removal. | 1 |

#### List queries (CIP-5)

Rows carry the signed document and proof of the record (C1). A placeholder
key holds no record: it is listed only by `QueryByParentOrderByKey`, as a
key-only row.

| Method | Shape / semantics | RT |
|---|---|---|
| `QueryByPrefix(prefix, schema?, author?, since, until, limit, order)` | range(key starts with prefix) ∧ eq(schema)? ∧ eq(author)? ∧ range(createdAt, inclusive), order(createdAt, id). Only keys holding a record. `prefix` is a literal: a backend using `LIKE` escapes `%`/`_`/`\`. `QueryRow.CreatedAt` is the createdAt the record was stored with (a reference record carries its target's). | 1. Firestore: 1 for a prefix ending at a `/`; a prefix ending inside a path segment is post-filtered from an overfetched page and reads further pages while the limit is not met. |
| `QueryByParent(parent, schema?, author?, since, until, limit, order)` | eq(parent) ∧ … as above; the owner root (`cckv://<owner>`) is a valid parent and lists top-level keys that hold a record. | 1 |
| `QueryByParentOrderByKey(parent, schema?, author?, sinceKey, untilKey, limit, order)` | eq(parent), range(key, inclusive bounds), order(key, **byte order**). Placeholder keys come back as key-only rows (`Row.CCKV` set, `Document` empty) unless a record filter (schema / author) drops them. `QueryRow.CreatedAt` is unused. | 1 |
| `QueryRecordSubtree(base, includeSelf, afterURI, limit)` | range(key = base ∨ key under base + "/") ∧ range(key > afterURI)?, order(key, byte order), limit when positive. Matching is by path hierarchy, never by string prefix (`item2` is not under `item`). Only keys holding a record. The exact set a range delete removes; the cursor and limit page it one `ApplyDelete` at a time. | 1 (Firestore: the base key is a get in parallel with the range query on the first page when `includeSelf`) |

#### Associations

| Method | Shape / semantics | RT |
|---|---|---|
| `GetAssociatedRecords(targetURI, schema?, variant?, author?, since, until, limit, order)` | eq(target, current incarnation) ∧ eq(schema)? ∧ eq(variant)? ∧ eq(author)? ∧ range(createdAt, inclusive), order(createdAt, id). CCFS is composed in the target owner's namespace. A target without a key lists nothing. | 1. Firestore: the query and a get of the target key (its current incarnation) in parallel; while rows of an ended incarnation of the same key await `Cleanup`, they are dropped from the page and further pages are read until it is full. |
| `ComputeAssociationSummary(targetURI, maxAuthors)` | eq(target, current incarnation) → `AssociationSummary`: the exact count and the earliest `createdAt` of every `(schema, variant)` (no variant counts under `""`), and the distinct authors, or `AuthorsOverflow` instead when there are more than `maxAuthors` (exactly `maxAuthors` is complete; the counts stay exact either way). A target without associations or without a key yields the zero summary. One scan; aggregate layer only (C2). | 1 (Firestore: the scan and a get of the target key in parallel; one document read per association) |

#### Acks (CIP-10)

| Method | Shape / semantics | RT |
|---|---|---|
| `GetAcknowledgeRecords(from \| to, schema?, since, until, limit, order)` | Exactly one of from/to: `from` lists the acker side of that ccid, `to` the target side. eq(from ∨ to) ∧ eq(valid) ∧ eq(schema)? ∧ range(createdAt, inclusive), order(createdAt, id). Each row is the commit the state is anchored to, its CCFS composed in the side owner's namespace. | 1 |
| `ComputeAcknowledgeCounts(from \| to, schema?)` | eq(from ∨ to) ∧ eq(valid) ∧ eq(schema)? → `map[schema]count`. Aggregate layer only (C2). | 1 |

#### Entities

| Method | Shape / semantics | RT |
|---|---|---|
| `GetEntityByCCID(ccid)` | get(ccid) → the entity with the signed document that registered it; `domain.NotFoundError` when unknown. | 1 |
| `GetEntityByAlias(alias)` | eq(alias) → the same. | 1 |

#### Chunkline views (CIP-11)

An item is a direct child of the timeline key that holds a record; its time
is the createdAt the record was stored with and its id the record's
redirect, or its key. Every field a view needs is on the key side.

| Method | Shape / semantics | RT |
|---|---|---|
| `GetChunklineManifest(uri)` | get(key) + eq(parent) order(recordCreatedAt asc) limit 1 → `FirstChunk` (nil without items). `domain.NotFoundError` when the key does not exist. | 1 (Firestore: the query and the get in parallel) |
| `LookupLocalItrs(uris, chunkID)` | per uri: eq(parent) ∧ range(recordCreatedAt < end of chunk), order desc, limit 1 → the chunk of the latest such item. Timelines without a key or without such an item are absent. | 1 on Postgres, SQLite and Spanner (one statement). Firestore: one bounded query per timeline and one batch get of the timeline keys, all in parallel (at most 16 at a time). |
| `LoadLocalBody(uri, chunkID)` | eq(parent) ∧ range(recordCreatedAt), order desc → the items of `chunkID` and the chunk before it, newest first, and at least a page (32) of items when the timeline has older ones. `domain.NotFoundError` when the key does not exist. | 1. Firestore: the first page and a get of the key in parallel, plus a second, sequential query when the two-chunk window holds more than 32 items. |

#### v1 methods that no longer exist

| v1 | v2 |
|---|---|
| `RunInTx`, `RepositoryTx`, every method taking a `tx` | gone; one `Apply*` call per commit (C4) |
| `CreateCommitLog`, `MarkCommitLogGcCandidate` | part of every `Apply*` (`Commit`, `Duplicate`, `GcCommitIDs`) |
| `CreateRecord`, `CreateEntity`, `CreateAssociation` | `ApplyRecord`, `ApplyEntity`, `ApplyAssociation` |
| `Acknowledge`, `UnAcknowledge`, `Acknowledged`, `UnAcknowledged` | `ApplyAck` (`Side`, `Valid`) |
| `DeleteRecordByKey`, `DeleteRecordByDocumentID`, `DeleteAssociation` | `ApplyDelete` (`RemoveKey`, `RemoveVersion`, `RemoveAssociation`) |
| `ListAssociationAuthors`, `ComputeAssociationCounts` | `ComputeAssociationSummary` (one scan, exact) |

Behaviour of the usecases that changed with them:

- Every read of a commit's plan precedes its write, and the redelivery
  check is part of the write. A redelivered commit whose plan reads now
  fail therefore returns that error instead of the silent success v1 gave
  it: an association whose target has been removed since and is not inlined
  in the commit, or a delete of an association by ccfs whose associated
  record is gone.
- The delete policy of a record is evaluated on the stack of the record's
  key however the delete addressed it (`evalDeletePolicy`), which makes
  deleting a retained, superseded version by its ccfs URI work.
- An unack arriving as the first state of a relationship is stored invalid
  on every backend (the v1 Postgres backend stored it as valid).

### Aggregate store (`internal/usecase/aggregate/store.go`)

The cache behind C2. It is not a repository of primary data, but the
record database of Postgres, Spanner and Firestore doubles as one, so its
contract is kept here. Stores: `internal/infra/aggregate/{postgres,spanner,firestore,redis,memcached}`;
the suite every store passes is `internal/infra/aggregate/storetest`
(including N goroutines × M `Add`s on one key losing nothing).

An entry is `Counters{Counts, Minima, Members, AsOf, Deltas}`: named
integer counters, the smallest value offered per counter name, a set of
distinct strings, when the entry was computed from the primary data, and
the number of `Add` calls since. A write is `Delta{Inc, Min, AddMembers}`:
added to `Counts` (may be negative, a missing counter starts at zero),
`Minima[k] = min(existing, v)`, set union. Keys are `name + ":" + target`.

| Method | Semantics |
|---|---|
| `Get(key)` | The entry; false when absent or expired. Empty maps and an empty member set may read back as nil. |
| `PutIfAbsent(key, entry, ttl)` | Stores the entry only when no live one exists and reports whether it did (the loser of a concurrent computation reads the winner's). The stored entry starts with `Deltas` 0. |
| `Add(key, delta)` | Folds the delta into the entry, increments `Deltas`, keeps `AsOf` and the ttl. False when there is no entry, and nothing is created then. Blind: concurrent `Add`s on one key all land, in any order, without the caller retrying and without a read of the entry. A store that cannot see expiry without reading (Firestore, Spanner) may report true for an entry that has expired but is not collected yet; the entry stays absent for `Get` and `PutIfAbsent` replaces it whole. |
| `Invalidate(key)` / `InvalidateDefinition(name)` / `InvalidateAll()` | Drop one entry, every entry of a definition, everything. A missing entry is fine. |

How the usecase uses it (`aggregate/usecase.go`, `record/aggregates.go`):

- `association-counts` (target = the cckv key): one counter per
  `(schema, variant)`, its minimum the earliest `createdAt` in unix
  nanoseconds (the variant order), the authors as members (trusted up to
  1000, "untracked" beyond). A committed association is a delta; a removed
  association is `Inc: -1` when the removal found it (`Removed[i]`), which
  leaves the author and the minimum in place (both only err towards one
  extra query and a stale order). Removing a key invalidates its entry. A
  fresh key (`RecordOutcome.FreshKey`) is seeded with the empty entry.
- `ack-counts` (target = `from:<ccid>` or `to:<ccid>`): one counter per
  schema; an `ApplyAck` whose `WasValid` and `IsValid` differ is `Inc: ±1`,
  a state that merely moved changes nothing.
- A counter at or below zero is a normal state (a blind decrement can land
  on an entry computed after the removal) and is left out of what readers
  report.
- Writers never read. `MaxDeltas` (`backends.aggregate.maxDeltas`, default
  10000) is enforced by the reader: `Get` drops and recomputes an entry
  whose `Deltas` exceed it. A computation that started before a write
  became part of the primary data may have missed it and gets the delta
  too (`AsOf` against the write's receipt time).
- A dump replay (`import-commitlog`) invalidates instead of adding deltas.
- Readers of one entry share reads (`aggregate/coalesce.go`). Clients
  re-read a target's counts and their own associations when an event about
  it arrives, so every session subscribed to a popular target asks for the
  same entry within milliseconds. At most one `Get` of a key is in flight
  per process; callers that arrive meanwhile wait for the read that starts
  when it ends and share its result (and its one computation when the entry
  is missing). A caller is never given a read that began before it asked,
  so a write folded in before a request is always visible to it. The shared
  `Counters` must not be modified by a caller.

### `usecase.EntityMetaRepository` (`internal/usecase/entitymeta.go`)

| Method | Shape / semantics |
|---|---|
| `SaveMeta(meta)` | upsert by ccid (inviter and info). |
| `GetMeta(ccid)` | get(ccid); `domain.NotFoundError` when absent. |
| `UpdateMetaInfo(ccid, info)` | update info only; `domain.NotFoundError` when absent. |
| `DeleteMeta(ccid)` | delete by ccid; idempotent. |
| `ListMetas(owner?)` | all, or eq(ccid). Offline dump. |

### `server.Repository` (`internal/usecase/server/server.go`)

| Method | Shape / semantics |
|---|---|
| `Resolve(identifier, hint)` | get(fqdn) or eq(csid) in the local cache; on a miss fetches the well-known through the client (csid needs `hint`) and upserts it by fqdn. |
| `List()` | all cached well-knowns (rows with an empty well-known are skipped). |

### `notification.Repository` (`internal/usecase/notification/notification.go`)

| Method | Shape / semantics |
|---|---|
| `Subscribe(subscription)` | upsert by `(vendorID, owner)`. |
| `List()` | all. |
| `Get(vendorID, owner)` | get; `domain.NotFoundError` when absent. |
| `Delete(vendorID, owner)` | delete; idempotent. |

### `abuse.Repository` (`internal/usecase/abuse/abuse.go`)

| Method | Shape / semantics |
|---|---|
| `CreateAbuseReport(report, reporter, ip)` | append-only insert; the report's id is backend-internal (C3: it never leaves the repository). |

### Identifiers crossing the contract (C3)

Nothing returns a backend-generated id. The `int64` values that do cross are
chunk ids (`unix / 600` time buckets), counts, and the number of commit logs
deleted. Cleanup tokens are backend-generated strings, but they are opaque
to everything outside the backend that issued them and identify no row a
caller can address.

## Conformance (C7)

Both suites run from every backend's `record` test package
(`suite_test.go`: `Test<Backend>Suite`, `cip_test.go`: `Test<Backend>CIP`)
and in CI (`.github/workflows/test.yml`: Postgres and SQLite in the `test`
job, Spanner and Firestore in jobs of their own).

- `ciptest` builds the real record and chunkline usecases over the backend
  with in-memory fakes for the job queue, signals, kvs, the policy
  evaluator, peer servers and the aggregate store, and runs 36 cases:
  record accept-if-newer, redelivery, onUpdate forget / retain, import, a
  policy denial storing nothing; entity accept-if-newer; association
  listing, uniqueness, a missing target, counts, delete-then-recreate; the
  ack state machine and its import; single, by-ccfs, retained-version,
  association and range deletes (with self, children only, paged through
  the continuation job, offline without a queue), cleanup changing nothing,
  the reference sweep; queries by parent, prefix, key and owner root;
  chunkline manifest / itr / body; the replication feed. Not covered: alias
  updates, a range delete of more than one continuation page, the
  federated paths (delivery to and from another server), and the real
  policy evaluator (a switch stands in for it).
- `repotest` runs the suites `Record`, `Association`, `Ack`, `Delete`,
  `Chunkline`, `Residence`, `Replication`, `Maintenance` and `World`
  against the contract methods only. Its `Inspector` is the two things no
  contract method can do: `SetCommitCDate` (pin a receipt time, so the
  replication suite does not depend on the store's clock) and
  `AbuseReportCount` (`abuse.Repository` is append-only and has no read).
- What a backend does beyond the contract has its own tests beside it: the
  retry of contention (`tx_retry_test.go`, `tx_abort_test.go`), Firestore's
  RPC budgets and incarnation paging (`budget_test.go`,
  `incarnation_test.go`), Spanner's mutation counts
  (`mutation_limit_test.go`), the pinned index sets and the idempotent
  migrations.

## Backend notes

What each backend does with the contract that the other backends do not
have to think about. None of it changes the contract's meaning.

### Postgres and SQLite (`internal/infra/repository/postgres`, `.../sqlite`)

- Every `Apply*` is one transaction on one pooled connection with the retry
  loop inside (up to 8 attempts with backoff; Postgres on
  serialization_failure / deadlock_detected, SQLite on BUSY / LOCKED of its
  `IMMEDIATE` transaction). The usecase's reads happen before it, so a
  commit never holds a connection while waiting for another. The v1 rule
  that a transaction needs a second connection for its reads is gone, and
  with it `backends.postgres.maxConcurrentTx`; a config that still sets
  `maxConcurrentTx` loads and the key is ignored.
- Transactions do not take the whole pool. A transaction keeps its
  connection for several round trips, a read for one, and `database/sql`
  gives a freed connection to any waiter, so a burst of commits would take
  every connection and leave each read waiting among them. Postgres admits `maxOpenConns / 2` `Apply*`
  transactions at a time (derived from the pool, no setting; the wait is
  `concrnt_record_tx_slot_wait_seconds` and holds no connection). SQLite
  has one writer, so its `Apply*` calls take turns in-process before they
  begin: the 8 pooled connections stay with the readers WAL lets run beside
  the writer instead of being parked in `BEGIN IMMEDIATE`.
- A `Stale` or `Duplicate` outcome rolls the transaction back, which is how
  it leaves no trace.
- C11 is `ON DELETE CASCADE`: an association references the row of its
  target key, removing the record removes the key row and that removes the
  associations, all inside the `ApplyDelete` transaction. A key written
  again later is a new row and starts without any. `ApplyDelete` issues no
  cleanup tokens and `Cleanup` returns `""` for any token.
- The signed document is not embedded on the key row: a listing joins
  `record_keys`, `records` and `commit_logs` in one statement, which is one
  round trip already.
- The unique index on `associations."unique"` is table-wide, not per
  target. The contract's scope (the same `Unique` on the same incarnation
  of a target) is narrower; the two agree because the usecase's `Unique`
  includes the target and the cascade removes an ended incarnation's rows.
  A caller passing the same `Unique` for two different targets would get
  `Stale` here and `Applied` on Spanner and Firestore; neither suite does.
- Keys link to their parent by the parent row's serial id. Removing a key
  that has children deletes its row; the children keep the dangling id and
  drop out of the parent listings (`QueryByParent*`, chunkline) until they
  are written again. Spanner and Firestore link by URI, so there the
  children stay listed. Neither suite asserts either behaviour.
- The aggregate store on Postgres is one row per entry with jsonb counters;
  `Add` is a single `UPDATE` (writers of one key queue on the row lock for
  the length of that statement and its commit: measured at 32 writers on
  one key, 0.4 ms per `Add` with 50 members and 1 ms with 900, i.e. a
  thousand or more per second and key).
  Migration step 6 drops and recreates
  `aggregate_entries` in the new shape. SQLite has no aggregate store of
  its own and defaults to Redis.
- `postgres/README.md` has the index notes (prefix scans, collation).

### Spanner (`internal/infra/repository/spanner`)

- GoogleSQL through the native client. Every `Apply*` is one
  `ReadWriteTransaction`: one statement reads everything the decision
  depends on (and locks it), the method decides, and the writes go out as
  buffered mutations with the commit. Nothing is written before the
  decision, so a `Stale` or `Duplicate` transaction has nothing to leave
  behind. `ABORTED` is retried by the client. Reads outside a transaction
  are single-use strong reads.
- The one exception to "mutations only": an `ApplyRecord` that forgets the
  version it replaces. Deleting a record takes the key row pointing at it
  along, and for buffered mutations Spanner decides that on the rows as
  they were before the transaction. That case is written as an ordered DML
  batch (the key row moves on first, then the old record goes), which costs
  one more round trip inside the transaction.
- `commit_logs.c_date` is a commit timestamp (`PENDING_COMMIT_TIMESTAMP()`),
  monotonic within the database, so `QueryCommitLogs`'s `(c_date, id)`
  order is exact. The column cannot be read in the transaction that writes
  it; nothing in the contract does. `Inspector.SetCommitCDate` writes an
  explicit past timestamp, which the column allows.
- No serial ids: `record_keys` is keyed by `uri` with a `parent_uri`
  column, associations reference the target by `uri`. `records`, `acks`,
  `ackeds` and `associations` are interleaved in `commit_logs` (`ON DELETE
  CASCADE`); `entities.document_id` and `record_keys.record_id` cascade
  through foreign keys.
- C11: a key row carries `gen`, the id of its incarnation, written when the
  row is created; an association carries the `target_gen` it was created
  on. Reads and uniqueness only see associations whose `target_gen` is the
  `gen` of the key row now at `target_uri`. Removing a key deletes the key
  row only. Schema batch 4 (`association_incarnations`) adds the two
  columns and re-keys every association index on `(target_uri,
  target_gen, ...)`, in place and without a backfill: existing rows get
  `''` on both sides, which matches. Batch 5 drops the foreign key
  `fk_associations_target` (the cascade from a key to its associations);
  GoogleSQL has no `DROP CONSTRAINT IF EXISTS`, so the batch carries a
  `Pending` probe on `INFORMATION_SCHEMA` and is recorded without running
  when the constraint is already gone.
- Cleanup tokens are `associations:<gen>:<uri>`, issued only for removed
  keys that have associations. A `Cleanup` step deletes up to 2,000
  association rows of that incarnation in one commit and returns the same
  token until a step finds less than a page. It leaves an incarnation whose
  key row still exists alone, and a token of another format is an error.
- 80,000 mutations per commit, index entries and cascaded rows included
  (Cloud Spanner enforces it, Spanner Omni does not but reports the count).
  The repository puts the count of every write on its span as
  `spanner.mutation_count`, and `mutation_limit_test.go` holds it against
  the limit: removing a key with 20,000 associations is 18 mutations (the
  test asserts under 20; the v1 cascade would have been 100,000), a
  `Cleanup` step exactly 10,000 (2,000 rows with four index entries each),
  a 500-key `ApplyDelete` page 6,010 (asserted under 10,000). What is
  left of the limit: one removal costs about a dozen mutations whatever
  hangs off the key, so it bounds the removals of one `ApplyDelete`, not
  the associations of a key.
- `MarkCommitLogsGcCandidateByOwner` is partitioned DML: an owner's log has
  no upper bound, the partitions commit separately, and the statement is
  idempotent.
- `DeleteCommitLogs` deletes 100 commits per transaction and then removes
  the associations of the keys that batch ended, page by page. A run
  interrupted between a batch and its cleanup leaves those association rows
  in place for good: no read returns them and no later run finds them, so
  they cost their storage and nothing else.
- The aggregate store is a delta log: `aggregate_entries` holds an entry as
  computed and `aggregate_deltas`, interleaved in it, one row per `Add` (an
  insert that reads nothing; the parent row it requires is the "live entry"
  check). `Get` reads the entry with its delta rows in one statement (a
  seek of the entry row and of the rows interleaved under it) and folds
  them; once there are more than 64 it replaces them by one delta row
  holding their sum and the number of `Add`s it stands for. Expired entries
  go with their deltas by the row deletion policy. Schema batch 3 drops and
  recreates both tables, batch 6 drops `aggregate_entries.deltas` (the
  count of folded deltas, now in the sum row).
- No write of the aggregate store reads in a transaction. A transaction
  that reads a row and then writes it has to turn its shared lock into an
  exclusive one, and several doing so on one row abort each other: with
  `PutIfAbsent` and the fold written that way, the readers of a popular
  target (all finding the entry missing, or all finding it due for a fold)
  made 16 % of all commits end `ABORTED` and waited on each other's locks
  (load test of 2026-11-11). So the entry row is written once, by an insert
  that fails `ALREADY_EXISTS` for every caller but one, and is never
  updated; the fold is one commit of mutations that updates every row it
  folds before deleting it, so it fails as a whole (`NOT_FOUND`, a no-op)
  when another fold took one of its rows first, and a process folds a key
  once at a time. Only the replacement of an entry that has expired and is
  not collected yet is a read-write transaction. These refused commits are
  the expected outcome of a race and show up as errors on the `Commit`
  span; `ABORTED` there is what should stay at zero.
- Every statement on `associations` names its index (`FORCE_INDEX`), so the
  range it reads, and inside `ApplyAssociation` the range it locks (one
  entry of the unique index), does not depend on the optimizer's
  statistics. Spanner reads an index forwards only: the listing indexes end
  with `(created_at DESC, id DESC)`, which makes the default order (`desc`)
  a seek that reads the page; `asc`, and a schema filter without a variant,
  read the whole range the filters leave and sort it. The index beginning
  with `author` serves the listing a client issues for every message it
  shows (its own associations on the target). Schema batch 7 builds the
  three descending indexes and drops the ascending ones and `(schema,
  variant, author)`; an association still has four index entries.
- Time-ordered primary keys (CDIDs) are written to monotonically increasing
  key ranges; no sharding at this scale. Adding a shard column later is a
  DDL batch plus the key predicates, nothing in the contract.
- Spanner Omni accepts only `projects/default/instances/default` as the
  instance path; the evaluation license runs 90 days from the first start
  of a data directory.

### Firestore (`internal/infra/repository/firestore`)

- Stored model (the package comment of `firestore/docs.go` is the
  reference). One top-level collection per kind; URI-keyed documents use a
  hash of the URI as id (ids may not contain `/`) and carry the URI as a
  field.
  - `commits/{cdid}`: the commit log, written once. `anchor` / `anchorID`
    name what the commit stored (the collection, and the cckv key / state
    id / ccid in it), so a ccfs lookup and the gc find the dependent by id.
  - `record_keys/{hash(uri)}`: one document per key, carrying `gen` (its
    incarnation) and, when it holds a record, the record's `document` and
    `proof` next to the fields the listings filter and order by. Keys link
    to their parent by `parentURI` (the owner root `cckv://<owner>` for
    top-level keys) and carry every strict ancestor in `ancestors`.
  - `retained_versions/{cdid}`: a marker for a version that asked to be
    retained and has been replaced; the version itself is served from
    `commits` by its ccfs URI.
  - `associations/{cdid}` with `targetGen`, `document`, `proof`;
    `association_uniques/{hash(targetURI, unique)}` stands in for the
    unique constraint and carries the `targetGen` it belongs to.
  - `acks` / `ackeds/{hash(from, to, schema)}` (the document and proof
    while the state is valid) and `entities/{ccid}` embed the signed
    document they are anchored to.
  - There is no `records` collection any more.
- Every `Apply*` is one transaction that reads once, a single batch get of
  documents whose ids follow from the arguments (the commit, the key and
  its ancestor chain, the target key, the uniqueness marker, the state, per
  removal the key and the retained marker or association), and then
  writes. No query runs inside a transaction, so a commit locks only the
  documents it names. `budget_test.go` pins this (one `BatchGetDocuments`,
  one `Commit`, nothing else) and the RPCs of every read. The SDK re-runs
  the closure on ABORTED, up to 8 attempts. A `Stale` or `Duplicate`
  transaction buffers no write.
- Reads are plain strong reads outside any transaction. Where the "RT"
  column says "in parallel", the get and the query are issued together.
- Placeholder keys hold none of the record fields: a query that orders by
  `recordCreatedAt` or filters on `schema` / `author` leaves them out by
  itself, which is the `IS NOT NULL` of the SQL backends.
  `QueryRecordSubtree` orders by `uri` then `recordCreatedAt` only for that
  reason. `QueryByPrefix` matches the directory of the prefix with
  `array-contains` on `ancestors` and filters the last path segment
  client-side; a prefix without an owner and a path has no indexed form and
  is rejected.
- C11: `gen` is random, set when the key document is created (placeholder
  included), kept by overwrites and gone with the document. An association
  is live only while the key document carries its `targetGen`; a uniqueness
  marker counts only while its `targetGen` is the target's current one (one
  left by an ended incarnation is overwritten). `RemoveKey` deletes the key
  document alone.
- Cleanup tokens are `gen1:<gen>,<gen>,...`, up to 30 gens per token (the
  cap of an `in` filter), issued for every removed key whether it has
  associations or not. A `Cleanup` step removes up to 100 associations of
  those incarnations with their markers, outside any transaction, and
  returns the same token until a step finds less than a page. A marker is
  deleted only while it still carries a gen of the token and only if it is
  unchanged since it was read (it may belong to a later incarnation by
  then); markers go before associations, so a step that fails halfway
  leaves associations for the next run, never a marker without one. A token
  of another format is an error.
- `DeleteCommitLogs` finds what each commit anchors by id, deletes those
  dependents (each only if it still points back at its commit and is
  unchanged since it was read), runs the cleanup of the keys it removed to
  the end, and deletes the commits last: a key never points at a commit
  that is gone. Every step is idempotent, but a run that dies during the
  inline cleanup has already deleted the keys, so a re-run no longer finds
  the gens: the associations left are invisible to every read and stay as
  garbage.
- `cDate` is a server timestamp set at commit; it is receipt time, not a
  strict commit sequence (as on Postgres). Timestamps have microsecond
  precision.
- Composite indexes are not inferred: `firestore/indexes.go` registers
  every query shape, generates `deploy/firestore/firestore.indexes.json`
  (37 composite indexes, pinned by `indexes_test.go`; `-args
  -update-indexes` regenerates) and probes each shape at server start. The
  emulator enforces none of them, so a shape added without an index only
  fails in production. Embedded documents, proofs and the other fields no
  query uses have their single-field indexes exempted in the same file.
- The aggregate store is one document per entry in `aggregate_entries`;
  `Add` is one `Update` of field transforms (`Increment`, minimum,
  `ArrayUnion`) under the exists precondition, without a transaction. Only
  `PutIfAbsent` runs one. Expired documents are ignored on read and removed
  by the TTL policy on `expiresAt`.
- Data written by the v1 backend cannot be read by this one (no embedded
  documents, no `gen`, no `anchor`); see `MIGRATION.md`.
- Time-ordered document ids (CDIDs) cap sustained writes to `commits` /
  `associations` around 500 per second.

## Changing the contract

1. Change (or add) the case in `ciptest` when the change is visible at the
   protocol level, and the suite in `repotest` for the method's own
   semantics, first. A deliberate change of behaviour is called out in the
   PR as `behavior:`.
2. Implement it in every backend in the same PR. Do not merge a state
   where only one backend passes.
3. If a method issues a new query shape, record it in the method table
   with its budget and add the index to every backend's own schema and
   pinned index test (and the RPC count to Firestore's `budget_test.go`).
4. Keep the doc comment in `usecase.go` and the method table here saying
   the same thing.
5. Offline tooling (`conctl`) builds the repositories it needs over the
   selected backend; a command that speaks SQL directly is documented as
   Postgres-only and refuses to run on any other backend.

## Running the suites

```sh
go test ./internal/infra/repository/postgres/... -run 'Suite|CIP' -count=1   # Postgres via dockertest
go test ./internal/infra/repository/sqlite/... -run 'Suite|CIP' -count=1     # SQLite on a temp file, no Docker
go test ./internal/infra/repository/spanner/... -run 'Suite|CIP' -count=1    # Spanner Omni via dockertest (CONCRNT_TEST_SPANNER_ENDPOINT reuses a running one)
go test ./internal/infra/repository/firestore/... -run 'Suite|CIP' -count=1  # Firestore emulator via dockertest (CONCRNT_TEST_FIRESTORE_HOST reuses a running one)
go test ./internal/infra/aggregate/... -count=1                              # the aggregate stores (storetest)
```

`repotest` and `ciptest` must not import a backend package:
`go list -deps ./internal/infra/repository/repotest ./internal/usecase/record/ciptest | grep 'repository/\(postgres\|sqlite\|spanner\|firestore\)'`
stays empty.
