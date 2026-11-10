# Repository contract v2 — design

Status: design, to be implemented on `feat/repository-contract-v2`. When the
implementation is complete this file is folded into `CONTRACT.md` and removed.

## Why v1 is being replaced

v1 was the method surface of the Postgres implementation turned into an
interface. Building and load-testing four backends on it showed five flaws:

1. **Writes are table operations composed inside a closure transaction**
   (`RunInTx` + `CreateCommitLog` + `CreateRecord` + ...). That asks every
   backend for interactive serializable transactions with read-your-writes,
   the strongest capability there is. Firestore had to build a write-buffer
   overlay (`tx.go`); Postgres needed a second connection per transaction for
   the closure's reads (pool exhaustion, the commit deadlock).
2. **"No joins" (C1) only constrained single queries.** The signed document,
   which every read returns, lives in `commits` and is re-fetched for every
   listed row: two round trips and 2N document reads per page.
3. **The aggregate store is an opaque byte blob with compare-and-swap**
   (`Apply(key, fn(old []byte))`). Every backend serialises writers per key;
   `aggregate_entries` was the top lock wait on Spanner and the collapse
   point of Firestore at 1000 DAU.
4. **Cascades are synchronous and unbounded.** Deleting a key removes its
   associations in the same transaction (a range query inside the
   transaction on Firestore, the mutation cap on Spanner).
5. **`repotest` pins Postgres behaviour method by method** through a
   white-box `Inspector`, which fixes the internal model of every backend.

## Principles

| # | Principle | Replaces |
|---|---|---|
| P1 | **A write is one commit-level operation.** The usecase reads and evaluates policy first (committed state, no transaction), then calls exactly one `Apply*` method. That method is atomic, idempotent on the commit id, and its read set and write set are known from its arguments (bounded, no data-dependent fan-out). There is no transaction handle in the contract. | C4 |
| P2 | **A read returns complete rows from one access path.** Whatever a read method returns (the signed document included) is stored with the entity the access path lands on. Every read method has a round-trip budget in the method table; a list read is one query, a point read one get. The commit log is written once and read only by id (ccfs resolve, dedupe), by the replication cursor and by offline tooling. | C1 |
| P3 | **Aggregates are caches of commutative counters.** A cache entry is a set of named integer counters, per-counter minima and a bounded member set. A write folds in with blind, commutative deltas (`Add`); no read-modify-write, no per-key serialisation. Recomputation from primary data stays the source of truth (`Compute*`). | C2 |
| P4 | **Dependents disappear atomically from reads and physically later.** When a key is removed, everything attached to it (associations) stops being returned by every read in that same operation; the physical removal is bounded follow-up work the backend hands back as opaque cleanup tokens. A backend that cascades synchronously within its limits simply returns no tokens. | cascades in C4 / backend notes |
| P5 | **Conformance is defined at two heights.** `ciptest` drives the real usecases (commit, query, associations, acks, delete, chunkline) and asserts protocol-visible behaviour: that is what "implements concrnt" means. `repotest` checks each contract method black-box, only through contract methods. Backend internals (collections, columns, whether a `records` table exists) are never asserted. | C7 |

Unchanged from v1: C3 (no auto-increment), C5 (indexes are the backend's
own), C6 (explicit uniqueness), C8 (migration is log replay), C9 (interfaces
split along backend boundaries), C10 (test code stays out of production
packages), and the byte-order key ordering section.

## `record.Repository` v2

Package `internal/usecase/record`. `RunInTx`, `RepositoryTx` and every
method taking a `tx` are removed.

### Write operations (P1)

```go
// Commit is the commit-log entry every write carries.
type Commit struct {
    ID       string        // CDID of the signed document
    IP       string
    Document string
    Proof    concrnt.Proof
    Owner    string        // commit-log owner (CIP-3 §3.1)
}

type ApplyStatus int
const (
    Applied   ApplyStatus = iota // everything the operation describes is stored
    Stale                        // lost accept-if-newer / uniqueness: NOTHING is stored, not even the commit log
    Duplicate                    // the commit id is already in the log: nothing changed (redelivery)
)

ApplyEntity(ctx, EntityCommit) (ApplyStatus, error)
ApplyRecord(ctx, RecordCommit) (RecordOutcome, error)
ApplyAssociation(ctx, AssociationCommit) (ApplyStatus, error)
ApplyAck(ctx, AckCommit) (AckOutcome, error)
ApplyDelete(ctx, DeleteCommit) (DeleteOutcome, error)
Cleanup(ctx, token string) (next string, err error)
```

- `EntityCommit{Commit, CCID, Alias *string, Domain string, CreatedAt}`:
  accept-if-newer on `CreatedAt` per ccid.
- `RecordCommit{Commit, Key, Author, Schema, OnUpdate *string, Policies *string, Distributions []string, Redirect *string, CreatedAt}`:
  accept-if-newer on `CreatedAt` per key (equal or older is `Stale`);
  creates missing ancestor placeholders; unless the *previous* version asked
  for `retain`, the previous version is forgotten and its commit flagged for
  gc. `RecordOutcome{Status, FreshKey bool, PreviousID string}`: `FreshKey`
  is true when the key held no record before (the usecase seeds the empty
  count cache from it instead of a pre-read), `PreviousID` is the document
  id it replaced ("" when fresh).
- `AssociationCommit{Commit, Target, Author, Schema, Variant *string, Unique string, CreatedAt}`:
  `Stale` when `Unique` already exists (C6); `domain.NotFoundError` when the
  target key does not exist. The association belongs to the *incarnation* of
  the target key that exists now (P4).
- `AckCommit{Commit, Side AckSide /* AckerSide | TargetSide */, From, To, Schema, Valid bool, CreatedAt}`:
  one method for the four v1 methods; accept-if-newer on `CreatedAt` per
  `(side, from, to, schema)`. `AckOutcome{Status, WasValid bool, IsValid bool}`
  so the caller can tell a validity flip from a mere state move (the count
  cache gets a real delta instead of an invalidation).
- `DeleteCommit{Commit *Commit, Removals []Removal, GcCommitIDs []string}`.
  `Commit` is nil for the continuation pages of a range delete (the commit
  was recorded by the first page). `Removal{Kind /* RemoveKey | RemoveVersion | RemoveAssociation */, URI string, DocumentID string}`:
  `RemoveKey` deletes the record a cckv key points at and the key (v1
  `DeleteRecordByKey`), `RemoveVersion` a record version by document id (v1
  `DeleteRecordByDocumentID`), `RemoveAssociation` an association by id. A
  removal of something already gone is a no-op. `GcCommitIDs` are flagged
  for gc in the same operation (the removed documents' commits unless
  retained, and the delete commit itself on the final page). The usecase
  computes the whole plan before calling (targets, policy, the reference
  sweep, retention) from committed reads; at most `rangeDeleteSyncLimit`
  removals per call. `DeleteOutcome{Status, Cleanups []string}`.
- `Cleanup(token)` runs one bounded step of deferred physical removal (P4)
  and returns the token of the next step, "" when done. Idempotent. The
  usecase enqueues a `RecordCleanup` job per token and runs the steps inline
  when there is no job queue (offline tooling). Tokens are opaque to the
  usecase.

Atomicity: each `Apply*` either stores everything or nothing. Contention is
retried inside the backend; the method returns only a final result. `Stale`
and `Duplicate` leave no trace, so the v1 "roll the commit log back on a
no-op" (`errCommitNoop`) is the operation's own semantics.

Usecase shape (`commit.go` and friends): `plan` (reads, policy, validation,
building the `*Commit` value and the post-commit actions as functions of the
outcome) then `apply` (one repository call) then post-commit actions. No
repository read happens after the `Apply*` call except inside post-commit
actions.

### Reads (P2)

The v1 read methods and their CIP semantics stay (query by prefix / parent /
key order, subtree, associations, acks, policies, entities, chunkline,
commit-log paging). Changes:

- Budgets become part of the method table. Target: every list method is
  **1 round trip**, every point read **1**, `GetSignedDocument(s)` by ccfs
  **2** (commit by id + the key pointing at it), chunkline body **1**
  (`NotFound` for a missing timeline may cost a parallel get, not a
  sequential one), `LookupLocalItrs` one bounded query per timeline in
  parallel on non-SQL backends.
- `ListAssociationAuthors` and `ComputeAssociationCounts` merge into
  `ComputeAssociationSummary(ctx, target, maxAuthors int) (AssociationSummary, error)`
  returning counts per `(schema, variant)`, each variant's earliest
  `createdAt`, and the distinct authors or `AuthorsOverflow` when there are
  more than `maxAuthors`. One scan; exact (v1's `limit*4` cut-off could miss
  authors).
- `GetTimelineRemoval` stays (the delete plan needs it before the removal).
- Methods no production or tooling code calls are deleted rather than
  ported. Optional filters are kept only where a CIP parameter maps to them.
- A row whose commit has been garbage-collected never shortens a page: the
  document is on the row.

### Dependents and incarnations (P4)

A key has an incarnation that starts when a record first occupies it (or
occupies it again after a removal) and ends when the key is removed.
Overwrites do not change it. Associations are bound to the incarnation they
were created on; `GetAssociatedRecords`, `ComputeAssociationSummary` and
uniqueness only see associations of the current incarnation. How a backend
realises this is its business:

- SQL backends may keep `ON DELETE CASCADE` (synchronous, no tokens), or
  switch to a generation column where the cascade is too large (Spanner's
  mutation cap).
- Firestore stores a random `gen` on the key and `targetGen` on the
  association and its uniqueness marker; `RemoveKey` deletes the key
  document only and returns a cleanup token naming `(uri, gen)`; `Cleanup`
  deletes that incarnation's associations and markers a page at a time
  outside any transaction.

`DeleteCommitLogs` (gc) keeps its v1 semantics (offline, not atomic over the
list, idempotent).

## Aggregate store v2 (P3)

Package `internal/usecase/aggregate`.

```go
type Counters struct {
    Counts   map[string]int64 // counter name -> value
    Minima   map[string]int64 // counter name -> smallest value offered (variant order)
    Members  []string         // distinct members (authors), unordered
    AsOf     time.Time        // when computed from primary data
    Deltas   int64            // Add calls since then
}

type Delta struct {
    Inc        map[string]int64 // added to Counts (may be negative)
    Min        map[string]int64 // Minima[k] = min(existing, v)
    AddMembers []string         // set union
}

type Store interface {
    Get(ctx, key) (*Counters, bool, error)
    PutIfAbsent(ctx, key, Counters, ttl) (bool, error)
    // Add folds delta into a live entry; false when there is none. Commutative
    // and blind: concurrent Adds on one key all land, in any order, without
    // the caller retrying and without serialising on the key.
    Add(ctx, key, Delta) (bool, error)
    Invalidate / InvalidateDefinition / InvalidateAll
}
```

- Association counts: counter name `schema + "\x1f" + variant`, `Minima`
  holds the earliest `createdAt` (unix nanos) per counter = variant order,
  `Members` the authors. More than `maxTrackedAuthors` members reads as
  "untracked". A removal is `Inc: -1`; the author set then may over-report
  (it only feeds the "can this listing have rows" shortcut, where a false
  positive costs one query and a false negative would hide rows), and a
  counter at zero is dropped by the reader.
- Ack counts: counter name = schema; `ApplyAck`'s `WasValid`/`IsValid` gives
  `Inc: ±1`.
- `MaxDeltas` is checked by the reader (`Get` sees `Deltas`) which
  invalidates and recomputes; writers never read.
- The `AsOf` / `receivedAt` rule of `Touch` (a computation that started
  before the write may have missed it and gets the delta too) is kept.
- Native forms: Firestore `Update` with `Increment` / `FieldTransformMinimum`
  / `ArrayUnion` under an exists precondition, no transaction; Redis one Lua
  script over a hash (the in-process same-key mutex goes away); Postgres /
  SQLite / Spanner one statement (or a row per counter with
  `ON CONFLICT DO UPDATE SET n = n + EXCLUDED.n`); memcached may keep an
  internal gets/cas loop (it has no multi-field atomics) as long as the
  interface behaviour holds.
- `storetest` gains a concurrency test: N goroutines × M `Add`s on one key
  lose nothing.

## Backend notes

### Firestore

- `record_keys/{hash(uri)}`: adds `document`, `proof`, `gen`; a listing
  reads only this collection. `associations/{cdid}`, `acks`, `ackeds`,
  `entities` likewise carry `document` and `proof`. Their single-field
  indexes are exempted. Whether `records/{cdid}` survives is the backend's
  call (retained old versions are served from `commits` by ccfs).
- Each `Apply*` is one `RunTransaction` whose reads are a single `GetAll`
  of ids derived from the arguments (commit, key + ancestors, target key,
  association, uniqueness marker, ack state), followed by the writes. No
  query runs inside a transaction. `tx.go` (the overlay) is deleted.
- `aggregate_entries` uses field transforms; `deploy/firestore` indexes are
  regenerated from the shapes.
- A budget test counts RPCs per contract read with a gRPC interceptor
  against the emulator and pins the table above.

### Postgres / SQLite / Spanner

- `Apply*` is one internal transaction (retry loop inside); with no
  closure reads the Postgres second-connection rule and `txSlots` sizing
  can go. Embedding the document on the key row is optional for SQL
  backends (a join is one round trip already); the budget is what counts.
- Spanner: `Apply*` as one `ReadWriteTransaction`; aggregate rows no longer
  take a read lock.

## Conformance (P5)

- `internal/usecase/record/ciptest` (name ends in `test`, C10): a suite
  taking a constructor for `record.Repository` + the other repositories,
  building the real usecases with in-memory fakes for queue / pubsub / kvs /
  policy where needed, and asserting CIP behaviour: accept-if-newer,
  redelivery, association uniqueness, ack state machine, single and range
  delete (including the continuation job and cleanup), reference sweep,
  query pagination in all orders, chunkline manifest / itr / body, counts
  after writes and deletes, delete-then-recreate hiding old associations.
- `repotest` is rewritten against the v2 interface, black-box: assertions go
  through contract reads only. The `Inspector` shrinks to what cannot be
  observed otherwise (overriding receipt time for replication-order tests);
  anything it exposed that a contract read can show is asserted through the
  read instead.
- Both suites run from every backend's `suite_test.go` and in CI.

## Migration of stored data

The stored shape of Firestore changes (embedded documents, `gen`), and
aggregate tables change everywhere. Aggregates are caches: the old
table/collection is dropped by the migration and refills. For Firestore the
supported path is the existing one, log replay into an empty database
(C8); no in-place converter. SQL backends keep their tables unless they opt
into embedding; if they do, an explicit migration backfills it.

## Implementation stages

Each stage leaves `go build ./... && go vet ./...` clean and the suites of
the backends it touched passing.

1. Aggregate store v2: interface, `aggregate` usecase, the five stores,
   `storetest`, `usecase/record/aggregates.go`.
2. Contract core: `record.Repository` v2, usecase restructuring (plan /
   apply), `RecordCleanup` job, `ciptest`, `repotest` rewrite, Postgres and
   SQLite backends, conctl.
3. Spanner and Firestore backends (independent of each other).
4. `CONTRACT.md` rewritten from this file, `MIGRATION.md`,
   `deploy/firestore/README.md`, this file removed.
