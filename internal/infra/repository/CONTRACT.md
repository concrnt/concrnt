# Repository contract

The usecase packages (`internal/usecase/*`) declare the `Repository`
interfaces; every backend under `internal/infra/repository/<name>`
implements exactly those, and `repository.Open` picks one from
`backends.database`. The contract is the interfaces plus the invariants
below. What a backend does internally (SQL joins, cascades, serial ids) is
its own business as long as the contract's meaning never depends on it.

## Principles

| # | Principle | Meaning |
|---|---|---|
| C1 | No joins assumed | Every read method must be satisfiable as "one collection, equality filters, at most one range condition, an ordering" or as a get by id(s). Fields a query needs live on the key side, denormalised. |
| C2 | Aggregates are caches | Counts are derived values recomputable from the primary data. Request paths read them through an aggregate layer; writes only adjust an entry that already exists. |
| C3 | No auto-increment in the contract | Contract identifiers are CDIDs, URI hashes and client-generated random ids. A backend's serial ids never leak out. |
| C4 | Transactions are `RunInTx(ctx, fn)` | `fn` may run more than once (contention retry), reaches the store only through the `tx` it is handed, and has no side effects of its own. Keep one transaction's writes bounded (Firestore caps at 500); bulk deletes go to the job queue. |
| C5 | Query shapes are declared | A backend lists the query shapes it issues in a shape registry and a test checks its indexes cover them (phase 2). |
| C6 | Uniqueness is explicit | Contract-level uniqueness (association `unique`, ack `(from, to, schema)`) is "a second write of the same key is a no-op / rejected", verified by repotest, however the backend enforces it. |
| C7 | repotest defines the contract | A backend that does not pass `repotest` does not implement the contract. Change repotest before changing the contract. |
| C9 | Split interfaces only along backend boundaries | A separate `Repository` interface exists only where the data behind it could live on a different backend from the rest (e.g. subscriptions on one store, records on another). Methods that read the same tables as an existing interface belong on that interface, even when only offline tooling calls them; `conctl` therefore uses `record.Repository` for gc / dump, and the record, entity and chunkline methods are one interface implemented by one package (`postgres/record`). Conversely `entity_metas` is keyed by ccid and never joined, so it is its own `usecase.EntityMetaRepository`. |
| C8 | Migration is log replay | `conctl op dump-commitlog` / `import-commitlog` work on every backend; no backend copies another's tables. |

## Changing the contract

1. Change (or add) the suite in `repotest` first; the expected behaviour is
   whatever the current Postgres implementation does unless the change is
   deliberate (then say so in the PR as `behavior:`).
2. Implement it in every backend in the same PR. Do not merge a state
   where only one backend passes.
3. If a backend issues a new query shape, add it to the shape registry and
   update the backend's index definitions; the shape test fails on drift.
4. Offline tooling (`conctl`) talks to the repositories in `repository.Set`;
   a command that needs SQL goes through `op.RequirePostgres()` and is
   documented as Postgres-only.

## Running the suites

```sh
go test ./internal/infra/repository/postgres/ -run Suite -count=1   # Postgres via dockertest
```

`repotest` must not import a backend package:
`go list -deps ./internal/infra/repository/repotest | grep repository/postgres` stays empty.
