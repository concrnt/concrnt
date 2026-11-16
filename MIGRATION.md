# Operations and migration notes

## Count caches (aggregates)

Association counts and ack counts are served from a cache
(`internal/usecase/aggregate`, configured under `backends.aggregate`). The
cache is derived data: every entry can be recomputed from the records, so
dropping it is always safe and never needs a log replay.

- **After a release that changes how a count is computed**, or after any
  tool that rewrites records behind the usecase (`conctl op delete-user`,
  `repair-*`), run `conctl op aggregate-invalidate --all`. Counts are
  recomputed on their next read or write. `gc-commitlog` does this itself.
- **To check the cache against the data**: `conctl op aggregate-verify
  --sample 1000` (add `--scan` for a uniform sample over the whole commit
  log). Drifted entries are dropped and rebuild on demand. Writes reach
  an entry as blind deltas, so a little drift is expected on a busy server
  (a decrement landing on an entry computed after the removal, for
  instance); an entry is recomputed after `backends.aggregate.maxDeltas`
  deltas and at the latest after `backends.aggregate.ttl`.
- **Switching the store** (`backends.aggregate.store: postgres | spanner | firestore | redis | memcached`)
  needs no migration: the new store starts cold and fills on demand. What
  the old store holds can be dropped: the table `aggregate_entries` on
  Postgres, `aggregate_entries` with `aggregate_deltas` on Spanner, the
  collection `aggregate_entries` on Firestore, the keys `aggregate:v2:*`
  in Redis; memcached items (`agg2:`) expire on their own. On Firestore
  expired entries are only removed by the TTL
  policy on `expiresAt` (`deploy/firestore/README.md`); until it is
  enabled they stay on disk, ignored.
- Existing databases get no pre-computed counts; the first read of each
  target computes it (one scan of that target's associations, which also
  lists their authors when there are at most 1000).

## Upgrading to the repository contract

The release that carries the repository contract
(`internal/infra/repository/CONTRACT.md`) changes how a commit is written
and where counts come from. Before it Postgres was the only backend, so an
existing server is a Postgres server:

- Start the new version. The numbered migration steps
  (`schema_migrations`) run once at start: the parent-key index is rebuilt
  in byte order, `record_keys.redirect` is added and backfilled from
  `records`, the ack indexes nothing reads are dropped, the gc flag index
  becomes partial and `aggregate_entries` (the count cache, filled on
  demand) is created. The backfill and the index rebuild take time
  proportional to the number of keys and the server waits for them.
- A commit takes one connection, and at most half of
  `backends.postgres.maxOpenConns` (default 64) are given to commits at
  once so that reads keep the rest: size the pool with that one setting.
  The wait for such a slot is the histogram
  `concrnt_record_tx_slot_wait_seconds`.
- SQLite, Spanner and Firestore are new backends: they start empty, or
  from a log replay (below).

Behaviour an operator may notice:

- A redelivered commit that used to be acknowledged silently can now be
  rejected when what it refers to has gone in the meantime (an association
  whose target was removed and is not inlined in the commit, a delete of an
  association by ccfs whose record is gone): the reads of a commit now all
  come before its write.
- Deleting a retained, superseded version of a record by its ccfs URI
  works.
- An unack arriving as the first state of a relationship used to be stored
  as valid; it is stored invalid now. Rows written before the upgrade are
  not corrected.
- Keys are ordered by byte order everywhere (`orderby=key` used to follow
  the database's collation), and the prefix of a prefix query is a literal
  (`_` and `%` used to be wildcards).
- A range delete of more than 500 records removes the first 500 with its
  commit and the rest in `RecordRangeDelete` jobs, so the tail of a large
  subtree disappears a moment later.

## Moving between backends (log replay)

Every backend holds the same commit log, and the only way data moves
between backends is dump → replay (CONTRACT.md C8); no backend copies
another's tables. The steps, with the source server's `conctl` config
pointing at the source backend and the target server running on the
target backend (an empty database, same FQDN and keys):

```sh
# 1. dump the source (reads the database directly; no server needed)
conctl op dump-commitlog source            # -> source.commits.jsonl, source.metas.jsonl
conctl op dump-subscriptions source.subscriptions.jsonl

# 2. start the target server on the new backend (backends.database: sqlite,
#    postgres, spanner or firestore, an empty store), then replay into it; conctl's own
#    config must point at the target backend too, it restores the entity
#    metas directly
CONCRNT_CONFIG=/etc/concrnt/target conctl op import-commitlog \
  --endpoint http://target:8000 --concurrency 4 --rejected-out rejected.jsonl \
  source.commits.jsonl source.metas.jsonl
CONCRNT_CONFIG=/etc/concrnt/target conctl op import-subscriptions source.subscriptions.jsonl

# 3. verify: a dump of the target lists the same commits
CONCRNT_CONFIG=/etc/concrnt/target conctl op dump-commitlog target
diff <(sort source.commits.jsonl) <(sort target.commits.jsonl) && echo identical
```

Notes:

- Replay order is dump order (document `createdAt`); lines a server rejects
  because they depend on a later line are re-sent in further passes until a
  pass accepts nothing more. With `--concurrency` above 1 (or through the
  retry passes) a newer generation of a key can land before an older one;
  the older one is then an accept-if-newer no-op and is not recorded, so
  the target's dump lacks that superseded commit while its live state is
  the same. Replay with `--concurrency 1` when the superseded history
  (`onUpdate: retain`) has to survive the move. A delete is held back until the lines touching
  its target are in, but a record that is only accepted in a retry pass
  after a delete of its key was accepted in an earlier pass is restored,
  not deleted: a dump taken while such a backdated write is in flight on
  the source can replay differently. Take the dump on a quiet source (or
  compare the dumps as in step 3) rather than relying on replay order for
  this corner.
- Counts are not part of the log: the target's aggregate cache starts empty
  and fills on demand.
- The job queue lives in Redis, not in the record store, and
  `RecordCleanup` jobs carry tokens only the backend that issued them
  understands (Spanner and Firestore issue them, Postgres and SQLite
  never do). Let the source server's queue drain before pointing a server
  with the same Redis at another backend (`concrnt_jobqueue_backlog` at
  zero for ready, pending and retry). A leftover job is an error on
  Spanner and Firestore (it retries and ends in the dead-letter queue) and
  is dropped as done on Postgres and SQLite; either way the target is
  unharmed and the source's cleanup stays unfinished.
- The target server needs the same outbound reach as the source: policy
  documents are fetched by URL during replay, so a container without CA
  certificates denies every write into a policy-governed timeline.
- A range delete of more than 500 targets is replayed like any other
  commit: the first 500 go in the commit, the rest in `RecordRangeDelete`
  jobs on the target server, so the target's job queue must be running
  (it is, when the server is) and the dump comparison of step 3 is only
  final once that queue is idle. The same queue runs the `RecordCleanup`
  jobs of a Spanner or Firestore target (the physical removal of a deleted
  key's associations); they change nothing a read or a dump shows.
- On Spanner the database itself must exist before the server starts
  (`spanner databases create concrnt` on Omni, the console or `gcloud` on
  Cloud Spanner); the server creates and migrates the tables.
- On Firestore the composite indexes must be deployed before the replay
  (`deploy/firestore/README.md`); the emulator needs none. Sequential
  document ids (commit ids are time-ordered) cap the sustained write rate
  around 500 commits/s, so a large replay runs at that pace at best. The
  emulator is far slower and degrades as the database grows (a 219k-commit
  devnet dump landed 24k commits in 85 minutes, the last batches at 4
  minutes per 400 lines): rehearse a migration against a real database.
- The round trips were verified this way on the first implementation of
  each backend, before the contract took its present form, and have not
  been repeated since: Postgres → SQLite → Postgres on a 218k-commit devnet
  dump (PR #115), Postgres → Spanner (Omni) → Postgres (PR #125), and
  Postgres → Firestore (emulator) → Postgres on the first 23,911 commits /
  167 metas of that dump (PR #124).
