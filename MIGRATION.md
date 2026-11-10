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

## Upgrading to the repository contract v2

The release that carries the repository contract v2
(`internal/infra/repository/CONTRACT.md`) changes how every backend writes
and what Firestore and the count caches store. Per backend:

- **Postgres**: start the new version; migration step 6 drops and
  recreates `aggregate_entries` (a cache, it refills on demand). The record
  tables are unchanged. `backends.postgres.maxConcurrentTx` no longer
  exists: a config that still sets it loads and the key is ignored, and the
  `concrnt_record_tx_slot*` metrics are gone with it (remove them from
  dashboards and alerts). A commit takes one connection, so
  `backends.postgres.maxOpenConns` is simply the concurrency wanted.
- **SQLite**: nothing to do.
- **Spanner**: start the new version; it applies schema batches 3
  (`aggregate_entries` recreated, `aggregate_deltas` added), 4
  (`record_keys.gen`, `associations.target_gen`, the association indexes
  rebuilt on `(target_uri, target_gen, ...)`) and 5 (the foreign key from
  associations to their target key dropped). All in place, no backfill;
  the index builds of batch 4 take time proportional to the number of
  associations and the server waits for them at start. Removing a key no
  longer deletes its associations in the same commit: `RecordCleanup` jobs
  remove them afterwards, so the job queue must be running.
- **Firestore**: the stored model changed (documents embedded in
  `record_keys`, `associations`, `acks`, `ackeds` and `entities`, key
  generations, `anchor` on commits, no `records` collection) and there is
  no in-place converter. Data written by the previous version is not
  readable by this one. The only path is the log replay below: dump with
  the old version, deploy the new composite indexes
  (`deploy/firestore/README.md`), replay into an **empty** database with
  the new version. The old database, its `records` collection included,
  can then be deleted. If the replay target is not a new database but the
  old one emptied by hand, empty `aggregate_entries` too, or run `conctl op
  aggregate-invalidate --all` before serving: an entry in the old format
  reads as an entry without counts until it expires.
- **Redis / memcached count caches**: the keys moved (`aggregate:v2:` in
  Redis, `agg2:` in memcached), so the cache starts cold; the old keys
  expire on their own.

Behaviour an operator may notice:

- A redelivered commit that used to be acknowledged silently can now be
  rejected when what it refers to has gone in the meantime (an association
  whose target was removed and is not inlined in the commit, a delete of an
  association by ccfs whose record is gone): the reads of a commit now all
  come before its write.
- Deleting a retained, superseded version of a record by its ccfs URI
  works.
- On Postgres an unack arriving as the first state of a relationship used
  to be stored as valid; it is stored invalid now. Rows written before the
  upgrade are not corrected.

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
- The round trip Postgres → SQLite → Postgres was verified this way on a
  218k-commit devnet dump; see PR #115 for the numbers.
  Postgres → Spanner (Omni) → Postgres: see PR #125.
  Postgres → Firestore (emulator) → Postgres was verified on the first
  23,911 commits / 167 metas of that dump (the emulator's pace, see above):
  the Firestore dump is a subset of the source and the Postgres dump
  replayed from it is identical, line for line; see PR #124.
