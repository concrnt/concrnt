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
  log). Drifted entries are dropped and rebuild on demand. A redis store
  may show transient drift from concurrent writers; it converges within
  `backends.aggregate.ttl`.
- **Switching the store** (`backends.aggregate.store: postgres | redis`)
  needs no migration: the new store starts cold and fills on demand. The
  old Postgres table (`aggregate_entries`) can be truncated.
- Existing databases get no pre-computed counts; the first read of each
  target computes it (one `GROUP BY` over that target's associations).

## Moving between backends (log replay)

Every backend holds the same commit log, and the only way data moves
between backends is dump → replay (CONTRACT.md C8); no backend copies
another's tables. The steps, with the source server's `conctl` config
pointing at the source backend and the target server running on the
target backend (an empty database, same FQDN and keys):

```sh
# 1. dump the source (reads the database directly; no server needed)
conctl op dump-commitlog source            # -> source.commits.jsonl, source.metas.jsonl
conctl op dump-subscriptions > source.subscriptions.jsonl

# 2. start the target server on the new backend (backends.database: sqlite
#    or postgres, an empty store), then replay into it; conctl's own config
#    must point at the target backend too, it restores the entity metas
#    directly
CONCRNT_CONFIG=/etc/concrnt/target conctl op import-commitlog \
  --endpoint http://target:8000 --concurrency 4 --rejected-out rejected.jsonl \
  source.commits.jsonl source.metas.jsonl
CONCRNT_CONFIG=/etc/concrnt/target conctl op import-subscriptions < source.subscriptions.jsonl

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
- The target server needs the same outbound reach as the source: policy
  documents are fetched by URL during replay, so a container without CA
  certificates denies every write into a policy-governed timeline.
- The round trip Postgres → SQLite → Postgres was verified this way on a
  218k-commit devnet dump; see the phase 2 pull request for the numbers.
