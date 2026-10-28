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
