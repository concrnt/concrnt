# Moving a server between backends

Backends never copy each other's tables. The commit log is the canonical,
replayable ledger, so a move is always "dump the commit log on the old
backend, start the server on the new one, replay". The runbook below is
written for Postgres → Firestore; the reverse (and any future backend) is
the same sequence with the roles swapped.

## 0. Prepare the target

- Create the Firestore database (Native mode) and deploy the composite
  indexes: `deploy/firestore/apply-indexes.sh <project> [database]` or
  `firebase deploy --only firestore:indexes` from this directory. Wait until
  every index is `READY`; the server logs the shapes it cannot serve at
  startup (`firestore composite index missing`).
- Prepare the target config: the **same `concrnt.fqdn` and
  `concrnt.privatekey`** as the source, `backends.database: firestore` and
  the `backends.firestore` block. Keep the source config untouched until the
  cut-over is done.

## 1. Freeze the source and dump

Stop accepting writes on the source (take it out of the ingress, or stop the
public listener) so the dump is complete. Then, with `CONCRNT_CONFIG`
pointing at the source config:

```sh
conctl op dump-commitlog migrate          # -> migrate.commits.jsonl, migrate.metas.jsonl
conctl op dump-subscriptions migrate.subscriptions.jsonl
```

`dump-commitlog` writes every commit in id order (one `concrnt.SignedDocument`
per line) plus the local-entity registration state (`entity_metas`) that is
not part of the log. Web push subscriptions are a separate table and get
their own file.

## 2. Start the server on the target

Start `concrnt` with the target config. `/ready` must answer 200 and the
log must show `database backend ready backend=firestore` followed by
`firestore composite indexes verified`. Do not expose it to users yet.

## 3. Replay

With `CONCRNT_CONFIG` pointing at the **target** config (the metas restore
writes to the target store; the commits are POSTed to the running target
server as the `system` service account):

```sh
conctl op import-commitlog migrate.commits.jsonl migrate.metas.jsonl \
  --endpoint http://<target-host>:8000 \
  --rejected-out migrate.rejected.jsonl
conctl op import-subscriptions migrate.subscriptions.jsonl
```

The import restores the entity metas first, then replays entity commits,
then everything else, and retries the lines the server rejected until a
pass accepts nothing new (a document whose dependency appears later in the
file lands on the retry). `--concurrency N` keeps N batches in flight;
batches holding a delete always run alone so ordering against their
targets is preserved. `--dry-run` only parses and counts.

What stays rejected is written to `--rejected-out`, verbatim, with the
server's reason per line in the summary. Expected rejections:

- documents older than the backdate window whose key was later overwritten
  or deleted: their commit survives on the source only as a gc tombstone,
  and replaying it is a no-op by design;
- documents signed by subkeys that were revoked later in the log, when the
  revocation lands before the retry reaches them;
- commits owned by entities whose domain no longer resolves to this server.

Anything else in the file needs a look before cut-over. Re-running the
import is safe: the commit log is first-write-wins on the commit id.

## 4. Compare

Take a dump from the target and diff it against the source dump:

```sh
CONCRNT_CONFIG=<target> conctl op dump-commitlog verify
sort migrate.commits.jsonl > a; sort verify.commits.jsonl > b; diff a b
diff <(sort migrate.metas.jsonl) <(sort verify.metas.jsonl)
```

Lines only in `a` must be exactly the rejected lines; lines only in `b`
must be the commits the target server produced itself (service-account
documents written at startup). The dump's time filters (`--since`,
`--until`, `--owner`) narrow a large comparison.

## 5. Cut over

Point the ingress at the target. Keep the source database read-only for
the rollback window; rolling back is the same replay in the other
direction, restricted with `--since` to what the target accepted after the
cut-over.

## Operational notes

- Aggregates (reaction / follower counts) are computed from the primary
  data on both backends; nothing needs rebuilding after a replay.
- `conctl op repair-*` and `migrate-*` speak SQL and refuse to run against
  a non-Postgres backend; everything else (`gc-commitlog`,
  `dump-commitlog`, `import-commitlog`, `dump-subscriptions`,
  `import-subscriptions`, `create-account`, `delete-user` where noted) goes
  through the repository contract.
