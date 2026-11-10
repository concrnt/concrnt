# Firestore backend

`backends.database: firestore` runs concrnt on Cloud Firestore (Native mode).
Redis and memcached are still required. The repository implementation is
`internal/infra/repository/firestore`; its contract notes are in
`internal/infra/repository/CONTRACT.md` and the stored model is described
in the package comment of `internal/infra/repository/firestore/docs.go`.

A database written by a version before the repository contract v2 cannot
be read by this one and is not converted in place: dump it with the old
version and replay the log into an empty database (`MIGRATION.md`).

## One-time setup

```sh
PROJECT=my-project; REGION=asia-northeast1; DB="(default)"   # or a named database
gcloud services enable firestore.googleapis.com --project "$PROJECT"
gcloud firestore databases create --project "$PROJECT" --database="$DB" \
  --location="$REGION" --type=firestore-native --delete-protection --enable-pitr
```

The free tier applies to one database per project; a named database
(`--database=concrnt`) keeps `(default)` free for other uses but is billed
from the first read.

## Indexes (deploy BEFORE switching traffic)

`firestore.indexes.json` is generated from the query shapes in
`internal/infra/repository/firestore/indexes.go`; `go test` fails when the
two drift (`go test ./internal/infra/repository/firestore -run
TestIndexesFileMatchesShapes -args -update-indexes` regenerates it). Queries
that lack a composite index fail with `FailedPrecondition` (the log carries
the console URL to create it), and the emulator does not enforce indexes, so
deploy them first and wait for `READY`:

```sh
cd deploy/firestore
firebase deploy --only firestore:indexes --project "$PROJECT"   # add "database" to firebase.json for a named DB
# or, without the Firebase CLI (this also enables the TTL policy below):
PROJECT="$PROJECT" DATABASE="$DB" sh apply-indexes.sh
gcloud firestore indexes composite list --project "$PROJECT" --database="$DB"
```

The server also probes every query shape once at startup and logs any
missing index.

The file holds 37 composite indexes and the single-field exemptions. The
repository contract v2 kept the composite indexes as they were and changed
the exemptions (the embedded `document` / `proof` fields, the uniqueness
markers, the count cache's `counters` / `members`), so redeploy the file
when upgrading. The cleanup of a deleted key's associations queries
`associations` by `targetGen` through the automatic single-field index.

## Count cache expiry (TTL policy)

The aggregate store (`backends.aggregate.store: firestore`, the default on
this backend) writes one `aggregate_entries` document per cached entry
(counters, members, `asOf`, `deltas`) with an `expiresAt` field. A write
adds to an entry with a single field-transform update, without a
transaction, so the writers of a popular target do not abort one another.
Expired documents are ignored on read; to have Firestore delete them,
enable a TTL policy on that field once per database (`apply-indexes.sh`
does it; TTL deletion lags up to 24 hours):

```sh
gcloud firestore fields ttls update expiresAt --collection-group=aggregate_entries \
  --enable-ttl --project "$PROJECT" --database="$DB"
```

## Credentials

The client uses Application Default Credentials. On GKE Autopilot bind the
pod's Kubernetes service account through Workload Identity and grant it
`roles/datastore.user` (index deployment needs `roles/datastore.indexAdmin`;
give that to operators/CI, not the pod):

```sh
gcloud projects add-iam-policy-binding "$PROJECT" --role=roles/datastore.user \
  --member="principal://iam.googleapis.com/projects/$PROJECT_NUMBER/locations/global/workloadIdentityPools/$PROJECT.svc.id.goog/subject/ns/$NAMESPACE/sa/$KSA"
```

Config:

```yaml
backends:
  database: firestore
  firestore:
    projectID: my-project      # optional on GCP (detected from the metadata server)
    databaseID: concrnt        # optional, "(default)" when omitted
```

Locally, `backends.firestore.emulatorHost` (or `FIRESTORE_EMULATOR_HOST` in
the environment) points the client at an emulator:

```sh
docker compose --profile firestore up -d firestore-emulator redis memcached
# backends: { database: firestore, firestore: { projectID: local, emulatorHost: firestore-emulator:8080 }, ... }
```

## Operational notes

- `conctl op gc-commitlog` / `dump-commitlog` / `import-commitlog` /
  `dump-subscriptions` / `import-subscriptions` / `aggregate-*` work on every
  backend. `delete-user`, `repair-*` and `migrate-*` are Postgres-only.
- Moving an existing server to Firestore is a commit-log replay, not a
  table copy; the runbook is in `MIGRATION.md`.
- Every commit is one transaction that reads a handful of documents by id
  (one batch get) and never queries, so a commit's cost in document reads
  does not grow with the data.
- Firestore has no cascading deletes. Deleting a record removes its key
  document only, which hides the key's associations from every read at
  once; the association documents and their uniqueness markers
  (`association_uniques`) are removed afterwards by `RecordCleanup` jobs,
  100 associations per step, so the job queue must be running and a
  popular key's storage is freed with a delay. GC (`gc-commitlog`) removes
  everything anchored to a commit and finishes that cleanup itself;
  re-running it finishes a partially failed pass, except that a pass that
  died while cleaning up a removed key's associations leaves those
  documents behind (unreadable, storage only).
- A listing returns the signed documents from the `record_keys` (or
  `associations`, `acks`, `ackeds`) documents its query lands on: one read
  per row, no second fetch from `commits`.
- Grouped counts (reaction counts, follower counts) scan the matching
  documents: cost is one read per association / ack of the target, which is
  why they are served from the aggregate cache.
- Commit ids are time-ordered CDIDs, so `commits` and `associations` have
  sequential document ids and timestamps: Firestore caps such collections
  around 500 writes/s (500/50/5 ramp-up). Irrelevant at small scale, but
  do not expect Postgres-level write throughput.
- Large string fields (documents, proofs, policies, well-known) and the
  other fields no query uses have their single-field indexes disabled in
  `firestore.indexes.json`; querying them is not supported.
