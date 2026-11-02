# Firestore backend

`backends.database: firestore` runs concrnt on Cloud Firestore (Native mode).
Redis and memcached are still required. The repository implementation is
`internal/infra/repository/firestore`; its contract notes are in
`internal/infra/repository/CONTRACT.md`.

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

## Count cache expiry (TTL policy)

The aggregate store (`backends.aggregate.store: firestore`, the default on
this backend) writes `aggregate_entries` documents with an `expiresAt`
field. Expired documents are ignored on read; to have Firestore delete them,
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
- Firestore has no cascading deletes; the repository fans them out itself
  (a record delete removes the key and its associations, GC removes
  everything hanging off a commit). Re-running GC finishes a partially
  failed pass.
- Grouped counts (reaction counts, follower counts) scan the matching
  documents: cost is one read per association / ack of the target, which is
  why they are served from the aggregate cache.
- Commit ids are time-ordered CDIDs, so `commits` / `records` /
  `associations` have sequential document ids and timestamps: Firestore caps
  such collections around 500 writes/s (500/50/5 ramp-up). Irrelevant at
  small scale, but do not expect Postgres-level write throughput.
- Large string fields (documents, proofs, policies, well-known) have their
  single-field indexes disabled in `firestore.indexes.json`; querying them
  is not supported.
