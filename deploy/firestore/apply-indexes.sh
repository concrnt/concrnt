#!/bin/sh
# Applies firestore.indexes.json with gcloud (for environments without the
# Firebase CLI): disables the single-field indexes listed in fieldOverrides,
# creates every composite index, asynchronously, and enables the TTL policy
# of the count cache. Idempotent: existing indexes are reported as errors by
# gcloud and skipped.
#
#   PROJECT=my-project [DATABASE='(default)'] sh deploy/firestore/apply-indexes.sh
#   gcloud firestore indexes composite list --project "$PROJECT" --database "$DATABASE"
set -eu

: "${PROJECT:?set PROJECT to the GCP project id}"
DATABASE="${DATABASE:-(default)}"
DIR="$(cd "$(dirname "$0")" && pwd)"

python3 - "$DIR/firestore.indexes.json" "$PROJECT" "$DATABASE" <<'EOF'
import json, subprocess, sys

path, project, database = sys.argv[1:4]
spec = json.load(open(path))
common = ["--project", project, "--database", database, "--quiet", "--async"]

def run(cmd):
    res = subprocess.run(cmd, capture_output=True, text=True)
    if res.returncode != 0 and "already exists" not in res.stderr:
        sys.stderr.write(res.stderr)
        sys.exit(res.returncode)

for fo in spec["fieldOverrides"]:
    run(["gcloud", "firestore", "indexes", "fields", "update", fo["fieldPath"],
         "--collection-group", fo["collectionGroup"], "--disable-indexes"] + common)
    print("exempt", fo["collectionGroup"], fo["fieldPath"])

for ix in spec["indexes"]:
    cmd = ["gcloud", "firestore", "indexes", "composite", "create",
           "--collection-group", ix["collectionGroup"], "--query-scope", ix["queryScope"]] + common
    for f in ix["fields"]:
        if "arrayConfig" in f:
            cmd.append("--field-config=field-path=%s,array-config=%s" % (f["fieldPath"], f["arrayConfig"].lower()))
        else:
            cmd.append("--field-config=field-path=%s,order=%s" % (f["fieldPath"], f["order"].lower()))
    run(cmd)
    print("index", ix["collectionGroup"], [f["fieldPath"] for f in ix["fields"]])

# the count cache's expiry: expired documents are ignored on read either way
run(["gcloud", "firestore", "fields", "ttls", "update", "expiresAt",
     "--collection-group", "aggregate_entries", "--enable-ttl"] + common)
print("ttl aggregate_entries expiresAt")
EOF
