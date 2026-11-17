#!/usr/bin/env bash
# Prints the semver version of the current checkout, derived from the nearest
# v* tag. Dev builds sort strictly between that tag and the next release, so
# the migrator orders them correctly:
#
#   on a tag                         -> the tag itself (v1.12.0, v1.12.0-beta.0)
#   N commits after v1.11.10         -> v1.11.11-dev.N+gHASH
#   N commits after v1.12.0-beta.0   -> v1.12.0-beta.0.dev.N+gHASH
set -euo pipefail

DESC=$(git describe --tags --long --match 'v[0-9]*')
HASH=${DESC##*-}
REST=${DESC%-*}
COUNT=${REST##*-}
TAG=${REST%-*}

if (( COUNT == 0 )); then
  echo "$TAG"
elif [[ $TAG == *-* ]]; then
  # pre-release: extend the identifiers so it stays below the next pre-release
  echo "${TAG}.dev.${COUNT}+${HASH}"
else
  PATCH=${TAG##*.}
  PREFIX=${TAG%.*}
  echo "${PREFIX}.$((PATCH + 1))-dev.${COUNT}+${HASH}"
fi
