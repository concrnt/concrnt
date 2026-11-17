#!/usr/bin/env bash
set -euo pipefail

DESC=$(git describe --tags --long)
HASH=${DESC##*-}
REST=${DESC%-*}
COUNT=${REST##*-}
TAG=${REST%-*}

if (( COUNT == 0 )); then
  VERSION="$TAG"
else
  PATCH=${TAG##*.}
  PREFIX=${TAG%.*}
  VERSION="${PREFIX}.$((PATCH + 1))-dev.${COUNT}+${HASH}"
fi

echo "Building version: $VERSION"

VERSION="$VERSION" docker compose build "$@"
