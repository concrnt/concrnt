#!/usr/bin/env bash
set -euo pipefail

VERSION=$("$(dirname "$0")/version.sh")

echo "Building version: $VERSION"

VERSION="$VERSION" docker compose build "$@"
