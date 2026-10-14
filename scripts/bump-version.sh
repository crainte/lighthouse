#!/usr/bin/env bash
# Set the project version everywhere it is declared.
#   scripts/bump-version.sh 0.2.0
# Used by the Release PR workflow; safe to run locally.
set -euo pipefail

version="${1:?usage: $0 <version>}"
version="${version#v}"
if ! [[ "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  echo "not a semver version: $version" >&2
  exit 1
fi

cd "$(dirname "$0")/.."

# Rust package version (first `version =` line is [package]).
sed -i "0,/^version = \".*\"/s//version = \"$version\"/" Cargo.toml
# Matching Cargo.lock entry.
sed -i "/^name = \"lighthouse\"$/{n;s/^version = \".*\"/version = \"$version\"/}" Cargo.lock
# Frontend package.
sed -i "0,/\"version\": \".*\"/s//\"version\": \"$version\"/" frontend/package.json

grep -q "^version = \"$version\"" Cargo.toml
grep -q "^version = \"$version\"" Cargo.lock
grep -q "\"version\": \"$version\"" frontend/package.json
echo "version set to $version"
