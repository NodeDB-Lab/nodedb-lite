#!/usr/bin/env bash
# Stamp the workspace version from a release tag.
#
#   scripts/ci/stamp_version.sh <version>       # e.g. 0.1.0, 0.1.0-beta.2
#
# Rewrites `[workspace.package] version` in the root Cargo.toml, and for a
# prerelease also pins the internal path-dep requirement to the exact version —
# a bare `version = "0.1.0"` requirement does not match `0.1.0-beta.2` under
# semver, so publishing nodedb-lite-ffi / nodedb-lite-wasm would fail without
# this.
#
# The pin matches only path deps (`{ ... path = ... version = ... }`), so the
# external `nodedb-* = "0.4"` crates.io requirements are left untouched — only
# the in-workspace `nodedb-lite` dependency is re-pinned.
#
# No-ops when Cargo.toml already carries the target version, which keeps
# re-running a stage idempotent.

set -euo pipefail

VERSION="${1:?usage: stamp_version.sh <version>}"

CURRENT=$(cargo metadata --no-deps --format-version=1 \
    | jq -r '.packages[] | select(.name == "nodedb-lite") | .version')

if [[ "$VERSION" == "$CURRENT" ]]; then
    echo "Version already $VERSION — nothing to stamp."
    exit 0
fi

# First `version = "..."` in the file is [workspace.package].
perl -i -pe 'if (!$done && /^version = "/) { s/^version = ".*"/version = "'"$VERSION"'"/; $done=1 }' Cargo.toml

if [[ "$VERSION" == *-* ]]; then
    sed -i -E 's/(nodedb[a-z0-9-]* = \{ [^}]*path = [^}]*version = )"[^"]*"/\1"='"$VERSION"'"/' Cargo.toml
    echo "Pinned internal path-dep requirement to =$VERSION"
fi

echo "Stamped workspace version: $CURRENT -> $VERSION"
