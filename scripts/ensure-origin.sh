#!/usr/bin/env bash
#
# nextest setup script: build the Origin server binary so the cross-repo sync
# interop tests run against a wire-protocol build that MATCHES the current Lite
# source — never a stale, manually-linked binary.
#
# Locating the Origin source: we do NOT hardcode a sibling path. The Lite dev
# build already resolves the shared `nodedb-*` crates through cargo, and during
# internal development `.cargo/config.toml` patches `nodedb-types` to a local
# path inside the Origin repo. We ask cargo where `nodedb-types` actually
# resolves from (`cargo metadata`) and derive the Origin workspace root from its
# manifest path — a single source of truth, no duplicated path literal.
#
# Idempotent: `cargo build` rebuilds only when sources change.
#
# Skip vs fail — the distinction is deliberate:
#   - If the Origin source is NOT available locally (`nodedb-types` resolves from
#     the registry, or the workspace can't be located) we exit 0 WITHOUT
#     exporting NODEDB_BIN, so the interop tests skip rather than fail — a
#     Lite-only checkout still passes `cargo nextest run`.
#   - If the Origin source IS available but its build fails (or produces no
#     binary), we exit NON-ZERO so nextest fails the run. A broken Origin build
#     must surface loudly; silently skipping every interop test while the suite
#     reports green would hide exactly the cross-repo regressions these tests exist
#     to catch.
#
# nextest runs this from the workspace root and reads exported env vars from the
# file named by $NEXTEST_ENV.

set -uo pipefail

if [ -z "${NEXTEST_ENV:-}" ]; then
    echo "ensure-origin: \$NEXTEST_ENV unset — not running under nextest" >&2
    exit 1
fi

# Find where nodedb-types resolves on disk (its Cargo.toml manifest path).
types_manifest="$(cargo metadata --format-version 1 2>/dev/null \
    | grep -o '"manifest_path":"[^"]*/nodedb-types/Cargo.toml"' \
    | head -n1 \
    | sed 's/.*"manifest_path":"//; s/"$//')"

if [ -z "$types_manifest" ] || [ ! -f "$types_manifest" ]; then
    echo "ensure-origin: could not locate nodedb-types source; interop tests will skip" >&2
    exit 0
fi

# A registry checkout means the Origin source isn't available locally — skip.
case "$types_manifest" in
    */registry/*|*/.cargo/*)
        echo "ensure-origin: nodedb-types resolves from the registry (no local Origin source); interop tests will skip" >&2
        exit 0
        ;;
esac

# Origin workspace root = parent of the nodedb-types crate dir.
origin_root="$(cd "$(dirname "$types_manifest")/.." 2>/dev/null && pwd)"
if [ -z "$origin_root" ] || [ ! -f "$origin_root/Cargo.toml" ] || [ ! -f "$origin_root/nodedb/Cargo.toml" ]; then
    echo "ensure-origin: Origin workspace not found near $types_manifest; interop tests will skip" >&2
    exit 0
fi

# Past this point the Origin source IS present locally. A build failure or a
# missing binary is now a REAL error, not a "no Origin available" skip — failing
# here loudly is the whole point: a broken Origin build must fail the interop
# suite rather than silently skip every test while the run still reports green.
echo "ensure-origin: building Origin binary (cargo build -p nodedb in $origin_root) ..." >&2
if ! cargo build --manifest-path "$origin_root/Cargo.toml" -p nodedb --bin nodedb >&2; then
    echo "ensure-origin: Origin build FAILED; failing the interop suite (Origin source is present, so this is a real error, not a skip)" >&2
    exit 1
fi

bin="$origin_root/target/debug/nodedb"
if [ ! -x "$bin" ]; then
    echo "ensure-origin: Origin build reported success but binary not found at $bin; failing the interop suite" >&2
    exit 1
fi

echo "NODEDB_BIN=$bin" >> "$NEXTEST_ENV"
echo "ensure-origin: NODEDB_BIN=$bin" >&2
