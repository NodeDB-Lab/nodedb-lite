# Changelog

All notable changes to NodeDB Lite are documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
NodeDB Lite uses [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [0.1.0-beta.1] — 2026-05-15

### Added

- Published explicit beta support matrix at `docs/lite-support-matrix.md`, covering platform surfaces, per-engine posture, SQL plan variants, and Lite-to-Origin sync capabilities.
- Documented definition sync (functions/triggers/procedures) as EXPERIMENTAL — NOT IN 0.1.0-beta.1: Lite's receive path is wired (`sync/transport.rs:317-319`, `sync_delegate.rs:82`) but Origin emits no `DefinitionSync` (0x70) frames from its DDL handlers.  Placeholder real-transport tests added at `tests/definition_sync_interop.rs` (all `#[ignore]`).  Promotion criteria and file:line evidence recorded in `docs/lite-sync-protocol.md` (Definition Sync section) and `docs/lite-support-matrix.md`.
- Documented array sync as EXPERIMENTAL / NOT IN 0.1.0-beta.1 interop gates: `tests/array_sync_*.rs` are edge-side simulations only; `tests/array_sync_interop.rs` provides `#[ignore]` real-transport placeholders.  The missing Lite receive path (`SyncMessageType::ArrayDelta` / `ArrayDeltaBatch`) and promotion criteria are recorded in `docs/lite-sync-protocol.md` (Array Sync section) and `docs/lite-support-matrix.md`.
- Public documentation aligned with the actual `NodeDb` trait API: method signatures, return types, and error variants match the implementation.
- WASM target builds: jemalloc and WAL POSIX-only paths are now gated behind `cfg` flags so `nodedb-lite-wasm` compiles cleanly for `wasm32-unknown-unknown`.
- WASM and C FFI bindings updated to the current `NodeDb` trait surface; graph methods are now collection-scoped.

### Changed

- Restored Rust API compatibility: `graph_stats` and `GraphStats` landed in shared types and are now accessible through the public crate API.
- Workspace version pinned to `0.1.0-beta.1` across all crates (`nodedb-lite`, `nodedb-lite-ffi`, `nodedb-lite-wasm`).
- npm package `@nodedb/lite` published alongside the WASM crate under Apache-2.0.

---

[0.1.0-beta.1]: https://github.com/nodedb/nodedb-lite/releases/tag/v0.1.0-beta.1
