# Changelog

All notable changes to NodeDB Lite are documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
NodeDB Lite uses [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [0.1.0-beta.1] — 2026-05-15

### Added

- Published explicit beta support matrix at `docs/lite-support-matrix.md`, covering platform surfaces, per-engine posture, SQL plan variants, and Lite-to-Origin sync capabilities.
- Public documentation aligned with the actual `NodeDb` trait API: method signatures, return types, and error variants match the implementation.
- WASM target builds: jemalloc and WAL POSIX-only paths are now gated behind `cfg` flags so `nodedb-lite-wasm` compiles cleanly for `wasm32-unknown-unknown`.
- WASM and C FFI bindings updated to the current `NodeDb` trait surface; graph methods are now collection-scoped.

### Changed

- Restored Rust API compatibility: `graph_stats` and `GraphStats` landed in shared types and are now accessible through the public crate API.
- Workspace version pinned to `0.1.0-beta.1` across all crates (`nodedb-lite`, `nodedb-lite-ffi`, `nodedb-lite-wasm`).
- npm package `@nodedb/lite` published alongside the WASM crate under Apache-2.0.

---

[0.1.0-beta.1]: https://github.com/nodedb/nodedb-lite/releases/tag/v0.1.0-beta.1
