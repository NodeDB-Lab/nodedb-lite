# NodeDB Lite 0.1.0-beta.1 Support Matrix

This document is the canonical record of what is supported, previewed, experimental, or absent in the `0.1.0-beta.1` release.

---

## Status legend

| Status           | Meaning                                                                                                          |
| ---------------- | ---------------------------------------------------------------------------------------------------------------- |
| **BETA**         | Stable in 0.1.0; semver-compatible changes only.                                                                 |
| **PREVIEW**      | Included in the release and intended for evaluation; breaking changes are possible without a minor-version bump. |
| **EXPERIMENTAL** | Shipped but explicitly unproven; do not rely on for production use.                                              |
| **NOT IN 0.1.0** | Known gap; landing in a later release.                                                                           |

---

## Surface support matrix

| Surface                         | Status       | Evidence                                                                             |
| ------------------------------- | ------------ | ------------------------------------------------------------------------------------ |
| Rust crate (`nodedb-lite`)      | BETA         | Full test suite passes: `tests/` — 415 tests green                                   |
| WASM crate (`nodedb-lite-wasm`) | PREVIEW      | Builds and browser-tested via CI; local storage only, no sync surface                |
| npm `@nodedb/lite`              | PREVIEW      | Published alongside the WASM crate; same scope as WASM                               |
| C FFI (`nodedb-lite-ffi`)       | BETA         | FFI tests pass: `nodedb-lite-ffi/tests/`                                             |
| Android JNI                     | PREVIEW      | Rust cross-compiles for `aarch64-linux-android`; no automated Android packaging gate |
| iOS                             | NOT IN 0.1.0 | No macOS build environment verified; documented gap in `docs/lite.md`                |

---

## Engine support matrix

| Engine                | Status                                                                       | Evidence                                                  |
| --------------------- | ---------------------------------------------------------------------------- | --------------------------------------------------------- |
| Strict document       | BETA                                                                         | `tests/strict_document.rs`                                |
| Document (schemaless) | BETA                                                                         | `tests/document.rs`                                       |
| Columnar              | BETA (bounded subset; HTAP materialized-view = EXPERIMENTAL)                 | `tests/columnar.rs`                                       |
| Vector                | BETA (HNSW + FP32 only; quantization / IVF-PQ / distributed = NOT IN 0.1.0)  | `tests/vector.rs`                                         |
| Graph                 | BETA (collection-scoped traversal, insert/delete edge, shortest path, stats) | `tests/graph.rs`                                          |
| Key-value             | BETA (narrow subset: put/get/delete; TTL + sorted-index = EXPERIMENTAL)      | `tests/kv.rs`                                             |
| Full-text             | EXPERIMENTAL (in-memory only, rebuilt on restart)                            | `nodedb-lite/src/engines/fts/`                            |
| Spatial               | EXPERIMENTAL (no dedicated correctness tests yet)                            | `nodedb-lite/src/engines/spatial/`                        |
| Timeseries            | EXPERIMENTAL (no dedicated correctness tests yet)                            | `nodedb-lite/src/engines/timeseries/`                     |
| Array (local)         | BETA (local operations only)                                                 | `tests/array.rs`                                          |
| Array (synced)        | EXPERIMENTAL (Origin transport phases not wired)                             | `tests/common/mod.rs` — sync path is simulated in-process |

---

## SQL support matrix

SQL is parsed via `nodedb-sql` and executed directly against local engines.

**Supported in 0.1.0-beta.1:**

- `ConstantResult` — constant-expression queries (e.g. `SELECT 1`)
- `Scan` — full collection scan (document-schemaless and strict engines)
- `PointGet` — single-key lookup by id (document-schemaless engine)
- `Insert` — insert rows with duplicate-key check
- `Upsert` — insert-or-replace (maps to CRDT upsert)
- `Update` — update rows by key list (literal values only)
- `Delete` — delete rows by key list
- `Truncate` — clear all documents in a collection

DDL (`CREATE COLLECTION`, etc.) is handled by the DDL path and is supported for documented collection types.

**Not supported — return `unsupported plan` error in beta:**

- JOIN
- Subquery
- CTE
- Window functions
- GROUP BY
- HAVING
- ORDER BY
- LIMIT
- Aggregate functions (COUNT, SUM, AVG, etc.)
- Cross-engine SQL
- FTS via SQL syntax (use the typed API: `text_search`)
- Vector search via SQL syntax (use the typed API: `vector_search`)

Source: `nodedb-lite/src/query/engine.rs` — plan variant dispatch.

---

## Cross-repo (Lite ↔ Origin) sync support matrix

Sync requires a running Origin cluster. Cross-repo interop is not gate-tested in `0.1.0-beta.1`; all sync paths listed below are implemented in-process and have not been validated against a live Origin node.

| Sync capability    | Status       | Note                                                                                         |
| ------------------ | ------------ | -------------------------------------------------------------------------------------------- |
| Handshake          | PREVIEW      | Protocol implemented; not tested against live Origin                                         |
| Delta push         | PREVIEW      | Loro delta serialization complete; `tests/sync/` exercises in-process simulation             |
| Delta ack          | PREVIEW      | ACK-based flow control (AIMD) present; no live Origin test gate                              |
| Compensation       | PREVIEW      | `CompensationHint` deserialization and dead-letter queue present; exercised in-process only  |
| Shape subscription | PREVIEW      | Shape filter wire format present; no live Origin validation in this release                  |
| Definition sync    | EXPERIMENTAL | Schema propagation path exists; correctness against Origin schema versioning not verified    |
| Array sync         | EXPERIMENTAL | Origin transport phases not wired; tested via in-process simulation in `tests/common/mod.rs` |
