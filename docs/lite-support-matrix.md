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
| Array (synced)        | EXPERIMENTAL — NOT IN 0.1.0-beta.1 interop gates (see note below)           | `tests/array_sync_*.rs` — edge-side simulation only; real-transport test is `tests/array_sync_interop.rs` (all `#[ignore]`) |

> **Array sync note**: Origin implements all inbound wire phases
> (`ArraySnapshot`, `ArraySnapshotChunk`, `ArrayCatchupRequest`, `ArraySchema`,
> `ArrayAck`) in `nodedb/nodedb/src/control/array_sync/` and dispatches them in
> `nodedb/nodedb/src/control/server/sync/session_handler.rs`.  The outbound
> fan-out path (`ArrayDeltaMsg` / `ArrayDeltaBatchMsg`) is implemented in
> `nodedb/nodedb/src/control/array_sync/outbound/`.  What is **missing** is the
> Lite receive path: `nodedb-lite/src/sync/client/receive.rs` does not yet handle
> `SyncMessageType::ArrayDelta` or `SyncMessageType::ArrayDeltaBatch`.  Until that
> is wired and gate-tested with `OriginServer::spawn()`, array sync is classified
> EXPERIMENTAL and excluded from 0.1.0-beta.1 interop guarantees.

---

## SQL support matrix

SQL is parsed via `nodedb-sql` and executed directly against local engines.
The full per-variant matrix with file:line anchors and known gaps is in
[`nodedb-lite/docs/lite-sql-support.md`](../nodedb-lite/docs/lite-sql-support.md).
The regression gate is `tests/sql_matrix.rs`.

**Supported `SqlPlan` variants (8 of 44) in 0.1.0-beta.1:**

| Variant | Status |
|---------|--------|
| `ConstantResult` | Supported — constant-expression queries |
| `Scan` | Partial — full scan on schemaless/strict; ORDER BY, LIMIT, WHERE, window functions guarded |
| `PointGet` | Supported — single-key lookup by id |
| `Insert` | Supported — with duplicate-key check |
| `Upsert` | Supported — maps to CRDT upsert |
| `Update` | Supported — literal-value assignments by key list |
| `Delete` | Supported — delete by key list |
| `Truncate` | Supported — clears collection |

**Unsupported — all 36 remaining variants return `LiteError::Unsupported`:**
JOIN, LateralTopK, LateralLoop, Aggregate, TimeseriesScan, TimeseriesIngest,
VectorSearch, MultiVectorSearch, TextSearch, HybridSearch, HybridSearchTriple,
SpatialScan, Union, Intersect, Except, Cte, RecursiveScan, RecursiveValue, Merge,
DocumentIndexLookup, RangeScan, KvInsert, InsertSelect, UpdateFrom,
CreateArray, DropArray, AlterArray, InsertArray, DeleteArray,
ArraySlice, ArrayProject, ArrayAgg, ArrayElementwise, ArrayFlush, ArrayCompact,
VectorPrimaryInsert.

Source: `nodedb-lite/src/query/engine.rs` — the `execute_plan` match.

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
| Definition sync    | EXPERIMENTAL — NOT IN 0.1.0-beta.1 | Lite's receive path is wired (`sync/transport.rs:317-319`, `sync_delegate.rs:82`), but Origin never emits `DefinitionSync` (0x70) frames — no DDL handler in `nodedb/nodedb/src/control/server/sync/` constructs or sends `DefinitionSyncMsg`.  Placeholder real-transport tests in `tests/definition_sync_interop.rs` (all `#[ignore]`). |
| Array sync         | EXPERIMENTAL — NOT IN 0.1.0-beta.1 | Lite's `sync/client/receive.rs` does not handle `ArrayDelta` / `ArrayDeltaBatch` frames; the real-transport round-trip is not gate-tested.  Simulated coverage only in `tests/array_sync_*.rs`.  Placeholder real-transport test in `tests/array_sync_interop.rs` (all `#[ignore]`). |
