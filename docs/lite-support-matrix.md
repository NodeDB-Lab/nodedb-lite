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
| Vector                | BETA (HNSW + FP32 only; quantization / IVF-PQ / distributed = NOT IN 0.1.0; sync-to-Origin = PREVIEW) | `tests/vector_engine_gate.rs`, `tests/sync_interop_vector.rs` |
| Graph                 | BETA (collection-scoped traversal, insert/delete edge, shortest path, stats) | `tests/graph_engine_gate.rs`                              |
| Key-value             | BETA (put/get/delete + TTL + range scan; gate test at tests/kv_ttl_and_range.rs) | `tests/kv_engine_gate.rs`, `tests/kv_ttl_and_range.rs`   |
| Full-text             | BETA (persistent index; restart loads without rebuild)                       | `nodedb-lite/src/engine/fts/`, `tests/fts_persistence.rs` |
| Spatial               | BETA (persistent R-tree; OGC predicates; gate test at tests/spatial_engine_gate.rs) | `tests/spatial_engine_gate.rs`                     |
| Timeseries            | EXPERIMENTAL (DML routing not yet wired in beta; `TimeseriesScan`/`TimeseriesIngest` return `LiteError::Unsupported`) | `tests/sql_parity/timeseries.rs`                          |
| Array (local)         | BETA (local operations only)                                                 | `tests/array.rs`                                          |
| Array (synced)        | BETA (ArrayDelta + ArrayDeltaBatch receive wired; gate test at `tests/array_sync_interop_real.rs`) | `tests/array_sync_*.rs` (in-process), `tests/array_sync_interop_real.rs` (dispatch-path gate, 5/5 passing) |

---

## Per-engine details

### Strict document

- **Local storage format / persistence**: redb-backed binary-tuple rows; engine, schema, CRUD, secondary indexes, and Arrow mapping in `nodedb-lite/nodedb-lite/src/engine/strict/` (`engine.rs`, `store.rs`, `schema.rs`, `crud.rs`, `secondary_index.rs`).
- **Query surface promised**: `Scan` (real row iteration via `StrictEngine::list_rows` + `TupleDecoder`), `PointGet`, `Insert`, `Upsert`, `Update`, `Delete`, `Truncate`, `ConstantResult`; schema DDL (`CREATE COLLECTION … WITH (engine='strict')`); secondary-index lookups. Guaranteed in 0.1.0-beta.1.
- **Sync-to-Origin status**: PREVIEW — delta push via Loro CRDT adapter (`src/engine/strict/crdt_adapter.rs`) is wired; cross-repo validation not yet gate-tested.
- **Result parity expectations vs Origin**: `tests/sql_parity/strict.rs` gates same-query / same-result parity for supported DDL and CRUD variants.

### Document (schemaless)

- **Local storage format / persistence**: Loro CRDT documents stored via `nodedb-lite/nodedb-lite/src/engine/crdt/engine.rs`; MessagePack blob payload, redb persistence through the Lite state layer.
- **Query surface promised**: `Insert`, `Upsert`, `Update`, `Delete`, `PointGet`, `Scan` (no WHERE pushdown beyond id). Guaranteed in 0.1.0-beta.1.
- **Sync-to-Origin status**: PREVIEW — CRDT delta serialization complete; handshake and push path exercised in-process; no live-Origin gate test in this release.
- **Result parity expectations vs Origin**: `tests/sql_parity/document.rs` gates the supported insert/read/delete surface; no schema-evolution parity expected in 0.1.0-beta.1.

### Columnar

- **Local storage format / persistence**: segment-based columnar store with per-column codecs, flush, and compaction in `nodedb-lite/nodedb-lite/src/engine/columnar/` (`engine.rs`, `memtable.rs`, `segments.rs`, `catalog.rs`, `manifest.rs`); HTAP materialized-view bridge in `src/engine/htap/`.
- **Query surface promised**: `Insert`, `Scan` (full row scan via `ColumnarEngine::list_rows` over memtable + segments), `Truncate`. HTAP materialized-view routing is EXPERIMENTAL. Guaranteed bounded subset in 0.1.0-beta.1.
- **Sync-to-Origin status**: PREVIEW — columnar inserts replicate via dedicated `ColumnarInsert` (0xA0) wire frame; Origin applies rows through its columnar Data Plane insert handler. Gate test at `tests/sync_interop_columnar.rs`.
- **Result parity expectations vs Origin**: `tests/sql_parity/columnar.rs::columnar_select_all_rows` gates insert/scan row-equality parity.

### Vector

- **Local storage format / persistence**: shared `nodedb-vector` HNSW core; index checkpointed via Lite state layer; entry point at `nodedb-lite/nodedb-lite/src/engine/vector/mod.rs`.
- **Query surface promised**: `VectorSearch` (HNSW + FP32; top-k ANN) via the `vector_search` trait method. Quantization, IVF-PQ, filtered search, and distributed modes are NOT IN 0.1.0. Guaranteed in 0.1.0-beta.1 for basic HNSW semantics.
- **Sync-to-Origin status**: PREVIEW — vector inserts and deletes replicate via dedicated `VectorInsert` (0xA2) and `VectorDelete` (0xA4) wire frames; Origin applies vectors through its HNSW Data Plane insert/delete path. Gate test at `tests/sync_interop_vector.rs`.
- **Result parity expectations vs Origin**: `tests/sql_parity/vector.rs` gates top-k recall parity on the FP32/HNSW path; quantization and hybrid parity have no gate — local correctness only.

### Graph

- **Local storage format / persistence**: shared `nodedb-graph` CSR adjacency index, per-collection `HashMap<String, CsrIndex>` with per-collection checkpoint keys `csr:{name}` under `Namespace::Graph`; CRDT edge docs namespaced as `__edges__{collection}`. Entry point at `nodedb-lite/nodedb-lite/src/engine/graph/mod.rs`, trait implementation in `src/nodedb/trait_impl/graph.rs`.
- **Query surface promised**: `graph_insert_edge(collection, …)`, `graph_delete_edge(collection, …)`, `graph_traverse(collection, …)` (BFS up to configurable depth), `graph_shortest_path(collection, …)`, `graph_stats(collection)`. All ops are collection-scoped — edges in collection A are not visible from collection B. Guaranteed in 0.1.0-beta.1.
- **Sync-to-Origin status**: PREVIEW — graph mutations propagate as CRDT document writes; full graph-query parity against a live Origin is not yet gate-tested.
- **Result parity expectations vs Origin**: `tests/sql_parity/graph.rs` gates traversal and shortest-path result parity for the supported collection-scoped API.

### Key-value

- **Local storage format / persistence**: `nodedb-lite/nodedb-lite/src/nodedb/collection/kv.rs`; dual-mode — direct redb for local-only, CRDT-backed dual-write for sync-enabled mode; no dedicated `engine/kv/` module.
- **Query surface promised**: `kv_put`, `kv_get`, `kv_delete`, `kv_put_with_ttl`, `kv_range_scan`, `kv_compact_expired`; `KvInsert` SQL plan is NOT IN 0.1.0 (returns `LiteError::Unsupported`). Guaranteed in 0.1.0-beta.1.
- **Sync-to-Origin status**: PREVIEW — sync-enabled mode dual-writes to Loro CRDT; Origin KV feature breadth (TTL, sorted-range, secondary predicate) is a known mismatch.
- **Result parity expectations vs Origin**: no parity gate — local correctness only; Origin KV surface is materially broader than the Lite subset.

### Full-text search

- **Local storage format / persistence**: `nodedb-fts` in-memory backend with checkpoint persistence under `Namespace::Fts`; index state (memtable postings, doc lengths, fieldnorm blobs, surrogate maps) is serialized via MessagePack on `flush()` and loaded on `NodeDbLite::open` without re-tokenizing source documents; entry point at `nodedb-lite/nodedb-lite/src/engine/fts/` (`manager.rs`, `checkpoint.rs`, `mod.rs`).
- **Query surface promised**: `text_search` trait method (BM25, top-k). `TextSearch` SQL plan variant is NOT in the supported 8-variant set and returns `LiteError::Unsupported`. BETA in 0.1.0-beta.1.
- **Sync-to-Origin status**: PREVIEW — FTS index changes replicate via `FtsIndex` (0xA6) / `FtsDelete` (0xA8) wire frames; Origin handler (`fts_handler.rs`) assigns a surrogate and dispatches to the Data Plane BM25 inverted index. Gate test at `tests/sync_interop_fts.rs`.
- **Result parity expectations vs Origin**: `tests/fts_persistence.rs` gates round-trip correctness (insert → flush → reopen → search returns identical results); no parity gate against Origin — local correctness only.

### Spatial

- **Local storage format / persistence**: R*-tree backed by `nodedb-spatial`; checkpoint written to `Namespace::Spatial` on `flush()` via `src/engine/spatial/checkpoint.rs`. Persists both R-tree bytes (CRC32C-wrapped) and the `doc_id → entry_id` mapping so that upserts and deletes after a cold open correctly remove stale entries. Cold-open restore uses the checkpoint; falls back to rebuild from CRDT documents only if checkpoint is absent or corrupt.
- **Query surface promised**: `spatial_insert`, `spatial_delete`, `spatial_search_bbox`, `spatial_nearest` native methods on `NodeDbLite`. OGC predicates (`st_contains`, `st_intersects`, `st_within`, `st_dwithin`, `st_distance`) available via `nodedb_spatial::predicates`. `SpatialScan` SQL plan variant returns `LiteError::Unsupported` — that path is orthogonal to this gate.
- **Sync-to-Origin status**: PREVIEW — spatial inserts/deletes replicate via `SpatialInsert` (0xAA) / `SpatialDelete` (0xAC) wire frames; gate test at `tests/sync_interop_spatial.rs`.
- **Result parity expectations vs Origin**: `tests/spatial_engine_gate.rs` gates round-trip correctness (insert → flush → reopen → query returns identical results) and OGC predicate correctness; no parity gate against Origin — local correctness only.

### Timeseries

- **Local storage format / persistence**: dedicated timeseries engine with segment-based storage in `nodedb-lite/nodedb-lite/src/engine/timeseries/` (`engine/`, `identity.rs`, `query_routing.rs`); flush and retention logic present.
- **Query surface promised**: `Insert` + `Scan` (timeseries collections back onto `ColumnarEngine` with `ColumnarProfile::Timeseries`; row scan via the shared columnar `list_rows`). `TimeseriesScan` / `TimeseriesIngest` specialized SQL plan variants are still NOT IN 0.1.0 — generic Scan covers the supported query path.
- **Sync-to-Origin status**: PREVIEW — timeseries inserts replicate via the `ColumnarInsert` (0xA0) wire frame (timeseries collections in Lite are backed by `ColumnarEngine` with `ColumnarProfile::Timeseries`, so they share the columnar sync path with no additional timeseries-specific wire plumbing); gate test at `tests/sync_interop_timeseries.rs`.
- **Result parity expectations vs Origin**: `tests/sql_parity/timeseries.rs::timeseries_select_all_rows` gates Lite-side row-count for inserted rows.

### Array (local)

- **Local storage format / persistence**: tile-based storage with catalog, manifest, memtable, segments, and retention in `nodedb-lite/nodedb-lite/src/engine/array/` (`engine.rs`, `catalog.rs`, `manifest.rs`, `memtable.rs`, `segments.rs`, `retention.rs`).
- **Query surface promised**: `CreateArray`, `InsertArray`, `ArraySlice`, `ArrayProject`, `ArrayFlush`, `ArrayCompact` via local engine; all are unsupported in the SQL plan layer and route through native API calls only. Local BETA in 0.1.0-beta.1.
- **Sync-to-Origin status**: EXPERIMENTAL — Lite `sync/client/receive.rs` does not yet handle `ArrayDelta` / `ArrayDeltaBatch` frames; real transport round-trip is not gate-tested.
- **Result parity expectations vs Origin**: `tests/array.rs` gates local array correctness; `tests/array_sync_interop.rs` is the intended parity gate but all tests are `#[ignore]` — no parity gate active in 0.1.0-beta.1.

### Array (synced)

- **Local storage format / persistence**: same tile-based store as Array (local); sync state tracked via `tests/array_sync_*.rs` (edge-side simulation only).
- **Query surface promised**: no additional query surface beyond local array operations; sync-side receive path not implemented.
- **Sync-to-Origin status**: EXPERIMENTAL — NOT IN 0.1.0-beta.1 interop gates; real `ArrayDelta`/`ArrayDeltaBatch` receive path missing in `nodedb-lite/nodedb-lite/src/sync/client/receive.rs`.
- **Result parity expectations vs Origin**: no parity gate — `tests/array_sync_interop.rs` exists but all tests are `#[ignore]`; local correctness only.

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
| Definition sync    | PREVIEW | Origin emits `DefinitionSync` (0x70) frames after WAL-durable DDL commit for `CREATE/DROP FUNCTION`, `CREATE/DROP TRIGGER`, and `CREATE/DROP PROCEDURE`. Lite's receive path (`sync/transport.rs`, `sync_delegate.rs`) applies the definition locally via `SyncDelegate::import_definition`. Gate tests in `tests/definition_sync_interop.rs` (4/4 passing against a live Origin). |
| Array sync         | BETA | `SyncDelegate::handle_array_delta` and `handle_array_delta_batch` wired in `sync/transport.rs`; `dispatch_frame` routes `ArrayDelta` (0x90) and `ArrayDeltaBatch` (0x91) to `ArrayInbound`; `ArrayAckMsg` queued for Origin GC.  Gate test: `tests/array_sync_interop_real.rs` (5/5 passing). |
| Columnar insert sync | PREVIEW | `ColumnarInsert` (0xA0) frame emitted by Lite on every `ColumnarEngine::insert`; `ColumnarInsertAck` (0xA1) returned by Origin after Data Plane apply. `ColumnarOutbound` queue in `sync/columnar_outbound.rs`; `SyncDelegate` wired in `nodedb/sync_delegate.rs`. Gate test: `tests/sync_interop_columnar.rs`. |
| FTS index sync       | PREVIEW | `FtsIndex` (0xA6) / `FtsIndexAck` (0xA7) and `FtsDelete` (0xA8) / `FtsDeleteAck` (0xA9) frames emitted by Lite on every `document_put` / `document_delete` that touches the FTS index. `FtsOutbound` queue in `sync/fts_outbound.rs`; Origin handler assigns surrogate and dispatches `TextOp::FtsIndexDoc` / `FtsDeleteDoc` to the Data Plane inverted index. `SyncDelegate` wired in `nodedb/sync_delegate.rs`. Gate test: `tests/sync_interop_fts.rs`. |
