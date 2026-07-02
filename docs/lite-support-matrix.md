# NodeDB Lite 0.1.0 Support Matrix

What ships in `0.1.0` — bindings, engines, SQL surface, and Lite-to-Origin
sync, with file and test evidence pinned to the current tree.

---

## Bindings

| Binding                         | Evidence                                                                          |
| ------------------------------- | --------------------------------------------------------------------------------- |
| Rust crate (`nodedb-lite`)      | Full workspace test suite green                                                   |
| C FFI (`nodedb-lite-ffi`)       | `nodedb-lite-ffi/tests/`                                                          |
| WASM crate (`nodedb-lite-wasm`) | `cargo check --target wasm32-unknown-unknown` clean; browser + Node tests via CI  |
| npm `@nodedb/lite`              | Published from the WASM crate on tag release (`release.yml` → `publish-npm`)      |
| Android JNI                    | Rust cross-compiles for `aarch64-linux-android`; no automated Android packaging gate yet |

iOS bindings require a macOS build environment and are not part of `0.1.0`.
See `docs/lite.md` for the current iOS status.

---

## Engines

| Engine                | What works                                                                                     | Evidence                                                                          |
| --------------------- | ---------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------- |
| Strict document       | Binary-tuple rows, schema, CRUD, secondary indexes, Arrow mapping, full row scan               | `tests/strict_document.rs`, `tests/sql_parity/strict.rs`                          |
| Document (schemaless) | Loro CRDT documents over redb, point-get, scan, insert/upsert/update/delete                    | `tests/document.rs`, `tests/sql_parity/document.rs`                               |
| Columnar              | Memtable + segment store, per-column codecs, flush, compaction, full row scan                  | `tests/columnar.rs`, `tests/sql_parity/columnar.rs`                               |
| Timeseries            | Backed by `ColumnarEngine` with `ColumnarProfile::Timeseries`; insert and row scan             | `tests/sql_parity/timeseries.rs`                                                  |
| Vector                | HNSW + FP32 top-k ANN via `vector_search`                                                      | `tests/vector_engine_gate.rs`, `tests/sync_interop_vector.rs`                     |
| Graph                 | Collection-scoped CSR adjacency, edge insert/delete, BFS traversal, shortest path, stats       | `tests/graph_engine_gate.rs`                                                      |
| Full-text             | BM25 top-k over `nodedb-fts` with persistent index; reopens without rebuild                    | `tests/fts_persistence.rs`                                                        |
| Spatial               | Persistent R-tree, bbox / nearest / OGC predicates                                             | `tests/spatial_engine_gate.rs`                                                    |
| Key-value             | `kv_put` / `kv_get` / `kv_delete`, TTL via `kv_put_with_ttl`, range scan, compact-expired      | `tests/kv_engine_gate.rs`, `tests/kv_ttl_and_range.rs`                            |
| Array                 | Tile-based ND store with catalog, manifest, memtable, segments, retention; sync receive wired  | `tests/array.rs`, `tests/array_sync_interop_real.rs`                              |

Vector quantization, filtered search, and distributed modes are NodeDB
Origin features that Lite does not implement; query them remotely.
HTAP materialized-view routing on top of the columnar engine is not
exercised by the 0.1.0 gate tests.

---

## SQL

SQL parses via `nodedb-sql` and executes against the local engines through
the same planner Origin uses. The `SqlPlan` variants executed in 0.1.0:

- `ConstantResult`
- `Scan` — schemaless, strict, columnar, timeseries (full row scan; no WHERE pushdown beyond id)
- `PointGet` — single-key lookup
- `Insert` — with duplicate-key check
- `Upsert` — maps to the CRDT upsert path
- `Update` — literal-value assignments by key list
- `Delete` — by key list
- `Truncate`

The regression gate is `tests/sql_matrix.rs`.

`SqlPlan` variants that the parser accepts but Lite does not execute
return `LiteError::Unsupported`. They are listed in the per-variant
matrix and include all Array DDL/DML variants, JOIN, aggregates, CTEs,
recursive queries, hybrid / multivec / spatial search variants, and the
specialized `TimeseriesScan` / `TimeseriesIngest` / `VectorSearch` /
`TextSearch` plans. Run those queries against Origin directly.

---

## Lite ↔ Origin sync

Sync runs over WebSocket against a `NodeDbServer` exposing the Lite sync
endpoint. Each engine has dedicated wire frames; the receive path is
wired both ways. Cross-repo gate tests boot a real Origin process and
drive a `NodeDbLite` instance against it.

| Capability                  | Wire frames                                                          | Gate test                                |
| --------------------------- | -------------------------------------------------------------------- | ---------------------------------------- |
| Handshake + vector clock    | `Handshake` / `HandshakeAck`                                         | `tests/sync_interop_handshake.rs`        |
| Delta push / ack / reject   | `DeltaPush` / `DeltaAck` / `DeltaReject` (with `CompensationHint`)   | `tests/sync_interop_delta.rs`            |
| Reconnect + replay dedup    | `Resume` / `Snapshot` / sequence-gap re-sync                         | `tests/sync_interop_resume.rs`           |
| Shape subscriptions         | `ShapeSubscribe` / `ShapeSnapshot` / `ShapeDelta`                    | `tests/sync_interop_shape.rs`            |
| Definition sync (DDL)       | `DefinitionSync` (`0x70`)                                            | `tests/definition_sync_interop.rs`       |
| Array sync                  | `ArrayDelta` (`0x90`), `ArrayDeltaBatch` (`0x91`), ack (`0x95`)      | `tests/array_sync_interop_real.rs`       |
| Columnar insert sync        | `ColumnarInsert` (`0xA0`), `ColumnarInsertAck` (`0xA1`)              | `tests/sync_interop_columnar.rs`         |
| Vector insert / delete sync | `VectorInsert` (`0xA2`/`0xA3`), `VectorDelete` (`0xA4`/`0xA5`)       | `tests/sync_interop_vector.rs`           |
| FTS index / delete sync     | `FtsIndex` (`0xA6`/`0xA7`), `FtsDelete` (`0xA8`/`0xA9`)              | `tests/sync_interop_fts.rs`              |
| Spatial insert / delete sync| `SpatialInsert` (`0xAA`/`0xAB`), `SpatialDelete` (`0xAC`/`0xAD`)     | `tests/sync_interop_spatial.rs`          |
| Timeseries insert sync      | Shares the `ColumnarInsert` (`0xA0`) frame                           | `tests/sync_interop_timeseries.rs`       |

A documented public version of the sync wire contract (wire version,
vector-clock encoding, frame catalogue) will land in `docs/` before 1.0.

---

## Notes on Origin parity

Lite is the embedded surface of NodeDB. Where Origin offers more — vector
quantization, distributed vector search, full SQL parity including JOIN
and aggregates, Array DDL/DML, OGC `SpatialScan` over arbitrary shapes,
or KV breadth like sorted-range with secondary predicates — Lite expects
applications to issue those queries against Origin directly via the
remote `NodeDb` client.
