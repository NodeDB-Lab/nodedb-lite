# NodeDB-Lite SQL Support — 0.1.0 Beta

This document is the authoritative SQL compatibility matrix for NodeDB-Lite 0.1.0 beta.
Every entry is anchored to the code that determines the behaviour.
The companion regression gate is `tests/sql_matrix.rs`.

---

## Status legend

| Status | Meaning |
|--------|---------|
| **SUPPORTED** | The plan variant is matched and executed. |
| **PARTIAL** | The variant is matched but some sub-paths return `Unsupported`. |
| **UNSUPPORTED** | Falls through to the `_ =>` arm in `execute_plan`; returns `LiteError::Unsupported`. |

---

## SqlPlan variant matrix

Source of truth: `src/query/engine.rs` — the `execute_plan` match.
Full variant list: `nodedb-sql/src/types/plan/variants.rs`.

### Constant queries

| Variant | Status | SQL example | Note |
|---------|--------|-------------|------|
| `ConstantResult` | **SUPPORTED** | `SELECT 42 AS answer` | `engine.rs:84` — single row, evaluated constants. |

### Read variants

| Variant | Status | SQL example | Note |
|---------|--------|-------------|------|
| `Scan` | **PARTIAL** | `SELECT id, document FROM coll` | `engine.rs:93` — ORDER BY, LIMIT, window functions, and WHERE predicates are guarded; each returns `Unsupported`. Plain full-scan is supported for `DocumentSchemaless` (returns `id`+`document` columns) and `DocumentStrict` (returns correct column names, zero rows — known gap). Other engine types return `QueryResult::empty()`. |
| `PointGet` | **SUPPORTED** | `SELECT id FROM coll WHERE id = 'k1'` | `engine.rs:136` — single-key lookup. Fully implemented for `DocumentSchemaless`. Other engine types return `QueryResult::empty()`. |
| `DocumentIndexLookup` | **UNSUPPORTED** | `SELECT id FROM coll WHERE email = 'x@y.z'` | Falls to `_ =>` arm. Secondary-index equality lookups not wired in Lite 0.1.0. |
| `RangeScan` | **UNSUPPORTED** | `SELECT id FROM coll WHERE ts BETWEEN 1 AND 100` | Falls to `_ =>` arm. Range predicates not wired. |

### Write variants

| Variant | Status | SQL example | Note |
|---------|--------|-------------|------|
| `Insert` | **SUPPORTED** | `INSERT INTO coll (id, name) VALUES ('k1', 'Alice')` | `engine.rs:142` — duplicate-key check honoured. Routes through CRDT upsert. |
| `Upsert` | **SUPPORTED** | `UPSERT INTO coll (id, name) VALUES ('k1', 'Alice')` | `engine.rs:160` — delegates to `execute_insert` with `if_absent=true`. |
| `Update` | **SUPPORTED** | `UPDATE coll SET name = 'Bob' WHERE id = 'k1'` | `engine.rs:148` — literal-value assignments only; `SqlExpr::Column` references silently ignored. |
| `Delete` | **SUPPORTED** | `DELETE FROM coll WHERE id = 'k1'` | `engine.rs:153` — deletes by key list. |
| `Truncate` | **SUPPORTED** | `TRUNCATE coll` | `engine.rs:159` — clears the CRDT collection. |
| `KvInsert` | **UNSUPPORTED** | `INSERT INTO kv_coll (key, value) VALUES ('k', 'v')` | Falls to `_ =>` arm. KV-specific insert plan not handled. |
| `InsertSelect` | **UNSUPPORTED** | `INSERT INTO dst SELECT id FROM src` | Falls to `_ =>` arm. |
| `UpdateFrom` | **UNSUPPORTED** | `UPDATE t SET col = s.col FROM src s WHERE t.id = s.id` | Falls to `_ =>` arm. |

### Join variants

| Variant | Status | SQL example | Note |
|---------|--------|-------------|------|
| `Join` | **UNSUPPORTED** | `SELECT a.id FROM a JOIN b ON a.id = b.aid` | Falls to `_ =>` arm. |
| `LateralTopK` | **UNSUPPORTED** | `SELECT ... FROM outer, LATERAL (SELECT ... ORDER BY x LIMIT k) AS l` | Falls to `_ =>` arm. |
| `LateralLoop` | **UNSUPPORTED** | Correlated LATERAL subquery | Falls to `_ =>` arm. |

### Aggregate variants

| Variant | Status | SQL example | Note |
|---------|--------|-------------|------|
| `Aggregate` | **UNSUPPORTED** | `SELECT COUNT(*) FROM coll` | Falls to `_ =>` arm. GROUP BY, HAVING, and all aggregate functions unsupported. |

### Timeseries variants

| Variant | Status | SQL example | Note |
|---------|--------|-------------|------|
| `TimeseriesScan` | **UNSUPPORTED** | `SELECT ts, value FROM ts_coll WHERE ts > 1000` | Falls to `_ =>` arm. |
| `TimeseriesIngest` | **UNSUPPORTED** | `INSERT INTO ts_coll (ts, value) VALUES (1000, 3.14)` | Falls to `_ =>` arm. Timeseries ingest uses the typed API. |

### Search variants

| Variant | Status | SQL example | Note |
|---------|--------|-------------|------|
| `VectorSearch` | **UNSUPPORTED** | `SELECT id FROM coll ORDER BY vector_distance(emb, '[1,0]') LIMIT 5` | Falls to `_ =>` arm. Use `NodeDb::vector_search` API. |
| `MultiVectorSearch` | **UNSUPPORTED** | Multi-vector MaxSim SQL | Falls to `_ =>` arm. |
| `TextSearch` | **UNSUPPORTED** | `SELECT id FROM coll WHERE SEARCH(content, 'hello')` | Falls to `_ =>` arm. Use `NodeDb::text_search` API. |
| `HybridSearch` | **UNSUPPORTED** | `SELECT rrf_score(...) FROM coll ORDER BY ...` | Falls to `_ =>` arm. |
| `HybridSearchTriple` | **UNSUPPORTED** | Vector + text + graph RRF fusion | Falls to `_ =>` arm. |
| `SpatialScan` | **UNSUPPORTED** | `SELECT id FROM coll WHERE ST_DWithin(geom, POINT(0 0), 5000)` | Falls to `_ =>` arm. |

### Set / composite variants

| Variant | Status | SQL example | Note |
|---------|--------|-------------|------|
| `Union` | **UNSUPPORTED** | `SELECT id FROM a UNION SELECT id FROM b` | Falls to `_ =>` arm. |
| `Intersect` | **UNSUPPORTED** | `SELECT id FROM a INTERSECT SELECT id FROM b` | Falls to `_ =>` arm. |
| `Except` | **UNSUPPORTED** | `SELECT id FROM a EXCEPT SELECT id FROM b` | Falls to `_ =>` arm. |

### CTE / recursive variants

| Variant | Status | SQL example | Note |
|---------|--------|-------------|------|
| `Cte` | **UNSUPPORTED** | `WITH cte AS (SELECT id FROM coll) SELECT * FROM cte` | Falls to `_ =>` arm. |
| `RecursiveScan` | **UNSUPPORTED** | `WITH RECURSIVE tree AS (...) SELECT * FROM tree` | Falls to `_ =>` arm. |
| `RecursiveValue` | **UNSUPPORTED** | `WITH RECURSIVE cnt(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM cnt WHERE n<5)` | Falls to `_ =>` arm. |

### Merge variant

| Variant | Status | SQL example | Note |
|---------|--------|-------------|------|
| `Merge` | **UNSUPPORTED** | `MERGE INTO target USING source ON ...` | Falls to `_ =>` arm. |

### Array DDL / DML / TVF variants

All Array variants fall through to the `_ =>` arm. Array DDL is also not intercepted by `try_handle_ddl`, so `plan_sql` itself may return a parse error before `execute_plan` is reached.

| Variant | Status | SQL example | Note |
|---------|--------|-------------|------|
| `CreateArray` | **UNSUPPORTED** | `CREATE ARRAY genome DIMS (...) ATTRS (...) TILE_EXTENTS (...)` | Parse error or `_ =>` arm. |
| `DropArray` | **UNSUPPORTED** | `DROP ARRAY genome` | Falls to `_ =>` arm. |
| `AlterArray` | **UNSUPPORTED** | `ALTER ARRAY genome SET (audit_retain_ms = 86400000)` | Falls to `_ =>` arm. |
| `InsertArray` | **UNSUPPORTED** | `INSERT INTO ARRAY genome COORDS (100) VALUES (0.5)` | Falls to `_ =>` arm. |
| `DeleteArray` | **UNSUPPORTED** | `DELETE FROM ARRAY genome WHERE COORDS IN ((100))` | Falls to `_ =>` arm. |
| `ArraySlice` | **UNSUPPORTED** | `SELECT * FROM ARRAY_SLICE(genome, {x:[0,99]})` | Falls to `_ =>` arm. |
| `ArrayProject` | **UNSUPPORTED** | `SELECT * FROM ARRAY_PROJECT(genome, ['allele'])` | Falls to `_ =>` arm. |
| `ArrayAgg` | **UNSUPPORTED** | `SELECT * FROM ARRAY_AGG(genome, allele, SUM)` | Falls to `_ =>` arm. |
| `ArrayElementwise` | **UNSUPPORTED** | `SELECT * FROM ARRAY_ELEMENTWISE(a, b, ADD, v)` | Falls to `_ =>` arm. |
| `ArrayFlush` | **UNSUPPORTED** | `SELECT ARRAY_FLUSH(genome)` | Falls to `_ =>` arm. |
| `ArrayCompact` | **UNSUPPORTED** | `SELECT ARRAY_COMPACT(genome)` | Falls to `_ =>` arm. |

### Vector-primary variant

| Variant | Status | SQL example | Note |
|---------|--------|-------------|------|
| `VectorPrimaryInsert` | **UNSUPPORTED** | INSERT into a `WITH (primary='vector')` collection | Falls to `_ =>` arm. |

---

## Summary

| Category | Supported | Unsupported |
|----------|-----------|-------------|
| Read | 2 (Scan partial, PointGet) | 2 (DocumentIndexLookup, RangeScan) |
| Write | 5 (Insert, Upsert, Update, Delete, Truncate) | 3 (KvInsert, InsertSelect, UpdateFrom) |
| Constant | 1 (ConstantResult) | 0 |
| Join | 0 | 3 (Join, LateralTopK, LateralLoop) |
| Aggregate | 0 | 1 (Aggregate) |
| Timeseries | 0 | 2 (TimeseriesScan, TimeseriesIngest) |
| Search | 0 | 6 (VectorSearch, MultiVectorSearch, TextSearch, HybridSearch, HybridSearchTriple, SpatialScan) |
| Set/Composite | 0 | 3 (Union, Intersect, Except) |
| CTE/Recursive | 0 | 3 (Cte, RecursiveScan, RecursiveValue) |
| Merge | 0 | 1 (Merge) |
| Array | 0 | 11 (all Array variants) |
| Vector-primary | 0 | 1 (VectorPrimaryInsert) |
| **Total** | **8** | **36** |

---

## DDL surface

DDL is intercepted by `try_handle_ddl` in `src/query/ddl/mod.rs` before the SQL planner runs.

| Statement | Status | Syntax | Note |
|-----------|--------|--------|------|
| Create schemaless collection | **SUPPORTED** | `CREATE COLLECTION <name>` (auto-created on first write) | Collections are created implicitly via the CRDT engine. |
| Create strict collection | **SUPPORTED** | `CREATE COLLECTION <name> (...) WITH storage = 'strict'` | `ddl/strict.rs` — dispatched when `STORAGE` + `STRICT` present in uppercase. |
| Create columnar collection | **SUPPORTED** | `CREATE COLLECTION <name> (...) WITH storage = 'columnar'` | `ddl/columnar.rs` — dispatched when `STORAGE` + `COLUMNAR` present. |
| Create KV collection | **SUPPORTED** | `CREATE COLLECTION <name> WITH storage = 'kv'` | `ddl/kv.rs` — dispatched when `is_kv_storage_mode` matches. |
| Create timeseries collection | **SUPPORTED** | `CREATE TIMESERIES [COLLECTION] <name> [PARTITION BY TIME(<interval>)]` | `ddl/timeseries.rs` — prefix `CREATE TIMESERIES `. |
| Drop collection | **SUPPORTED** | `DROP COLLECTION <name>` | `ddl/mod.rs:89` — checks strict/columnar engines for dispatch; CRDT collections are not explicitly dropped. |
| Describe collection | **SUPPORTED** | `DESCRIBE <name>` | `ddl/mod.rs:108` — strict schema description. Lite-only. |
| Alter table add column | **SUPPORTED** | `ALTER TABLE <name> ADD COLUMN <col_def>` | `ddl/alter.rs` — dispatched when `ALTER TABLE` + `ADD COLUMN` present. |
| Create materialized view | **SUPPORTED** | `CREATE MATERIALIZED VIEW <target> FROM <source>` | `ddl/htap.rs` — HTAP bridge. |
| Drop materialized view | **SUPPORTED** | `DROP MATERIALIZED VIEW <target>` | `ddl/htap.rs` — HTAP bridge. |
| Create continuous aggregate | **SUPPORTED** | `CREATE CONTINUOUS AGGREGATE <name> ON <source> ...` | `ddl/continuous_agg.rs`. |
| Drop continuous aggregate | **SUPPORTED** | `DROP CONTINUOUS AGGREGATE <name>` | `ddl/continuous_agg.rs`. |
| Show continuous aggregates | **SUPPORTED** | `SHOW CONTINUOUS AGGREGATES [FOR <source>]` | `ddl/continuous_agg.rs`. |
| Convert collection | **SUPPORTED** | `CONVERT COLLECTION <name> TO strict\|columnar\|document` | `ddl/convert.rs`. |
| Create index | **UNSUPPORTED** | `CREATE INDEX <name> ON <coll> (<field>)` | Not intercepted by DDL handler; falls through to `plan_sql` which may succeed in parsing but returns `Unsupported` at `execute_plan`. |
| Drop index | **UNSUPPORTED** | `DROP INDEX <name> ON <coll>` | Same as above. |
| Create array | **UNSUPPORTED** | `CREATE ARRAY ...` | Not intercepted by DDL handler; parse error or `Unsupported`. |

---

## Scan sub-path guards

`Scan` is matched but four sub-conditions reject immediately with `LiteError::Unsupported`
(`src/query/engine.rs:105–133`):

| Guard | Unsupported example | Reason |
|-------|---------------------|--------|
| `sort_keys` non-empty | `SELECT id FROM coll ORDER BY id` | Sorting not implemented. |
| `limit` is `Some(_)` | `SELECT id FROM coll LIMIT 10` | Limit not implemented. |
| `window_functions` non-empty | `SELECT id, ROW_NUMBER() OVER (ORDER BY id) FROM coll` | Window functions not implemented. |
| `filters` non-empty | `SELECT id FROM coll WHERE name = 'Alice'` | WHERE predicates not evaluated (point-get handles `id =` case before Scan). |

---

## Known gaps to address before GA

- `DocumentStrict` scan returns correct column names but zero rows (`engine.rs:192–204`).
- `execute_scan` for non-schemaless/non-strict engine types returns `QueryResult::empty()` instead of `Unsupported`.
- `execute_point_get` for non-schemaless engine types returns `QueryResult::empty()` instead of `Unsupported`.
- `Update` silently drops `SqlExpr::Column` references in assignments (only `SqlExpr::Literal` is applied).
- `KvInsert` falls to the catch-all `_ =>` arm; KV SQL DML is not wired.
