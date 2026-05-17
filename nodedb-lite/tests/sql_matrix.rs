//! SQL compatibility matrix regression gate.
//!
//! This test file is the machine-checkable form of the SQL support
//! matrix in `docs/lite-support-matrix.md`. Every supported `SqlPlan` variant has at
//! least one test that asserts the query succeeds (any non-error result is
//! acceptable — row content is verified in `tests/sql_parity/`). Every
//! unsupported variant has at least one test that asserts `LiteError::Unsupported`
//! is returned.
//!
//! If a future change silently adds or removes support for a documented
//! variant, this file will fail immediately.
//!
//! Run with:
//!   cargo nextest run -p nodedb-lite --test sql_matrix

mod common;

use std::sync::Arc;

use nodedb_client::NodeDb;
use nodedb_lite::storage::redb_storage::RedbStorage;
use nodedb_lite::{NodeDbLite, RedbStorage as RS};
use nodedb_types::document::Document;
use nodedb_types::value::Value;

// ── Setup helpers ─────────────────────────────────────────────────────────────

async fn open_db() -> Arc<NodeDbLite<RedbStorage>> {
    let storage = RS::open_in_memory().expect("open_in_memory");
    Arc::new(
        NodeDbLite::open(storage, 1)
            .await
            .expect("NodeDbLite::open"),
    )
}

/// Seed a schemaless collection so it appears in the SQL catalog.
async fn seed(db: &Arc<NodeDbLite<RedbStorage>>, collection: &str, id: &str) {
    let mut doc = Document::new(id);
    doc.set("_seed", Value::Bool(true));
    db.document_put(collection, doc)
        .await
        .unwrap_or_else(|e| panic!("seed {collection}/{id}: {e}"));
}

/// Assert the query succeeds (any `Ok` result is acceptable).
async fn assert_ok(db: &Arc<NodeDbLite<RedbStorage>>, sql: &str) {
    db.execute_sql(sql, &[])
        .await
        .unwrap_or_else(|e| panic!("expected Ok for SQL: {sql:?}\n  got: {e}"));
}

/// Assert the query returns a typed Unsupported error.
async fn assert_unsupported(db: &Arc<NodeDbLite<RedbStorage>>, sql: &str) {
    let result = db.execute_sql(sql, &[]).await;
    match result {
        Err(e) => {
            let msg = e.to_string();
            assert!(
                msg.contains("unsupported")
                    || msg.contains("Unsupported")
                    || msg.contains("not supported"),
                "expected Unsupported error for SQL: {sql:?}\n  got: {msg}"
            );
        }
        Ok(r) => panic!(
            "expected Unsupported error but query succeeded for SQL: {sql:?}\n  \
             columns: {:?}, rows: {}",
            r.columns,
            r.rows.len()
        ),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// SUPPORTED variants
// ─────────────────────────────────────────────────────────────────────────────

// ── ConstantResult ────────────────────────────────────────────────────────────

#[tokio::test]
async fn supported_constant_result_integer() {
    let db = open_db().await;
    let r = db
        .execute_sql("SELECT 42 AS answer", &[])
        .await
        .expect("ConstantResult must succeed");
    assert_eq!(
        r.rows.len(),
        1,
        "ConstantResult must produce exactly one row"
    );
}

#[tokio::test]
async fn supported_constant_result_string() {
    let db = open_db().await;
    let r = db
        .execute_sql("SELECT 'hello' AS greeting", &[])
        .await
        .expect("ConstantResult string must succeed");
    assert_eq!(r.rows.len(), 1);
}

// ── Scan ─────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn supported_scan_plain() {
    let db = open_db().await;
    seed(&db, "scan_coll", "s1").await;
    assert_ok(&db, "SELECT id, document FROM scan_coll").await;
}

// ── PointGet ─────────────────────────────────────────────────────────────────

#[tokio::test]
async fn supported_point_get() {
    let db = open_db().await;
    seed(&db, "pg_coll", "p1").await;
    assert_ok(&db, "SELECT id FROM pg_coll WHERE id = 'p1'").await;
}

// ── Insert ────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn supported_insert_single_row() {
    let db = open_db().await;
    seed(&db, "ins_coll", "existing").await;
    let r = db
        .execute_sql(
            "INSERT INTO ins_coll (id, name) VALUES ('ins1', 'Alice')",
            &[],
        )
        .await
        .expect("Insert must succeed");
    assert!(
        r.rows_affected >= 1,
        "Insert must report rows_affected >= 1"
    );
}

#[tokio::test]
async fn supported_insert_on_conflict_do_nothing() {
    let db = open_db().await;
    seed(&db, "ins_coll2", "existing").await;
    // First insert succeeds.
    db.execute_sql(
        "INSERT INTO ins_coll2 (id, name) VALUES ('dup1', 'Alice')",
        &[],
    )
    .await
    .expect("first insert");
    // Second insert with ON CONFLICT DO NOTHING must also succeed (no error).
    db.execute_sql(
        "INSERT INTO ins_coll2 (id, name) VALUES ('dup1', 'Bob') ON CONFLICT DO NOTHING",
        &[],
    )
    .await
    .expect("insert on conflict do nothing must succeed");
}

// ── Upsert ────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn supported_upsert() {
    let db = open_db().await;
    seed(&db, "ups_coll", "existing").await;
    assert_ok(
        &db,
        "UPSERT INTO ups_coll (id, name) VALUES ('u1', 'Alice')",
    )
    .await;
}

// ── Update ────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn supported_update_by_key() {
    let db = open_db().await;
    seed(&db, "upd_coll", "row1").await;
    assert_ok(
        &db,
        "UPDATE upd_coll SET name = 'Charlie' WHERE id = 'row1'",
    )
    .await;
}

// ── Delete ────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn supported_delete_by_key() {
    let db = open_db().await;
    seed(&db, "del_coll", "d1").await;
    assert_ok(&db, "DELETE FROM del_coll WHERE id = 'd1'").await;
}

// ── Truncate ─────────────────────────────────────────────────────────────────

#[tokio::test]
async fn supported_truncate() {
    let db = open_db().await;
    seed(&db, "trunc_coll", "t1").await;
    assert_ok(&db, "TRUNCATE trunc_coll").await;
}

// ─────────────────────────────────────────────────────────────────────────────
// UNSUPPORTED variants
// ─────────────────────────────────────────────────────────────────────────────

// ── Scan guards ──────────────────────────────────────────────────────────────

#[tokio::test]
async fn scan_order_by_sorts_rows() {
    let db = open_db().await;
    seed(&db, "ob_coll", "b").await;
    seed(&db, "ob_coll", "a").await;
    let r = db
        .execute_sql("SELECT id FROM ob_coll ORDER BY id", &[])
        .await
        .expect("ORDER BY must succeed");
    assert_eq!(r.rows.len(), 2, "ORDER BY must return all rows");
    // First row must sort before second (ascending).
    let first = r.rows[0][0].to_string();
    let second = r.rows[1][0].to_string();
    assert!(
        first <= second,
        "ORDER BY id must produce ascending order; got {first:?} before {second:?}"
    );
}

#[tokio::test]
async fn scan_limit_truncates_rows() {
    let db = open_db().await;
    for i in 0..5u32 {
        seed(&db, "lim_coll", &format!("r{i}")).await;
    }
    let r = db
        .execute_sql("SELECT id FROM lim_coll LIMIT 3", &[])
        .await
        .expect("LIMIT must succeed");
    assert_eq!(r.rows.len(), 3, "LIMIT 3 must return exactly 3 rows");
}

#[tokio::test]
async fn scan_window_function_works() {
    let db = open_db().await;
    seed(&db, "win_coll", "w1").await;
    seed(&db, "win_coll", "w2").await;
    let r = db
        .execute_sql(
            "SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM win_coll",
            &[],
        )
        .await
        .expect("window function must succeed");
    assert_eq!(r.rows.len(), 2, "window function must return all rows");
}

#[tokio::test]
async fn unsupported_scan_where_predicate() {
    let db = open_db().await;
    seed(&db, "ng_where", "s1").await;
    // WHERE on a non-id field should be unsupported (Scan with filters).
    assert_unsupported(&db, "SELECT id FROM ng_where WHERE _seed = true").await;
}

// ── Join ─────────────────────────────────────────────────────────────────────
// Join is now implemented — negative test removed.

// ── Aggregate ────────────────────────────────────────────────────────────────
// Aggregate, GROUP BY, and HAVING are now implemented — negative tests removed.

// ── Subquery / CTE ────────────────────────────────────────────────────────────
// Subquery (IN with SELECT) is now implemented via Join lowering — negative test removed.

#[tokio::test]
async fn cte_resolves_inline() {
    let db = open_db().await;
    seed(&db, "cte_coll", "c1").await;
    // CTE must execute without error (previously returned Unsupported).
    assert_ok(
        &db,
        "WITH cte AS (SELECT id FROM cte_coll) SELECT id FROM cte",
    )
    .await;
}

// ── Vector / FTS / Spatial ────────────────────────────────────────────────────

#[tokio::test]
async fn vector_distance_sql() {
    let db = open_db().await;
    seed(&db, "ng_vec", "v1").await;
    assert_ok(
        &db,
        "SELECT id FROM ng_vec ORDER BY vector_distance(emb, '[1,0,0]') LIMIT 5",
    )
    .await;
}

#[tokio::test]
async fn fts_search_sql() {
    let db = open_db().await;
    seed(&db, "ng_fts", "f1").await;
    assert_ok(
        &db,
        "SELECT id FROM ng_fts WHERE SEARCH(content, 'hello world')",
    )
    .await;
}

// ── Set operations ────────────────────────────────────────────────────────────
// UNION / INTERSECT / EXCEPT are now implemented — negative tests removed.

// ── Index DDL ────────────────────────────────────────────────────────────────

#[tokio::test]
async fn create_index() {
    let db = open_db().await;
    seed(&db, "ng_idx", "i1").await;
    assert_ok(&db, "CREATE INDEX idx_name ON ng_idx (name)").await;
}

#[tokio::test]
async fn drop_index() {
    let db = open_db().await;
    // DROP INDEX does not require the collection to have any indexed rows.
    assert_ok(&db, "DROP INDEX idx_name ON ng_idx").await;
}

// ── Array DDL/DML ─────────────────────────────────────────────────────────────

#[tokio::test]
async fn unsupported_create_array_ddl() {
    // CREATE ARRAY is not intercepted by try_handle_ddl and is not valid SQL.
    // The query must return an error (parse error or Unsupported).
    let db = open_db().await;
    let result = db
        .execute_sql(
            "CREATE ARRAY genome DIMS (pos INT64 [0, 1000000]) ATTRS (allele TEXT) TILE_EXTENTS (1000)",
            &[],
        )
        .await;
    assert!(result.is_err(), "CREATE ARRAY must return an error on Lite");
}

// ── Graph MATCH ───────────────────────────────────────────────────────────────

#[tokio::test]
async fn unsupported_graph_match_parse_error() {
    // MATCH pattern syntax is not valid SQL — parse error is acceptable.
    let db = open_db().await;
    let result = db
        .execute_sql(
            "SELECT * FROM MATCH (a)-[:KNOWS]->(b) WHERE a.id = 'u1'",
            &[],
        )
        .await;
    assert!(result.is_err(), "MATCH syntax must return an error on Lite");
}
