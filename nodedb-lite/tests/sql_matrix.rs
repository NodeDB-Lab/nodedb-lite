//! SQL compatibility matrix regression gate.
//!
//! This test file is the machine-checkable form of
//! `docs/lite-sql-support.md`. Every supported `SqlPlan` variant has at
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
async fn unsupported_scan_order_by() {
    let db = open_db().await;
    seed(&db, "ng_scan", "s1").await;
    assert_unsupported(&db, "SELECT id FROM ng_scan ORDER BY id").await;
}

#[tokio::test]
async fn unsupported_scan_limit() {
    let db = open_db().await;
    seed(&db, "ng_scan_limit", "s1").await;
    assert_unsupported(&db, "SELECT id FROM ng_scan_limit LIMIT 5").await;
}

#[tokio::test]
async fn unsupported_scan_window_function() {
    let db = open_db().await;
    seed(&db, "ng_win", "s1").await;
    assert_unsupported(
        &db,
        "SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM ng_win",
    )
    .await;
}

#[tokio::test]
async fn unsupported_scan_where_predicate() {
    let db = open_db().await;
    seed(&db, "ng_where", "s1").await;
    // WHERE on a non-id field should be unsupported (Scan with filters).
    assert_unsupported(&db, "SELECT id FROM ng_where WHERE _seed = true").await;
}

// ── Join ─────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn unsupported_join() {
    let db = open_db().await;
    seed(&db, "ng_a", "a1").await;
    seed(&db, "ng_b", "b1").await;
    assert_unsupported(
        &db,
        "SELECT a.id, b.id FROM ng_a a JOIN ng_b b ON a.id = b.id",
    )
    .await;
}

// ── Aggregate ────────────────────────────────────────────────────────────────

#[tokio::test]
async fn unsupported_aggregate_count() {
    let db = open_db().await;
    seed(&db, "ng_agg", "a1").await;
    assert_unsupported(&db, "SELECT COUNT(*) FROM ng_agg").await;
}

#[tokio::test]
async fn unsupported_group_by() {
    let db = open_db().await;
    seed(&db, "ng_grp", "a1").await;
    assert_unsupported(&db, "SELECT id, COUNT(*) FROM ng_grp GROUP BY id").await;
}

#[tokio::test]
async fn unsupported_having() {
    let db = open_db().await;
    seed(&db, "ng_hav", "a1").await;
    assert_unsupported(
        &db,
        "SELECT id, COUNT(*) FROM ng_hav GROUP BY id HAVING COUNT(*) > 1",
    )
    .await;
}

// ── Subquery / CTE ────────────────────────────────────────────────────────────

#[tokio::test]
async fn unsupported_subquery_in_where() {
    let db = open_db().await;
    seed(&db, "ng_sub_outer", "o1").await;
    seed(&db, "ng_sub_inner", "i1").await;
    assert_unsupported(
        &db,
        "SELECT id FROM ng_sub_outer WHERE id IN (SELECT id FROM ng_sub_inner)",
    )
    .await;
}

#[tokio::test]
async fn unsupported_cte() {
    let db = open_db().await;
    seed(&db, "ng_cte", "c1").await;
    assert_unsupported(&db, "WITH cte AS (SELECT id FROM ng_cte) SELECT * FROM cte").await;
}

// ── Vector / FTS / Spatial ────────────────────────────────────────────────────

#[tokio::test]
async fn unsupported_vector_distance_sql() {
    let db = open_db().await;
    seed(&db, "ng_vec", "v1").await;
    assert_unsupported(
        &db,
        "SELECT id FROM ng_vec ORDER BY vector_distance(emb, '[1,0,0]') LIMIT 5",
    )
    .await;
}

#[tokio::test]
async fn unsupported_fts_search_sql() {
    let db = open_db().await;
    seed(&db, "ng_fts", "f1").await;
    assert_unsupported(
        &db,
        "SELECT id FROM ng_fts WHERE SEARCH(content, 'hello world')",
    )
    .await;
}

// ── Set operations ────────────────────────────────────────────────────────────

#[tokio::test]
async fn unsupported_union() {
    let db = open_db().await;
    seed(&db, "ng_union_a", "a1").await;
    seed(&db, "ng_union_b", "b1").await;
    assert_unsupported(
        &db,
        "SELECT id FROM ng_union_a UNION SELECT id FROM ng_union_b",
    )
    .await;
}

// ── Index DDL ────────────────────────────────────────────────────────────────

#[tokio::test]
async fn unsupported_create_index() {
    let db = open_db().await;
    seed(&db, "ng_idx", "i1").await;
    assert_unsupported(&db, "CREATE INDEX idx_name ON ng_idx (name)").await;
}

#[tokio::test]
async fn unsupported_drop_index() {
    let db = open_db().await;
    // DROP INDEX does not require the collection to exist.
    assert_unsupported(&db, "DROP INDEX idx_name ON ng_idx").await;
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
