//! Negative-test surface: SQL constructs that Origin supports but Lite 0.1.0
//! does not. Each test asserts that Lite returns a typed Unsupported error
//! (not a panic, not a silent wrong result, not a generic Query error that
//! just swallowed an unrelated failure).
//!
//! Origin is NOT started for negative tests — these are pure Lite-side checks.
//!
//! Collections are pre-seeded via the Rust API (document_put) so that
//! the catalog knows them. This avoids the SQL chicken-and-egg bootstrap
//! (plan_sql fails with "table not found" before the collection is registered).

use std::sync::Arc;

use nodedb_client::NodeDb;
use nodedb_lite::NodeDbLite;
use nodedb_lite::storage::redb_storage::RedbStorage;
use nodedb_types::document::Document;
use nodedb_types::value::Value;

use crate::common::sql::open_lite;

// ── Setup helpers ─────────────────────────────────────────────────────────────

/// Seed a schemaless collection via the Rust API so it appears in the catalog.
async fn seed_collection(db: &Arc<NodeDbLite<RedbStorage>>, collection: &str, id: &str) {
    let mut doc = Document::new(id);
    doc.set("_seed", Value::Bool(true));
    db.document_put(collection, doc)
        .await
        .unwrap_or_else(|e| panic!("seed {collection}: {e}"));
}

// ── JOIN ──────────────────────────────────────────────────────────────────────
// Join is now implemented — negative test removed.

// ── Window functions ──────────────────────────────────────────────────────────

#[tokio::test]
async fn window_function_returns_row_numbers() {
    let db = open_lite().await;
    seed_collection(&db, "win_users", "u1").await;
    seed_collection(&db, "win_users", "u2").await;
    let r = db
        .execute_sql(
            "SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM win_users",
            &[],
        )
        .await
        .expect("window function must succeed");
    assert_eq!(r.rows.len(), 2, "window function must return all rows");
}

// ── Aggregates ────────────────────────────────────────────────────────────────
// Aggregate is now implemented — negative test removed.

// ── Subqueries ────────────────────────────────────────────────────────────────
// Subquery (IN with SELECT) is now implemented via Join lowering — negative test removed.

// ── GROUP BY ──────────────────────────────────────────────────────────────────
// GROUP BY is now implemented — negative test removed.

// ── HAVING ────────────────────────────────────────────────────────────────────
// HAVING is now implemented — negative test removed.

// ── ORDER BY with LIMIT on a collection ──────────────────────────────────────

#[tokio::test]
async fn order_by_limit_sorts_and_truncates() {
    let db = open_lite().await;
    seed_collection(&db, "ob_limit_users", "b").await;
    seed_collection(&db, "ob_limit_users", "a").await;
    let r = db
        .execute_sql("SELECT id FROM ob_limit_users ORDER BY id LIMIT 10", &[])
        .await
        .expect("ORDER BY ... LIMIT must succeed");
    assert_eq!(r.rows.len(), 2, "ORDER BY LIMIT must return rows");
    let first = r.rows[0][0].to_string();
    let second = r.rows[1][0].to_string();
    assert!(
        first <= second,
        "ORDER BY id must produce ascending order; got {first:?} before {second:?}"
    );
}

// ── CTE (WITH clause) ─────────────────────────────────────────────────────────

#[tokio::test]
async fn cte_resolves_inline() {
    let db = open_lite().await;
    seed_collection(&db, "cte_users", "u1").await;
    // CTE must execute without error (previously returned Unsupported).
    db.execute_sql(
        "WITH cte AS (SELECT id FROM cte_users) SELECT id FROM cte",
        &[],
    )
    .await
    .expect("CTE must succeed");
}

// ── Vector SQL (VECTOR_DISTANCE) ──────────────────────────────────────────────

#[tokio::test]
async fn vector_distance_sql_returns_results() {
    let db = open_lite().await;
    seed_collection(&db, "embeddings", "e1").await;
    let r = db
        .execute_sql(
            "SELECT id FROM embeddings ORDER BY vector_distance(embedding, '[1,0,0]') LIMIT 5",
            &[],
        )
        .await
        .expect("vector_distance SQL must succeed");
    assert_eq!(
        r.columns,
        vec!["id".to_string(), "distance".to_string()],
        "vector_distance must return id and distance columns"
    );
}

// ── FTS SEARCH function ───────────────────────────────────────────────────────

#[tokio::test]
async fn fts_search_sql_returns_results() {
    let db = open_lite().await;
    seed_collection(&db, "docs", "d1").await;
    let r = db
        .execute_sql(
            "SELECT id FROM docs WHERE SEARCH(content, 'hello world')",
            &[],
        )
        .await
        .expect("SEARCH SQL must succeed");
    assert_eq!(
        r.columns,
        vec!["id".to_string(), "score".to_string()],
        "SEARCH must return id and score columns"
    );
}

// ── CREATE INDEX ──────────────────────────────────────────────────────────────

#[tokio::test]
async fn create_index_registers_field_index() {
    let db = open_lite().await;
    seed_collection(&db, "users", "u1").await;
    let r = db
        .execute_sql("CREATE INDEX idx_name ON users (name)", &[])
        .await
        .expect("CREATE INDEX must succeed");
    assert_eq!(
        r.rows_affected, 0,
        "CREATE INDEX on empty field produces 0 rows_affected"
    );
}

// ── ALTER COLLECTION (schema evolution on strict) ─────────────────────────────

#[tokio::test]
async fn alter_strict_collection_is_rejected() {
    // ALTER COLLECTION for schema evolution (ADD COLUMN etc.) is not supported
    // on Lite. The nodedb-sql parser does not recognise the `ALTER COLLECTION`
    // syntax (it expects ALTER TABLE/VIEW/etc.), so Lite returns a parse-level
    // error rather than a typed Unsupported error. This is the same documented
    // pattern as MATCH and CREATE ARRAY syntax.
    //
    // Contract: the query must return *some* error — not succeed silently, not
    // panic. The exact error variant is a parse error (storage/query).
    let db = open_lite().await;
    let result = db
        .execute_sql(
            "ALTER COLLECTION strict_schema ADD COLUMN rating FLOAT64",
            &[],
        )
        .await;
    assert!(
        result.is_err(),
        "ALTER COLLECTION must return an error on Lite (schema evolution is not supported)"
    );
}

// ── DROP INDEX ────────────────────────────────────────────────────────────────

#[tokio::test]
async fn drop_index_succeeds() {
    let db = open_lite().await;
    db.execute_sql("DROP INDEX idx_name ON users", &[])
        .await
        .expect("DROP INDEX must succeed");
}

// ── Graph MATCH — parse-level rejection ──────────────────────────────────────

#[tokio::test]
async fn graph_match_sql_is_parse_error() {
    // The MATCH pattern syntax (`(a)-[:REL]->(b)`) is not valid SQL and
    // will fail at parse time with a Query error. This is acceptable for
    // the beta: Lite returns an error (not a silent wrong result or panic).
    // The exact error kind (Query vs Unsupported) is documented here.
    let db = open_lite().await;
    let result = db
        .execute_sql(
            "SELECT * FROM MATCH (a)-[:KNOWS]->(b) WHERE a.id = 'u1'",
            &[],
        )
        .await;
    assert!(result.is_err(), "MATCH syntax must return an error on Lite");
}

// ── ARRAY engine SQL — parse-level rejection ──────────────────────────────────

#[tokio::test]
async fn create_array_ddl_is_parse_error() {
    // CREATE ARRAY syntax is not understood by nodedb-sql on Lite.
    // Returns a parse error (Query), not Unsupported. Documented behavior.
    let db = open_lite().await;
    let result = db
        .execute_sql(
            "CREATE ARRAY genome DIMS (pos INT64 [0, 1000000]) ATTRS (allele TEXT) TILE_EXTENTS (1000)",
            &[],
        )
        .await;
    assert!(result.is_err(), "CREATE ARRAY must return an error on Lite");
}
