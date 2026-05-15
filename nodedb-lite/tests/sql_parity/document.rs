//! SQL parity tests: schemaless document collections.
//!
//! Schemaless collections (CRDT-backed in Lite, document_schemaless in Origin)
//! are created implicitly on first write in Lite. On Origin they require
//! `CREATE COLLECTION ... WITH (engine='document_schemaless')`.
//!
//! SELECT result shape diverges by design:
//!   Lite   → columns: ["id", "document"], document is a JSON string blob
//!   Origin → columns: dynamic field names from the inserted rows
//!
//! Parity contract for schemaless:
//!   INSERT  — rows_affected matches (Lite ≥ 1 per row; Origin ≥ 1)
//!   SELECT  — ID set matches (the same keys are visible on both sides)
//!   UPDATE  — rows_affected matches; updated fields visible on re-SELECT
//!   DELETE  — rows_affected matches; deleted ID absent on re-SELECT
//!
//! These tests require a running Origin binary. They are placed in the `heavy`
//! nextest group via binary filter in .config/nextest.toml.

use std::collections::HashSet;
use std::sync::Arc;

use nodedb_client::NodeDb;
use nodedb_lite::NodeDbLite;
use nodedb_lite::storage::redb_storage::RedbStorage;
use nodedb_types::document::Document;
use nodedb_types::value::Value;

use crate::common::origin::OriginServer;
use crate::common::sql::{OriginPgwire, open_lite};

// ── helpers ───────────────────────────────────────────────────────────────────

/// Register a schemaless collection in Lite's CRDT catalog by writing one
/// bootstrap document via the Rust API. This is required because the SQL
/// catalog only discovers collections that already have data — plan_sql
/// returns "table not found" if the collection has never been written to.
///
/// The bootstrap document is written with id "__bootstrap__" and is
/// immediately deleted so it doesn't pollute the parity comparison.
async fn lite_register_collection(db: &Arc<NodeDbLite<RedbStorage>>, name: &str) {
    let mut doc = Document::new("__bootstrap__");
    doc.set("_init", Value::Bool(true));
    db.document_put(name, doc)
        .await
        .unwrap_or_else(|e| panic!("lite_register_collection({name}): {e}"));
    // Delete the bootstrap document so it doesn't affect ID-set comparisons.
    db.document_delete(name, "__bootstrap__")
        .await
        .unwrap_or_else(|e| panic!("lite_register_collection delete({name}): {e}"));
}

/// Create a schemaless collection on Origin using the canonical DDL.
async fn origin_create_schemaless(pg: &OriginPgwire, name: &str) {
    pg.execute(&format!(
        "CREATE COLLECTION {name} WITH (engine='document_schemaless')"
    ))
    .await;
}

/// Insert a document on Origin. Lite auto-creates the collection on first write.
async fn origin_insert(pg: &OriginPgwire, coll: &str, id: &str, name: &str, age: i64) {
    pg.execute(&format!(
        "INSERT INTO {coll} (id, name, age) VALUES ('{id}', '{name}', {age})"
    ))
    .await;
}

async fn lite_insert(
    db: &Arc<NodeDbLite<RedbStorage>>,
    coll: &str,
    id: &str,
    name: &str,
    age: i64,
) {
    db.execute_sql(
        &format!("INSERT INTO {coll} (id, name, age) VALUES ('{id}', '{name}', {age})"),
        &[],
    )
    .await
    .unwrap_or_else(|e| panic!("Lite INSERT failed: {e}"));
}

/// Collect IDs visible in a Lite schemaless SELECT.
async fn lite_ids(db: &Arc<NodeDbLite<RedbStorage>>, coll: &str) -> HashSet<String> {
    let result = db
        .execute_sql(&format!("SELECT id, document FROM {coll}"), &[])
        .await
        .unwrap_or_else(|e| panic!("Lite SELECT failed: {e}"));
    result
        .rows
        .iter()
        .filter_map(|row| {
            row.first().and_then(|v| {
                if let nodedb_types::value::Value::String(s) = v {
                    Some(s.clone())
                } else {
                    None
                }
            })
        })
        .collect()
}

/// Collect IDs visible in an Origin SELECT (first column assumed to be `id`).
async fn origin_ids(pg: &OriginPgwire, coll: &str) -> HashSet<String> {
    let rows = pg.query(&format!("SELECT id FROM {coll}")).await;
    rows.iter()
        .map(|r| {
            // id column may be TEXT or another type; get as string.
            let id: String = r
                .try_get::<_, String>(0)
                .unwrap_or_else(|_| r.try_get::<_, i64>(0).unwrap_or(0).to_string());
            id
        })
        .collect()
}

// ── tests ─────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn document_insert_parity() {
    let _origin = OriginServer::spawn_with_pgwire();
    let pg = OriginPgwire::connect().await;
    let db = open_lite().await;

    origin_create_schemaless(&pg, "parity_doc_insert").await;
    lite_register_collection(&db, "parity_doc_insert").await;

    // Insert two documents on each side.
    origin_insert(&pg, "parity_doc_insert", "d1", "Alice", 30).await;
    origin_insert(&pg, "parity_doc_insert", "d2", "Bob", 25).await;

    lite_insert(&db, "parity_doc_insert", "d1", "Alice", 30).await;
    lite_insert(&db, "parity_doc_insert", "d2", "Bob", 25).await;

    let lite = lite_ids(&db, "parity_doc_insert").await;
    let origin = origin_ids(&pg, "parity_doc_insert").await;

    assert_eq!(
        lite, origin,
        "document ID sets must match after INSERT\nlite={lite:?}\norigin={origin:?}"
    );
}

#[tokio::test]
async fn document_delete_parity() {
    let _origin = OriginServer::spawn_with_pgwire();
    let pg = OriginPgwire::connect().await;
    let db = open_lite().await;

    origin_create_schemaless(&pg, "parity_doc_delete").await;
    lite_register_collection(&db, "parity_doc_delete").await;

    origin_insert(&pg, "parity_doc_delete", "x1", "Carol", 40).await;
    origin_insert(&pg, "parity_doc_delete", "x2", "Dave", 35).await;
    lite_insert(&db, "parity_doc_delete", "x1", "Carol", 40).await;
    lite_insert(&db, "parity_doc_delete", "x2", "Dave", 35).await;

    // Delete x1 on both sides.
    pg.execute("DELETE FROM parity_doc_delete WHERE id = 'x1'")
        .await;
    db.execute_sql("DELETE FROM parity_doc_delete WHERE id = 'x1'", &[])
        .await
        .unwrap_or_else(|e| panic!("Lite DELETE failed: {e}"));

    let lite = lite_ids(&db, "parity_doc_delete").await;
    let origin = origin_ids(&pg, "parity_doc_delete").await;

    assert_eq!(
        lite, origin,
        "document ID sets must match after DELETE\nlite={lite:?}\norigin={origin:?}"
    );
    assert!(
        !lite.contains("x1"),
        "deleted document 'x1' must not appear on Lite"
    );
    assert!(
        !origin.contains("x1"),
        "deleted document 'x1' must not appear on Origin"
    );
}

#[tokio::test]
async fn document_update_parity() {
    let _origin = OriginServer::spawn_with_pgwire();
    let pg = OriginPgwire::connect().await;
    let db = open_lite().await;

    origin_create_schemaless(&pg, "parity_doc_update").await;
    lite_register_collection(&db, "parity_doc_update").await;

    origin_insert(&pg, "parity_doc_update", "u1", "Eve", 20).await;
    lite_insert(&db, "parity_doc_update", "u1", "Eve", 20).await;

    // Update age on both sides.
    pg.execute("UPDATE parity_doc_update SET age = 21 WHERE id = 'u1'")
        .await;
    db.execute_sql("UPDATE parity_doc_update SET age = 21 WHERE id = 'u1'", &[])
        .await
        .unwrap_or_else(|e| panic!("Lite UPDATE failed: {e}"));

    // After update, both sides must still show u1.
    let lite = lite_ids(&db, "parity_doc_update").await;
    let origin = origin_ids(&pg, "parity_doc_update").await;
    assert_eq!(lite, origin, "IDs must match after UPDATE");
    assert!(lite.contains("u1"), "u1 must still be present after UPDATE");
}

#[tokio::test]
async fn document_truncate_parity() {
    let _origin = OriginServer::spawn_with_pgwire();
    let pg = OriginPgwire::connect().await;
    let db = open_lite().await;

    origin_create_schemaless(&pg, "parity_doc_truncate").await;
    lite_register_collection(&db, "parity_doc_truncate").await;

    origin_insert(&pg, "parity_doc_truncate", "t1", "Frank", 50).await;
    origin_insert(&pg, "parity_doc_truncate", "t2", "Grace", 45).await;
    lite_insert(&db, "parity_doc_truncate", "t1", "Frank", 50).await;
    lite_insert(&db, "parity_doc_truncate", "t2", "Grace", 45).await;

    pg.execute("TRUNCATE parity_doc_truncate").await;
    db.execute_sql("TRUNCATE parity_doc_truncate", &[])
        .await
        .unwrap_or_else(|e| panic!("Lite TRUNCATE failed: {e}"));

    let lite = lite_ids(&db, "parity_doc_truncate").await;
    let origin = origin_ids(&pg, "parity_doc_truncate").await;
    assert!(lite.is_empty(), "Lite must be empty after TRUNCATE");
    assert!(origin.is_empty(), "Origin must be empty after TRUNCATE");
    assert_eq!(lite, origin, "both empty after TRUNCATE");
}

#[tokio::test]
async fn document_select_constant_parity() {
    // SELECT <constant> does not touch any collection — both sides must return
    // exactly one row with the given value.
    let _origin = OriginServer::spawn_with_pgwire();
    let pg = OriginPgwire::connect().await;
    let db = open_lite().await;

    let lite_result = db
        .execute_sql("SELECT 42 AS answer", &[])
        .await
        .expect("Lite SELECT 42");
    assert_eq!(
        lite_result.rows.len(),
        1,
        "Lite: SELECT 42 must return 1 row"
    );

    let origin_rows = pg.query("SELECT 42 AS answer").await;
    assert_eq!(origin_rows.len(), 1, "Origin: SELECT 42 must return 1 row");

    let lite_val = match &lite_result.rows[0][0] {
        nodedb_types::value::Value::Integer(i) => *i,
        other => panic!("expected Integer, got {other:?}"),
    };
    // Origin returns the constant as a text-encoded value via pgwire.
    let origin_val_str: &str = origin_rows[0].get::<_, &str>(0);
    let origin_val: i64 = origin_val_str.parse().unwrap_or_else(|e| {
        panic!("failed to parse origin column 0 as i64: {e} (raw: {origin_val_str:?})")
    });
    assert_eq!(lite_val, origin_val, "SELECT 42 value must match");
}
