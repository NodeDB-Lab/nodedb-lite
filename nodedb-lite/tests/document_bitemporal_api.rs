// SPDX-License-Identifier: Apache-2.0

//! Integration tests for the bitemporal Document public API (Stage B).
//!
//! These tests exercise the full public `NodeDb` trait path for bitemporal
//! document collections: DDL flag persistence, `document_put`, `document_get`,
//! `document_get_as_of`, `document_put_with_valid_time`, and `document_delete`.
//!
//! Stage A storage-layer tests remain in `document_bitemporal_storage.rs`.

use nodedb_client::NodeDb;
use nodedb_lite::{NodeDbLite, PagedbStorageMem};
use nodedb_types::document::Document;
use nodedb_types::value::Value;

use nodedb_lite::engine::document::history::ops::is_bitemporal;

async fn open_db() -> NodeDbLite<PagedbStorageMem> {
    let storage = PagedbStorageMem::open_in_memory().await.unwrap();
    NodeDbLite::open(storage, 1).await.unwrap()
}

/// Open a db and return a cloned storage handle for direct inspection.
async fn open_db_with_storage() -> (NodeDbLite<PagedbStorageMem>, PagedbStorageMem) {
    let storage = PagedbStorageMem::open_in_memory().await.unwrap();
    let storage_clone = storage.clone();
    let db = NodeDbLite::open(storage, 1).await.unwrap();
    (db, storage_clone)
}

// ---------------------------------------------------------------------------
// Test 1 — create_bitemporal_collection_persists_flag
// ---------------------------------------------------------------------------

/// `CREATE COLLECTION foo WITH (bitemporal=true)` must persist the flag so
/// that subsequent `is_bitemporal(storage, "foo")` returns `true`.
#[tokio::test]
async fn create_bitemporal_collection_persists_flag() {
    let (db, storage) = open_db_with_storage().await;

    db.execute_sql(
        "CREATE COLLECTION bitemp_flag_test WITH (bitemporal=true)",
        &[],
    )
    .await
    .unwrap();

    // Access the cloned storage to verify the flag persisted.
    let flag = is_bitemporal(&storage, "bitemp_flag_test").await.unwrap();
    assert!(flag, "bitemporal flag must be persisted after DDL");
}

// ---------------------------------------------------------------------------
// Test 2 — put_then_get_returns_doc
// ---------------------------------------------------------------------------

/// Basic bitemporal put + current get round-trip via the public API.
#[tokio::test]
async fn put_then_get_returns_doc() {
    let db = open_db().await;

    db.execute_sql("CREATE COLLECTION bt_roundtrip WITH (bitemporal=true)", &[])
        .await
        .unwrap();

    let mut doc = Document::new("doc1");
    doc.set("title", Value::String("hello".into()));
    db.document_put("bt_roundtrip", doc).await.unwrap();

    let result = db.document_get("bt_roundtrip", "doc1").await.unwrap();
    let fetched = result.expect("document must be present after put");
    assert_eq!(fetched.id, "doc1");
    assert_eq!(fetched.get_str("title"), Some("hello"));
}

// ---------------------------------------------------------------------------
// Test 3 — put_v1_then_v2_then_get_as_of_returns_correct_version
// ---------------------------------------------------------------------------

/// Two versions written at controlled timestamps via the storage layer.
/// AS-OF queries return the correct version at each point in system time.
#[tokio::test]
async fn put_v1_then_v2_then_get_as_of_returns_correct_version() {
    use nodedb_lite::engine::document::history::ops::versioned_put;

    let (db, storage) = open_db_with_storage().await;

    db.execute_sql("CREATE COLLECTION bt_asof WITH (bitemporal=true)", &[])
        .await
        .unwrap();

    // Write v1 at system_from = 100 directly via the storage layer.
    let body_v1 =
        nodedb_types::json_msgpack::json_to_msgpack_or_empty(&serde_json::json!({"version": "v1"}));
    versioned_put(&storage, "bt_asof", "doc1", &body_v1, 100, None, None)
        .await
        .unwrap();

    // Write v2 at system_from = 200 directly via the storage layer.
    let body_v2 =
        nodedb_types::json_msgpack::json_to_msgpack_or_empty(&serde_json::json!({"version": "v2"}));
    versioned_put(&storage, "bt_asof", "doc1", &body_v2, 200, None, None)
        .await
        .unwrap();

    // AS-OF t=150: should see v1 (100 <= 150 < 200).
    let v1 = db
        .document_get_as_of("bt_asof", "doc1", Some(150), None)
        .await
        .unwrap()
        .expect("v1 must be visible at t=150");
    assert_eq!(v1.get_str("version"), Some("v1"));

    // AS-OF t=250: should see v2 (most recent, 200 <= 250).
    let v2 = db
        .document_get_as_of("bt_asof", "doc1", Some(250), None)
        .await
        .unwrap()
        .expect("v2 must be visible at t=250");
    assert_eq!(v2.get_str("version"), Some("v2"));
}

// ---------------------------------------------------------------------------
// Test 4 — delete_on_bitemporal_appends_tombstone_not_hard_delete
// ---------------------------------------------------------------------------

/// After delete on a bitemporal collection:
/// - `document_get` returns `None` (tombstone wins for current reads).
/// - `document_get_as_of(t_before_delete)` still returns the document.
#[tokio::test]
async fn delete_on_bitemporal_appends_tombstone_not_hard_delete() {
    use nodedb_lite::engine::document::history::ops::{versioned_put, versioned_tombstone};

    let (db, storage) = open_db_with_storage().await;

    db.execute_sql("CREATE COLLECTION bt_delete WITH (bitemporal=true)", &[])
        .await
        .unwrap();

    // Write a live version at t=100 directly via storage.
    let body =
        nodedb_types::json_msgpack::json_to_msgpack_or_empty(&serde_json::json!({"name": "alive"}));
    versioned_put(&storage, "bt_delete", "doc1", &body, 100, None, None)
        .await
        .unwrap();

    // Append a tombstone at t=200 via storage to simulate a timed delete.
    versioned_tombstone(&storage, "bt_delete", "doc1", 200)
        .await
        .unwrap();

    // Current get via trait returns None (tombstone wins).
    let current = db.document_get("bt_delete", "doc1").await.unwrap();
    assert!(
        current.is_none(),
        "document must not be returned after tombstone"
    );

    // AS-OF t=150 (before the tombstone at t=200) still returns the doc.
    let historical = db
        .document_get_as_of("bt_delete", "doc1", Some(150), None)
        .await
        .unwrap()
        .expect("document must be visible before tombstone timestamp");
    assert_eq!(historical.get_str("name"), Some("alive"));
}

// ---------------------------------------------------------------------------
// Test 5 — put_with_valid_time_then_query_with_valid_filter
// ---------------------------------------------------------------------------

/// `document_put_with_valid_time(valid_from=300, valid_until=500)`:
/// - `document_get_as_of(t=2000, valid_time=400)` returns the doc.
/// - `document_get_as_of(t=2000, valid_time=200)` returns `None`.
#[tokio::test]
async fn put_with_valid_time_then_query_with_valid_filter() {
    let db = open_db().await;

    db.execute_sql(
        "CREATE COLLECTION bt_valid_time WITH (bitemporal=true)",
        &[],
    )
    .await
    .unwrap();

    let mut doc = Document::new("evt1");
    doc.set("event", Value::String("scheduled".into()));

    db.document_put_with_valid_time(
        "bt_valid_time",
        doc,
        Some(300), // valid_from_ms
        Some(500), // valid_until_ms
    )
    .await
    .unwrap();

    // valid_time 400 is within [300, 500).
    let found = db
        .document_get_as_of("bt_valid_time", "evt1", None, Some(400))
        .await
        .unwrap()
        .expect("event must be visible at valid_time=400");
    assert_eq!(found.get_str("event"), Some("scheduled"));

    // valid_time 200 is before valid_from=300.
    let not_found = db
        .document_get_as_of("bt_valid_time", "evt1", None, Some(200))
        .await
        .unwrap();
    assert!(
        not_found.is_none(),
        "valid_time 200 is before valid_from 300"
    );

    // valid_time 500 is at valid_until (exclusive).
    let not_found_at_boundary = db
        .document_get_as_of("bt_valid_time", "evt1", None, Some(500))
        .await
        .unwrap();
    assert!(
        not_found_at_boundary.is_none(),
        "valid_time 500 == valid_until is excluded"
    );
}

// ---------------------------------------------------------------------------
// Test 6 — get_as_of_on_non_bitemporal_collection_errors
// ---------------------------------------------------------------------------

/// Calling `document_get_as_of` on a plain (non-bitemporal) collection must
/// return a typed error, not silently succeed or panic.
#[tokio::test]
async fn get_as_of_on_non_bitemporal_collection_errors() {
    let db = open_db().await;

    // Create a plain document collection (no bitemporal flag).
    let mut doc = Document::new("x1");
    doc.set("field", Value::String("value".into()));
    db.document_put("plain_docs", doc).await.unwrap();

    let result = db
        .document_get_as_of("plain_docs", "x1", Some(9999), None)
        .await;

    assert!(
        result.is_err(),
        "AS-OF on a non-bitemporal collection must return an error"
    );
}

// ---------------------------------------------------------------------------
// Test 7 — non_bitemporal_collection_still_works_unchanged
// ---------------------------------------------------------------------------

/// Regression guard: creating a plain collection and doing put + get + delete
/// must behave exactly as before Stage B.
#[tokio::test]
async fn non_bitemporal_collection_still_works_unchanged() {
    let db = open_db().await;

    let mut doc = Document::new("n1");
    doc.set("body", Value::String("original".into()));
    db.document_put("notes", doc).await.unwrap();

    let fetched = db.document_get("notes", "n1").await.unwrap();
    let fetched = fetched.expect("document must exist after put");
    assert_eq!(fetched.get_str("body"), Some("original"));

    // Update the document.
    let mut updated = Document::new("n1");
    updated.set("body", Value::String("updated".into()));
    db.document_put("notes", updated).await.unwrap();

    let after_update = db
        .document_get("notes", "n1")
        .await
        .unwrap()
        .expect("document must exist after update");
    assert_eq!(after_update.get_str("body"), Some("updated"));

    // Delete the document.
    db.document_delete("notes", "n1").await.unwrap();

    let after_delete = db.document_get("notes", "n1").await.unwrap();
    assert!(
        after_delete.is_none(),
        "document must be absent after hard delete on plain collection"
    );
}

// ---------------------------------------------------------------------------
// Regression: issue #3 / #6 — SQL-DDL bitemporal document collection is
// SELECT-able and persists CollectionMeta.
// ---------------------------------------------------------------------------

/// `CREATE COLLECTION ... WITH (bitemporal=true)` via SQL DDL must:
///   1. persist a `CollectionMeta` under `collection:{name}` (issue #6 — the
///      key the sync outbound announce and the SQL catalog both read), and
///   2. resolve for a plain `SELECT ... FROM <name>` instead of returning
///      "table not found" (issue #3), surfacing the real bitemporal flag.
#[tokio::test]
async fn sql_ddl_bitemporal_collection_is_queryable_and_persists_meta() {
    use nodedb_lite::PagedbStorageMem;
    use nodedb_lite::storage::engine::StorageEngine;

    let storage = PagedbStorageMem::open_in_memory().await.unwrap();
    let storage_clone = storage.clone();
    let db = NodeDbLite::open(storage, 1).await.unwrap();

    db.execute_sql("CREATE COLLECTION entries WITH (bitemporal=true)", &[])
        .await
        .expect("create bitemporal collection via SQL DDL");

    // Issue #6: CollectionMeta persisted under `collection:entries`, carrying
    // the real bitemporal flag — this is what the announce path reads.
    let raw = storage_clone
        .get(nodedb_types::Namespace::Meta, b"collection:entries")
        .await
        .expect("meta read")
        .expect("CollectionMeta must be persisted for a SQL-DDL bitemporal collection");
    let meta: nodedb_lite::nodedb::collection::CollectionMeta =
        sonic_rs::from_slice(&raw).expect("decode CollectionMeta");
    assert!(
        meta.bitemporal,
        "persisted meta must carry bitemporal=true, got {meta:?}"
    );

    // Insert three documents.
    for i in 0..3 {
        let mut doc = Document::new(format!("d{i}"));
        doc.set("body", Value::String(format!("row {i}")));
        db.document_put("entries", doc).await.unwrap();
    }

    // Issue #3: a bare SELECT must resolve the relation (not "table not found")
    // and return the inserted rows.
    let r = db
        .execute_sql("SELECT id FROM entries", &[])
        .await
        .expect("SELECT on a bitemporal document collection must resolve, not error");
    assert_eq!(
        r.rows.len(),
        3,
        "SELECT id FROM entries must return the three inserted documents"
    );
}
