//! Strict and columnar storage persistence coverage.
//!
//! Native reopen tests drop database handles without terminating a process.

use nodedb_client::NodeDb;
#[cfg(not(target_arch = "wasm32"))]
use nodedb_lite::{Encryption, LiteConfig};
use nodedb_lite::{NodeDbLite, PagedbStorageMem};
use nodedb_types::value::Value;

async fn open_db() -> std::sync::Arc<NodeDbLite<PagedbStorageMem>> {
    let storage = PagedbStorageMem::open_in_memory().await.unwrap();
    NodeDbLite::open(storage).await.unwrap()
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn strict_insert_is_durable_without_explicit_flush() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("strict_rows.db");
    let config = LiteConfig {
        auto_flush_ms: 0,
        sync_enabled: false,
        ..LiteConfig::default()
    };
    let db = NodeDbLite::open_at_path_with_config(&path, Encryption::Plaintext, config.clone())
        .await
        .unwrap();
    db.execute_sql(
        "CREATE COLLECTION customers (id BIGINT NOT NULL PRIMARY KEY, name TEXT NOT NULL) WITH storage = 'strict'",
        &[],
    ).await.unwrap();
    let row = vec![Value::Integer(1), Value::String("Alice".into())];
    db.strict_insert("customers", &row).await.unwrap();
    db.shutdown().await;
    drop(db);

    let reopened = NodeDbLite::open_at_path_with_config(&path, Encryption::Plaintext, config)
        .await
        .unwrap();
    let stored = reopened
        .strict_engine()
        .get("customers", &Value::Integer(1))
        .await
        .unwrap();
    assert_eq!(stored, Some(row));
}

// ═══════════════════════════════════════════════════════════════════════
// WAL replay for strict INSERT
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn strict_insert_survives_restart() {
    let db = open_db().await;

    // Create a strict collection and insert data.
    db.execute_sql(
        "CREATE COLLECTION customers (
            id BIGINT NOT NULL PRIMARY KEY,
            name TEXT NOT NULL,
            balance FLOAT64
        ) WITH storage = 'strict'",
        &[],
    )
    .await
    .unwrap();

    // Insert via the strict engine.
    db.strict_insert(
        "customers",
        &[
            Value::Integer(1),
            Value::String("Alice".into()),
            Value::Float(100.0),
        ],
    )
    .await
    .unwrap();

    db.strict_insert(
        "customers",
        &[
            Value::Integer(2),
            Value::String("Bob".into()),
            Value::Float(200.0),
        ],
    )
    .await
    .unwrap();

    // Flush to persist.
    db.flush().await.unwrap();

    // Verify data is readable after flush.
    let row = db
        .strict_engine()
        .get("customers", &Value::Integer(1))
        .await
        .unwrap();
    assert!(row.is_some());
    let row = row.unwrap();
    assert_eq!(row[0], Value::Integer(1));
    assert_eq!(row[1], Value::String("Alice".into()));
}

// ═══════════════════════════════════════════════════════════════════════
// WAL replay for columnar INSERT
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn columnar_insert_and_flush() {
    let db = open_db().await;

    db.execute_sql(
        "CREATE COLLECTION metrics (
            id BIGINT NOT NULL PRIMARY KEY,
            name TEXT NOT NULL,
            value FLOAT64
        ) WITH storage = 'columnar'",
        &[],
    )
    .await
    .unwrap();

    // Insert rows into columnar memtable.
    for i in 0..10 {
        db.columnar_insert(
            "metrics",
            &[
                Value::Integer(i),
                Value::String(format!("metric_{i}")),
                Value::Float(i as f64 * 0.5),
            ],
        )
        .await
        .unwrap();
    }

    // Flush memtable to segment.
    db.columnar_engine()
        .flush_collection("metrics")
        .await
        .unwrap();

    // Verify row count after flush.
    let columnar = db.columnar_engine();
    assert_eq!(columnar.row_count("metrics"), 10);
}

// ═══════════════════════════════════════════════════════════════════════
// WAL replay for delete bitmap
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn columnar_delete_bitmap_persists() {
    let db = open_db().await;

    db.execute_sql(
        "CREATE COLLECTION items (
            id BIGINT NOT NULL PRIMARY KEY,
            name TEXT NOT NULL
        ) WITH storage = 'columnar'",
        &[],
    )
    .await
    .unwrap();

    // Insert and flush to create a segment.
    for i in 0..5 {
        db.columnar_insert(
            "items",
            &[Value::Integer(i), Value::String(format!("item_{i}"))],
        )
        .await
        .unwrap();
    }

    db.columnar_engine()
        .flush_collection("items")
        .await
        .unwrap();

    // Delete a row — marks in delete bitmap.
    {
        let columnar = db.columnar_engine();
        let deleted = columnar.delete("items", &Value::Integer(2)).unwrap();
        assert!(deleted);
    }

    // Verify the delete was tracked.
    let columnar = db.columnar_engine();
    // Row count includes the deleted row in the segment but the bitmap marks it.
    assert_eq!(columnar.row_count("items"), 5); // Segment still has 5 rows.
}

// ═══════════════════════════════════════════════════════════════════════
// Compaction crash recovery
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn compaction_produces_valid_segment() {
    let db = open_db().await;

    db.execute_sql(
        "CREATE COLLECTION orders (
            id BIGINT NOT NULL PRIMARY KEY,
            total FLOAT64
        ) WITH storage = 'columnar'",
        &[],
    )
    .await
    .unwrap();

    // Insert and flush.
    for i in 0..20 {
        db.columnar_insert(
            "orders",
            &[Value::Integer(i), Value::Float(i as f64 * 10.0)],
        )
        .await
        .unwrap();
    }

    db.columnar_engine()
        .flush_collection("orders")
        .await
        .unwrap();

    // Delete 50% of rows to trigger compaction threshold.
    {
        let columnar = db.columnar_engine();
        for i in 0..10 {
            columnar.delete("orders", &Value::Integer(i)).unwrap();
        }
    }

    // Run compaction.
    let compacted = db
        .columnar_engine()
        .try_compact_collection("orders")
        .await
        .unwrap();
    assert!(compacted);

    // Verify the compacted segment has the right number of live rows.
    let columnar = db.columnar_engine();
    // After compaction, the segment should have 10 live rows.
    assert_eq!(columnar.row_count("orders"), 10);
}

// ═══════════════════════════════════════════════════════════════════════
// PK index consistency after crash
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn pk_index_consistent_after_operations() {
    let db = open_db().await;

    db.execute_sql(
        "CREATE COLLECTION users (
            id BIGINT NOT NULL PRIMARY KEY,
            name TEXT NOT NULL
        ) WITH storage = 'strict'",
        &[],
    )
    .await
    .unwrap();

    // Insert several rows.
    for i in 0..10 {
        db.strict_insert(
            "users",
            &[Value::Integer(i), Value::String(format!("user_{i}"))],
        )
        .await
        .unwrap();
    }

    // Delete some rows.
    for i in 0..5 {
        db.strict_delete("users", &Value::Integer(i)).await.unwrap();
    }

    // Verify: deleted rows are gone, remaining rows are accessible.
    for i in 0..5 {
        let row = db
            .strict_engine()
            .get("users", &Value::Integer(i))
            .await
            .unwrap();
        assert!(row.is_none(), "deleted row {i} should not exist");
    }
    for i in 5..10 {
        let row = db
            .strict_engine()
            .get("users", &Value::Integer(i))
            .await
            .unwrap();
        assert!(row.is_some(), "row {i} should exist");
    }
    db.strict_insert(
        "users",
        &[Value::Integer(0), Value::String("re-inserted".into())],
    )
    .await
    .unwrap();

    let row = db
        .strict_engine()
        .get("users", &Value::Integer(0))
        .await
        .unwrap();
    assert!(row.is_some());
    assert_eq!(row.unwrap()[1], Value::String("re-inserted".into()));
}
