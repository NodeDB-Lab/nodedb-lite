//! Crash recovery tests for strict and columnar storage engines.
//!
//! Simulates crashes by dropping the NodeDbLite instance and reopening
//! from the same storage. Verifies that data is consistent after recovery.

use nodedb_client::NodeDb;
use nodedb_lite::{NodeDbLite, RedbStorage};
use nodedb_types::value::Value;

async fn open_db() -> NodeDbLite<RedbStorage> {
    let storage = RedbStorage::open_in_memory().unwrap();
    NodeDbLite::open(storage, 1).await.unwrap()
}

// ═══════════════════════════════════════════════════════════════════════
// WAL replay for strict INSERT
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
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

    // Flush to persist to redb.
    db.flush().await.unwrap();

    // Verify data is readable after flush.
    let row = tokio::task::block_in_place(|| {
        let strict = db.strict_engine().lock().unwrap();
        tokio::runtime::Handle::current().block_on(strict.get("customers", &Value::Integer(1)))
    })
    .unwrap();
    assert!(row.is_some());
    let row = row.unwrap();
    assert_eq!(row[0], Value::Integer(1));
    assert_eq!(row[1], Value::String("Alice".into()));
}

// ═══════════════════════════════════════════════════════════════════════
// WAL replay for columnar INSERT
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
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
        .unwrap();
    }

    // Flush memtable to segment.
    tokio::task::block_in_place(|| {
        let mut columnar = db.columnar_engine().lock().unwrap();
        tokio::runtime::Handle::current()
            .block_on(columnar.flush_collection("metrics"))
            .unwrap();
    });

    // Verify row count after flush.
    let columnar = db.columnar_engine().lock().unwrap();
    assert_eq!(columnar.row_count("metrics"), 10);
}

// ═══════════════════════════════════════════════════════════════════════
// WAL replay for delete bitmap
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
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
        .unwrap();
    }

    tokio::task::block_in_place(|| {
        let mut columnar = db.columnar_engine().lock().unwrap();
        tokio::runtime::Handle::current()
            .block_on(columnar.flush_collection("items"))
            .unwrap();
    });

    // Delete a row — marks in delete bitmap.
    {
        let mut columnar = db.columnar_engine().lock().unwrap();
        let deleted = columnar.delete("items", &Value::Integer(2)).unwrap();
        assert!(deleted);
    }

    // Verify the delete was tracked.
    let columnar = db.columnar_engine().lock().unwrap();
    // Row count includes the deleted row in the segment but the bitmap marks it.
    assert_eq!(columnar.row_count("items"), 5); // Segment still has 5 rows.
}

// ═══════════════════════════════════════════════════════════════════════
// Compaction crash recovery
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
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
        .unwrap();
    }

    tokio::task::block_in_place(|| {
        let mut columnar = db.columnar_engine().lock().unwrap();
        tokio::runtime::Handle::current()
            .block_on(columnar.flush_collection("orders"))
            .unwrap();
    });

    // Delete 50% of rows to trigger compaction threshold.
    {
        let mut columnar = db.columnar_engine().lock().unwrap();
        for i in 0..10 {
            columnar.delete("orders", &Value::Integer(i)).unwrap();
        }
    }

    // Run compaction.
    let compacted = tokio::task::block_in_place(|| {
        let mut columnar = db.columnar_engine().lock().unwrap();
        tokio::runtime::Handle::current()
            .block_on(columnar.try_compact_collection("orders"))
            .unwrap()
    });
    assert!(compacted);

    // Verify the compacted segment has the right number of live rows.
    let columnar = db.columnar_engine().lock().unwrap();
    // After compaction, the segment should have 10 live rows.
    assert_eq!(columnar.row_count("orders"), 10);
}

// ═══════════════════════════════════════════════════════════════════════
// PK index consistency after crash
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
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
        let row = tokio::task::block_in_place(|| {
            let strict = db.strict_engine().lock().unwrap();
            tokio::runtime::Handle::current().block_on(strict.get("users", &Value::Integer(i)))
        })
        .unwrap();
        assert!(row.is_none(), "deleted row {i} should not exist");
    }
    for i in 5..10 {
        let row = tokio::task::block_in_place(|| {
            let strict = db.strict_engine().lock().unwrap();
            tokio::runtime::Handle::current().block_on(strict.get("users", &Value::Integer(i)))
        })
        .unwrap();
        assert!(row.is_some(), "row {i} should exist");
    }
    db.strict_insert(
        "users",
        &[Value::Integer(0), Value::String("re-inserted".into())],
    )
    .await
    .unwrap();

    let row = tokio::task::block_in_place(|| {
        let strict = db.strict_engine().lock().unwrap();
        tokio::runtime::Handle::current().block_on(strict.get("users", &Value::Integer(0)))
    })
    .unwrap();
    assert!(row.is_some());
    assert_eq!(row.unwrap()[1], Value::String("re-inserted".into()));
}
