//! Concurrency tests for strict and columnar storage engines.
//!
//! Verifies correct behavior under concurrent read/write operations
//! using tokio tasks for parallelism.

use std::sync::Arc;

use nodedb_client::NodeDb;
use nodedb_lite::{NodeDbLite, RedbStorage};
use nodedb_types::value::Value;

async fn open_db() -> Arc<NodeDbLite<RedbStorage>> {
    let storage = RedbStorage::open_in_memory().unwrap();
    Arc::new(NodeDbLite::open(storage, 1).await.unwrap())
}

// ═══════════════════════════════════════════════════════════════════════
// Concurrent scan + delete (columnar)
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_columnar_scan_and_delete() {
    let db = open_db().await;

    db.execute_sql(
        "CREATE COLLECTION products (
            id BIGINT NOT NULL PRIMARY KEY,
            name TEXT NOT NULL,
            price FLOAT64
        ) WITH storage = 'columnar'",
        &[],
    )
    .await
    .unwrap();

    // Insert rows.
    for i in 0..50 {
        db.columnar_insert(
            "products",
            &[
                Value::Integer(i),
                Value::String(format!("product_{i}")),
                Value::Float(i as f64 * 0.25),
            ],
        )
        .unwrap();
    }

    // Flush to create a segment.
    tokio::task::block_in_place(|| {
        let mut columnar = db.columnar_engine().lock().unwrap();
        tokio::runtime::Handle::current()
            .block_on(columnar.flush_collection("products"))
            .unwrap();
    });

    // Spawn concurrent delete and scan operations.
    let db_del = Arc::clone(&db);
    let delete_task = tokio::spawn(async move {
        for i in 0..10 {
            tokio::task::block_in_place(|| {
                let mut columnar = db_del.columnar_engine().lock().unwrap();
                let _ = columnar.delete("products", &Value::Integer(i));
            });
            tokio::task::yield_now().await;
        }
    });

    let db_scan = Arc::clone(&db);
    let scan_task = tokio::spawn(async move {
        // Scan should see a consistent snapshot — either pre-delete or post-delete.
        let columnar = db_scan.columnar_engine().lock().unwrap();
        let count = columnar.row_count("products");
        // Count should be between 40 (all deletes done) and 50 (no deletes yet).
        assert!((40..=50).contains(&count), "unexpected count: {count}");
    });

    delete_task.await.unwrap();
    scan_task.await.unwrap();
}

// ═══════════════════════════════════════════════════════════════════════
// Concurrent scan + compaction
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_scan_and_compaction() {
    let db = open_db().await;

    db.execute_sql(
        "CREATE COLLECTION logs (
            id BIGINT NOT NULL PRIMARY KEY,
            message TEXT NOT NULL
        ) WITH storage = 'columnar'",
        &[],
    )
    .await
    .unwrap();

    for i in 0..30 {
        db.columnar_insert(
            "logs",
            &[Value::Integer(i), Value::String(format!("log_{i}"))],
        )
        .unwrap();
    }

    tokio::task::block_in_place(|| {
        let mut columnar = db.columnar_engine().lock().unwrap();
        tokio::runtime::Handle::current()
            .block_on(columnar.flush_collection("logs"))
            .unwrap();
    });

    // Delete enough rows to trigger compaction (>20%).
    {
        let mut columnar = db.columnar_engine().lock().unwrap();
        for i in 0..10 {
            columnar.delete("logs", &Value::Integer(i)).unwrap();
        }
    }

    // Compact and scan concurrently.
    let db_compact = Arc::clone(&db);
    let compact_task = tokio::spawn(async move {
        tokio::task::block_in_place(|| {
            let mut columnar = db_compact.columnar_engine().lock().unwrap();
            let handle = tokio::runtime::Handle::current();
            let _ = handle.block_on(columnar.try_compact_collection("logs"));
        });
    });

    let db_scan = Arc::clone(&db);
    let scan_task = tokio::spawn(async move {
        // Yield to let compaction start.
        tokio::task::yield_now().await;
        let columnar = db_scan.columnar_engine().lock().unwrap();
        let count = columnar.row_count("logs");
        // After compaction: 20 rows. Before: 30 (with 10 deleted).
        assert!((20..=30).contains(&count), "unexpected count: {count}");
    });

    compact_task.await.unwrap();
    scan_task.await.unwrap();
}

// ═══════════════════════════════════════════════════════════════════════
// Concurrent insert + flush
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_insert_and_flush() {
    let db = open_db().await;

    db.execute_sql(
        "CREATE COLLECTION events (
            id BIGINT NOT NULL PRIMARY KEY,
            data TEXT NOT NULL
        ) WITH storage = 'columnar'",
        &[],
    )
    .await
    .unwrap();

    // Insert initial batch and flush.
    for i in 0..20 {
        db.columnar_insert(
            "events",
            &[Value::Integer(i), Value::String(format!("event_{i}"))],
        )
        .unwrap();
    }

    // Spawn flush and insert concurrently.
    let db_flush = Arc::clone(&db);
    let flush_task = tokio::spawn(async move {
        tokio::task::block_in_place(|| {
            let mut columnar = db_flush.columnar_engine().lock().unwrap();
            let handle = tokio::runtime::Handle::current();
            handle
                .block_on(columnar.flush_collection("events"))
                .unwrap();
        });
    });

    let db_insert = Arc::clone(&db);
    let insert_task = tokio::spawn(async move {
        // Yield to let flush start.
        tokio::task::yield_now().await;
        // Insert more rows — these go to a fresh memtable after flush.
        for i in 20..30 {
            let _ = db_insert.columnar_insert(
                "events",
                &[Value::Integer(i), Value::String(format!("event_{i}"))],
            );
        }
    });

    flush_task.await.unwrap();
    insert_task.await.unwrap();

    // Total should be 20 (flushed) + up to 10 (post-flush inserts).
    let columnar = db.columnar_engine().lock().unwrap();
    let count = columnar.row_count("events");
    assert!(count >= 20, "at least flushed rows: {count}");
}

// ═══════════════════════════════════════════════════════════════════════
// Concurrent strict document reads + writes
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_strict_reads_and_writes() {
    let db = open_db().await;

    db.execute_sql(
        "CREATE COLLECTION accounts (
            id BIGINT NOT NULL PRIMARY KEY,
            name TEXT NOT NULL,
            balance FLOAT64
        ) WITH storage = 'strict'",
        &[],
    )
    .await
    .unwrap();

    // Seed data.
    for i in 0..20 {
        db.strict_insert(
            "accounts",
            &[
                Value::Integer(i),
                Value::String(format!("account_{i}")),
                Value::Float(1000.0),
            ],
        )
        .await
        .unwrap();
    }

    // Concurrent reads and writes.
    let db_write = Arc::clone(&db);
    let write_task = tokio::spawn(async move {
        for i in 20..30 {
            let _ = db_write
                .strict_insert(
                    "accounts",
                    &[
                        Value::Integer(i),
                        Value::String(format!("account_{i}")),
                        Value::Float(500.0),
                    ],
                )
                .await;
        }
    });

    let db_read = Arc::clone(&db);
    let read_task = tokio::spawn(async move {
        // Read existing rows while writes are happening.
        for i in 0..20 {
            let row = tokio::task::block_in_place(|| {
                let strict = db_read.strict_engine().lock().unwrap();
                tokio::runtime::Handle::current()
                    .block_on(strict.get("accounts", &Value::Integer(i)))
            });
            // Row should always exist — redb MVCC ensures consistent reads.
            assert!(row.is_ok());
            if let Ok(Some(vals)) = row {
                assert_eq!(vals[0], Value::Integer(i));
            }
        }
    });

    write_task.await.unwrap();
    read_task.await.unwrap();

    // All rows should be accessible after both tasks complete.
    let count = tokio::task::block_in_place(|| {
        let strict = db.strict_engine().lock().unwrap();
        tokio::runtime::Handle::current().block_on(strict.count("accounts"))
    })
    .unwrap();
    assert_eq!(count, 30);
}
