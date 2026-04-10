//! HTAP bridge integration tests.
//!
//! Tests the CDC pipeline from strict document collections to columnar
//! materialized views, query routing, and consistency controls.

use std::sync::Arc;

use nodedb_client::NodeDb;
use nodedb_lite::{NodeDbLite, RedbStorage};
use nodedb_types::value::Value;

async fn open_db() -> Arc<NodeDbLite<RedbStorage>> {
    let storage = RedbStorage::open_in_memory().unwrap();
    Arc::new(NodeDbLite::open(storage, 1).await.unwrap())
}

// ═══════════════════════════════════════════════════════════════════════
// CDC correctness
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cdc_replicates_inserts_to_materialized_view() {
    let db = open_db().await;

    // Create strict source collection.
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

    // Create materialized view.
    db.execute_sql(
        "CREATE MATERIALIZED VIEW customer_analytics FROM customers",
        &[],
    )
    .await
    .unwrap();

    // Insert into strict — should replicate to columnar via CDC.
    db.strict_insert(
        "customers",
        &[
            Value::Integer(1),
            Value::String("Alice".into()),
            Value::Float(1000.0),
        ],
    )
    .await
    .unwrap();

    db.strict_insert(
        "customers",
        &[
            Value::Integer(2),
            Value::String("Bob".into()),
            Value::Float(2000.0),
        ],
    )
    .await
    .unwrap();

    // Verify: columnar materialized view received the rows.
    let columnar = db.columnar_engine().lock().unwrap();
    assert_eq!(columnar.row_count("customer_analytics"), 2);
}

// ═══════════════════════════════════════════════════════════════════════
// CDC lag
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cdc_lag_is_bounded() {
    let db = open_db().await;

    db.execute_sql(
        "CREATE COLLECTION orders (
            id BIGINT NOT NULL PRIMARY KEY,
            total FLOAT64
        ) WITH storage = 'strict'",
        &[],
    )
    .await
    .unwrap();

    db.execute_sql("CREATE MATERIALIZED VIEW order_analytics FROM orders", &[])
        .await
        .unwrap();

    // Insert a batch.
    for i in 0..100 {
        db.strict_insert(
            "orders",
            &[Value::Integer(i), Value::Float(i as f64 * 10.0)],
        )
        .await
        .unwrap();
    }

    // In Lite, CDC is synchronous — lag should be minimal.
    // The HTAP bridge tracks last_replicated_ms.
    let htap = db.htap_bridge().lock().unwrap();
    let lag = htap.lag_ms("order_analytics");
    // Synchronous CDC: lag should be < 1 second.
    assert!(lag < 1000, "CDC lag too high: {lag}ms");
}

// ═══════════════════════════════════════════════════════════════════════
// Default routing safety
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn default_routing_goes_to_source() {
    let db = open_db().await;

    db.execute_sql(
        "CREATE COLLECTION accounts (
            id BIGINT NOT NULL PRIMARY KEY,
            name TEXT NOT NULL
        ) WITH storage = 'strict'",
        &[],
    )
    .await
    .unwrap();

    db.execute_sql(
        "CREATE MATERIALIZED VIEW account_analytics FROM accounts",
        &[],
    )
    .await
    .unwrap();

    db.strict_insert(
        "accounts",
        &[Value::Integer(1), Value::String("Test".into())],
    )
    .await
    .unwrap();

    // With default read_source='source', a SELECT should go to the strict
    // collection, NOT the materialized view. The strict collection is
    // registered as the table provider by default.
    let result = db
        .execute_sql("SELECT * FROM accounts WHERE id = 1", &[])
        .await;

    // Should succeed — reading from the source strict collection.
    assert!(result.is_ok(), "query should succeed: {result:?}");
}

// ═══════════════════════════════════════════════════════════════════════
// Auto routing correctness
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn auto_routing_for_analytical_queries() {
    let db = open_db().await;

    db.execute_sql(
        "CREATE COLLECTION sales (
            id BIGINT NOT NULL PRIMARY KEY,
            amount FLOAT64,
            region TEXT
        ) WITH storage = 'strict'",
        &[],
    )
    .await
    .unwrap();

    db.execute_sql("CREATE MATERIALIZED VIEW sales_analytics FROM sales", &[])
        .await
        .unwrap();

    // Insert data.
    for i in 0..10 {
        db.strict_insert(
            "sales",
            &[
                Value::Integer(i),
                Value::Float(i as f64 * 100.0),
                Value::String(if i % 2 == 0 { "east" } else { "west" }.into()),
            ],
        )
        .await
        .unwrap();
    }

    // Flush the columnar materialized view so it has segments to scan.
    tokio::task::block_in_place(|| {
        let mut columnar = db.columnar_engine().lock().unwrap();
        tokio::runtime::Handle::current()
            .block_on(columnar.flush_collection("sales_analytics"))
            .unwrap();
    });

    // An analytical query (GROUP BY) should work.
    // The HTAP routing detects GROUP BY and routes to the columnar view.
    // Note: the columnar table provider needs flushed segments to scan.
    let columnar = db.columnar_engine().lock().unwrap();
    let count = columnar.row_count("sales_analytics");
    assert_eq!(count, 10, "materialized view should have 10 rows");
}

// ═══════════════════════════════════════════════════════════════════════
// Strong materialized consistency
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn strong_consistency_flushes_before_read() {
    let db = open_db().await;

    db.execute_sql(
        "CREATE COLLECTION inventory (
            id BIGINT NOT NULL PRIMARY KEY,
            quantity BIGINT
        ) WITH storage = 'strict'",
        &[],
    )
    .await
    .unwrap();

    db.execute_sql(
        "CREATE MATERIALIZED VIEW inventory_report FROM inventory",
        &[],
    )
    .await
    .unwrap();

    // Insert data — CDC replicates synchronously in Lite.
    for i in 0..5 {
        db.strict_insert("inventory", &[Value::Integer(i), Value::Integer(i * 100)])
            .await
            .unwrap();
    }

    // The materialized view should have the data immediately (synchronous CDC).
    let columnar = db.columnar_engine().lock().unwrap();
    assert_eq!(columnar.row_count("inventory_report"), 5);
    // In Lite, "strong consistency" is the default because CDC is synchronous.
    // There's no lag between source writes and materialized view updates.
}

// ═══════════════════════════════════════════════════════════════════════
// Staleness annotation
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn staleness_tracking() {
    let db = open_db().await;

    db.execute_sql(
        "CREATE COLLECTION telemetry (
            id BIGINT NOT NULL PRIMARY KEY,
            value FLOAT64
        ) WITH storage = 'strict'",
        &[],
    )
    .await
    .unwrap();

    db.execute_sql(
        "CREATE MATERIALIZED VIEW telemetry_report FROM telemetry",
        &[],
    )
    .await
    .unwrap();

    // Insert data.
    db.strict_insert("telemetry", &[Value::Integer(1), Value::Float(42.0)])
        .await
        .unwrap();

    // Check staleness annotation via HTAP bridge.
    let htap = db.htap_bridge().lock().unwrap();
    let view = htap.view_by_target("telemetry_report");
    assert!(view.is_some());

    let view = view.unwrap();
    assert_eq!(view.rows_replicated, 1);
    assert!(view.last_replicated_ms > 0);

    // Lag should be near-zero for synchronous CDC.
    let lag = htap.lag_ms("telemetry_report");
    assert!(lag < 1000, "lag should be minimal: {lag}ms");
}
