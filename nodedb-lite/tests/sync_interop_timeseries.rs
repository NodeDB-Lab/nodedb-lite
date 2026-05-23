//! Gate test: timeseries insert sync — Lite → Origin round-trip.
//!
//! Proves that rows inserted into a timeseries collection on Lite replicate
//! to Origin via the `ColumnarInsert` (0xA0) wire frame and can be read
//! back from Origin via pgwire.
//!
//! Timeseries collections in Lite are backed by `ColumnarEngine` with
//! `ColumnarProfile::Timeseries`.  Inserts therefore flow through the
//! existing `ColumnarOutbound` queue — no dedicated timeseries wire frame
//! is needed (Path A).
//!
//! ## How to run
//!
//! Build the Origin binary first:
//! ```text
//! cd <project-root>/nodedb && cargo build -p nodedb
//! ```
//! Then run from the nodedb-lite workspace root:
//! ```text
//! cargo nextest run -p nodedb-lite --test sync_interop_timeseries
//! ```
//!
//! The test is placed in the `heavy` nextest group (serialized) by the
//! `binary(/sync_interop/)` filter in `.config/nextest.toml`.

mod common;

use std::sync::Arc;
use std::time::Duration;

use nodedb_client::NodeDb;
use nodedb_lite::sync::{SyncClient, SyncConfig, run_sync_loop};
use nodedb_lite::{NodeDbLite, PagedbStorageMem};

use common::origin::OriginServer;
use common::sql::OriginPgwire;

// ── Collection DDL ──────────────────────────────────────────────────────────

/// CREATE COLLECTION for Origin (pgwire dialect, timeseries engine).
const CREATE_ORIGIN: &str = "CREATE COLLECTION ts_sync_test (
    time  TIMESTAMP NOT NULL,
    host  TEXT,
    cpu   FLOAT64
) WITH (engine='timeseries')";

/// CREATE TIMESERIES COLLECTION for Lite.
///
/// Lite parses this sugar and creates a columnar collection with
/// `ColumnarProfile::Timeseries`.  The column schema must match Origin's.
const CREATE_LITE: &str = "CREATE TIMESERIES COLLECTION ts_sync_test (
    time  TIMESTAMP NOT NULL,
    host  TEXT,
    cpu   FLOAT64
) PARTITION BY TIME(1h)";

// ── Helper: open a Lite DB backed by in-memory storage ─────────────────────────

async fn open_lite() -> Arc<NodeDbLite<PagedbStorageMem>> {
    let storage = PagedbStorageMem::open_in_memory()
        .await
        .expect("open_in_memory");
    Arc::new(
        NodeDbLite::open(storage, 1)
            .await
            .expect("NodeDbLite::open"),
    )
}

// ── Helper: wait for sync connection ────────────────────────────────────────

async fn wait_for_connected(client: &Arc<SyncClient>) {
    let deadline = tokio::time::sleep(Duration::from_secs(10));
    tokio::pin!(deadline);
    loop {
        tokio::select! {
            _ = &mut deadline => panic!("sync connection did not establish within 10 seconds"),
            _ = tokio::time::sleep(Duration::from_millis(50)) => {
                if client.state().await == nodedb_lite::sync::SyncState::Connected {
                    break;
                }
            }
        }
    }
}

// ── Test ─────────────────────────────────────────────────────────────────────

/// Lite inserts 3 rows into a timeseries collection; Origin receives them via
/// the `ColumnarInsert` sync frame and they are readable via pgwire SELECT.
///
/// This test validates Path A: timeseries collections on Lite are backed by
/// `ColumnarEngine` with `ColumnarProfile::Timeseries`, so inserts flow
/// through `ColumnarOutbound` without any additional timeseries-specific
/// wire plumbing.
#[tokio::test]
async fn timeseries_inserts_replicate_to_origin() {
    let _origin = OriginServer::spawn_with_pgwire();
    let pg = OriginPgwire::connect().await;

    // Create the collection on both sides.
    pg.execute(CREATE_ORIGIN).await;

    let lite = open_lite().await;
    lite.execute_sql(CREATE_LITE, &[])
        .await
        .expect("Lite CREATE TIMESERIES ts_sync_test");

    // Wire up sync transport.
    let sync_config = SyncConfig::new(common::origin::ORIGIN_WS, "");
    let sync_client = Arc::new(SyncClient::new(sync_config, 1));
    let delegate = Arc::clone(&lite) as Arc<dyn nodedb_lite::sync::SyncDelegate>;
    let client_clone = Arc::clone(&sync_client);
    tokio::spawn(async move {
        run_sync_loop(client_clone, delegate).await;
    });

    wait_for_connected(&sync_client).await;

    // Insert 3 rows on Lite using SQL INSERT.
    let rows = [
        ("2024-06-01 12:00:00", "web01", 0.45_f64),
        ("2024-06-01 12:01:00", "web02", 0.60_f64),
        ("2024-06-01 12:02:00", "web03", 0.72_f64),
    ];
    for (ts, host, cpu) in &rows {
        let sql =
            format!("INSERT INTO ts_sync_test (time, host, cpu) VALUES ('{ts}', '{host}', {cpu})");
        lite.execute_sql(&sql, &[])
            .await
            .unwrap_or_else(|e| panic!("Lite INSERT ts_sync_test ({host}): {e}"));
    }

    // Wait up to 5 seconds for replication to Origin.
    let mut origin_row_count: i64 = 0;
    let deadline = tokio::time::sleep(Duration::from_secs(5));
    tokio::pin!(deadline);
    loop {
        tokio::select! {
            _ = &mut deadline => break,
            _ = tokio::time::sleep(Duration::from_millis(200)) => {
                let result = pg.query("SELECT time FROM ts_sync_test").await;
                let count = result.len() as i64;
                if count >= 3 {
                    origin_row_count = count;
                    break;
                }
            }
        }
    }

    assert_eq!(
        origin_row_count, 3,
        "Origin must have 3 rows after timeseries sync via ColumnarInsert; got {origin_row_count}"
    );

    // Cleanup.
    pg.execute("DROP COLLECTION ts_sync_test").await;
}

/// Rows inserted into a Lite timeseries collection before the sync connection
/// is established are flushed once the connection comes up.
#[tokio::test]
async fn timeseries_pre_connection_inserts_sync_after_connect() {
    let _origin = OriginServer::spawn_with_pgwire();
    let pg = OriginPgwire::connect().await;

    pg.execute(CREATE_ORIGIN).await;

    let lite = open_lite().await;
    lite.execute_sql(CREATE_LITE, &[])
        .await
        .expect("Lite CREATE TIMESERIES ts_sync_test");

    // Insert rows BEFORE starting sync.
    let rows = [
        ("2024-07-01 08:00:00", "db01", 0.30_f64),
        ("2024-07-01 08:01:00", "db02", 0.55_f64),
    ];
    for (ts, host, cpu) in &rows {
        let sql =
            format!("INSERT INTO ts_sync_test (time, host, cpu) VALUES ('{ts}', '{host}', {cpu})");
        lite.execute_sql(&sql, &[])
            .await
            .unwrap_or_else(|e| panic!("Lite pre-connect INSERT ({host}): {e}"));
    }

    // Now start sync transport.
    let sync_config = SyncConfig::new(common::origin::ORIGIN_WS, "");
    let sync_client = Arc::new(SyncClient::new(sync_config, 2));
    let delegate = Arc::clone(&lite) as Arc<dyn nodedb_lite::sync::SyncDelegate>;
    let client_clone = Arc::clone(&sync_client);
    tokio::spawn(async move {
        run_sync_loop(client_clone, delegate).await;
    });

    // Wait up to 8 seconds for replication.
    let mut origin_row_count: i64 = 0;
    let deadline = tokio::time::sleep(Duration::from_secs(8));
    tokio::pin!(deadline);
    loop {
        tokio::select! {
            _ = &mut deadline => break,
            _ = tokio::time::sleep(Duration::from_millis(200)) => {
                let result = pg.query("SELECT time FROM ts_sync_test").await;
                let count = result.len() as i64;
                if count >= 2 {
                    origin_row_count = count;
                    break;
                }
            }
        }
    }

    assert_eq!(
        origin_row_count, 2,
        "pre-connection timeseries rows must replicate once sync connects; got {origin_row_count}"
    );

    pg.execute("DROP COLLECTION ts_sync_test").await;
}
