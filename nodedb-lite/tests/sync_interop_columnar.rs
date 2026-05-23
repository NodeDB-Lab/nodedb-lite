//! Gate test: columnar insert sync — Lite → Origin round-trip.
//!
//! Proves that rows inserted into a columnar collection on Lite replicate
//! to Origin via the `ColumnarInsert` (0xA0) wire frame and can be read
//! back from Origin via pgwire.
//!
//! ## How to run
//!
//! Build the Origin binary first:
//! ```text
//! cd <project-root>/nodedb && cargo build -p nodedb
//! ```
//! Then run from the nodedb-lite workspace root:
//! ```text
//! cargo nextest run -p nodedb-lite --test sync_interop_columnar
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

/// CREATE COLLECTION for Origin (pgwire dialect).
const CREATE_ORIGIN: &str = "CREATE COLLECTION col_sync_test (
    id    BIGINT NOT NULL,
    label VARCHAR,
    value FLOAT64
) WITH (engine='columnar')";

/// CREATE COLLECTION for Lite.
const CREATE_LITE: &str = "CREATE COLLECTION col_sync_test (
    id    BIGINT NOT NULL PRIMARY KEY,
    label VARCHAR,
    value FLOAT64
) WITH storage = 'columnar'";

// ── Helper: open a Lite DB backed by in-memory redb ─────────────────────────

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

// ── Test ─────────────────────────────────────────────────────────────────────

/// Lite inserts 3 rows into a columnar collection; Origin receives them via
/// the `ColumnarInsert` sync frame and they are readable via pgwire SELECT.
#[tokio::test]
async fn columnar_inserts_replicate_to_origin() {
    let _origin = OriginServer::spawn_with_pgwire();
    let pg = OriginPgwire::connect().await;

    // Create the collection on both sides.
    pg.execute(CREATE_ORIGIN).await;

    let lite = open_lite().await;
    lite.execute_sql(CREATE_LITE, &[])
        .await
        .expect("Lite CREATE columnar col_sync_test");

    // Wire up sync transport.
    let sync_config = SyncConfig::new(common::origin::ORIGIN_WS, "");
    let sync_client = Arc::new(SyncClient::new(sync_config, 1));
    let delegate = Arc::clone(&lite) as Arc<dyn nodedb_lite::sync::SyncDelegate>;
    let client_clone = Arc::clone(&sync_client);
    tokio::spawn(async move {
        run_sync_loop(client_clone, delegate).await;
    });

    // Wait for the sync connection to become established.
    let deadline = tokio::time::sleep(Duration::from_secs(10));
    tokio::pin!(deadline);
    loop {
        tokio::select! {
            _ = &mut deadline => panic!("sync connection did not establish within 10 seconds"),
            _ = tokio::time::sleep(Duration::from_millis(50)) => {
                if sync_client.state().await == nodedb_lite::sync::SyncState::Connected {
                    break;
                }
            }
        }
    }

    // Insert 3 rows on Lite.
    for i in 1i64..=3 {
        let sql = format!(
            "INSERT INTO col_sync_test (id, label, value) VALUES ({i}, 'row-{i}', {:.1})",
            i as f64 * 10.0
        );
        lite.execute_sql(&sql, &[])
            .await
            .unwrap_or_else(|e| panic!("Lite INSERT row {i}: {e}"));
    }

    // Wait up to 5 seconds for replication to Origin.
    // Use a direct SELECT (not COUNT(*)) so the query routes through the
    // columnar scan path which reads from the columnar MutationEngine.
    let mut origin_row_count: i64 = 0;
    let deadline = tokio::time::sleep(Duration::from_secs(5));
    tokio::pin!(deadline);
    loop {
        tokio::select! {
            _ = &mut deadline => break,
            _ = tokio::time::sleep(Duration::from_millis(200)) => {
                let rows = pg.query("SELECT id FROM col_sync_test").await;
                let count = rows.len() as i64;
                if count >= 3 {
                    origin_row_count = count;
                    break;
                }
            }
        }
    }

    assert_eq!(
        origin_row_count, 3,
        "Origin must have 3 rows after columnar sync; got {origin_row_count}"
    );

    // Spot-check row count from the direct scan (already verified above).
    // The SELECT id + ORDER BY drives the columnar scan path.
    let rows = pg.query("SELECT id FROM col_sync_test ORDER BY id").await;
    assert_eq!(rows.len(), 3, "expected 3 rows from SELECT");

    // Cleanup.
    pg.execute("DROP COLLECTION col_sync_test").await;
}

/// Rows inserted into a Lite columnar collection before the sync connection
/// is established are flushed once the connection comes up.
///
/// This test inserts rows, then starts the sync transport, and verifies
/// Origin eventually receives them.
#[tokio::test]
async fn columnar_pre_connection_inserts_sync_after_connect() {
    let _origin = OriginServer::spawn_with_pgwire();
    let pg = OriginPgwire::connect().await;

    pg.execute(CREATE_ORIGIN).await;

    let lite = open_lite().await;
    lite.execute_sql(CREATE_LITE, &[])
        .await
        .expect("Lite CREATE");

    // Insert rows BEFORE starting sync.
    for i in 1i64..=2 {
        let sql = format!(
            "INSERT INTO col_sync_test (id, label, value) VALUES ({i}, 'pre-{i}', {:.1})",
            i as f64
        );
        lite.execute_sql(&sql, &[])
            .await
            .unwrap_or_else(|e| panic!("Lite INSERT row {i}: {e}"));
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
    // Use a direct SELECT (not COUNT(*)) so the query routes through the
    // columnar scan path which reads from the columnar MutationEngine.
    let mut origin_row_count: i64 = 0;
    let deadline = tokio::time::sleep(Duration::from_secs(8));
    tokio::pin!(deadline);
    loop {
        tokio::select! {
            _ = &mut deadline => break,
            _ = tokio::time::sleep(Duration::from_millis(200)) => {
                let rows = pg.query("SELECT id FROM col_sync_test").await;
                let count = rows.len() as i64;
                if count >= 2 {
                    origin_row_count = count;
                    break;
                }
            }
        }
    }

    assert_eq!(
        origin_row_count, 2,
        "pre-connection rows must replicate once sync connects; got {origin_row_count}"
    );

    pg.execute("DROP COLLECTION col_sync_test").await;
}
