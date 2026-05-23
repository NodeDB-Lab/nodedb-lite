// SPDX-License-Identifier: Apache-2.0

//! Gate test: vector insert/delete sync — Lite → Origin round-trip.
//!
//! Proves that vectors inserted into a vector collection on Lite replicate
//! to Origin via the `VectorInsert` (0xA2) wire frame and are returned by a
//! nearest-neighbour query on Origin.  Also proves that `VectorDelete` (0xA4)
//! removes the vector so it no longer appears in subsequent searches.
//!
//! ## Important caveat
//!
//! The sync path writes vectors to Origin's HNSW index via `VectorOp::Insert`.
//! The vector search response uses surrogate identifiers (u32 hex), not the
//! original string ids from Lite.  Presence is therefore verified by result
//! count rather than id string matching.
//!
//! ## How to run
//!
//! Build the Origin binary first:
//! ```text
//! cd <project-root>/nodedb && cargo build -p nodedb
//! ```
//! Then run from the nodedb-lite workspace root:
//! ```text
//! cargo nextest run -p nodedb-lite --test sync_interop_vector
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

/// Dim-3 FP32 vector collection on Origin (pgwire dialect).
///
/// The collection must be pre-created on Origin because the sync path writes
/// directly to the HNSW index via `VectorOp::Insert`, which calls
/// `get_or_create_vector_index`. The DDL establishes the schema so that
/// vector_distance queries work.
const CREATE_ORIGIN: &str = "CREATE COLLECTION vec_sync_test \
    FIELDS (id TEXT, embedding VECTOR(3)) \
    WITH (engine='vector', m=8, ef_construction=50)";

// NOTE: The Lite vector engine auto-creates collections on first `vector_insert`.
// No explicit DDL is required on the Lite side.

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

/// Wire up the sync transport and wait until the connection is established.
async fn start_sync(lite: Arc<NodeDbLite<PagedbStorageMem>>, peer_id: u64) -> Arc<SyncClient> {
    let sync_config = SyncConfig::new(common::origin::ORIGIN_WS, "");
    let sync_client = Arc::new(SyncClient::new(sync_config, peer_id));
    let delegate = Arc::clone(&lite) as Arc<dyn nodedb_lite::sync::SyncDelegate>;
    let client_clone = Arc::clone(&sync_client);
    tokio::spawn(async move {
        run_sync_loop(client_clone, delegate).await;
    });

    // Wait up to 10 s for the connection to become established.
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

    sync_client
}

// ── Tests ─────────────────────────────────────────────────────────────────────

/// Inserts 5 vectors on Lite; waits for replication to Origin; asserts that
/// a nearest-neighbour query on Origin returns 5 results.
///
/// The search result "id" column contains the surrogate hex (not the original
/// string id from Lite), so presence is verified by result count.
#[tokio::test]
async fn vector_inserts_replicate_to_origin() {
    let _origin = OriginServer::spawn_with_pgwire();
    let pg = OriginPgwire::connect().await;

    // Create the collection on Origin.
    pg.execute(CREATE_ORIGIN).await;

    let lite = open_lite().await;
    let _sync = start_sync(Arc::clone(&lite), 1).await;

    // Insert 5 well-separated vectors.
    for i in 0u32..5 {
        let embedding: Vec<f32> = vec![i as f32, 0.0, 0.0];
        lite.vector_insert("vec_sync_test", &format!("v{i}"), &embedding, None)
            .await
            .unwrap_or_else(|e| panic!("Lite vector_insert v{i}: {e}"));
    }

    // Wait up to 5 s for Origin's HNSW index to return 5 results for top-5 search.
    let mut origin_count: usize = 0;
    let deadline = tokio::time::sleep(Duration::from_secs(5));
    tokio::pin!(deadline);
    loop {
        tokio::select! {
            _ = &mut deadline => break,
            _ = tokio::time::sleep(Duration::from_millis(200)) => {
                let rows = pg
                    .query(
                        "SELECT id FROM vec_sync_test \
                         ORDER BY vector_distance(embedding, ARRAY[0.0, 0.0, 0.0]) \
                         LIMIT 5",
                    )
                    .await;
                if rows.len() >= 5 {
                    origin_count = rows.len();
                    break;
                }
            }
        }
    }

    assert_eq!(
        origin_count, 5,
        "Origin HNSW must return 5 results after sync; got {origin_count}"
    );

    // Cleanup.
    pg.execute("DROP COLLECTION vec_sync_test").await;
}

/// Inserts 6 vectors on Lite (5 background + 1 target), waits for replication,
/// deletes the target on Lite, waits for delete to replicate, then asserts the
/// nearest-neighbour result count drops from 6 to 5.
#[tokio::test]
async fn vector_delete_replicates_to_origin() {
    let _origin = OriginServer::spawn_with_pgwire();
    let pg = OriginPgwire::connect().await;

    pg.execute(CREATE_ORIGIN).await;

    let lite = open_lite().await;
    let _sync = start_sync(Arc::clone(&lite), 2).await;

    // Insert 5 background vectors and 1 target (6 total).
    for i in 0u32..5 {
        let embedding: Vec<f32> = vec![i as f32 * 10.0, 0.0, 0.0];
        lite.vector_insert("vec_sync_test", &format!("bg{i}"), &embedding, None)
            .await
            .unwrap_or_else(|e| panic!("Lite vector_insert bg{i}: {e}"));
    }
    let target: Vec<f32> = vec![1.0, 0.0, 0.0];
    lite.vector_insert("vec_sync_test", "target", &target, None)
        .await
        .expect("Lite vector_insert target");

    // Wait for Origin to have all 6 vectors.
    let deadline = tokio::time::sleep(Duration::from_secs(5));
    tokio::pin!(deadline);
    let mut all_appeared = false;
    loop {
        tokio::select! {
            _ = &mut deadline => break,
            _ = tokio::time::sleep(Duration::from_millis(200)) => {
                let rows = pg
                    .query(
                        "SELECT id FROM vec_sync_test \
                         ORDER BY vector_distance(embedding, ARRAY[0.0, 0.0, 0.0]) \
                         LIMIT 6",
                    )
                    .await;
                if rows.len() >= 6 {
                    all_appeared = true;
                    break;
                }
            }
        }
    }
    assert!(
        all_appeared,
        "all 6 vectors must appear on Origin before testing delete"
    );

    // Delete the target on Lite.
    lite.vector_delete("vec_sync_test", "target")
        .await
        .expect("Lite vector_delete target");

    // Wait for Origin's count to drop to 5.
    let deadline = tokio::time::sleep(Duration::from_secs(5));
    tokio::pin!(deadline);
    let mut count_after: usize = 6;
    loop {
        tokio::select! {
            _ = &mut deadline => break,
            _ = tokio::time::sleep(Duration::from_millis(200)) => {
                let rows = pg
                    .query(
                        "SELECT id FROM vec_sync_test \
                         ORDER BY vector_distance(embedding, ARRAY[0.0, 0.0, 0.0]) \
                         LIMIT 6",
                    )
                    .await;
                if rows.len() <= 5 {
                    count_after = rows.len();
                    break;
                }
            }
        }
    }
    assert_eq!(
        count_after, 5,
        "after delete replicates, Origin must return 5 results; got {count_after}"
    );

    // Cleanup.
    pg.execute("DROP COLLECTION vec_sync_test").await;
}

/// Vectors inserted before the sync connection is established are flushed
/// once the connection comes up (same guarantee as columnar).
#[tokio::test]
async fn vector_pre_connection_inserts_sync_after_connect() {
    let _origin = OriginServer::spawn_with_pgwire();
    let pg = OriginPgwire::connect().await;

    pg.execute(CREATE_ORIGIN).await;

    let lite = open_lite().await;

    // Insert 3 vectors BEFORE starting sync.
    for i in 0u32..3 {
        let embedding: Vec<f32> = vec![i as f32, 0.0, 0.0];
        lite.vector_insert("vec_sync_test", &format!("pre{i}"), &embedding, None)
            .await
            .unwrap_or_else(|e| panic!("Lite pre-sync vector_insert pre{i}: {e}"));
    }

    // Now start sync transport.
    let _sync = start_sync(Arc::clone(&lite), 3).await;

    // Wait up to 8 s for Origin to have 3 vectors.
    let mut origin_count: usize = 0;
    let deadline = tokio::time::sleep(Duration::from_secs(8));
    tokio::pin!(deadline);
    loop {
        tokio::select! {
            _ = &mut deadline => break,
            _ = tokio::time::sleep(Duration::from_millis(200)) => {
                let rows = pg
                    .query(
                        "SELECT id FROM vec_sync_test \
                         ORDER BY vector_distance(embedding, ARRAY[0.0, 0.0, 0.0]) \
                         LIMIT 3",
                    )
                    .await;
                if rows.len() >= 3 {
                    origin_count = rows.len();
                    break;
                }
            }
        }
    }

    assert_eq!(
        origin_count, 3,
        "pre-connection vectors must replicate once sync connects; got {origin_count}"
    );

    pg.execute("DROP COLLECTION vec_sync_test").await;
}
