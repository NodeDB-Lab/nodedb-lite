// SPDX-License-Identifier: Apache-2.0

//! Gate test: spatial index insert/delete sync — Lite → Origin round-trip.
//!
//! Proves that geometries inserted via `spatial_insert` on Lite replicate to
//! Origin via the `SpatialInsert` (0xAA) wire frame and are returned by an
//! `st_dwithin` query on Origin.  Also proves that `SpatialDelete` (0xAC)
//! removes the geometry so it no longer appears in subsequent spatial queries.
//!
//! ## How to run
//!
//! Build the Origin binary first:
//! ```text
//! cd <project-root>/nodedb && cargo build -p nodedb
//! ```
//! Then run from the nodedb-lite workspace root:
//! ```text
//! cargo nextest run -p nodedb-lite --test sync_interop_spatial
//! ```
//!
//! The test is placed in the `heavy` nextest group (serialized) by the
//! `binary(/sync_interop/)` filter in `.config/nextest.toml`.

mod common;

use std::sync::Arc;
use std::time::Duration;

use nodedb_lite::sync::{SyncClient, SyncConfig, run_sync_loop};
use nodedb_lite::{NodeDbLite, PagedbStorageMem};
use nodedb_types::geometry::Geometry;

use common::origin::OriginServer;
use common::sql::OriginPgwire;

// ── Collection / index DDL ──────────────────────────────────────────────────

const COLLECTION: &str = "spatial_sync_test";

/// Schemaless collection — the spatial handler writes doc bytes directly to
/// the sparse store and the in-memory R-tree.  No explicit `CREATE SPATIAL
/// INDEX` is required; the spatial scan handler falls back to a full-scan
/// with predicate refinement when no persistent index exists, and the
/// in-memory R-tree populated by the sync path is used when it does.
const CREATE_ORIGIN: &str =
    "CREATE COLLECTION spatial_sync_test WITH (engine='document_schemaless')";

const FIELD: &str = "location";

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

/// Query Origin for documents within `distance_m` metres of `(lng, lat)`.
///
/// Returns the row count.
async fn query_dwithin(pg: &OriginPgwire, lng: f64, lat: f64, distance_m: f64) -> usize {
    let sql = format!(
        "SELECT id FROM {COLLECTION} WHERE st_dwithin(location, \
         '{{\"type\":\"Point\",\"coordinates\":[{lng},{lat}]}}', {distance_m}) LIMIT 100"
    );
    pg.query(&sql).await.len()
}

// ── Tests ─────────────────────────────────────────────────────────────────────

/// Inserts 3 points on Lite; waits for replication to Origin; asserts that
/// an `st_dwithin` query on Origin returns all 3 documents.
#[tokio::test]
async fn spatial_inserts_replicate_to_origin() {
    let _origin = OriginServer::spawn_with_pgwire();
    let pg = OriginPgwire::connect().await;

    pg.execute(CREATE_ORIGIN).await;

    let lite = open_lite().await;
    let _sync = start_sync(Arc::clone(&lite), 20).await;

    // Three points close to London (within a 50 km radius of 0,51.5).
    let points: &[(&str, f64, f64)] = &[
        ("london_a", -0.10, 51.50),
        ("london_b", -0.15, 51.52),
        ("london_c", -0.08, 51.48),
    ];

    for (id, lng, lat) in points {
        let geom = Geometry::point(*lng, *lat);
        lite.spatial_insert(COLLECTION, FIELD, id, &geom);
    }

    // Wait up to 5 s for all 3 points to appear on Origin via st_dwithin.
    // Query from the centroid with a 50 km radius — all 3 points are within range.
    let mut origin_count: usize = 0;
    let deadline = tokio::time::sleep(Duration::from_secs(5));
    tokio::pin!(deadline);
    loop {
        tokio::select! {
            _ = &mut deadline => break,
            _ = tokio::time::sleep(Duration::from_millis(200)) => {
                let count = query_dwithin(&pg, -0.11, 51.50, 50_000.0).await;
                if count >= 3 {
                    origin_count = count;
                    break;
                }
            }
        }
    }

    assert_eq!(
        origin_count, 3,
        "Origin st_dwithin must return 3 results after sync; got {origin_count}"
    );

    pg.execute(&format!("DROP COLLECTION {COLLECTION}")).await;
}

/// Inserts a point on Lite, waits for replication, deletes it on Lite,
/// waits for the delete to replicate, then asserts the point no longer
/// appears in spatial queries on Origin.
#[tokio::test]
async fn spatial_delete_replicates_to_origin() {
    let _origin = OriginServer::spawn_with_pgwire();
    let pg = OriginPgwire::connect().await;

    pg.execute(CREATE_ORIGIN).await;

    let lite = open_lite().await;
    let _sync = start_sync(Arc::clone(&lite), 21).await;

    // Insert 2 background points and 1 target point near London.
    let bg_points: &[(&str, f64, f64)] = &[("bg_a", -0.20, 51.55), ("bg_b", -0.05, 51.45)];
    for (id, lng, lat) in bg_points {
        let geom = Geometry::point(*lng, *lat);
        lite.spatial_insert(COLLECTION, FIELD, id, &geom);
    }
    let target_geom = Geometry::point(-0.13, 51.51);
    lite.spatial_insert(COLLECTION, FIELD, "target_pt", &target_geom);

    // Wait for all 3 points to appear on Origin.
    let deadline = tokio::time::sleep(Duration::from_secs(5));
    tokio::pin!(deadline);
    let mut all_appeared = false;
    loop {
        tokio::select! {
            _ = &mut deadline => break,
            _ = tokio::time::sleep(Duration::from_millis(200)) => {
                let count = query_dwithin(&pg, -0.12, 51.50, 50_000.0).await;
                if count >= 3 {
                    all_appeared = true;
                    break;
                }
            }
        }
    }
    assert!(
        all_appeared,
        "all 3 points must appear on Origin before testing delete"
    );

    // Delete the target point on Lite.
    lite.spatial_delete(COLLECTION, FIELD, "target_pt");

    // Query specifically for target_pt by using a very tight 100 m radius
    // centered exactly on its coordinates — only target_pt is within range.
    let deadline = tokio::time::sleep(Duration::from_secs(5));
    tokio::pin!(deadline);
    let mut target_gone = false;
    loop {
        tokio::select! {
            _ = &mut deadline => break,
            _ = tokio::time::sleep(Duration::from_millis(200)) => {
                let count = query_dwithin(&pg, -0.13, 51.51, 100.0).await;
                if count == 0 {
                    target_gone = true;
                    break;
                }
            }
        }
    }
    assert!(
        target_gone,
        "after SpatialDelete replicates, target_pt must return 0 results on Origin"
    );

    // Background points must still be present.
    let bg_count = query_dwithin(&pg, -0.12, 51.50, 50_000.0).await;
    assert_eq!(
        bg_count, 2,
        "background points must still be present after target delete; got {bg_count}"
    );

    pg.execute(&format!("DROP COLLECTION {COLLECTION}")).await;
}

/// Points inserted before sync connects are flushed once the connection
/// comes up — same guarantee as fts/vector/columnar.
#[tokio::test]
async fn spatial_pre_connection_inserts_sync_after_connect() {
    let _origin = OriginServer::spawn_with_pgwire();
    let pg = OriginPgwire::connect().await;

    pg.execute(CREATE_ORIGIN).await;

    let lite = open_lite().await;

    // Insert 2 points BEFORE starting sync.
    let pre_points: &[(&str, f64, f64)] = &[
        ("pre_a", 2.30, 48.85), // Paris area
        ("pre_b", 2.35, 48.87),
    ];
    for (id, lng, lat) in pre_points {
        let geom = Geometry::point(*lng, *lat);
        lite.spatial_insert(COLLECTION, FIELD, id, &geom);
    }

    // Now start sync transport.
    let _sync = start_sync(Arc::clone(&lite), 22).await;

    // Wait up to 8 s for both pre-connection points to appear on Origin.
    let mut origin_count: usize = 0;
    let deadline = tokio::time::sleep(Duration::from_secs(8));
    tokio::pin!(deadline);
    loop {
        tokio::select! {
            _ = &mut deadline => break,
            _ = tokio::time::sleep(Duration::from_millis(200)) => {
                let count = query_dwithin(&pg, 2.32, 48.86, 50_000.0).await;
                if count >= 2 {
                    origin_count = count;
                    break;
                }
            }
        }
    }

    assert_eq!(
        origin_count, 2,
        "pre-connection spatial inserts must sync after connect; got {origin_count}"
    );

    pg.execute(&format!("DROP COLLECTION {COLLECTION}")).await;
}
