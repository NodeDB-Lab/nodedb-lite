//! Gate tests for the spatial engine in NodeDB Lite.
//!
//! Covers:
//! - Insert geometry → bbox query returns expected subset.
//! - OGC predicates (point-in-polygon, intersects, contains, bbox) via the
//!   `nodedb_spatial::predicates` API applied to known geometries.
//! - Persistence round-trip: insert, flush, drop, reopen with the same
//!   on-disk path, query returns identical results (no rebuild from CRDT).

use nodedb_lite::storage::engine::StorageEngine;
use nodedb_lite::{NodeDbLite, PagedbStorageDefault, PagedbStorageMem};
use nodedb_spatial::predicates::{contains::st_contains, intersects::st_intersects};
use nodedb_types::BoundingBox;
use nodedb_types::geometry::Geometry;

// ── Helpers ───────────────────────────────────────────────────────────────────

const COLLECTION: &str = "places";
const FIELD: &str = "location";

/// Five points across the globe.
///
/// - "london"     (  -0.1,  51.5)  — Europe
/// - "new_york"   ( -74.0,  40.7)  — Americas
/// - "tokyo"      ( 139.7,  35.7)  — Asia
/// - "sydney"     ( 151.2, -33.9)  — Australia
/// - "nairobi"    (  36.8,  -1.3)  — Africa
fn sample_points() -> Vec<(&'static str, Geometry)> {
    vec![
        ("london", Geometry::point(-0.1278, 51.5074)),
        ("new_york", Geometry::point(-74.0060, 40.7128)),
        ("tokyo", Geometry::point(139.6917, 35.6895)),
        ("sydney", Geometry::point(151.2093, -33.8688)),
        ("nairobi", Geometry::point(36.8219, -1.2921)),
    ]
}

/// Bounding box covering only the European region (roughly).
fn europe_bbox() -> BoundingBox {
    BoundingBox::new(-30.0, 30.0, 40.0, 72.0)
}

/// Bounding box covering Europe + Asia (northern hemisphere wide).
fn eurasia_bbox() -> BoundingBox {
    BoundingBox::new(-30.0, 0.0, 180.0, 72.0)
}

/// A polygon enclosing Western Europe (approximate).
fn western_europe_polygon() -> Geometry {
    Geometry::polygon(vec![vec![
        [-25.0, 35.0],
        [30.0, 35.0],
        [30.0, 65.0],
        [-25.0, 65.0],
        [-25.0, 35.0], // closed
    ]])
}

async fn open_in_memory() -> NodeDbLite<PagedbStorageMem> {
    let storage = PagedbStorageMem::open_in_memory()
        .await
        .expect("open in-memory storage");
    NodeDbLite::open(storage, 1).await.expect("open NodeDbLite")
}

// ── Test 1: Insert 5 points — bbox query returns expected subset ──────────────

#[tokio::test]
async fn bbox_query_returns_expected_subset() {
    let db = open_in_memory().await;

    for (id, geom) in sample_points() {
        db.spatial_insert(COLLECTION, FIELD, id, &geom);
    }

    // Europe bbox should include only "london".
    let europe_results = db.spatial_search_bbox(COLLECTION, FIELD, &europe_bbox());
    assert_eq!(
        europe_results.len(),
        1,
        "expected exactly 1 hit in Europe bbox, got {}: {:?}",
        europe_results.len(),
        europe_results,
    );

    // Eurasia bbox should include "london" and "tokyo".
    let eurasia_results = db.spatial_search_bbox(COLLECTION, FIELD, &eurasia_bbox());
    assert_eq!(
        eurasia_results.len(),
        2,
        "expected 2 hits in Eurasia bbox, got {}: {:?}",
        eurasia_results.len(),
        eurasia_results,
    );

    // Global bbox should return all 5.
    let global_bbox = BoundingBox::new(-180.0, -90.0, 180.0, 90.0);
    let global_results = db.spatial_search_bbox(COLLECTION, FIELD, &global_bbox);
    assert_eq!(
        global_results.len(),
        5,
        "expected all 5 points in global bbox, got {}",
        global_results.len(),
    );
}

// ── Test 2: OGC predicates (point-in-polygon, intersects, contains) ───────────

#[tokio::test]
async fn ogc_predicates_point_in_polygon() {
    let _db = open_in_memory().await;
    let poly = western_europe_polygon();

    // london is inside western Europe polygon.
    let london = Geometry::point(-0.1278, 51.5074);
    assert!(
        st_contains(&poly, &london),
        "expected London to be inside western_europe_polygon"
    );

    // tokyo is outside.
    let tokyo = Geometry::point(139.6917, 35.6895);
    assert!(
        !st_contains(&poly, &tokyo),
        "expected Tokyo to be outside western_europe_polygon"
    );
}

#[tokio::test]
async fn ogc_predicates_intersects() {
    let _db = open_in_memory().await;

    let poly_a = western_europe_polygon();
    // A polygon covering eastern Europe — overlaps with poly_a.
    let poly_b = Geometry::polygon(vec![vec![
        [10.0, 35.0],
        [50.0, 35.0],
        [50.0, 65.0],
        [10.0, 65.0],
        [10.0, 35.0],
    ]]);

    assert!(
        st_intersects(&poly_a, &poly_b),
        "overlapping polygons should intersect"
    );

    // A polygon in the southern hemisphere should not intersect western Europe.
    let southern = Geometry::polygon(vec![vec![
        [-20.0, -60.0],
        [20.0, -60.0],
        [20.0, -20.0],
        [-20.0, -20.0],
        [-20.0, -60.0],
    ]]);

    assert!(
        !st_intersects(&poly_a, &southern),
        "non-overlapping polygons should not intersect"
    );
}

#[tokio::test]
async fn ogc_predicates_contains() {
    let _db = open_in_memory().await;

    let outer = western_europe_polygon();

    // A smaller polygon fully inside outer.
    let inner = Geometry::polygon(vec![vec![
        [-5.0, 40.0],
        [10.0, 40.0],
        [10.0, 55.0],
        [-5.0, 55.0],
        [-5.0, 40.0],
    ]]);

    assert!(
        st_contains(&outer, &inner),
        "outer polygon should contain inner polygon"
    );

    // A polygon that straddles the outer boundary should not be contained.
    let straddling = Geometry::polygon(vec![vec![
        [25.0, 35.0],
        [45.0, 35.0],
        [45.0, 55.0],
        [25.0, 55.0],
        [25.0, 35.0],
    ]]);

    assert!(
        !st_contains(&outer, &straddling),
        "straddling polygon should not be fully contained"
    );
}

// ── Test 3: Persistence round-trip ───────────────────────────────────────────

#[tokio::test]
async fn spatial_index_persists_across_restart() {
    let dir = tempfile::tempdir().expect("create tempdir");
    let path = dir.path().join("spatial_test.db");

    let pre_restart_ids: Vec<u64>;

    // ── First open: insert points, query, flush, drop ────────────────────────
    {
        let storage = PagedbStorageDefault::open(&path)
            .await
            .expect("open storage");
        let db = NodeDbLite::open(storage, 42)
            .await
            .expect("open NodeDbLite");

        for (id, geom) in sample_points() {
            db.spatial_insert(COLLECTION, FIELD, id, &geom);
        }

        // Confirm query works before flush.
        let results_before = db.spatial_search_bbox(
            COLLECTION,
            FIELD,
            &BoundingBox::new(-180.0, -90.0, 180.0, 90.0),
        );
        assert_eq!(results_before.len(), 5, "expected 5 results before flush");

        pre_restart_ids = results_before.iter().map(|e| e.id).collect();

        db.flush().await.expect("flush");
        // db dropped here, releasing file lock.
    }

    // Sanity: Namespace::Spatial must have entries after flush.
    {
        use nodedb_types::Namespace;
        let storage = PagedbStorageDefault::open(&path)
            .await
            .expect("storage for count check");
        let spatial_count = storage
            .count(Namespace::Spatial)
            .await
            .expect("spatial count");
        assert!(
            spatial_count > 0,
            "Namespace::Spatial should have entries after flush, got 0"
        );
    }

    // ── Second open: reopen, query, assert identical results ─────────────────
    {
        let storage = PagedbStorageDefault::open(&path)
            .await
            .expect("reopen storage");
        let db = NodeDbLite::open(storage, 42)
            .await
            .expect("reopen NodeDbLite");

        let results_after = db.spatial_search_bbox(
            COLLECTION,
            FIELD,
            &BoundingBox::new(-180.0, -90.0, 180.0, 90.0),
        );

        assert_eq!(
            results_after.len(),
            5,
            "expected 5 results after restart, got {}",
            results_after.len()
        );

        let mut post_ids: Vec<u64> = results_after.iter().map(|e| e.id).collect();
        let mut pre_sorted = pre_restart_ids.clone();
        post_ids.sort_unstable();
        pre_sorted.sort_unstable();

        assert_eq!(
            pre_sorted, post_ids,
            "entry IDs must be identical after restart"
        );

        // Bounding-box subset query must still work after restart.
        let europe_after = db.spatial_search_bbox(COLLECTION, FIELD, &europe_bbox());
        assert_eq!(
            europe_after.len(),
            1,
            "Europe bbox should return 1 result after restart"
        );
    }
}

// ── Test 4: Upsert semantics survive a round-trip ────────────────────────────

#[tokio::test]
async fn upsert_and_delete_after_restart() {
    let dir = tempfile::tempdir().expect("create tempdir");
    let path = dir.path().join("spatial_upsert.db");

    // Insert "london", flush, reopen, upsert "london" to a new position,
    // verify old position no longer returns and new position does.
    {
        let storage = PagedbStorageDefault::open(&path)
            .await
            .expect("open storage");
        let db = NodeDbLite::open(storage, 1).await.expect("open db");
        db.spatial_insert(
            COLLECTION,
            FIELD,
            "london",
            &Geometry::point(-0.1278, 51.5074),
        );
        db.flush().await.expect("flush");
    }

    {
        let storage = PagedbStorageDefault::open(&path)
            .await
            .expect("reopen storage");
        let db = NodeDbLite::open(storage, 1).await.expect("reopen db");

        // Upsert london to a totally different location (south Atlantic).
        db.spatial_insert(COLLECTION, FIELD, "london", &Geometry::point(-25.0, -40.0));

        // Old European position should no longer be found.
        let europe = db.spatial_search_bbox(COLLECTION, FIELD, &europe_bbox());
        assert!(
            europe.is_empty(),
            "after upsert, old European position should be gone"
        );

        // New southern-Atlantic position should be found.
        let south_atlantic = BoundingBox::new(-30.0, -50.0, -20.0, -30.0);
        let south = db.spatial_search_bbox(COLLECTION, FIELD, &south_atlantic);
        assert_eq!(
            south.len(),
            1,
            "upserted position should appear in south-atlantic bbox"
        );
    }
}
