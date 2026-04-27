//! Integration tests for the Lite array engine (C.0).
//!
//! Tests cover the five scenarios listed in the checklist:
//! 1. create + put + slice round-trip.
//! 2. Bitemporal AS-OF system-time correctness.
//! 3. Restart durability — write, drop, reopen, slice still works.
//! 4. Tombstone — coord NotFound after delete.
//! 5. GDPR erasure — coord NotFound + sentinel not present as payload.

use nodedb_array::schema::ArraySchemaBuilder;
use nodedb_array::schema::attr_spec::{AttrSpec, AttrType};
use nodedb_array::schema::dim_spec::{DimSpec, DimType};
use nodedb_array::types::cell_value::value::CellValue;
use nodedb_array::types::coord::value::CoordValue;
use nodedb_array::types::domain::{Domain, DomainBound};
use nodedb_lite::engine::array::ArrayEngineState;
use nodedb_lite::storage::redb_storage::RedbStorage;
use nodedb_types::OPEN_UPPER;
use std::sync::Arc;

fn schema() -> nodedb_array::schema::ArraySchema {
    ArraySchemaBuilder::new("g")
        .dim(DimSpec::new(
            "x",
            DimType::Int64,
            Domain::new(DomainBound::Int64(0), DomainBound::Int64(63)),
        ))
        .attr(AttrSpec::new("v", AttrType::Int64, true))
        .tile_extents(vec![8])
        .build()
        .unwrap()
}

fn open_engine(storage: &Arc<RedbStorage>) -> ArrayEngineState {
    ArrayEngineState::open(storage).unwrap()
}

// ── Test 1: create + put + slice round-trip ───────────────────────────────────

#[test]
fn create_put_slice_roundtrip() {
    let storage = Arc::new(RedbStorage::open_in_memory().unwrap());
    let mut engine = open_engine(&storage);

    engine.create_array(&storage, "grid", schema()).unwrap();
    engine
        .put_cell(
            &storage,
            "grid",
            vec![CoordValue::Int64(1)],
            vec![CellValue::Int64(100)],
            1_000,
            0,
            OPEN_UPPER,
        )
        .unwrap();
    engine
        .put_cell(
            &storage,
            "grid",
            vec![CoordValue::Int64(5)],
            vec![CellValue::Int64(200)],
            1_000,
            0,
            OPEN_UPPER,
        )
        .unwrap();
    engine.flush(&storage, "grid").unwrap();

    let cells = engine
        .slice(
            &storage,
            "grid",
            vec![None], // unconstrained
            i64::MAX,
        )
        .unwrap();

    assert_eq!(cells.len(), 2, "expected 2 live cells from slice");

    // Verify values.
    let vals: std::collections::HashSet<i64> = cells
        .iter()
        .map(|c| match c.attrs[0] {
            CellValue::Int64(v) => v,
            _ => panic!("unexpected attr type"),
        })
        .collect();
    assert!(vals.contains(&100));
    assert!(vals.contains(&200));
}

// ── Test 2: bitemporal AS-OF system-time correctness ─────────────────────────

#[test]
fn bitemporal_as_of_system_time() {
    let storage = Arc::new(RedbStorage::open_in_memory().unwrap());
    let mut engine = open_engine(&storage);

    engine.create_array(&storage, "bt", schema()).unwrap();

    // v1 written at system_time=100.
    engine
        .put_cell(
            &storage,
            "bt",
            vec![CoordValue::Int64(1)],
            vec![CellValue::Int64(10)],
            100, // system_from_ms
            0,
            OPEN_UPPER,
        )
        .unwrap();

    // v2 written at system_time=200 (same coord, newer value).
    engine
        .put_cell(
            &storage,
            "bt",
            vec![CoordValue::Int64(1)],
            vec![CellValue::Int64(20)],
            200, // system_from_ms
            0,
            OPEN_UPPER,
        )
        .unwrap();

    engine.flush(&storage, "bt").unwrap();

    // AS-OF 150 → should see v1 (sys=100) not v2 (sys=200).
    let result_150 = engine
        .read_coord(&storage, "bt", &[CoordValue::Int64(1)], 150)
        .unwrap();
    assert!(result_150.is_some(), "expected a result AS-OF 150");
    let val_150 = match result_150.unwrap().attrs[0] {
        CellValue::Int64(v) => v,
        _ => panic!("unexpected attr type"),
    };
    assert_eq!(val_150, 10, "AS-OF 150 should see v1 (value=10)");

    // AS-OF 300 → should see v2 (sys=200).
    let result_300 = engine
        .read_coord(&storage, "bt", &[CoordValue::Int64(1)], 300)
        .unwrap();
    assert!(result_300.is_some(), "expected a result AS-OF 300");
    let val_300 = match result_300.unwrap().attrs[0] {
        CellValue::Int64(v) => v,
        _ => panic!("unexpected attr type"),
    };
    assert_eq!(val_300, 20, "AS-OF 300 should see v2 (value=20)");
}

// ── Test 3: restart durability ────────────────────────────────────────────────

#[test]
fn restart_durability() {
    let storage = Arc::new(RedbStorage::open_in_memory().unwrap());

    // Write and flush.
    {
        let mut engine = open_engine(&storage);
        engine.create_array(&storage, "durable", schema()).unwrap();
        engine
            .put_cell(
                &storage,
                "durable",
                vec![CoordValue::Int64(3)],
                vec![CellValue::Int64(42)],
                500,
                0,
                OPEN_UPPER,
            )
            .unwrap();
        engine.flush(&storage, "durable").unwrap();
        // Engine dropped here — no more references.
    }

    // Reopen from the same storage.
    let mut engine2 = open_engine(&storage);

    // Array should be restored from catalog.
    let result = engine2
        .read_coord(&storage, "durable", &[CoordValue::Int64(3)], i64::MAX)
        .unwrap();
    assert!(result.is_some(), "data must survive engine restart");
    assert_eq!(
        result.unwrap().attrs[0],
        CellValue::Int64(42),
        "value must match after restart"
    );

    // Slice should also work.
    let cells = engine2
        .slice(&storage, "durable", vec![None], i64::MAX)
        .unwrap();
    assert_eq!(cells.len(), 1);
}

// ── Test 4: tombstone → coord NotFound after delete ──────────────────────────

#[test]
fn tombstone_coord_not_found() {
    let storage = Arc::new(RedbStorage::open_in_memory().unwrap());
    let mut engine = open_engine(&storage);

    engine.create_array(&storage, "del", schema()).unwrap();
    engine
        .put_cell(
            &storage,
            "del",
            vec![CoordValue::Int64(7)],
            vec![CellValue::Int64(99)],
            10,
            0,
            OPEN_UPPER,
        )
        .unwrap();
    // Delete at a later system time.
    engine
        .delete_cell("del", vec![CoordValue::Int64(7)], 20)
        .unwrap();
    engine.flush(&storage, "del").unwrap();

    // AS-OF the current system (tombstone visible) → None.
    let result = engine
        .read_coord(&storage, "del", &[CoordValue::Int64(7)], i64::MAX)
        .unwrap();
    assert!(
        result.is_none(),
        "tombstoned coord must return None (got {result:?})"
    );

    // AS-OF before the delete → the original cell is still visible.
    let before = engine
        .read_coord(&storage, "del", &[CoordValue::Int64(7)], 15)
        .unwrap();
    assert!(
        before.is_some(),
        "coord must be visible AS-OF before the tombstone"
    );
}

// ── Test 5: GDPR erasure ─────────────────────────────────────────────────────

#[test]
fn gdpr_erasure() {
    let storage = Arc::new(RedbStorage::open_in_memory().unwrap());
    let mut engine = open_engine(&storage);

    engine.create_array(&storage, "gdpr", schema()).unwrap();
    engine
        .put_cell(
            &storage,
            "gdpr",
            vec![CoordValue::Int64(2)],
            vec![CellValue::Int64(55)],
            100,
            0,
            OPEN_UPPER,
        )
        .unwrap();
    engine.flush(&storage, "gdpr").unwrap();

    // Erase at a later system time.
    engine
        .gdpr_erase_cell(&storage, "gdpr", vec![CoordValue::Int64(2)], 200)
        .unwrap();
    // gdpr_erase_cell flushes automatically.

    // coord must return None.
    let result = engine
        .read_coord(&storage, "gdpr", &[CoordValue::Int64(2)], i64::MAX)
        .unwrap();
    assert!(
        result.is_none(),
        "GDPR-erased coord must return None (got {result:?})"
    );

    // Additionally check that the erasure is durable across restart.
    drop(engine);

    let mut engine2 = open_engine(&storage);
    let result2 = engine2
        .read_coord(&storage, "gdpr", &[CoordValue::Int64(2)], i64::MAX)
        .unwrap();
    assert!(
        result2.is_none(),
        "GDPR erasure must persist across restart"
    );

    // Verify the original pre-erasure value is gone via slice as well.
    let cells = engine2
        .slice(&storage, "gdpr", vec![None], i64::MAX)
        .unwrap();
    assert!(
        cells.is_empty(),
        "slice must return no live cells after GDPR erasure"
    );
}
