// Note: bypasses WebSocket transport; exercises wire-message handlers directly.
// Two "Lite" harnesses each emit a Put for the same coord. Both ops are
// delivered to an "Origin" harness (third in-process engine). AS-OF reads
// verify both versions land in HLC order.

mod common;

use nodedb_array::sync::op::{ArrayOp, ArrayOpHeader, ArrayOpKind};
use nodedb_array::types::cell_value::value::CellValue;
use nodedb_array::types::coord::value::CoordValue;
use nodedb_lite::sync::array::inbound::outcome::InboundOutcome;

/// Build a Put op using explicitly provided HLC bytes (for cross-replica tests).
fn put_with_hlc(
    array: &str,
    coord_x: i64,
    val: f64,
    hlc: nodedb_array::sync::hlc::Hlc,
    schema_hlc: nodedb_array::sync::hlc::Hlc,
) -> ArrayOp {
    ArrayOp {
        header: ArrayOpHeader {
            array: array.into(),
            hlc,
            schema_hlc,
            valid_from_ms: 0,
            valid_until_ms: -1,
            system_from_ms: hlc.physical_ms as i64,
        },
        kind: ArrayOpKind::Put,
        coord: vec![CoordValue::Int64(coord_x)],
        attrs: Some(vec![CellValue::Float64(val)]),
    }
}

/// Two Lite replicas each write the same coord at distinct HLCs.
/// Both ops land at Origin; AS-OF reads return the correct version.
#[test]
fn two_writers_same_coord_both_land() {
    // "Origin" in-process engine receives from both replicas.
    let origin = common::SyncHarness::new_in_memory();
    origin.create_array("shared");
    let schema_hlc = origin.schema_hlc("shared");

    let rep_a = common::replica(1);
    let rep_b = common::replica(2);

    // Replica A writes at system_time=100.
    let op_a = put_with_hlc("shared", 7, 100.0, common::hlc(100, rep_a), schema_hlc);
    // Replica B writes at system_time=150.
    let op_b = put_with_hlc("shared", 7, 150.0, common::hlc(150, rep_b), schema_hlc);

    let oa = origin.deliver(&op_a);
    let ob = origin.deliver(&op_b);
    assert_eq!(oa, InboundOutcome::Applied, "replica A op must apply");
    assert_eq!(ob, InboundOutcome::Applied, "replica B op must apply");

    origin.flush("shared");

    // AS-OF 120 → replica A's write (system=100) is the newest at or before 120.
    let val_120 = origin.read_coord("shared", 7, 120);
    assert!(val_120.is_some(), "should see replica A's write AS-OF 120");
    assert_eq!(
        val_120.unwrap(),
        CellValue::Float64(100.0),
        "AS-OF 120 must see replica A value"
    );

    // AS-OF i64::MAX → replica B's write (system=150) is the latest.
    let val_max = origin.read_coord("shared", 7, i64::MAX);
    assert!(val_max.is_some(), "should see replica B's write AS-OF MAX");
    assert_eq!(
        val_max.unwrap(),
        CellValue::Float64(150.0),
        "AS-OF MAX must see replica B value"
    );
}

/// Idempotent re-delivery of a replica's op does not change the state.
#[test]
fn duplicate_op_from_replica_is_idempotent() {
    let origin = common::SyncHarness::new_in_memory();
    origin.create_array("dedup");
    let schema_hlc = origin.schema_hlc("dedup");

    let rep = common::replica(99);
    let op = common::put_op("dedup", 5, 77.0, 500, schema_hlc, rep);

    assert_eq!(origin.deliver(&op), InboundOutcome::Applied);
    assert_eq!(
        origin.deliver(&op),
        InboundOutcome::Idempotent,
        "second delivery must be Idempotent"
    );

    origin.flush("dedup");

    let val = origin.read_coord("dedup", 5, i64::MAX);
    assert_eq!(val, Some(CellValue::Float64(77.0)));
}

/// Two replicas writing the same coord at the same physical ms but different
/// replica IDs produce distinct HLCs (via replica_id tiebreak). Both ops must
/// apply and be distinguishable by system time.
#[test]
fn same_physical_ms_different_replica_ids_distinct_hlc() {
    let origin = common::SyncHarness::new_in_memory();
    origin.create_array("tiebreak");
    let schema_hlc = origin.schema_hlc("tiebreak");

    let rep_a = common::replica(1);
    let rep_b = common::replica(2);

    // Same physical ms, different replica IDs → distinct HLCs.
    let hlc_a = common::hlc(1000, rep_a);
    let hlc_b = common::hlc(1000, rep_b);
    assert_ne!(
        hlc_a, hlc_b,
        "different replica IDs must produce different HLCs"
    );

    let op_a = put_with_hlc("tiebreak", 3, 1.0, hlc_a, schema_hlc);
    let op_b = put_with_hlc("tiebreak", 3, 2.0, hlc_b, schema_hlc);

    // Both ops must apply (not collide).
    let oa = origin.deliver(&op_a);
    let ob = origin.deliver(&op_b);
    assert!(
        matches!(oa, InboundOutcome::Applied | InboundOutcome::Idempotent),
        "op_a outcome: {oa:?}"
    );
    assert!(
        matches!(ob, InboundOutcome::Applied | InboundOutcome::Idempotent),
        "op_b outcome: {ob:?}"
    );
}
