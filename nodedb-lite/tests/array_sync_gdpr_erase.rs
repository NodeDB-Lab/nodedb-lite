//! Edge-side simulation — does NOT exercise real Origin transport.
//! All tests here call Lite's inbound/outbound handlers directly, bypassing
//! the WebSocket connection to a live Origin node.
//!
//! The real-transport round-trip (Lite → Origin WebSocket → Lite) is not covered
//! by any test in this file.  See §13 of the release checklist for the decision
//! record and the placeholder real-transport test in `tests/array_sync_interop.rs`.
//!
//! Original note: bypasses WebSocket transport; exercises wire-message handlers directly.

mod common;

use nodedb_array::types::cell_value::value::CellValue;
use nodedb_lite::sync::array::inbound::outcome::InboundOutcome;

/// Write a cell, then deliver an Erase op. The cell must not be readable
/// at any system time after the erase (GDPR hard tombstone).
#[test]
fn erase_tombstone_removes_cell() {
    let harness = common::SyncHarness::new_in_memory();
    harness.create_array("gdpr");
    let schema_hlc = harness.schema_hlc("gdpr");
    let rep = common::replica(1);

    // Write the cell at system_from_ms=100.
    let put = common::put_op("gdpr", 2, 55.0, 100, schema_hlc, rep);
    assert_eq!(harness.deliver(&put), InboundOutcome::Applied);
    harness.flush("gdpr");

    // Verify cell is visible before erase.
    let before = harness.read_coord("gdpr", 2, i64::MAX);
    assert_eq!(
        before,
        Some(CellValue::Float64(55.0)),
        "cell must be visible before erase"
    );

    // Erase at system_from_ms=200.
    let erase = common::erase_op("gdpr", 2, 200, schema_hlc, rep);
    let outcome = harness.deliver(&erase);
    assert_eq!(outcome, InboundOutcome::Applied, "Erase op must be Applied");
    harness.flush("gdpr");

    // Cell must be gone after erase (at any system time).
    let after = harness.read_coord("gdpr", 2, i64::MAX);
    assert!(
        after.is_none(),
        "GDPR-erased cell must return None (got {after:?})"
    );
}

/// Re-issuing the exact same Erase op must be Idempotent.
#[test]
fn erase_op_is_idempotent() {
    let harness = common::SyncHarness::new_in_memory();
    harness.create_array("gdpr_idem");
    let schema_hlc = harness.schema_hlc("gdpr_idem");
    let rep = common::replica(1);

    let put = common::put_op("gdpr_idem", 3, 11.0, 50, schema_hlc, rep);
    harness.deliver(&put);
    harness.flush("gdpr_idem");

    let erase = common::erase_op("gdpr_idem", 3, 100, schema_hlc, rep);

    let first = harness.deliver(&erase);
    assert_eq!(first, InboundOutcome::Applied);

    // Second delivery of the same erase.
    let second = harness.deliver(&erase);
    assert_eq!(
        second,
        InboundOutcome::Idempotent,
        "re-delivering the same Erase must be Idempotent"
    );
}

/// Erase op propagated from a second Lite replica also removes the cell
/// (multi-Lite scenario: Lite A writes, Lite B erases).
#[test]
fn cross_replica_erase_removes_cell() {
    let harness = common::SyncHarness::new_in_memory();
    harness.create_array("xr_gdpr");
    let schema_hlc = harness.schema_hlc("xr_gdpr");

    let rep_a = common::replica(1);
    let rep_b = common::replica(2);

    let put = common::put_op("xr_gdpr", 9, 77.0, 100, schema_hlc, rep_a);
    harness.deliver(&put);
    harness.flush("xr_gdpr");

    let before = harness.read_coord("xr_gdpr", 9, i64::MAX);
    assert!(
        before.is_some(),
        "cell must exist before cross-replica erase"
    );

    // Replica B erases the cell at a later system time.
    let erase = common::erase_op("xr_gdpr", 9, 300, schema_hlc, rep_b);
    let outcome = harness.deliver(&erase);
    assert_eq!(outcome, InboundOutcome::Applied);
    harness.flush("xr_gdpr");

    let after = harness.read_coord("xr_gdpr", 9, i64::MAX);
    assert!(
        after.is_none(),
        "cell must be gone after cross-replica Erase"
    );
}

/// Erasing a coord that was never written is a no-op (does not panic).
#[test]
fn erase_nonexistent_coord_is_safe() {
    let harness = common::SyncHarness::new_in_memory();
    harness.create_array("no_cell");
    let schema_hlc = harness.schema_hlc("no_cell");
    let rep = common::replica(1);

    let erase = common::erase_op("no_cell", 99, 500, schema_hlc, rep);
    // Must not panic.
    let _ = harness.deliver(&erase);
    harness.flush("no_cell");

    let val = harness.read_coord("no_cell", 99, i64::MAX);
    assert!(val.is_none(), "non-existent coord must remain None");
}
