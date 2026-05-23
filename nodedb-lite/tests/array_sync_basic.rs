//! Edge-side simulation — does NOT exercise real Origin transport.
//! All tests here call Lite's inbound/outbound handlers directly, bypassing
//! the WebSocket connection to a live Origin node.
//!
//! The real-transport round-trip (Lite → Origin WebSocket → Lite) is not covered
//! by any test in this file.  See §13 of the release checklist for the decision
//! record and the placeholder real-transport test in `tests/array_sync_interop.rs`.
//!
//! Original note: Phases F-I (Origin receive/send/catch-up/distributed) are not
//! yet validated end-to-end, so "Origin" in this file is an in-process Lite
//! inbound + engine state.

mod common;

use nodedb_array::types::cell_value::value::CellValue;
use nodedb_array::types::coord::value::CoordValue;
use nodedb_lite::sync::array::inbound::outcome::InboundOutcome;

/// Lite writes a cell via the outbound path, then delivers the encoded op
/// directly to the inbound handler (simulating the Origin round-trip).
/// Origin's local engine state is verified to contain the value.
#[tokio::test(flavor = "multi_thread")]
async fn basic_put_cell_roundtrip() {
    let harness = common::SyncHarness::new_in_memory().await;
    harness.create_array("grid").await;

    let schema_hlc = harness.schema_hlc("grid");
    let rep = common::replica(1);

    let op = common::put_op("grid", 5, 42.0, 1_000, schema_hlc, rep);
    let outcome = harness.deliver(&op);

    assert_eq!(
        outcome,
        InboundOutcome::Applied,
        "op must be Applied on first delivery"
    );

    harness.flush("grid").await;

    let val = harness.read_coord("grid", 5, i64::MAX).await;
    assert!(val.is_some(), "cell must be readable after inbound apply");
    assert_eq!(
        val.unwrap(),
        CellValue::Float64(42.0),
        "value must match the emitted Put"
    );
}

/// A second delivery of the exact same op must be Idempotent.
#[tokio::test(flavor = "multi_thread")]
async fn basic_idempotent_redelivery() {
    let harness = common::SyncHarness::new_in_memory().await;
    harness.create_array("idem").await;

    let schema_hlc = harness.schema_hlc("idem");
    let rep = common::replica(1);
    let op = common::put_op("idem", 3, 7.0, 500, schema_hlc, rep);

    let first = harness.deliver(&op);
    assert_eq!(first, InboundOutcome::Applied);

    let second = harness.deliver(&op);
    assert_eq!(
        second,
        InboundOutcome::Idempotent,
        "re-delivering the same op must be Idempotent"
    );
}

/// Outbound emitter writes to the pending queue; that queue entry can be
/// drain-read and re-delivered as an inbound delta on a second harness,
/// simulating the full Lite→Origin→Lite loop with two in-process engines.
#[tokio::test(flavor = "multi_thread")]
async fn basic_outbound_feeds_inbound() {
    // "Lite A" — sends the put.
    let sender = common::make_outbound_harness().await;
    sender
        .schemas
        .put_schema("shared", &common::simple_schema("shared"))
        .await
        .expect("put_schema");
    sender
        .outbound
        .emit_put(
            "shared",
            vec![CoordValue::Int64(10)],
            vec![CellValue::Float64(99.0)],
            0,
            i64::MAX,
        )
        .await
        .expect("emit_put");

    // "Lite B" — receives the op.
    let receiver = common::SyncHarness::new_in_memory().await;
    receiver.create_array("shared").await;

    // Drain pending from sender, re-deliver to receiver's inbound.
    let ops = sender
        .outbound
        .pending()
        .drain_batch(1)
        .await
        .expect("drain_batch");
    assert_eq!(ops.len(), 1, "one op must be pending after emit_put");

    for op in &ops {
        let outcome = receiver.deliver(op);
        assert_eq!(outcome, InboundOutcome::Applied);
    }

    receiver.flush("shared").await;

    let val = receiver.read_coord("shared", 10, i64::MAX).await;
    assert!(val.is_some(), "receiver must see the value after apply");
    assert_eq!(val.unwrap(), CellValue::Float64(99.0));
}
