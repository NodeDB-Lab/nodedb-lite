//! Edge-side simulation — does NOT exercise real Origin transport.
//! All tests here call Lite's inbound/outbound handlers directly, bypassing
//! the WebSocket connection to a live Origin node.
//!
//! The real-transport round-trip (Lite → Origin WebSocket → Lite) is not covered
//! by any test in this file.  See §13 of the release checklist for the decision
//! record and the placeholder real-transport test in `tests/array_sync_interop.rs`.
//!
//! Original note: "Rejection" is synthesised by delivering an ArrayRejectMsg
//! directly to the inbound handler — matching what Origin would send over the wire.

mod common;

use nodedb_array::sync::op_codec;
use nodedb_lite::sync::array::inbound::outcome::InboundOutcome;
use nodedb_types::sync::wire::array::{ArrayRejectMsg, ArrayRejectReason};

/// Helper: enqueue one op in the pending queue and return its HLC bytes.
async fn enqueue_and_get_hlc_bytes(harness: &common::SyncHarness, array: &str) -> [u8; 18] {
    let schema_hlc = harness.schema_hlc(array);
    let rep = common::replica(7);
    let op = common::put_op(array, 1, 55.0, 300, schema_hlc, rep);

    // Enqueue directly into the pending queue (simulates a locally-emitted op
    // that hasn't been acked by Origin yet).
    harness
        .pending
        .enqueue(&op)
        .await
        .expect("enqueue pending op");

    op.header.hlc.to_bytes()
}

/// Origin rejects an op with `ArrayUnknown`; the op is removed from the
/// pending queue and the outcome is `RejectAcknowledged`.
#[tokio::test(flavor = "multi_thread")]
async fn reject_array_unknown_drops_from_pending() {
    let harness = common::SyncHarness::new_in_memory().await;
    harness.create_array("rej_arr").await;

    let hlc_bytes = enqueue_and_get_hlc_bytes(&harness, "rej_arr").await;

    let before = harness.pending.len().await.expect("len");
    assert_eq!(before, 1, "one op must be pending before reject");

    let reject_msg = ArrayRejectMsg {
        array: "rej_arr".into(),
        op_hlc_bytes: hlc_bytes,
        reason: ArrayRejectReason::ArrayUnknown,
        detail: "array not found on Origin".into(),
    };

    let outcome = harness
        .inbound
        .handle_reject(&reject_msg)
        .await
        .expect("handle_reject");
    assert_eq!(
        outcome,
        InboundOutcome::RejectAcknowledged,
        "reject must return RejectAcknowledged"
    );

    let after = harness.pending.len().await.expect("len");
    assert_eq!(after, 0, "rejected op must be removed from pending queue");
}

/// Rejection with `RetentionFloor` marks the array as needing a full catch-up.
#[tokio::test(flavor = "multi_thread")]
async fn reject_retention_floor_marks_catchup_needed() {
    let harness = common::SyncHarness::new_in_memory().await;
    harness.create_array("ret_floor").await;

    let hlc_bytes = enqueue_and_get_hlc_bytes(&harness, "ret_floor").await;

    let reject_msg = ArrayRejectMsg {
        array: "ret_floor".into(),
        op_hlc_bytes: hlc_bytes,
        reason: ArrayRejectReason::RetentionFloor,
        detail: "op predates retention window".into(),
    };

    harness
        .inbound
        .handle_reject(&reject_msg)
        .await
        .expect("handle_reject");

    // Verify the catchup tracker now flags this array.
    let needs = harness.catchup.arrays_needing_catchup();
    assert!(
        needs.contains(&"ret_floor".to_owned()),
        "RetentionFloor reject must flag the array as needing catchup; got: {needs:?}"
    );
}

/// Rejecting an op that is no longer in the pending queue is a no-op
/// (idempotent) — returns `RejectAcknowledged` without error.
#[tokio::test(flavor = "multi_thread")]
async fn reject_missing_op_is_idempotent() {
    let harness = common::SyncHarness::new_in_memory().await;
    harness.create_array("ghost").await;

    let rep = common::replica(1);
    let schema_hlc = harness.schema_hlc("ghost");
    let op = common::put_op("ghost", 0, 0.0, 1, schema_hlc, rep);

    // The op is NOT enqueued — simulate a reject arriving for an already-acked op.
    let reject_msg = ArrayRejectMsg {
        array: "ghost".into(),
        op_hlc_bytes: op.header.hlc.to_bytes(),
        reason: ArrayRejectReason::ArrayUnknown,
        detail: "ghost op".into(),
    };

    let outcome = harness
        .inbound
        .handle_reject(&reject_msg)
        .await
        .expect("handle_reject must not fail for missing op");
    assert_eq!(outcome, InboundOutcome::RejectAcknowledged);
}

/// Deliver an op whose `schema_hlc` is far in the future so the apply engine
/// returns `SchemaTooNew`. This is not a wire rejection but an apply-level
/// rejection — surfaces as `InboundOutcome::Rejected(SchemaTooNew)`.
#[tokio::test(flavor = "multi_thread")]
async fn schema_too_new_surfaces_as_rejected_outcome() {
    use nodedb_array::sync::apply::ApplyRejection;
    use nodedb_array::sync::hlc::Hlc;
    use nodedb_array::sync::op::{ArrayOp, ArrayOpHeader, ArrayOpKind};
    use nodedb_array::sync::replica_id::ReplicaId;
    use nodedb_array::types::cell_value::value::CellValue;
    use nodedb_array::types::coord::value::CoordValue;
    use nodedb_types::sync::wire::array::ArrayDeltaMsg;

    let harness = common::SyncHarness::new_in_memory().await;
    harness.create_array("schema_rej").await;

    // schema_hlc far in the future — apply engine doesn't know about it.
    let future_schema_hlc = Hlc::new(u64::MAX >> 16, 0, ReplicaId::new(99)).expect("valid HLC");

    let op = ArrayOp {
        header: ArrayOpHeader {
            array: "schema_rej".into(),
            hlc: common::hlc1(10),
            schema_hlc: future_schema_hlc,
            valid_from_ms: 0,
            valid_until_ms: -1,
            system_from_ms: 10,
        },
        kind: ArrayOpKind::Put,
        coord: vec![CoordValue::Int64(0)],
        attrs: Some(vec![CellValue::Float64(0.0)]),
    };

    let payload = op_codec::encode_op(&op).expect("encode_op");
    let msg = ArrayDeltaMsg {
        array: "schema_rej".into(),
        op_payload: payload,
        producer_id: 0,
        epoch: 0,
        seq: 0,
    };

    let outcome = harness.inbound.handle_delta(&msg).expect("handle_delta");
    assert!(
        matches!(
            outcome,
            InboundOutcome::Rejected(ApplyRejection::SchemaTooNew { .. })
        ),
        "future schema_hlc must yield SchemaTooNew rejection, got: {outcome:?}"
    );
}
