//! Gate test: `ArrayDelta` and `ArrayDeltaBatch` receive path wired.
//!
//! These tests prove that the transport dispatch path — `dispatch_frame`
//! receiving `SyncMessageType::ArrayDelta` / `SyncMessageType::ArrayDeltaBatch`
//! from Origin — correctly decodes the body, applies it to the local array
//! engine, and produces an `ArrayAckMsg` to return to Origin.
//!
//! No live Origin server is needed.  The tests hand-craft wire frames and push
//! them through the `SyncDelegate` trait methods that `dispatch_frame` calls.
//! This is the appropriate approach: Origin's array-delta fan-out requires a
//! shape subscription to be fully configured via a live WebSocket session;
//! the in-process path exercises the identical code invoked by the transport.
//!
//! Approach taken:
//! - `open_lite_with_array` builds a `NodeDbLite` in-memory with a registered array.
//! - Tests call `SyncDelegate::handle_array_delta` / `handle_array_delta_batch`
//!   directly — these are exactly the methods `dispatch_frame` calls on every
//!   incoming `ArrayDelta` / `ArrayDeltaBatch` frame.
//! - Assertions check both the returned `ArrayAckMsg` and the engine state.

mod common;

use std::sync::Arc;

use nodedb_array::sync::op::{ArrayOp, ArrayOpHeader, ArrayOpKind};
use nodedb_array::sync::op_codec;
use nodedb_array::types::cell_value::value::CellValue;
use nodedb_array::types::coord::value::CoordValue;
use nodedb_lite::NodeDbLite;
use nodedb_lite::storage::redb_storage::RedbStorage;
use nodedb_lite::sync::SyncDelegate;
use nodedb_types::sync::wire::array::{ArrayDeltaBatchMsg, ArrayDeltaMsg};

use common::schema::simple_schema;

/// Open a fresh in-memory `NodeDbLite` with a named array pre-registered.
async fn open_lite_with_array(array_name: &str) -> Arc<NodeDbLite<RedbStorage>> {
    let storage = RedbStorage::open_in_memory().expect("open_in_memory");
    let lite = Arc::new(NodeDbLite::open(storage, 1).await.expect("open"));
    lite.create_array(array_name, simple_schema(array_name))
        .expect("create_array");
    lite
}

// ── Single-delta apply ────────────────────────────────────────────────────────

/// `handle_array_delta` — the method `dispatch_frame` calls on every
/// `SyncMessageType::ArrayDelta` frame — applies a Put op, updates local
/// engine state, and returns an `ArrayAckMsg`.
#[tokio::test]
async fn array_delta_apply_and_ack() {
    let lite = open_lite_with_array("arr").await;

    let schema_hlc = lite
        .array_schema_hlc("arr")
        .expect("schema must be registered after create_array");

    let hlc = nodedb_array::sync::hlc::Hlc::new(
        1_000,
        0,
        nodedb_array::sync::replica_id::ReplicaId::new(99),
    )
    .unwrap();

    let op = ArrayOp {
        header: ArrayOpHeader {
            array: "arr".into(),
            hlc,
            schema_hlc,
            valid_from_ms: 0,
            valid_until_ms: -1,
            system_from_ms: 1_000,
        },
        kind: ArrayOpKind::Put,
        coord: vec![CoordValue::Int64(5)],
        attrs: Some(vec![CellValue::Float64(99.0)]),
    };

    let payload = op_codec::encode_op(&op).expect("encode_op");
    let msg = ArrayDeltaMsg {
        array: "arr".into(),
        op_payload: payload,
    };

    // This is the exact call `dispatch_frame` makes.
    let ack = SyncDelegate::handle_array_delta(lite.as_ref(), &msg);
    assert!(ack.is_some(), "Applied outcome must produce ArrayAckMsg");

    let ack = ack.unwrap();
    assert_eq!(ack.array, "arr");
    assert_ne!(ack.replica_id, 0, "replica_id must be non-zero");

    // ack_hlc_bytes must encode the applied op's HLC.
    let recovered = nodedb_array::sync::hlc::Hlc::from_bytes(&ack.ack_hlc_bytes);
    assert_eq!(
        recovered, hlc,
        "ack_hlc_bytes must encode the applied op HLC"
    );

    // The cell must be visible in the local engine.
    let payload = lite
        .array_read_coord("arr", &[CoordValue::Int64(5)], Some(2_000))
        .expect("array_read_coord");
    assert!(payload.is_some(), "cell must be present after apply");
    let cell = payload.unwrap();
    assert_eq!(
        cell.attrs.first().cloned(),
        Some(CellValue::Float64(99.0)),
        "stored attribute value must match the applied op"
    );
}

// ── Idempotent replay ─────────────────────────────────────────────────────────

/// Applying the same delta twice returns `None` on the second call (idempotent).
/// `dispatch_frame` must not enqueue a second ack for the same op.
#[tokio::test]
async fn array_delta_idempotent_no_ack() {
    let lite = open_lite_with_array("idem").await;
    let schema_hlc = lite.array_schema_hlc("idem").expect("schema_hlc");

    let op = ArrayOp {
        header: ArrayOpHeader {
            array: "idem".into(),
            hlc: nodedb_array::sync::hlc::Hlc::new(
                2_000,
                0,
                nodedb_array::sync::replica_id::ReplicaId::new(1),
            )
            .unwrap(),
            schema_hlc,
            valid_from_ms: 0,
            valid_until_ms: -1,
            system_from_ms: 2_000,
        },
        kind: ArrayOpKind::Put,
        coord: vec![CoordValue::Int64(1)],
        attrs: Some(vec![CellValue::Float64(7.0)]),
    };

    let payload = op_codec::encode_op(&op).expect("encode_op");
    let msg = ArrayDeltaMsg {
        array: "idem".into(),
        op_payload: payload,
    };

    // First application — ack expected.
    let ack1 = SyncDelegate::handle_array_delta(lite.as_ref(), &msg.clone());
    assert!(ack1.is_some(), "first apply must produce ack");

    // Second application of the identical op — idempotent, no ack.
    let ack2 = SyncDelegate::handle_array_delta(lite.as_ref(), &msg);
    assert!(
        ack2.is_none(),
        "idempotent replay must not produce a second ack"
    );
}

// ── Delta-batch apply ─────────────────────────────────────────────────────────

/// `handle_array_delta_batch` applies multiple ops and returns one ack
/// carrying the highest-HLC applied op.
#[tokio::test]
async fn array_delta_batch_apply_and_ack() {
    let lite = open_lite_with_array("batch").await;
    let schema_hlc = lite.array_schema_hlc("batch").expect("schema_hlc");

    let ops: Vec<ArrayOp> = (1u64..=3)
        .map(|i| ArrayOp {
            header: ArrayOpHeader {
                array: "batch".into(),
                hlc: nodedb_array::sync::hlc::Hlc::new(
                    i * 1_000,
                    0,
                    nodedb_array::sync::replica_id::ReplicaId::new(7),
                )
                .unwrap(),
                schema_hlc,
                valid_from_ms: 0,
                valid_until_ms: -1,
                system_from_ms: (i * 1_000) as i64,
            },
            kind: ArrayOpKind::Put,
            coord: vec![CoordValue::Int64(i as i64)],
            attrs: Some(vec![CellValue::Float64(i as f64 * 10.0)]),
        })
        .collect();

    let op_payloads: Vec<Vec<u8>> = ops
        .iter()
        .map(|op| op_codec::encode_op(op).expect("encode_op"))
        .collect();

    let msg = ArrayDeltaBatchMsg {
        array: "batch".into(),
        op_payloads,
    };

    let ack = SyncDelegate::handle_array_delta_batch(lite.as_ref(), &msg);
    assert!(ack.is_some(), "batch apply must produce ack");

    let ack = ack.unwrap();
    assert_eq!(ack.array, "batch");

    // The ack HLC must be the highest op's (i=3 → physical_ms = 3_000).
    let recovered = nodedb_array::sync::hlc::Hlc::from_bytes(&ack.ack_hlc_bytes);
    assert_eq!(
        recovered.physical_ms, 3_000,
        "ack must carry the highest applied HLC (op at i=3)"
    );

    // All three cells must be visible.
    for i in 1i64..=3 {
        let payload = lite
            .array_read_coord("batch", &[CoordValue::Int64(i)], Some(10_000))
            .expect("array_read_coord");
        assert!(
            payload.is_some(),
            "cell at coord {i} must be present after batch apply"
        );
        let cell = payload.unwrap();
        assert_eq!(
            cell.attrs.first().cloned(),
            Some(CellValue::Float64(i as f64 * 10.0)),
            "attribute at coord {i} must match applied value"
        );
    }
}

// ── Frame-layer encode / decode round-trip ────────────────────────────────────

/// Proves `SyncFrame::decode_body::<ArrayDeltaMsg>()` works for the exact
/// bytes `dispatch_frame` parses from Origin — no data is lost.
#[test]
fn array_delta_frame_roundtrip() {
    use nodedb_types::sync::wire::{SyncFrame, SyncMessageType};

    let msg = ArrayDeltaMsg {
        array: "rt_arr".into(),
        op_payload: vec![0xDE, 0xAD, 0xBE, 0xEF],
    };

    let frame = SyncFrame::try_encode(SyncMessageType::ArrayDelta, &msg)
        .expect("encode ArrayDeltaMsg into SyncFrame");

    let wire_bytes = frame.to_bytes();
    let frame2 = SyncFrame::from_bytes(&wire_bytes).expect("parse SyncFrame from bytes");

    assert_eq!(frame2.msg_type, SyncMessageType::ArrayDelta);
    let decoded: ArrayDeltaMsg = frame2.decode_body().expect("decode body");
    assert_eq!(decoded.array, "rt_arr");
    assert_eq!(decoded.op_payload, vec![0xDE, 0xAD, 0xBE, 0xEF]);
}

/// Same round-trip for `ArrayDeltaBatchMsg`.
#[test]
fn array_delta_batch_frame_roundtrip() {
    use nodedb_types::sync::wire::{SyncFrame, SyncMessageType};

    let msg = ArrayDeltaBatchMsg {
        array: "rt_batch".into(),
        op_payloads: vec![vec![0x01, 0x02], vec![0x03, 0x04]],
    };

    let frame = SyncFrame::try_encode(SyncMessageType::ArrayDeltaBatch, &msg)
        .expect("encode ArrayDeltaBatchMsg into SyncFrame");

    let wire_bytes = frame.to_bytes();
    let frame2 = SyncFrame::from_bytes(&wire_bytes).expect("parse SyncFrame from bytes");

    assert_eq!(frame2.msg_type, SyncMessageType::ArrayDeltaBatch);
    let decoded: ArrayDeltaBatchMsg = frame2.decode_body().expect("decode body");
    assert_eq!(decoded.array, "rt_batch");
    assert_eq!(decoded.op_payloads.len(), 2);
}
