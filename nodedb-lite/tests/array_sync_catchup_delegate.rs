//! Array catch-up state on the `SyncDelegate` apply paths of `NodeDbLite`.
//!
//! The transport calls these methods for every `ArrayDelta`,
//! `ArrayDeltaBatch`, `ArraySnapshot`, `ArraySnapshotChunk`, and
//! `ArrayReject` frame, and calls `begin_array_catchup` once per session.
//! Each test drives the delegate directly and reads the resulting catch-up
//! state back through the request the next session sends.

pub mod common;

use nodedb_array::sync::hlc::Hlc;
use nodedb_array::sync::replica_id::ReplicaId;
use nodedb_array::types::cell_value::value::CellValue;
use nodedb_lite::sync::SyncDelegate;
use nodedb_lite::{NodeDbLite, PagedbStorageMem};
use nodedb_types::sync::wire::array::{
    ArrayCatchupRequestMsg, ArrayDeltaMsg, ArrayRejectMsg, ArrayRejectReason,
};

use common::array_wire::{
    delta_batch, open_lite_with_array, read_value, remote_put, snapshot_frames,
};
use common::clock::{hlc, replica};

const ARRAY: &str = "catchup";

fn remote_hlc(ms: u64) -> Hlc {
    hlc(ms, replica(common::array_wire::REMOTE_REPLICA))
}

/// The single request the next session sends for `ARRAY`.
fn next_request(lite: &NodeDbLite<PagedbStorageMem>) -> ArrayCatchupRequestMsg {
    let mut requests = SyncDelegate::begin_array_catchup(lite);
    assert_eq!(requests.len(), 1, "one array, one catch-up request");
    requests.remove(0)
}

fn retention_floor(array: &str) -> ArrayRejectMsg {
    ArrayRejectMsg {
        array: array.into(),
        op_hlc_bytes: Hlc::ZERO.to_bytes(),
        reason: ArrayRejectReason::RetentionFloor,
        detail: "subscriber cursor is below snapshot_hlc".into(),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn catchup_round_is_empty_when_the_replica_holds_no_arrays() {
    let storage = PagedbStorageMem::open_in_memory()
        .await
        .expect("open_in_memory");
    let lite = NodeDbLite::open(storage).await.expect("open");
    assert!(SyncDelegate::begin_array_catchup(lite.as_ref()).is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn first_catchup_request_starts_at_zero() {
    let lite = open_lite_with_array(ARRAY).await;
    let request = next_request(&lite);
    assert_eq!(request.array, ARRAY);
    assert_eq!(request.from_hlc_bytes, Hlc::ZERO.to_bytes());
}

#[tokio::test(flavor = "multi_thread")]
async fn applied_delta_advances_the_catchup_request() {
    let lite = open_lite_with_array(ARRAY).await;
    let schema_hlc = lite.array_schema_hlc(ARRAY).expect("schema_hlc");
    let op = remote_put(ARRAY, 4, 1_500, schema_hlc);
    let msg = ArrayDeltaMsg {
        array: ARRAY.into(),
        op_payload: nodedb_array::sync::op_codec::encode_op(&op).expect("encode_op"),
        producer_id: 0,
        epoch: 0,
        seq: 0,
    };

    let ack = SyncDelegate::handle_array_delta(lite.as_ref(), &msg).await;
    assert!(ack.is_some(), "an applied delta is acked");
    assert_eq!(
        next_request(&lite).from_hlc_bytes,
        remote_hlc(1_500).to_bytes()
    );

    // A replay is idempotent: no second ack, and the mark stays put.
    assert!(
        SyncDelegate::handle_array_delta(lite.as_ref(), &msg)
            .await
            .is_none()
    );
    assert_eq!(
        next_request(&lite).from_hlc_bytes,
        remote_hlc(1_500).to_bytes()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn applied_batch_advances_the_catchup_request_to_its_highest_hlc() {
    let lite = open_lite_with_array(ARRAY).await;
    let schema_hlc = lite.array_schema_hlc(ARRAY).expect("schema_hlc");
    let ops: Vec<_> = (1..=3)
        .map(|i| remote_put(ARRAY, i, i as u64 * 1_000, schema_hlc))
        .collect();

    let ack = SyncDelegate::handle_array_delta_batch(lite.as_ref(), &delta_batch(ARRAY, &ops))
        .await
        .expect("an applied batch is acked");
    assert_eq!(ack.ack_hlc_bytes, remote_hlc(3_000).to_bytes());
    assert_eq!(
        next_request(&lite).from_hlc_bytes,
        remote_hlc(3_000).to_bytes()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn catchup_request_stops_before_a_rejected_op_in_a_batch() {
    let lite = open_lite_with_array(ARRAY).await;
    let schema_hlc = lite.array_schema_hlc(ARRAY).expect("schema_hlc");
    let future_schema = Hlc::new(u64::MAX >> 16, 0, ReplicaId::new(7)).expect("future hlc");
    let ops = vec![
        remote_put(ARRAY, 1, 1_000, schema_hlc),
        remote_put(ARRAY, 2, 2_000, schema_hlc),
        // Schema newer than the local one: rejected as SchemaTooNew.
        remote_put(ARRAY, 3, 3_000, future_schema),
        remote_put(ARRAY, 4, 4_000, schema_hlc),
    ];

    let ack = SyncDelegate::handle_array_delta_batch(lite.as_ref(), &delta_batch(ARRAY, &ops))
        .await
        .expect("the applied ops are acked");
    assert_eq!(
        ack.ack_hlc_bytes,
        remote_hlc(4_000).to_bytes(),
        "the ack carries the highest applied op"
    );
    assert_eq!(
        next_request(&lite).from_hlc_bytes,
        remote_hlc(2_000).to_bytes(),
        "the next catch-up asks again for the rejected op"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn retention_floor_reject_requests_a_snapshot_from_zero() {
    let lite = open_lite_with_array(ARRAY).await;
    let schema_hlc = lite.array_schema_hlc(ARRAY).expect("schema_hlc");
    let ops = vec![remote_put(ARRAY, 1, 1_000, schema_hlc)];
    SyncDelegate::handle_array_delta_batch(lite.as_ref(), &delta_batch(ARRAY, &ops)).await;

    let request = SyncDelegate::handle_array_reject(lite.as_ref(), &retention_floor(ARRAY))
        .await
        .expect("a RetentionFloor reject yields a catch-up request");
    assert_eq!(request.array, ARRAY);
    assert_eq!(request.from_hlc_bytes, Hlc::ZERO.to_bytes());
    assert_eq!(
        next_request(&lite).from_hlc_bytes,
        Hlc::ZERO.to_bytes(),
        "the flag persists until a snapshot completes"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn other_reject_reasons_request_no_catchup() {
    let lite = open_lite_with_array(ARRAY).await;
    let mut reject = retention_floor(ARRAY);
    reject.reason = ArrayRejectReason::EngineRejected;
    assert!(
        SyncDelegate::handle_array_reject(lite.as_ref(), &reject)
            .await
            .is_none()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn completed_snapshot_applies_acks_and_clears_the_retention_floor_flag() {
    let lite = open_lite_with_array(ARRAY).await;
    let schema_hlc = lite.array_schema_hlc(ARRAY).expect("schema_hlc");
    SyncDelegate::handle_array_reject(lite.as_ref(), &retention_floor(ARRAY)).await;

    let ops: Vec<_> = (1..=5)
        .map(|i| remote_put(ARRAY, i, i as u64 * 100, schema_hlc))
        .collect();
    let snapshot_hlc = remote_hlc(9_000);
    let (header, chunks) = snapshot_frames(ARRAY, &ops, snapshot_hlc, schema_hlc, 64);
    assert!(chunks.len() > 1, "the snapshot spans several chunks");

    SyncDelegate::handle_array_snapshot(lite.as_ref(), &header).await;
    let last = chunks.len() - 1;
    for (i, chunk) in chunks.iter().enumerate() {
        let ack = SyncDelegate::handle_array_snapshot_chunk(lite.as_ref(), chunk).await;
        if i < last {
            assert!(ack.is_none(), "no ack while chunks remain");
        } else {
            let ack = ack.expect("the last chunk is acked");
            assert_eq!(ack.array, ARRAY);
            assert_eq!(ack.ack_hlc_bytes, snapshot_hlc.to_bytes());
        }
    }

    for i in 1..=5 {
        assert_eq!(
            read_value(&lite, ARRAY, i).await,
            Some(CellValue::Float64(i as f64 * 10.0)),
            "coord {i} comes from the snapshot"
        );
    }
    assert_eq!(
        next_request(&lite).from_hlc_bytes,
        snapshot_hlc.to_bytes(),
        "the flag is cleared and the mark sits at the snapshot HLC"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn refused_snapshot_flags_the_array_for_a_new_snapshot() {
    let lite = open_lite_with_array(ARRAY).await;
    let schema_hlc = lite.array_schema_hlc(ARRAY).expect("schema_hlc");
    let ops = vec![remote_put(ARRAY, 1, 1_000, schema_hlc)];
    SyncDelegate::handle_array_delta_batch(lite.as_ref(), &delta_batch(ARRAY, &ops)).await;

    let (_header, chunks) = snapshot_frames(ARRAY, &ops, remote_hlc(5_000), schema_hlc, 64);
    // A chunk with no header buffered is refused.
    assert!(
        SyncDelegate::handle_array_snapshot_chunk(lite.as_ref(), &chunks[0])
            .await
            .is_none()
    );
    assert_eq!(next_request(&lite).from_hlc_bytes, Hlc::ZERO.to_bytes());
}
