// SPDX-License-Identifier: Apache-2.0

//! Array catch-up over a real WebSocket against a mock Origin.
//!
//! Each test runs the public `run_sync_loop` against a loopback mock Origin
//! that plays Origin's side of the catch-up protocol:
//!
//! 1. After the handshake the client sends one `ArrayCatchupRequest` per
//!    array it holds, starting at the array's catch-up high-water mark.
//! 2. Origin answers with `ArrayDeltaBatch` frames, or with `ArraySnapshot`
//!    plus `ArraySnapshotChunk` frames when the mark is below its GC boundary.
//! 3. The client acks what it applied with `ArrayAck`.
//! 4. A `RetentionFloor` `ArrayReject` makes the client ask again from
//!    `Hlc::ZERO`, which Origin answers with a snapshot.
//!
//! The first group routes frames to a recording `MockDelegate`; the second
//! drives a real `NodeDbLite` replica and checks that it converges.

#![cfg(feature = "sync")]

use std::sync::Arc;
use std::time::Duration;

use nodedb_array::sync::hlc::Hlc;
use nodedb_array::types::cell_value::value::CellValue;
use nodedb_lite::sync::{SyncClient, SyncConfig, SyncDelegate, run_sync_loop};
use nodedb_lite::{NodeDbLite, PagedbStorageMem};
use nodedb_types::sync::wire::array::{
    ArrayAckMsg, ArrayCatchupRequestMsg, ArrayRejectMsg, ArrayRejectReason,
};
use nodedb_types::sync::wire::{SyncFrame, SyncMessageType};

pub mod common;

use common::array_wire::{
    REMOTE_REPLICA, delta_batch, open_lite_with_array, read_value, remote_put, snapshot_frames,
};
use common::clock::{hlc, replica};
use common::mock_delegate::MockDelegate;
use common::ws_origin::{
    MockOrigin, OriginSocket, await_until, collect_frames_for, next_frame, send_frame,
};

/// Window for negative assertions on outbound traffic. The push loop ticks
/// every 100ms, so this covers several ticks.
const PUSH_OBSERVATION_WINDOW: Duration = Duration::from_millis(600);

const ARRAY: &str = "grid";

/// Aborts the sync loop when the test ends, so a client that is mid-reconnect
/// does not outlive its mock Origin.
struct LoopGuard(tokio::task::JoinHandle<()>);

impl Drop for LoopGuard {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// A client that reconnects quickly, so reconnect tests stay well inside the
/// mock Origin's accept timeout.
fn make_client(origin: &MockOrigin) -> Arc<SyncClient> {
    let mut config = SyncConfig::new(origin.url(), "test.jwt.token");
    config.min_backoff = Duration::from_millis(50);
    Arc::new(SyncClient::new(config))
}

fn spawn_loop<D: SyncDelegate>(client: &Arc<SyncClient>, delegate: Arc<D>) -> LoopGuard {
    let delegate: Arc<dyn SyncDelegate> = delegate;
    LoopGuard(tokio::spawn(run_sync_loop(Arc::clone(client), delegate)))
}

fn remote_hlc(ms: u64) -> Hlc {
    hlc(ms, replica(REMOTE_REPLICA))
}

fn catchup(array: &str, from: Hlc) -> ArrayCatchupRequestMsg {
    ArrayCatchupRequestMsg {
        array: array.into(),
        from_hlc_bytes: from.to_bytes(),
    }
}

fn retention_floor(array: &str) -> ArrayRejectMsg {
    ArrayRejectMsg {
        array: array.into(),
        op_hlc_bytes: Hlc::ZERO.to_bytes(),
        reason: ArrayRejectReason::RetentionFloor,
        detail: "subscriber cursor is below snapshot_hlc; issue CatchupRequest to resync".into(),
    }
}

/// The next frame of `msg_type` the client sends, skipping every other type.
async fn next_frame_of(ws: &mut OriginSocket, msg_type: SyncMessageType) -> SyncFrame {
    loop {
        let frame = next_frame(ws)
            .await
            .unwrap_or_else(|| panic!("client closed before sending {msg_type:?}"));
        if frame.msg_type == msg_type {
            return frame;
        }
    }
}

async fn next_catchup(ws: &mut OriginSocket) -> ArrayCatchupRequestMsg {
    next_frame_of(ws, SyncMessageType::ArrayCatchupRequest)
        .await
        .decode_body()
        .expect("decode ArrayCatchupRequest")
}

/// Wait for the `ArrayAck` carrying `hlc`. The client acks only after the
/// apply and the catch-up record complete, so this orders the test after both.
async fn await_ack(ws: &mut OriginSocket, array: &str, hlc: Hlc) {
    loop {
        let ack: ArrayAckMsg = next_frame_of(ws, SyncMessageType::ArrayAck)
            .await
            .decode_body()
            .expect("decode ArrayAck");
        if ack.array == array && ack.ack_hlc_bytes == hlc.to_bytes() {
            return;
        }
    }
}

/// Close the session from Origin's side. The client sees a clean close and
/// reconnects after `min_backoff`.
async fn disconnect(mut ws: OriginSocket) {
    let _ = ws.close(None).await;
}

// ── Routing, with a recording delegate ──────────────────────────────────────

#[tokio::test]
async fn catchup_requests_are_sent_after_every_handshake() {
    let origin = MockOrigin::bind().await;
    let client = make_client(&origin);
    let mock = Arc::new(MockDelegate::new());
    mock.set_array_catchups(vec![
        catchup("alpha", Hlc::ZERO),
        catchup("beta", remote_hlc(700)),
    ]);
    let _guard = spawn_loop(&client, Arc::clone(&mock));

    let mut ws = origin.accept_handshaked().await;
    assert_eq!(next_catchup(&mut ws).await, catchup("alpha", Hlc::ZERO));
    assert_eq!(
        next_catchup(&mut ws).await,
        catchup("beta", remote_hlc(700))
    );
    disconnect(ws).await;

    let mut ws = origin.accept_handshaked().await;
    assert_eq!(next_catchup(&mut ws).await, catchup("alpha", Hlc::ZERO));
    assert_eq!(
        next_catchup(&mut ws).await,
        catchup("beta", remote_hlc(700))
    );
    assert_eq!(mock.array_catchup_rounds(), 2, "one round per session");
}

#[tokio::test]
async fn retention_floor_reject_sends_a_catchup_request_from_zero() {
    let origin = MockOrigin::bind().await;
    let client = make_client(&origin);
    let mock = Arc::new(MockDelegate::new());
    let _guard = spawn_loop(&client, Arc::clone(&mock));

    let mut ws = origin.accept_handshaked().await;
    send_frame(
        &mut ws,
        SyncMessageType::ArrayReject,
        &retention_floor(ARRAY),
    )
    .await;
    assert_eq!(next_catchup(&mut ws).await, catchup(ARRAY, Hlc::ZERO));
}

#[tokio::test]
async fn snapshot_frames_reach_the_delegate_and_the_last_chunk_is_acked() {
    let origin = MockOrigin::bind().await;
    let client = make_client(&origin);
    let mock = Arc::new(MockDelegate::new());
    let _guard = spawn_loop(&client, Arc::clone(&mock));

    let schema_hlc = remote_hlc(1);
    let ops: Vec<_> = (1..=4)
        .map(|i| remote_put(ARRAY, i, i as u64 * 100, schema_hlc))
        .collect();
    let snapshot_hlc = remote_hlc(9_000);
    let (header, chunks) = snapshot_frames(ARRAY, &ops, snapshot_hlc, schema_hlc, 64);
    assert!(chunks.len() > 1, "the snapshot spans several chunks");

    let mut ws = origin.accept_handshaked().await;
    send_frame(&mut ws, SyncMessageType::ArraySnapshot, &header).await;
    for chunk in &chunks {
        send_frame(&mut ws, SyncMessageType::ArraySnapshotChunk, chunk).await;
    }
    await_ack(&mut ws, ARRAY, snapshot_hlc).await;

    let mut expected = vec![format!("header:{ARRAY}")];
    expected.extend((0..chunks.len()).map(|i| format!("chunk:{ARRAY}:{i}")));
    assert_eq!(mock.array_snapshot_frames(), expected);
}

// ── Convergence, with a real replica ────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn no_catchup_request_is_sent_when_the_replica_holds_no_arrays() {
    let origin = MockOrigin::bind().await;
    let client = make_client(&origin);
    let storage = PagedbStorageMem::open_in_memory()
        .await
        .expect("open_in_memory");
    let lite = NodeDbLite::open(storage).await.expect("open");
    let _guard = spawn_loop(&client, lite);

    let mut ws = origin.accept_handshaked().await;
    let frames = collect_frames_for(&mut ws, PUSH_OBSERVATION_WINDOW).await;
    assert!(
        frames
            .iter()
            .all(|f| f.msg_type != SyncMessageType::ArrayCatchupRequest),
        "a replica with no arrays has nothing to catch up"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn replica_that_missed_deltas_while_disconnected_converges_after_reconnect() {
    let origin = MockOrigin::bind().await;
    let client = make_client(&origin);
    let lite = open_lite_with_array(ARRAY).await;
    let schema_hlc = lite.array_schema_hlc(ARRAY).expect("schema_hlc");
    let _guard = spawn_loop(&client, Arc::clone(&lite));

    // First session: nothing applied yet, so the request asks for all history.
    let mut ws = origin.accept_handshaked().await;
    assert_eq!(next_catchup(&mut ws).await, catchup(ARRAY, Hlc::ZERO));
    let first: Vec<_> = (1..=3)
        .map(|i| remote_put(ARRAY, i, i as u64 * 1_000, schema_hlc))
        .collect();
    send_frame(
        &mut ws,
        SyncMessageType::ArrayDeltaBatch,
        &delta_batch(ARRAY, &first),
    )
    .await;
    await_ack(&mut ws, ARRAY, remote_hlc(3_000)).await;
    disconnect(ws).await;

    // Ops 4 and 5 reach Origin while the replica is offline. The reconnect
    // asks from the last applied HLC; Origin streams every op at or above it.
    let mut ws = origin.accept_handshaked().await;
    assert_eq!(
        next_catchup(&mut ws).await,
        catchup(ARRAY, remote_hlc(3_000)),
        "the reconnect resumes at the last applied op"
    );
    let missed: Vec<_> = (3..=5)
        .map(|i| remote_put(ARRAY, i, i as u64 * 1_000, schema_hlc))
        .collect();
    send_frame(
        &mut ws,
        SyncMessageType::ArrayDeltaBatch,
        &delta_batch(ARRAY, &missed),
    )
    .await;
    await_ack(&mut ws, ARRAY, remote_hlc(5_000)).await;

    for i in 1..=5 {
        assert_eq!(
            read_value(&lite, ARRAY, i).await,
            Some(CellValue::Float64(i as f64 * 10.0)),
            "coord {i} converged"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn retention_floor_reject_resyncs_the_replica_from_a_snapshot() {
    let origin = MockOrigin::bind().await;
    let client = make_client(&origin);
    let lite = open_lite_with_array(ARRAY).await;
    let schema_hlc = lite.array_schema_hlc(ARRAY).expect("schema_hlc");
    let _guard = spawn_loop(&client, Arc::clone(&lite));

    let mut ws = origin.accept_handshaked().await;
    assert_eq!(next_catchup(&mut ws).await, catchup(ARRAY, Hlc::ZERO));
    let early = vec![remote_put(ARRAY, 1, 1_000, schema_hlc)];
    send_frame(
        &mut ws,
        SyncMessageType::ArrayDeltaBatch,
        &delta_batch(ARRAY, &early),
    )
    .await;
    await_ack(&mut ws, ARRAY, remote_hlc(1_000)).await;

    // Origin's GC passed this session's cursor: the op stream cannot resume.
    send_frame(
        &mut ws,
        SyncMessageType::ArrayReject,
        &retention_floor(ARRAY),
    )
    .await;
    assert_eq!(
        next_catchup(&mut ws).await,
        catchup(ARRAY, Hlc::ZERO),
        "the re-request starts below the GC boundary so Origin sends a snapshot"
    );

    let compacted: Vec<_> = (1..=5)
        .map(|i| remote_put(ARRAY, i, i as u64 * 1_000, schema_hlc))
        .collect();
    let snapshot_hlc = remote_hlc(8_000);
    let (header, chunks) = snapshot_frames(ARRAY, &compacted, snapshot_hlc, schema_hlc, 64);
    assert!(chunks.len() > 1, "the snapshot spans several chunks");
    send_frame(&mut ws, SyncMessageType::ArraySnapshot, &header).await;
    for chunk in &chunks {
        send_frame(&mut ws, SyncMessageType::ArraySnapshotChunk, chunk).await;
    }
    await_ack(&mut ws, ARRAY, snapshot_hlc).await;

    for i in 1..=5 {
        assert_eq!(
            read_value(&lite, ARRAY, i).await,
            Some(CellValue::Float64(i as f64 * 10.0)),
            "coord {i} comes from the snapshot"
        );
    }
    disconnect(ws).await;

    // The completed snapshot cleared the flag and moved the mark to the
    // snapshot HLC, so the next session resumes there.
    let mut ws = origin.accept_handshaked().await;
    assert_eq!(next_catchup(&mut ws).await, catchup(ARRAY, snapshot_hlc));
}

#[tokio::test(flavor = "multi_thread")]
async fn interrupted_snapshot_is_requested_again_on_reconnect() {
    let origin = MockOrigin::bind().await;
    let client = make_client(&origin);
    let lite = open_lite_with_array(ARRAY).await;
    let schema_hlc = lite.array_schema_hlc(ARRAY).expect("schema_hlc");
    let _guard = spawn_loop(&client, Arc::clone(&lite));

    let mut ws = origin.accept_handshaked().await;
    assert_eq!(next_catchup(&mut ws).await, catchup(ARRAY, Hlc::ZERO));
    send_frame(
        &mut ws,
        SyncMessageType::ArrayReject,
        &retention_floor(ARRAY),
    )
    .await;
    assert_eq!(next_catchup(&mut ws).await, catchup(ARRAY, Hlc::ZERO));

    let compacted: Vec<_> = (1..=5)
        .map(|i| remote_put(ARRAY, i, i as u64 * 1_000, schema_hlc))
        .collect();
    let snapshot_hlc = remote_hlc(8_000);
    let (header, chunks) = snapshot_frames(ARRAY, &compacted, snapshot_hlc, schema_hlc, 64);
    assert!(chunks.len() > 1, "the snapshot spans several chunks");

    // The session drops after the first chunk.
    send_frame(&mut ws, SyncMessageType::ArraySnapshot, &header).await;
    send_frame(&mut ws, SyncMessageType::ArraySnapshotChunk, &chunks[0]).await;
    disconnect(ws).await;

    // The flag survives, so the next session asks for the snapshot again,
    // and the full re-sent transfer applies cleanly.
    let mut ws = origin.accept_handshaked().await;
    assert_eq!(next_catchup(&mut ws).await, catchup(ARRAY, Hlc::ZERO));
    send_frame(&mut ws, SyncMessageType::ArraySnapshot, &header).await;
    for chunk in &chunks {
        send_frame(&mut ws, SyncMessageType::ArraySnapshotChunk, chunk).await;
    }
    await_ack(&mut ws, ARRAY, snapshot_hlc).await;

    let reader = lite.as_ref();
    await_until(
        move || async move { read_value(reader, ARRAY, 5).await.is_some() },
        "the re-sent snapshot to apply",
    )
    .await;
}
