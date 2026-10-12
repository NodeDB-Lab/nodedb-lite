// SPDX-License-Identifier: Apache-2.0

//! KV push and row-refusal transport, end to end over a real WebSocket.
//!
//! Each test runs the public `run_sync_loop` against a mock Origin on
//! loopback and a recording `SyncDelegate`:
//! - a pending KV write goes out as a `KvPush` frame;
//! - each `KvPushAck` status retires, keeps, or re-sends the write;
//! - a `RowPush` this replica refuses goes back as a `RowPushReject`.

#![cfg(feature = "sync")]

use std::sync::Arc;
use std::time::Duration;

use nodedb_lite::sync::{
    PendingKvOp, PendingKvWrite, SyncClient, SyncConfig, SyncDelegate, run_sync_loop,
};
use nodedb_types::sync::wire::{
    AckStatus, EngineKind, KvPushAckMsg, KvPushMsg, KvPushOp, RowOp, RowPushMsg, RowPushRefusal,
    RowPushRejectMsg, SyncMessageType, stream_id_for,
};

mod common;

use common::mock_delegate::MockDelegate;
use common::ws_origin::{MockOrigin, await_until, collect_frames_for, next_frame, send_frame};

/// Window for observing outbound traffic: several 100ms push ticks.
const PUSH_OBSERVATION_WINDOW: Duration = Duration::from_millis(600);

/// Aborts the sync loop when the test ends.
struct LoopGuard(tokio::task::JoinHandle<()>);

impl Drop for LoopGuard {
    fn drop(&mut self) {
        self.0.abort();
    }
}

fn spawn_loop(origin: &MockOrigin, mock: &Arc<MockDelegate>) -> LoopGuard {
    let client = Arc::new(SyncClient::new(SyncConfig::new(
        origin.url(),
        "test.jwt.token",
    )));
    let delegate: Arc<dyn SyncDelegate> = Arc::clone(mock) as _;
    LoopGuard(tokio::spawn(run_sync_loop(client, delegate)))
}

fn durable_key(batch_id: u64) -> Vec<u8> {
    batch_id.to_be_bytes().to_vec()
}

fn pending_put(batch_id: u64, key: &str) -> (Vec<u8>, PendingKvWrite) {
    let op = PendingKvOp::put_raw(key.as_bytes(), b"v1", 0).expect("row");
    (
        durable_key(batch_id),
        PendingKvWrite::new("cfg", key.as_bytes(), op),
    )
}

fn ack(batch_id: u64, status: AckStatus) -> KvPushAckMsg {
    KvPushAckMsg {
        collection: "cfg".into(),
        key: b"k1".to_vec(),
        batch_id,
        accepted: matches!(status, AckStatus::Applied | AckStatus::Duplicate),
        reject_reason: None,
        applied_seq: 1,
        status,
    }
}

#[tokio::test]
async fn a_pending_kv_write_goes_out_as_a_kv_push() {
    let origin = MockOrigin::bind().await;
    let mock = Arc::new(MockDelegate::new());
    mock.set_pending_kv(vec![pending_put(7, "k1")]);
    let _guard = spawn_loop(&origin, &mock);

    let mut ws = origin.accept_handshaked().await;
    let frames = collect_frames_for(&mut ws, PUSH_OBSERVATION_WINDOW).await;
    let pushes: Vec<KvPushMsg> = frames
        .iter()
        .filter(|f| f.msg_type == SyncMessageType::KvPush)
        .map(|f| f.decode_body::<KvPushMsg>().expect("KvPush body"))
        .collect();

    assert_eq!(pushes.len(), 1, "an in-flight write is sent once");
    assert_eq!(pushes[0].collection, "cfg");
    assert_eq!(pushes[0].key, b"k1".to_vec());
    assert_eq!(pushes[0].batch_id, 7);
    assert!(matches!(pushes[0].op, KvPushOp::Put { .. }));
}

#[tokio::test]
async fn an_applied_ack_retires_the_write_and_records_the_stream_ack() {
    let origin = MockOrigin::bind().await;
    let mock = Arc::new(MockDelegate::new());
    let _guard = spawn_loop(&origin, &mock);

    let mut ws = origin.accept_handshaked().await;
    send_frame(
        &mut ws,
        SyncMessageType::KvPushAck,
        &ack(7, AckStatus::Applied),
    )
    .await;

    let recorded = mock.as_ref();
    await_until(
        move || async move { !recorded.retired_kv().is_empty() },
        "an applied KvPushAck to retire the write",
    )
    .await;
    assert_eq!(mock.retired_kv(), vec![7]);
    assert_eq!(
        mock.stream_acks(),
        vec![(stream_id_for(EngineKind::Kv, "cfg"), 1)]
    );
}

#[tokio::test]
async fn a_rejected_ack_retires_the_write() {
    let origin = MockOrigin::bind().await;
    let mock = Arc::new(MockDelegate::new());
    let _guard = spawn_loop(&origin, &mock);

    let mut ws = origin.accept_handshaked().await;
    let status = AckStatus::Rejected {
        reason: "quota exceeded".into(),
    };
    send_frame(&mut ws, SyncMessageType::KvPushAck, &ack(9, status)).await;

    let recorded = mock.as_ref();
    await_until(
        move || async move { !recorded.retired_kv().is_empty() },
        "a rejected KvPushAck to retire the write",
    )
    .await;
    assert_eq!(mock.retired_kv(), vec![9]);
    assert!(
        mock.stream_acks().is_empty(),
        "a refusal advances no frontier"
    );
}

#[tokio::test]
async fn a_gap_ack_re_sends_and_retires_nothing() {
    let origin = MockOrigin::bind().await;
    let mock = Arc::new(MockDelegate::new());
    mock.set_pending_kv(vec![pending_put(3, "k1")]);
    let _guard = spawn_loop(&origin, &mock);

    let mut ws = origin.accept_handshaked().await;
    let first = next_kv_push(&mut ws).await;
    assert_eq!(first.batch_id, 3);

    send_frame(
        &mut ws,
        SyncMessageType::KvPushAck,
        &ack(3, AckStatus::Gap { expected: 1 }),
    )
    .await;

    let resent = next_kv_push(&mut ws).await;
    assert_eq!(resent.batch_id, 3, "a gap re-sends the in-flight write");
    assert!(mock.retired_kv().is_empty());
}

/// The next `KvPush` the client sends, skipping other frames.
async fn next_kv_push(ws: &mut common::ws_origin::OriginSocket) -> KvPushMsg {
    loop {
        let frame = next_frame(ws).await.expect("a KvPush frame");
        if frame.msg_type == SyncMessageType::KvPush {
            return frame.decode_body::<KvPushMsg>().expect("KvPush body");
        }
    }
}

#[tokio::test]
async fn a_refused_row_push_goes_back_as_a_row_push_reject() {
    let origin = MockOrigin::bind().await;
    let mock = Arc::new(MockDelegate::new());
    mock.refuse_rows();
    let _guard = spawn_loop(&origin, &mock);

    let mut ws = origin.accept_handshaked().await;
    let row = RowPushMsg {
        collection: "cfg".into(),
        document_id: "k1".into(),
        payload: vec![0xc0],
        op: RowOp::Upsert,
        lsn: 5,
        peer_id: 2,
        sequence: 1,
    };
    send_frame(&mut ws, SyncMessageType::RowPush, &row).await;

    let frames = collect_frames_for(&mut ws, PUSH_OBSERVATION_WINDOW).await;
    let rejects: Vec<RowPushRejectMsg> = frames
        .iter()
        .filter(|f| f.msg_type == SyncMessageType::RowPushReject)
        .map(|f| {
            f.decode_body::<RowPushRejectMsg>()
                .expect("RowPushReject body")
        })
        .collect();
    assert_eq!(rejects.len(), 1);
    assert_eq!(rejects[0].collection, "cfg");
    assert_eq!(rejects[0].document_id, "k1");
    assert_eq!(rejects[0].sequence, 1);
    assert_eq!(rejects[0].peer_id, 2);
    assert!(matches!(
        rejects[0].refusal,
        RowPushRefusal::Malformed { .. }
    ));
}
