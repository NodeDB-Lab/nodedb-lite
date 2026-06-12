//! §8.4 — Capstone: the durable idempotent gate deduplicates a fenced
//! producer's re-sent delta.
//!
//! This is the end-to-end proof of the whole idempotent-producer chain:
//!   1. A fenced handshake (real lite_id + epoch) makes Origin assign a
//!      non-zero `producer_id` — i.e. the Data-Plane gate is LIVE (not the
//!      `producer_id == 0` no-op sentinel).
//!   2. A delta sent with `seq = 1` is Applied.
//!   3. The SAME delta re-sent with the SAME `seq = 1` (the reconnect-mid-flight
//!      scenario, where Lite reuses the stable per-write seq) is reported
//!      `Duplicate` by the gate — NOT applied a second time.
//!
//! Before this work `producer_id` was always 0 (the client never sent its
//! identity), so the gate was dormant and a re-send would double-apply.

use std::time::Duration;

use futures::{SinkExt, StreamExt};
use nodedb_lite::engine::crdt::CrdtEngine;
use nodedb_types::sync::wire::{
    AckStatus, DeltaAckMsg, DeltaPushMsg, HandshakeMsg, SyncFrame, SyncMessageType,
};
use tokio_tungstenite::tungstenite::Message;

use super::helpers::{Ws, minimal_hs, raw_connect, recv_ack, send_hs};
use crate::common::origin::OriginServer;

/// Send a `DeltaPush` frame and read until the matching `DeltaAck` (failing
/// loudly on a `DeltaReject`).
async fn send_delta_expect_ack(ws: &mut Ws, msg: &DeltaPushMsg) -> DeltaAckMsg {
    let bytes = SyncFrame::try_encode(SyncMessageType::DeltaPush, msg)
        .expect("encode DeltaPush")
        .to_bytes();
    ws.send(Message::Binary(bytes.into()))
        .await
        .expect("send DeltaPush");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let raw = tokio::time::timeout_at(deadline, ws.next())
            .await
            .expect("timeout waiting for DeltaAck")
            .expect("stream closed before DeltaAck")
            .expect("WebSocket error reading DeltaAck");
        let Message::Binary(data) = raw else { continue };
        let Some(frame) = SyncFrame::from_bytes(&data) else {
            continue;
        };
        match frame.msg_type {
            SyncMessageType::DeltaAck => {
                return frame.decode_body::<DeltaAckMsg>().expect("decode DeltaAck");
            }
            SyncMessageType::DeltaReject => {
                panic!("expected DeltaAck, got DeltaReject for the delta");
            }
            _ => continue, // skip unrelated frames (snapshots, pings, etc.)
        }
    }
}

#[tokio::test]
async fn fenced_resend_same_seq_is_deduped_by_gate() {
    let Some(_server) = OriginServer::try_spawn() else {
        eprintln!("SKIP: Origin binary unavailable (set NODEDB_BIN or run via `cargo nextest`)");
        return;
    };
    let mut ws = raw_connect(_server.ws_url).await;

    // Fenced handshake: a real lite_id + epoch ⇒ Origin registers a producer
    // and returns a non-zero producer_id, engaging the gate.
    let hs = HandshakeMsg {
        lite_id: "capstone-dedup-lite-id-7x9q".into(),
        epoch: 1,
        ..minimal_hs()
    };
    send_hs(&mut ws, &hs).await;
    let ack = recv_ack(&mut ws).await;
    assert!(
        ack.success,
        "fenced handshake must succeed: {:?}",
        ack.error
    );
    assert_ne!(
        ack.producer_id, 0,
        "fenced handshake must assign a non-zero producer_id (gate is live, not the no-op sentinel)"
    );

    // Build a real Loro delta.
    let mut engine = CrdtEngine::new(7001).expect("create CrdtEngine");
    engine
        .upsert("dedup_col", "doc-1", &[("x", loro::LoroValue::I64(1))])
        .expect("upsert");
    let delta_bytes = engine.pending_deltas()[0].delta_bytes.clone();

    let push = DeltaPushMsg {
        collection: "dedup_col".into(),
        document_id: "doc-1".into(),
        delta: delta_bytes,
        peer_id: 7001,
        mutation_id: 1,
        checksum: 0,
        device_valid_time_ms: None,
        producer_id: ack.producer_id,
        epoch: ack.accepted_epoch,
        seq: 1,
    };

    // First send at seq=1: applied.
    let first = send_delta_expect_ack(&mut ws, &push).await;
    assert_eq!(
        first.status,
        AckStatus::Applied,
        "first fenced delta (seq=1) must be Applied, got {:?}",
        first.status
    );

    // Re-send the SAME (producer, stream, seq=1): the durable gate must report
    // Duplicate — proving a reconnect-mid-flight re-send is NOT double-applied.
    let second = send_delta_expect_ack(&mut ws, &push).await;
    assert_eq!(
        second.status,
        AckStatus::Duplicate,
        "re-sent same-seq fenced delta must be Duplicate (gate dedup), got {:?}",
        second.status
    );
}
