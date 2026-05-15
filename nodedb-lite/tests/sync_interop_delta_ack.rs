//! §7.3 — End-to-end delta push → Origin validation → DeltaAck.
//!
//! Pushes real Loro CRDT deltas from a `NodeDbLite` instance through the
//! WebSocket sync transport to a live Origin server and verifies that each
//! delta is acknowledged with a `DeltaAck`.
//!
//! Each test spawns its own Origin instance with a fresh temp data dir.

mod common;

use std::time::Duration;

use futures::{SinkExt, StreamExt};
use nodedb_lite::engine::crdt::CrdtEngine;
use nodedb_types::sync::wire::{
    DeltaAckMsg, DeltaPushMsg, PingPongMsg, SyncFrame, SyncMessageType, VectorClockSyncMsg,
};
use nodedb_types::wire_version::WIRE_FORMAT_VERSION;
use tokio_tungstenite::tungstenite::Message;

use common::origin::{OriginServer, connect_and_handshake};

// ── helper ────────────────────────────────────────────────────────────────────

async fn push_and_recv(
    ws: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    msg: &DeltaPushMsg,
) -> SyncFrame {
    let bytes = SyncFrame::try_encode(SyncMessageType::DeltaPush, msg)
        .expect("encode DeltaPush")
        .to_bytes();
    ws.send(Message::Binary(bytes.into()))
        .await
        .expect("send DeltaPush");

    let resp = tokio::time::timeout(Duration::from_secs(10), ws.next())
        .await
        .expect("timeout waiting for delta response")
        .expect("stream closed before response")
        .expect("WebSocket read error");

    SyncFrame::from_bytes(resp.into_data().as_ref()).expect("decode response frame")
}

// ── tests ─────────────────────────────────────────────────────────────────────

/// §7.3a — A real Loro CRDT delta is acknowledged by Origin.
#[tokio::test]
async fn real_loro_delta_gets_acked() {
    let _server = OriginServer::spawn();
    let mut ws = connect_and_handshake(_server.ws_url).await;

    let mut engine = CrdtEngine::new(1001).expect("create CrdtEngine");
    engine
        .upsert(
            "interop_test",
            "doc-ack",
            &[("field", loro::LoroValue::String("value".into()))],
        )
        .expect("upsert");

    let deltas = engine.pending_deltas();
    assert!(!deltas.is_empty(), "engine must produce at least one delta");

    let msg = DeltaPushMsg {
        collection: "interop_test".into(),
        document_id: "doc-ack".into(),
        delta: deltas[0].delta_bytes.clone(),
        peer_id: 1001,
        mutation_id: 1,
        checksum: 0,
        device_valid_time_ms: None,
    };

    let frame = push_and_recv(&mut ws, &msg).await;

    assert_eq!(
        frame.msg_type,
        SyncMessageType::DeltaAck,
        "expected DeltaAck, got {:?}",
        frame.msg_type
    );

    let ack: DeltaAckMsg = frame.decode_body().expect("decode DeltaAckMsg");
    assert_eq!(ack.mutation_id, 1, "ack must echo the mutation_id we sent");
}

/// §7.3b — Empty delta payload is rejected immediately.
#[tokio::test]
async fn empty_delta_is_rejected() {
    let _server = OriginServer::spawn();
    let mut ws = connect_and_handshake(_server.ws_url).await;

    let msg = DeltaPushMsg {
        collection: "interop_test".into(),
        document_id: "doc-empty".into(),
        delta: Vec::new(),
        peer_id: 1002,
        mutation_id: 2,
        checksum: 0,
        device_valid_time_ms: None,
    };

    let frame = push_and_recv(&mut ws, &msg).await;

    assert_eq!(
        frame.msg_type,
        SyncMessageType::DeltaReject,
        "empty delta must be rejected, got {:?}",
        frame.msg_type
    );
}

/// §7.3c — CRC32C checksum mismatch is rejected.
#[tokio::test]
async fn crc_mismatch_delta_is_rejected() {
    let _server = OriginServer::spawn();
    let mut ws = connect_and_handshake(_server.ws_url).await;

    let payload = vec![1u8, 2, 3, 4, 5];
    let bad_checksum = 0xDEAD_BEEFu32;

    let msg = DeltaPushMsg {
        collection: "interop_test".into(),
        document_id: "doc-crc".into(),
        delta: payload,
        peer_id: 1003,
        mutation_id: 3,
        checksum: bad_checksum,
        device_valid_time_ms: None,
    };

    let frame = push_and_recv(&mut ws, &msg).await;

    assert_eq!(
        frame.msg_type,
        SyncMessageType::DeltaReject,
        "CRC mismatch must be rejected, got {:?}",
        frame.msg_type
    );
}

/// §7.3d — Multiple sequential deltas from the same peer are all acked.
#[tokio::test]
async fn sequential_deltas_all_acked() {
    let _server = OriginServer::spawn();
    let mut ws = connect_and_handshake(_server.ws_url).await;

    let mut engine = CrdtEngine::new(1004).expect("create CrdtEngine");

    for i in 1u64..=3 {
        engine
            .upsert(
                "interop_seq",
                &format!("doc-{i}"),
                &[("idx", loro::LoroValue::I64(i as i64))],
            )
            .expect("upsert");

        let deltas = engine.pending_deltas();
        let delta_bytes = deltas
            .iter()
            .find(|d| d.document_id == format!("doc-{i}"))
            .map(|d| d.delta_bytes.clone())
            .unwrap_or_else(|| deltas[0].delta_bytes.clone());

        let msg = DeltaPushMsg {
            collection: "interop_seq".into(),
            document_id: format!("doc-{i}"),
            delta: delta_bytes,
            peer_id: 1004,
            mutation_id: i,
            checksum: 0,
            device_valid_time_ms: None,
        };

        let frame = push_and_recv(&mut ws, &msg).await;
        assert_eq!(
            frame.msg_type,
            SyncMessageType::DeltaAck,
            "delta {i} must be acked, got {:?}",
            frame.msg_type
        );
    }
}

/// §7.3e — Ping/pong round-trip works after a successful handshake.
#[tokio::test]
async fn ping_pong_round_trip() {
    let _server = OriginServer::spawn();
    let mut ws = connect_and_handshake(_server.ws_url).await;

    let ping = PingPongMsg {
        timestamp_ms: 123_456_789,
        is_pong: false,
    };
    let bytes = SyncFrame::try_encode(SyncMessageType::PingPong, &ping)
        .expect("encode ping")
        .to_bytes();
    ws.send(Message::Binary(bytes.into()))
        .await
        .expect("send ping");

    let resp = tokio::time::timeout(Duration::from_secs(5), ws.next())
        .await
        .expect("timeout waiting for pong")
        .expect("stream closed")
        .expect("WebSocket error");

    let frame = SyncFrame::from_bytes(resp.into_data().as_ref()).expect("decode pong frame");
    assert_eq!(frame.msg_type, SyncMessageType::PingPong);

    let pong: PingPongMsg = frame.decode_body().expect("decode PingPongMsg");
    assert!(pong.is_pong, "response must have is_pong=true");
    assert_eq!(
        pong.timestamp_ms, 123_456_789,
        "pong must echo the ping timestamp"
    );
}

/// §7.3f — VectorClockSync message is processed without error.
#[tokio::test]
async fn vector_clock_sync_accepted() {
    let _server = OriginServer::spawn();
    let mut ws = connect_and_handshake(_server.ws_url).await;

    let clock = VectorClockSyncMsg {
        clocks: {
            let mut m = std::collections::HashMap::new();
            m.insert(format!("{:016x}", WIRE_FORMAT_VERSION as u64), 42u64);
            m
        },
        sender_id: 1001,
    };
    let bytes = SyncFrame::try_encode(SyncMessageType::VectorClockSync, &clock)
        .expect("encode VectorClockSync")
        .to_bytes();
    ws.send(Message::Binary(bytes.into()))
        .await
        .expect("send VectorClockSync");

    let result = tokio::time::timeout(Duration::from_secs(2), ws.next()).await;

    match result {
        Err(_) => {}
        Ok(Some(Ok(msg))) => {
            if let tokio_tungstenite::tungstenite::Message::Close(_) = msg {
                panic!("Origin closed session after VectorClockSync — unexpected")
            }
        }
        Ok(Some(Err(e))) => panic!("WebSocket error after VectorClockSync: {e}"),
        Ok(None) => panic!("stream ended unexpectedly after VectorClockSync"),
    }
}
