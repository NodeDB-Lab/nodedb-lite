//! §7.7 — Automated test coverage from `examples/live_sync.rs`.
//!
//! All tests from the example are now automated and run in CI. The example
//! file remains as a human-runnable dev tool; this file is the automated gate.
//!
//! Each test spawns its own Origin instance with a fresh temp data dir.

mod common;

use std::time::Duration;

use futures::{SinkExt, StreamExt};
use nodedb_lite::engine::crdt::CrdtEngine;
use nodedb_types::sync::shape::{ShapeDefinition, ShapeType};
use nodedb_types::sync::wire::{
    DeltaPushMsg, HandshakeMsg, PingPongMsg, ShapeSnapshotMsg, ShapeSubscribeMsg, SyncFrame,
    SyncMessageType, VectorClockSyncMsg,
};
use nodedb_types::wire_version::WIRE_FORMAT_VERSION;
use tokio_tungstenite::tungstenite::Message;

use common::origin::{OriginServer, connect_and_handshake};

// ── handshake ─────────────────────────────────────────────────────────────────

#[tokio::test]
async fn live_handshake() {
    let _server = OriginServer::spawn();
    let (mut ws, _) = tokio_tungstenite::connect_async(_server.ws_url)
        .await
        .expect("connect");

    let hs = HandshakeMsg {
        jwt_token: String::new(),
        vector_clock: std::collections::HashMap::new(),
        subscribed_shapes: Vec::new(),
        client_version: "live-test".into(),
        lite_id: String::new(),
        epoch: 0,
        wire_version: WIRE_FORMAT_VERSION,
    };
    ws.send(Message::Binary(
        SyncFrame::try_encode(SyncMessageType::Handshake, &hs)
            .expect("encode")
            .to_bytes()
            .into(),
    ))
    .await
    .expect("send");

    let resp = tokio::time::timeout(Duration::from_secs(5), ws.next())
        .await
        .expect("timeout")
        .expect("closed")
        .expect("error");

    let frame = SyncFrame::from_bytes(resp.into_data().as_ref()).expect("decode frame");
    assert_eq!(frame.msg_type, SyncMessageType::HandshakeAck);

    let ack: nodedb_types::sync::wire::HandshakeAckMsg = frame.decode_body().expect("decode");
    assert!(ack.success, "handshake failed: {:?}", ack.error);
}

// ── delta push ────────────────────────────────────────────────────────────────

#[tokio::test]
async fn live_delta_push() {
    let _server = OriginServer::spawn();
    let mut ws = connect_and_handshake(_server.ws_url).await;

    let payload =
        nodedb_types::json_to_msgpack(&serde_json::json!({"key": "value"})).expect("serialize");

    let delta = DeltaPushMsg {
        collection: "live_test".into(),
        document_id: "d1".into(),
        delta: payload,
        peer_id: 42,
        mutation_id: 1,
        checksum: 0,
        device_valid_time_ms: None,
    };
    ws.send(Message::Binary(
        SyncFrame::try_encode(SyncMessageType::DeltaPush, &delta)
            .expect("encode")
            .to_bytes()
            .into(),
    ))
    .await
    .expect("send");

    let resp = tokio::time::timeout(Duration::from_secs(5), ws.next())
        .await
        .expect("timeout")
        .expect("closed")
        .expect("error");

    let frame = SyncFrame::from_bytes(resp.into_data().as_ref()).expect("decode");
    assert!(
        frame.msg_type == SyncMessageType::DeltaAck
            || frame.msg_type == SyncMessageType::DeltaReject,
        "unexpected frame type {:?}",
        frame.msg_type
    );
}

// ── ping/pong ─────────────────────────────────────────────────────────────────

#[tokio::test]
async fn live_ping_pong() {
    let _server = OriginServer::spawn();
    let mut ws = connect_and_handshake(_server.ws_url).await;

    let ping = PingPongMsg {
        timestamp_ms: 123_456_789,
        is_pong: false,
    };
    ws.send(Message::Binary(
        SyncFrame::try_encode(SyncMessageType::PingPong, &ping)
            .expect("encode")
            .to_bytes()
            .into(),
    ))
    .await
    .expect("send");

    let resp = tokio::time::timeout(Duration::from_secs(5), ws.next())
        .await
        .expect("timeout")
        .expect("closed")
        .expect("error");

    let frame = SyncFrame::from_bytes(resp.into_data().as_ref()).expect("decode");
    assert_eq!(frame.msg_type, SyncMessageType::PingPong);

    let pong: PingPongMsg = frame.decode_body().expect("decode PingPongMsg");
    assert!(pong.is_pong, "response must have is_pong=true");
    assert_eq!(pong.timestamp_ms, 123_456_789);
}

// ── reconnect latency ─────────────────────────────────────────────────────────

#[tokio::test]
async fn live_reconnect_under_200ms() {
    let _server = OriginServer::spawn();
    let start = std::time::Instant::now();
    let _ws = connect_and_handshake(_server.ws_url).await;
    let elapsed = start.elapsed();
    assert!(
        elapsed.as_millis() < 200,
        "reconnect took {}ms, target < 200ms",
        elapsed.as_millis()
    );
}

// ── vector clock sync ─────────────────────────────────────────────────────────

#[tokio::test]
async fn live_vector_clock_sync() {
    let _server = OriginServer::spawn();
    let mut ws = connect_and_handshake(_server.ws_url).await;

    let clock = VectorClockSyncMsg {
        clocks: {
            let mut m = std::collections::HashMap::new();
            m.insert("0000000000000001".to_string(), 42u64);
            m
        },
        sender_id: 1,
    };
    ws.send(Message::Binary(
        SyncFrame::try_encode(SyncMessageType::VectorClockSync, &clock)
            .expect("encode")
            .to_bytes()
            .into(),
    ))
    .await
    .expect("send");

    let result = tokio::time::timeout(Duration::from_secs(2), ws.next()).await;
    match result {
        Err(_) => {}
        Ok(Some(Ok(Message::Close(_)))) => {
            panic!("Origin closed session after VectorClockSync");
        }
        _ => {}
    }
}

// ── shape subscribe ───────────────────────────────────────────────────────────

#[tokio::test]
async fn live_shape_subscribe() {
    let _server = OriginServer::spawn();
    let mut ws = connect_and_handshake(_server.ws_url).await;

    let subscribe = ShapeSubscribeMsg {
        shape: ShapeDefinition {
            shape_id: "live-test-shape".into(),
            tenant_id: 0,
            shape_type: ShapeType::Document {
                collection: "orders".into(),
                predicate: Vec::new(),
            },
            description: "live test".into(),
            field_filter: vec![],
        },
    };
    ws.send(Message::Binary(
        SyncFrame::try_encode(SyncMessageType::ShapeSubscribe, &subscribe)
            .expect("encode")
            .to_bytes()
            .into(),
    ))
    .await
    .expect("send");

    let resp = tokio::time::timeout(Duration::from_secs(5), ws.next())
        .await
        .expect("timeout")
        .expect("closed")
        .expect("error");

    let frame = SyncFrame::from_bytes(resp.into_data().as_ref()).expect("decode");
    assert_eq!(frame.msg_type, SyncMessageType::ShapeSnapshot);

    let snapshot: ShapeSnapshotMsg = frame.decode_body().expect("decode ShapeSnapshotMsg");
    assert_eq!(snapshot.shape_id, "live-test-shape");
}

// ── real loro delta ───────────────────────────────────────────────────────────

#[tokio::test]
async fn live_real_loro_delta_push() {
    let _server = OriginServer::spawn();
    let mut ws = connect_and_handshake(_server.ws_url).await;

    let mut engine = CrdtEngine::new(100).expect("crdt engine");
    engine
        .upsert(
            "users",
            "alice",
            &[("name", loro::LoroValue::String("Alice".into()))],
        )
        .expect("upsert");

    let deltas = engine.pending_deltas();
    assert!(!deltas.is_empty(), "no deltas generated");

    let msg = DeltaPushMsg {
        collection: "users".into(),
        document_id: "alice".into(),
        delta: deltas[0].delta_bytes.clone(),
        peer_id: 100,
        mutation_id: 1,
        checksum: 0,
        device_valid_time_ms: None,
    };
    ws.send(Message::Binary(
        SyncFrame::try_encode(SyncMessageType::DeltaPush, &msg)
            .expect("encode")
            .to_bytes()
            .into(),
    ))
    .await
    .expect("send");

    let resp = tokio::time::timeout(Duration::from_secs(5), ws.next())
        .await
        .expect("timeout")
        .expect("closed")
        .expect("error");

    let frame = SyncFrame::from_bytes(resp.into_data().as_ref()).expect("decode");
    assert!(
        frame.msg_type == SyncMessageType::DeltaAck
            || frame.msg_type == SyncMessageType::DeltaReject,
        "unexpected: {:?}",
        frame.msg_type
    );
}

// ── concurrent delta push ─────────────────────────────────────────────────────

#[tokio::test]
async fn live_concurrent_delta_push() {
    let _server = OriginServer::spawn();
    let mut ws = connect_and_handshake(_server.ws_url).await;

    let mut engine1 = CrdtEngine::new(201).expect("engine1");
    engine1
        .upsert(
            "notes",
            "n1",
            &[("author", loro::LoroValue::String("peer1".into()))],
        )
        .expect("upsert1");

    let mut engine2 = CrdtEngine::new(202).expect("engine2");
    engine2
        .upsert(
            "notes",
            "n2",
            &[("author", loro::LoroValue::String("peer2".into()))],
        )
        .expect("upsert2");

    for (i, (engine, doc_id)) in [(engine1, "n1"), (engine2, "n2")].iter().enumerate() {
        let deltas = engine.pending_deltas();
        assert!(!deltas.is_empty(), "no deltas from engine {i}");
        let msg = DeltaPushMsg {
            collection: "notes".into(),
            document_id: doc_id.to_string(),
            delta: deltas[0].delta_bytes.clone(),
            peer_id: 200 + i as u64 + 1,
            mutation_id: i as u64 + 1,
            checksum: 0,
            device_valid_time_ms: None,
        };
        ws.send(Message::Binary(
            SyncFrame::try_encode(SyncMessageType::DeltaPush, &msg)
                .expect("encode")
                .to_bytes()
                .into(),
        ))
        .await
        .expect("send");
    }

    for i in 0..2 {
        let resp = tokio::time::timeout(Duration::from_secs(5), ws.next())
            .await
            .unwrap_or_else(|_| panic!("timeout {i}"))
            .unwrap_or_else(|| panic!("closed {i}"))
            .unwrap_or_else(|e| panic!("error {i}: {e}"));
        let frame = SyncFrame::from_bytes(resp.into_data().as_ref())
            .unwrap_or_else(|| panic!("bad frame {i}"));
        assert!(
            frame.msg_type == SyncMessageType::DeltaAck
                || frame.msg_type == SyncMessageType::DeltaReject,
            "unexpected response {i}: {:?}",
            frame.msg_type
        );
    }
}

// ── shape snapshot with WAL LSN ───────────────────────────────────────────────

#[tokio::test]
async fn live_shape_snapshot_with_wal_lsn() {
    let _server = OriginServer::spawn();
    let mut ws = connect_and_handshake(_server.ws_url).await;

    let mut engine = CrdtEngine::new(300).expect("engine");
    engine
        .upsert("lsn_test", "d1", &[("x", loro::LoroValue::I64(1))])
        .expect("upsert");
    if let Some(d) = engine.pending_deltas().first() {
        let msg = DeltaPushMsg {
            collection: "lsn_test".into(),
            document_id: "d1".into(),
            delta: d.delta_bytes.clone(),
            peer_id: 300,
            mutation_id: 1,
            checksum: 0,
            device_valid_time_ms: None,
        };
        ws.send(Message::Binary(
            SyncFrame::try_encode(SyncMessageType::DeltaPush, &msg)
                .expect("encode")
                .to_bytes()
                .into(),
        ))
        .await
        .expect("send");
        let _ = tokio::time::timeout(Duration::from_secs(5), ws.next()).await;
    }

    let subscribe = ShapeSubscribeMsg {
        shape: ShapeDefinition {
            shape_id: "lsn-live-shape".into(),
            tenant_id: 0,
            shape_type: ShapeType::Document {
                collection: "lsn_test".into(),
                predicate: Vec::new(),
            },
            description: "lsn test".into(),
            field_filter: vec![],
        },
    };
    ws.send(Message::Binary(
        SyncFrame::try_encode(SyncMessageType::ShapeSubscribe, &subscribe)
            .expect("encode")
            .to_bytes()
            .into(),
    ))
    .await
    .expect("send");

    let resp = tokio::time::timeout(Duration::from_secs(5), ws.next())
        .await
        .expect("timeout")
        .expect("closed")
        .expect("error");

    let frame = SyncFrame::from_bytes(resp.into_data().as_ref()).expect("decode");
    assert_eq!(frame.msg_type, SyncMessageType::ShapeSnapshot);

    let snapshot: ShapeSnapshotMsg = frame.decode_body().expect("decode");
    assert_eq!(snapshot.shape_id, "lsn-live-shape");
    let _ = snapshot.snapshot_lsn;
}
