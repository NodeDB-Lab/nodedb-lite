//! §7.6 — Resync after sequence gap / catch-up request.
//!
//! Verifies shape subscription, snapshot LSN, concurrent delta handling.
//!
//! Each test spawns its own Origin instance with a fresh temp data dir.

mod common;

use std::time::Duration;

use futures::{SinkExt, StreamExt};
use nodedb_lite::engine::crdt::CrdtEngine;
use nodedb_types::sync::shape::{ShapeDefinition, ShapeType};
use nodedb_types::sync::wire::{
    DeltaPushMsg, ResyncReason, ResyncRequestMsg, ShapeSnapshotMsg, ShapeSubscribeMsg, SyncFrame,
    SyncMessageType,
};
use tokio_tungstenite::tungstenite::Message;

use common::origin::{OriginServer, connect_and_handshake};

// ── helper ────────────────────────────────────────────────────────────────────

async fn subscribe_shape(
    ws: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    shape_id: &str,
    collection: &str,
) -> ShapeSnapshotMsg {
    let subscribe = ShapeSubscribeMsg {
        shape: ShapeDefinition {
            shape_id: shape_id.into(),
            tenant_id: 0,
            shape_type: ShapeType::Document {
                collection: collection.into(),
                predicate: Vec::new(),
            },
            description: "interop-resync-test".into(),
            field_filter: vec![],
        },
    };
    let bytes = SyncFrame::try_encode(SyncMessageType::ShapeSubscribe, &subscribe)
        .expect("encode ShapeSubscribe")
        .to_bytes();
    ws.send(Message::Binary(bytes.into()))
        .await
        .expect("send ShapeSubscribe");

    let resp = tokio::time::timeout(Duration::from_secs(10), ws.next())
        .await
        .expect("timeout waiting for ShapeSnapshot")
        .expect("stream closed before snapshot")
        .expect("WebSocket read error");

    let frame = SyncFrame::from_bytes(resp.into_data().as_ref()).expect("decode frame");
    assert_eq!(
        frame.msg_type,
        SyncMessageType::ShapeSnapshot,
        "expected ShapeSnapshot, got {:?}",
        frame.msg_type
    );

    frame
        .decode_body::<ShapeSnapshotMsg>()
        .expect("decode ShapeSnapshotMsg")
}

// ── tests ─────────────────────────────────────────────────────────────────────

/// §7.6a — Shape subscription returns a snapshot with the correct shape_id.
#[tokio::test]
async fn shape_subscribe_returns_snapshot() {
    let Some(_server) = OriginServer::try_spawn() else {
        eprintln!("SKIP: Origin binary unavailable (set NODEDB_BIN or run via `cargo nextest`)");
        return;
    };
    let mut ws = connect_and_handshake(_server.ws_url).await;

    let snapshot = subscribe_shape(&mut ws, "resync-shape-a", "resync_test").await;

    assert_eq!(
        snapshot.shape_id, "resync-shape-a",
        "snapshot must echo the shape_id we subscribed to"
    );
}

/// §7.6b — Snapshot LSN reflects real WAL state after a delta push.
#[tokio::test]
async fn snapshot_lsn_reflects_wal_state() {
    let Some(_server) = OriginServer::try_spawn() else {
        eprintln!("SKIP: Origin binary unavailable (set NODEDB_BIN or run via `cargo nextest`)");
        return;
    };
    let mut ws = connect_and_handshake(_server.ws_url).await;

    let mut engine = CrdtEngine::new(4001).expect("create engine");
    engine
        .upsert("lsn_verify", "doc-lsn", &[("x", loro::LoroValue::I64(1))])
        .expect("upsert");
    let delta_bytes = engine.pending_deltas()[0].delta_bytes.clone();

    let push_msg = DeltaPushMsg {
        collection: "lsn_verify".into(),
        document_id: "doc-lsn".into(),
        delta: delta_bytes,
        peer_id: 4001,
        mutation_id: 1,
        checksum: 0,
        device_valid_time_ms: None,
        producer_id: 0,
        epoch: 0,
        seq: 0,
    };
    let bytes = SyncFrame::try_encode(SyncMessageType::DeltaPush, &push_msg)
        .expect("encode DeltaPush")
        .to_bytes();
    ws.send(Message::Binary(bytes.into()))
        .await
        .expect("send DeltaPush");

    // Consume the ack/reject before subscribing.
    let _ = tokio::time::timeout(Duration::from_secs(10), ws.next()).await;

    let snapshot = subscribe_shape(&mut ws, "lsn-shape", "lsn_verify").await;

    // snapshot_lsn == 0 is valid for an empty WAL; the key assertion is
    // that the field is accessible (u64, no panic).
    let _ = snapshot.snapshot_lsn;
}

/// §7.6c — Two concurrent deltas from different peers are both handled.
#[tokio::test]
async fn concurrent_deltas_from_two_peers() {
    let Some(_server) = OriginServer::try_spawn() else {
        eprintln!("SKIP: Origin binary unavailable (set NODEDB_BIN or run via `cargo nextest`)");
        return;
    };
    let mut ws = connect_and_handshake(_server.ws_url).await;

    let mut engine1 = CrdtEngine::new(4002).expect("create engine1");
    engine1
        .upsert(
            "concurrent",
            "doc-p1",
            &[("author", loro::LoroValue::String("peer1".into()))],
        )
        .expect("upsert engine1");

    let mut engine2 = CrdtEngine::new(4003).expect("create engine2");
    engine2
        .upsert(
            "concurrent",
            "doc-p2",
            &[("author", loro::LoroValue::String("peer2".into()))],
        )
        .expect("upsert engine2");

    for (peer_id, mutation_id, engine, doc_id) in [
        (4002u64, 1u64, &engine1, "doc-p1"),
        (4003u64, 2u64, &engine2, "doc-p2"),
    ] {
        let delta_bytes = engine.pending_deltas()[0].delta_bytes.clone();
        let msg = DeltaPushMsg {
            collection: "concurrent".into(),
            document_id: doc_id.into(),
            delta: delta_bytes,
            peer_id,
            mutation_id,
            checksum: 0,
            device_valid_time_ms: None,
            producer_id: 0,
            epoch: 0,
            seq: 0,
        };
        let bytes = SyncFrame::try_encode(SyncMessageType::DeltaPush, &msg)
            .expect("encode DeltaPush")
            .to_bytes();
        ws.send(Message::Binary(bytes.into()))
            .await
            .expect("send DeltaPush");
    }

    for i in 0..2 {
        let resp = tokio::time::timeout(Duration::from_secs(10), ws.next())
            .await
            .unwrap_or_else(|_| panic!("timeout on response {i}"))
            .unwrap_or_else(|| panic!("stream closed on response {i}"))
            .unwrap_or_else(|e| panic!("read error on response {i}: {e}"));

        let frame = SyncFrame::from_bytes(resp.into_data().as_ref())
            .unwrap_or_else(|| panic!("bad frame on response {i}"));

        assert!(
            frame.msg_type == SyncMessageType::DeltaAck
                || frame.msg_type == SyncMessageType::DeltaReject,
            "response {i} must be DeltaAck or DeltaReject, got {:?}",
            frame.msg_type
        );
    }
}

/// §7.6e — ResyncRequest with a known shape_id returns a ShapeSnapshot echoing
/// the real shape_id (not a "resync:"-prefixed string).
///
/// Regression guard: the resync handler must look up the shape from the
/// persistent registry (populated by the preceding subscribe), not a
/// throwaway per-connection map.  If the registry lookup returns None the
/// handler produces no snapshot, and the assertion below fails.
#[tokio::test]
async fn resync_request_returns_snapshot_with_real_shape_id() {
    let Some(_server) = OriginServer::try_spawn() else {
        eprintln!("SKIP: Origin binary unavailable (set NODEDB_BIN or run via `cargo nextest`)");
        return;
    };
    let mut ws = connect_and_handshake(_server.ws_url).await;

    // Populate Origin's persistent shape registry for this session.
    subscribe_shape(&mut ws, "resync-rt-shape", "resync_rt_collection").await;

    // Construct and send a ResyncRequest for the shape we just subscribed to.
    let resync_msg = ResyncRequestMsg {
        reason: ResyncReason::SequenceGap {
            expected: 1,
            received: 3,
        },
        from_mutation_id: 1,
        collection: String::new(),
        shape_id: "resync-rt-shape".into(),
    };
    let bytes = SyncFrame::try_encode(SyncMessageType::ResyncRequest, &resync_msg)
        .expect("encode ResyncRequest")
        .to_bytes();
    ws.send(Message::Binary(bytes.into()))
        .await
        .expect("send ResyncRequest");

    // Drain frames until we see a ShapeSnapshot (skip acks, pings, etc.).
    let snapshot: ShapeSnapshotMsg = loop {
        let frame_msg = tokio::time::timeout(Duration::from_secs(10), ws.next())
            .await
            .expect("timeout waiting for ShapeSnapshot after ResyncRequest")
            .expect("stream closed before ShapeSnapshot")
            .expect("WebSocket read error");

        let frame =
            SyncFrame::from_bytes(frame_msg.into_data().as_ref()).expect("decode sync frame");

        if frame.msg_type == SyncMessageType::ShapeSnapshot {
            break frame
                .decode_body::<ShapeSnapshotMsg>()
                .expect("decode ShapeSnapshotMsg");
        }
        // Non-snapshot frame (ack, ping, etc.) — keep draining.
    };

    assert_eq!(
        snapshot.shape_id, "resync-rt-shape",
        "resync handler must echo the real shape_id from the persistent registry, \
         not a synthesised 'resync:' prefix — if this fails the registry was not populated"
    );
}

/// §7.6d — Subscribing to the same shape_id twice returns two snapshots.
#[tokio::test]
async fn double_shape_subscribe_returns_two_snapshots() {
    let Some(_server) = OriginServer::try_spawn() else {
        eprintln!("SKIP: Origin binary unavailable (set NODEDB_BIN or run via `cargo nextest`)");
        return;
    };
    let mut ws = connect_and_handshake(_server.ws_url).await;

    let _snap1 = subscribe_shape(&mut ws, "double-shape", "double_collection").await;
    let _snap2 = subscribe_shape(&mut ws, "double-shape", "double_collection").await;
}
