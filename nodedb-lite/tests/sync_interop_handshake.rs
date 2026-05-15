//! §7.2 — Handshake interoperability tests.
//!
//! Verifies that a real `nodedb-lite` sync client successfully completes the
//! WebSocket handshake with a real Origin server in trust mode and current wire
//! version, and that the server correctly rejects stale / invalid handshakes.
//!
//! Requires the Origin binary to be present. See `tests/common/origin.rs`.
//!
//! Each test spawns its own Origin instance (with a fresh temp data dir) and
//! kills it on exit. The `heavy` nextest group serializes these so port 9090
//! is never contested.

mod common;

use std::time::Duration;

use futures::StreamExt;
use nodedb_types::sync::wire::{HandshakeAckMsg, HandshakeMsg, SyncFrame, SyncMessageType};
use nodedb_types::wire_version::WIRE_FORMAT_VERSION;
use tokio_tungstenite::tungstenite::Message;

use common::origin::{OriginServer, connect_and_handshake};

// ── helpers ──────────────────────────────────────────────────────────────────

async fn raw_connect(
    ws_url: &str,
) -> tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>> {
    tokio_tungstenite::connect_async(ws_url)
        .await
        .unwrap_or_else(|e| panic!("connect: {e}"))
        .0
}

async fn send_handshake(
    ws: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    msg: &HandshakeMsg,
) {
    use futures::SinkExt;
    let bytes = SyncFrame::try_encode(SyncMessageType::Handshake, msg)
        .expect("encode handshake")
        .to_bytes();
    ws.send(Message::Binary(bytes.into()))
        .await
        .expect("send handshake");
}

async fn recv_handshake_ack(
    ws: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
) -> HandshakeAckMsg {
    let resp = tokio::time::timeout(Duration::from_secs(10), ws.next())
        .await
        .expect("timeout waiting for HandshakeAck")
        .expect("stream closed before ack")
        .expect("WebSocket error on read");
    let frame = SyncFrame::from_bytes(resp.into_data().as_ref()).expect("decode frame");
    assert_eq!(frame.msg_type, SyncMessageType::HandshakeAck);
    frame
        .decode_body::<HandshakeAckMsg>()
        .expect("decode HandshakeAckMsg")
}

// ── tests ─────────────────────────────────────────────────────────────────────

/// §7.2a — Trust-mode handshake with current wire version succeeds.
#[tokio::test]
async fn handshake_trust_mode_succeeds() {
    let _server = OriginServer::spawn();
    let mut ws = raw_connect(_server.ws_url).await;

    let hs = HandshakeMsg {
        jwt_token: String::new(),
        vector_clock: std::collections::HashMap::new(),
        subscribed_shapes: Vec::new(),
        client_version: "interop-handshake-test".into(),
        lite_id: String::new(),
        epoch: 0,
        wire_version: WIRE_FORMAT_VERSION,
    };

    send_handshake(&mut ws, &hs).await;
    let ack = recv_handshake_ack(&mut ws).await;

    assert!(
        ack.success,
        "trust-mode handshake should succeed, got error: {:?}",
        ack.error
    );
    assert!(
        !ack.session_id.is_empty(),
        "session_id must be non-empty on success"
    );
    assert!(
        !ack.fork_detected,
        "no fork should be detected for a fresh client"
    );
}

/// §7.2b — Server returns wire version in ack so the client can detect mismatches.
#[tokio::test]
async fn handshake_ack_contains_server_wire_version() {
    let _server = OriginServer::spawn();
    let mut ws = raw_connect(_server.ws_url).await;

    let hs = HandshakeMsg {
        jwt_token: String::new(),
        vector_clock: std::collections::HashMap::new(),
        subscribed_shapes: Vec::new(),
        client_version: "version-probe".into(),
        lite_id: String::new(),
        epoch: 0,
        wire_version: WIRE_FORMAT_VERSION,
    };

    send_handshake(&mut ws, &hs).await;
    let ack = recv_handshake_ack(&mut ws).await;

    assert!(ack.success, "handshake should succeed");
    assert!(
        ack.server_wire_version >= 1,
        "server_wire_version must be >= 1, got {}",
        ack.server_wire_version
    );
}

/// §7.2c — Stale wire version (0) is rejected with a clear error.
#[tokio::test]
async fn handshake_rejects_wire_version_zero() {
    let _server = OriginServer::spawn();
    let mut ws = raw_connect(_server.ws_url).await;

    let hs = HandshakeMsg {
        jwt_token: String::new(),
        vector_clock: std::collections::HashMap::new(),
        subscribed_shapes: Vec::new(),
        client_version: "old-client".into(),
        lite_id: String::new(),
        epoch: 0,
        wire_version: 0,
    };

    send_handshake(&mut ws, &hs).await;
    let ack = recv_handshake_ack(&mut ws).await;

    assert!(
        !ack.success,
        "wire_version=0 must be rejected, got success=true"
    );
    let error = ack
        .error
        .expect("error message must be present on rejection");
    assert!(
        error.contains("wire version") || error.contains("incompatible"),
        "error should mention wire version incompatibility, got: {error}"
    );
}

/// §7.2d — The high-level `connect_and_handshake` helper completes end-to-end.
#[tokio::test]
async fn helper_connect_and_handshake_works() {
    let _server = OriginServer::spawn();
    let _ws = connect_and_handshake(_server.ws_url).await;
    // Success = no panic.
}

/// §7.2e — Multiple sequential connections are all accepted (no session leak).
#[tokio::test]
async fn multiple_sequential_handshakes_all_succeed() {
    let _server = OriginServer::spawn();

    for i in 0..5 {
        let mut ws = raw_connect(_server.ws_url).await;
        let hs = HandshakeMsg {
            jwt_token: String::new(),
            vector_clock: std::collections::HashMap::new(),
            subscribed_shapes: Vec::new(),
            client_version: format!("seq-client-{i}"),
            lite_id: String::new(),
            epoch: 0,
            wire_version: WIRE_FORMAT_VERSION,
        };
        send_handshake(&mut ws, &hs).await;
        let ack = recv_handshake_ack(&mut ws).await;
        assert!(
            ack.success,
            "connection {i} should succeed, got error: {:?}",
            ack.error
        );
    }
}
