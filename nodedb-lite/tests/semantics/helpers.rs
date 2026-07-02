//! Shared helpers for sync_interop_semantics tests.

use std::collections::HashMap;
use std::time::Duration;

use futures::{SinkExt, StreamExt};
use nodedb_types::sync::wire::{HandshakeAckMsg, HandshakeMsg, SyncFrame, SyncMessageType};
use nodedb_types::wire_version::WIRE_FORMAT_VERSION;
use tokio_tungstenite::tungstenite::Message;

pub type Ws =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

pub async fn raw_connect(ws_url: &str) -> Ws {
    tokio_tungstenite::connect_async(ws_url)
        .await
        .unwrap_or_else(|e| panic!("connect to {ws_url}: {e}"))
        .0
}

pub async fn send_hs(ws: &mut Ws, msg: &HandshakeMsg) {
    let bytes = SyncFrame::try_encode(SyncMessageType::Handshake, msg)
        .expect("encode handshake frame")
        .to_bytes();
    ws.send(Message::Binary(bytes.into()))
        .await
        .expect("send handshake frame");
}

pub async fn recv_ack(ws: &mut Ws) -> HandshakeAckMsg {
    let raw = tokio::time::timeout(Duration::from_secs(10), ws.next())
        .await
        .expect("timeout waiting for HandshakeAck")
        .expect("stream closed before ack")
        .expect("WebSocket error reading HandshakeAck");
    let frame =
        SyncFrame::from_bytes(raw.into_data().as_ref()).expect("decode SyncFrame for HandshakeAck");
    assert_eq!(
        frame.msg_type,
        SyncMessageType::HandshakeAck,
        "expected HandshakeAck, got {:?}",
        frame.msg_type
    );
    frame
        .decode_body::<HandshakeAckMsg>()
        .expect("decode HandshakeAckMsg body")
}

/// Build a minimal valid handshake with trust mode (empty JWT).
pub fn minimal_hs() -> HandshakeMsg {
    HandshakeMsg {
        jwt_token: String::new(),
        vector_clock: HashMap::new(),
        subscribed_shapes: Vec::new(),
        client_version: "semantics-test".into(),
        lite_id: String::new(),
        epoch: 0,
        wire_version: WIRE_FORMAT_VERSION,
    }
}
