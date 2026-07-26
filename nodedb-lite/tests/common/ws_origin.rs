//! Mock Origin WebSocket server, for driving the real `run_sync_loop`.
//!
//! The sync client connects outbound, so exercising it through its public
//! entry point requires something on the other end of the socket. This binds
//! an ephemeral loopback port, speaks just enough of the protocol to get the
//! client into `Connected` (accept the handshake, answer with a
//! `HandshakeAck`), and then hands the raw socket to the test to send inbound
//! frames and read whatever the client pushes back.

use std::time::Duration;

use futures::{SinkExt, StreamExt};
use nodedb_types::sync::wire::{HandshakeAckMsg, SyncFrame, SyncMessageType};
use nodedb_types::wire_version::WIRE_FORMAT_VERSION;
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message;

/// The accepted server-side socket, after the handshake has been answered.
pub type OriginSocket = WebSocketStream<TcpStream>;

/// Upper bound on any wait for client-side progress. Every helper that blocks
/// on the client is capped by it so a regression that stalls the transport
/// fails the test loudly instead of hanging the suite.
const WAIT_TIMEOUT: Duration = Duration::from_secs(5);

/// A listening mock Origin. Kept alive across reconnects: `run_sync_loop`
/// retries after a dropped socket, so the listener must outlive one session.
pub struct MockOrigin {
    listener: TcpListener,
    url: String,
}

impl MockOrigin {
    /// Bind an ephemeral loopback port.
    pub async fn bind() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind mock origin");
        let port = listener.local_addr().expect("local addr").port();
        Self {
            listener,
            url: format!("ws://127.0.0.1:{port}/sync"),
        }
    }

    /// URL to hand to `SyncConfig::new`.
    pub fn url(&self) -> &str {
        &self.url
    }

    /// Accept one client and answer its handshake, leaving it `Connected`.
    ///
    /// The returned socket is positioned at the first post-handshake frame,
    /// so a test can immediately push inbound frames or read outbound ones.
    pub async fn accept_handshaked(&self) -> OriginSocket {
        let (stream, _) = tokio::time::timeout(WAIT_TIMEOUT, self.listener.accept())
            .await
            .expect("timed out waiting for the client to connect")
            .expect("accept connection");
        let mut ws = tokio_tungstenite::accept_async(stream)
            .await
            .expect("websocket upgrade");

        let frame = next_frame(&mut ws)
            .await
            .expect("client closed before sending its handshake");
        assert_eq!(
            frame.msg_type,
            SyncMessageType::Handshake,
            "first client frame must be the handshake"
        );

        let ack = HandshakeAckMsg {
            success: true,
            session_id: "mock-session".to_string(),
            server_clock: std::collections::HashMap::new(),
            error: None,
            fork_detected: false,
            server_wire_version: WIRE_FORMAT_VERSION,
            producer_id: 1,
            accepted_epoch: 1,
        };
        send_frame(&mut ws, SyncMessageType::HandshakeAck, &ack).await;

        ws
    }
}

/// Send one encoded `SyncFrame` to the client.
pub async fn send_frame<T: zerompk::ToMessagePack>(
    ws: &mut OriginSocket,
    msg_type: SyncMessageType,
    msg: &T,
) {
    let frame = SyncFrame::try_encode(msg_type, msg).expect("encode frame");
    ws.send(Message::Binary(frame.to_bytes().into()))
        .await
        .expect("send frame to client");
}

/// Read the next `SyncFrame` the client sent, or `None` if it closed.
///
/// Non-binary messages (ping/pong/text) carry no `SyncFrame` and are skipped.
pub async fn next_frame(ws: &mut OriginSocket) -> Option<SyncFrame> {
    loop {
        let msg = tokio::time::timeout(WAIT_TIMEOUT, ws.next())
            .await
            .expect("timed out waiting for a frame from the client")?
            .expect("read from client");
        match msg {
            Message::Binary(bytes) => {
                return Some(SyncFrame::from_bytes(&bytes).expect("client sent a malformed frame"));
            }
            Message::Close(_) => return None,
            _ => continue,
        }
    }
}

/// Collect every frame the client sends during `window`.
///
/// Used for negative assertions about outbound traffic ("this is never sent
/// again"), which need a bounded observation window rather than a wait for
/// something to appear.
pub async fn collect_frames_for(ws: &mut OriginSocket, window: Duration) -> Vec<SyncFrame> {
    let mut frames = Vec::new();
    let deadline = tokio::time::Instant::now() + window;

    while let Some(remaining) = deadline.checked_duration_since(tokio::time::Instant::now()) {
        match tokio::time::timeout(remaining, ws.next()).await {
            Err(_) => break,
            Ok(None) => break,
            Ok(Some(msg)) => match msg.expect("read from client") {
                Message::Binary(bytes) => {
                    let frame =
                        SyncFrame::from_bytes(&bytes).expect("client sent a malformed frame");
                    frames.push(frame);
                }
                Message::Close(_) => break,
                _ => continue,
            },
        }
    }

    frames
}

/// Poll `cond` until it holds, or panic once `WAIT_TIMEOUT` elapses.
///
/// Delegate callbacks land on the client's own task, so tests observe them by
/// polling rather than by awaiting a signal the transport does not emit.
pub async fn await_until<F, Fut>(mut cond: F, what: &str)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let poll = async {
        while !cond().await {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    };
    if tokio::time::timeout(WAIT_TIMEOUT, poll).await.is_err() {
        panic!("timed out waiting for {what}");
    }
}
