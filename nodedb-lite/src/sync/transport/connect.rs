//! WebSocket connect + handshake. One attempt per `connect_and_run` call;
//! retries are handled by the outer `run_sync_loop` with exponential backoff.

use std::sync::Arc;
use std::time::Duration;

use futures::{SinkExt, StreamExt};
use tokio::sync::Mutex;
use tokio_tungstenite::tungstenite::Message;

use nodedb_types::sync::wire::{SyncFrame, SyncMessageType};

use super::delegate::SyncDelegate;
use super::dispatch::receive_loop;
use super::push::{delta_push_loop, ping_loop};
use crate::error::LiteError;
use crate::sync::client::{SyncClient, SyncState};

/// Single connection attempt: connect → handshake → message loop.
///
/// Returns `Ok(())` on a clean server-initiated close and `Err` for any
/// transport, handshake, or read error. Background push and ping tasks are
/// always cancelled before this function returns.
pub(super) async fn connect_and_run(
    client: &Arc<SyncClient>,
    delegate: &Arc<dyn SyncDelegate>,
) -> Result<(), LiteError> {
    // Reset state for a fresh connection.
    client.reset_sequence_tracking().await;
    client.reset_flow_control().await;

    // ── Connect ──
    let (ws_stream, _response) = tokio_tungstenite::connect_async(&client.config().url)
        .await
        .map_err(|e| LiteError::Sync {
            detail: format!("WebSocket connect failed: {e}"),
        })?;

    let (mut sink, mut stream) = ws_stream.split();

    // ── Handshake ──
    let handshake = client.build_handshake().await;
    let frame = SyncFrame::try_encode(SyncMessageType::Handshake, &handshake).ok_or_else(|| {
        LiteError::Sync {
            detail: "failed to encode handshake frame".to_string(),
        }
    })?;
    sink.send(Message::Binary(frame.to_bytes().into()))
        .await
        .map_err(|e| LiteError::Sync {
            detail: format!("handshake send failed: {e}"),
        })?;

    let ack_msg = tokio::time::timeout(Duration::from_secs(10), stream.next())
        .await
        .map_err(|_| LiteError::Sync {
            detail: "handshake timeout".to_string(),
        })?
        .ok_or_else(|| LiteError::Sync {
            detail: "connection closed before handshake ack".to_string(),
        })?
        .map_err(|e| LiteError::Sync {
            detail: format!("handshake read error: {e}"),
        })?;

    let ack_bytes = match &ack_msg {
        Message::Binary(b) => b.as_ref(),
        _ => {
            return Err(LiteError::Sync {
                detail: "expected binary handshake ack".to_string(),
            });
        }
    };

    let ack_frame = SyncFrame::from_bytes(ack_bytes).ok_or_else(|| LiteError::Sync {
        detail: "invalid handshake ack frame".to_string(),
    })?;

    if ack_frame.msg_type != SyncMessageType::HandshakeAck {
        return Err(LiteError::Sync {
            detail: format!("expected HandshakeAck, got {:?}", ack_frame.msg_type),
        });
    }

    let ack: nodedb_types::sync::wire::HandshakeAckMsg =
        ack_frame.decode_body().ok_or_else(|| LiteError::Sync {
            detail: "failed to decode HandshakeAck".to_string(),
        })?;

    if !client.handle_handshake_ack(&ack).await {
        return Err(LiteError::Sync {
            detail: format!("handshake rejected: {}", ack.error.unwrap_or_default()),
        });
    }

    // ── Message loop ──
    let sink = Arc::new(Mutex::new(sink));

    let push_sink = Arc::clone(&sink);
    let push_client = Arc::clone(client);
    let push_delegate = Arc::clone(delegate);
    let push_handle = tokio::spawn(async move {
        delta_push_loop(&push_client, &push_delegate, &push_sink).await;
    });

    let ping_sink = Arc::clone(&sink);
    let ping_client = Arc::clone(client);
    let ping_handle = tokio::spawn(async move {
        ping_loop(&ping_client, &ping_sink).await;
    });

    let recv_result = receive_loop(client, delegate, &mut stream).await;

    push_handle.abort();
    ping_handle.abort();

    client.set_state(SyncState::Disconnected).await;
    recv_result
}
