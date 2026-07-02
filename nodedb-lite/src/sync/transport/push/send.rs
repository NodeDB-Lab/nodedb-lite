//! Shared send helpers used by every per-engine push module.
//!
//! Send-failure semantics: a transport write error always re-queues the
//! pending entry at the head of its outbound queue (callers handle that)
//! and the helper signals `ControlFlow::Break` so the surrounding loop can
//! tear the connection down. Encoding failures are non-recoverable per-entry
//! events — they log and skip without re-queueing (a malformed payload
//! would loop forever).

use std::ops::ControlFlow;

use futures::SinkExt;
use tokio::sync::Mutex;
use tokio_tungstenite::tungstenite::Message;

use nodedb_types::sync::wire::{SyncFrame, SyncMessageType};

/// Send a serialised frame over the sink, propagating any transport error.
pub(super) async fn send_binary<S>(sink: &Mutex<S>, frame: SyncFrame) -> Result<(), S::Error>
where
    S: SinkExt<Message> + Unpin,
{
    let mut guard = sink.lock().await;
    guard.send(Message::Binary(frame.to_bytes().into())).await
}

/// Encode an outbound message body and send it.
///
/// On encode failure the frame is dropped with an error log and the caller
/// continues. On send failure the caller is signalled to break out of the
/// push loop.
pub(super) async fn encode_and_send<S, T>(
    sink: &Mutex<S>,
    msg_type: SyncMessageType,
    body: &T,
    label: &'static str,
) -> ControlFlow<()>
where
    S: SinkExt<Message> + Unpin,
    S::Error: std::fmt::Display,
    T: zerompk::ToMessagePack,
{
    let Some(frame) = SyncFrame::try_encode(msg_type, body) else {
        tracing::error!(label, "failed to encode {label} frame; skipping");
        return ControlFlow::Continue(());
    };
    match send_binary(sink, frame).await {
        Ok(()) => ControlFlow::Continue(()),
        Err(e) => {
            tracing::warn!(label, error = %e, "{label} send failed");
            ControlFlow::Break(())
        }
    }
}
