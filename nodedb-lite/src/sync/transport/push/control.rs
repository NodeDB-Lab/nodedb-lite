//! Control / latched single-shot messages: reactive token refresh, pending
//! resync requests, pending array acks, and the CRDT delta push (the original
//! sync flow). Drained once per tick before the per-engine queues.

use std::ops::ControlFlow;
use std::sync::Arc;

use futures::SinkExt;
use tokio::sync::Mutex;
use tokio_tungstenite::tungstenite::Message;

use nodedb_types::sync::wire::{SyncFrame, SyncMessageType};

use super::send::{encode_and_send, send_binary};
use crate::sync::client::SyncClient;
use crate::sync::transport::delegate::SyncDelegate;

/// Drain control messages: token refresh (when paused for auth), resync
/// requests, and array acks. Returns `Break` if push must pause this tick
/// (auth pause) or the connection is lost.
pub(super) async fn push_control_messages<S>(
    client: &Arc<SyncClient>,
    sink: &Arc<Mutex<S>>,
) -> ControlFlow<()>
where
    S: SinkExt<Message> + Unpin,
    S::Error: std::fmt::Display,
{
    if client.is_push_paused_for_auth().await {
        if let Some(refresh_msg) = client.initiate_token_refresh().await {
            encode_and_send(
                sink,
                SyncMessageType::TokenRefresh,
                &refresh_msg,
                "TokenRefresh (reactive)",
            )
            .await?;
        }
        // Paused — emit nothing else this tick.
        return ControlFlow::Break(());
    }

    if let Some(resync) = client.take_pending_resync().await {
        encode_and_send(
            sink,
            SyncMessageType::ResyncRequest,
            &resync,
            "ResyncRequest",
        )
        .await?;
        tracing::info!(
            reason = ?resync.reason,
            from_mutation_id = resync.from_mutation_id,
            "sent ResyncRequest to Origin"
        );
    }

    if let Some(ack) = client.take_pending_array_ack().await
        && let Some(frame) = SyncFrame::try_encode(SyncMessageType::ArrayAck, &ack)
    {
        if let Err(e) = send_binary(sink, frame).await {
            tracing::warn!(array = %ack.array, error = %e, "ArrayAck send failed");
            return ControlFlow::Break(());
        }
        tracing::debug!(array = %ack.array, "sent ArrayAck to Origin");
    }

    ControlFlow::Continue(())
}

/// Push pending CRDT deltas, respecting the flow control window.
pub(super) async fn push_crdt_deltas<S>(
    client: &Arc<SyncClient>,
    delegate: &Arc<dyn SyncDelegate>,
    sink: &Arc<Mutex<S>>,
) -> ControlFlow<()>
where
    S: SinkExt<Message> + Unpin,
    S::Error: std::fmt::Display,
{
    let pending = delegate.pending_deltas();
    if pending.is_empty() {
        return ControlFlow::Continue(());
    }

    let pending_bytes: usize = pending.iter().map(|d| d.delta_bytes.len()).sum();
    client
        .update_pending_stats(pending.len(), pending_bytes)
        .await;

    let msgs = client.build_delta_pushes(&pending).await;
    if msgs.is_empty() {
        return ControlFlow::Continue(()); // flow control window full — wait for ACKs
    }

    let mutation_ids: Vec<u64> = msgs.iter().map(|m| m.mutation_id).collect();
    {
        let mut sink_guard = sink.lock().await;
        for msg in &msgs {
            let Some(frame) = SyncFrame::try_encode(SyncMessageType::DeltaPush, msg) else {
                tracing::error!("failed to encode delta push frame; dropping batch");
                return ControlFlow::Break(());
            };
            if let Err(e) = sink_guard
                .send(Message::Binary(frame.to_bytes().into()))
                .await
            {
                tracing::warn!(error = %e, "delta push send failed");
                return ControlFlow::Break(()); // connection lost
            }
        }
    }

    client.record_push(&mutation_ids).await;
    tracing::debug!(count = msgs.len(), "pushed deltas to Origin");
    ControlFlow::Continue(())
}
