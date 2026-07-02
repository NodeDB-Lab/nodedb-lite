//! Control / latched single-shot messages: reactive token refresh, pending
//! resync requests, pending array acks, and the CRDT delta push (the original
//! sync flow). Drained once per tick before the per-engine queues.

use std::ops::ControlFlow;
use std::sync::Arc;

use futures::SinkExt;
use tokio::sync::Mutex;
use tokio_tungstenite::tungstenite::Message;

use nodedb_types::hlc::Hlc;
use nodedb_types::sync::wire::{
    CollectionSchemaSyncMsg, EngineKind, SyncFrame, SyncMessageType, stream_id_for,
};

use super::send::{encode_and_send, send_binary};
use crate::sync::client::SyncClient;
use crate::sync::collection_schema_builder::descriptor_from_meta;
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
        // Only attempt a refresh if the backoff interval has elapsed.
        if client.is_refresh_backoff_elapsed().await
            && let Some(refresh_msg) = client.initiate_token_refresh().await
        {
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

    for ack in client.drain_pending_array_acks().await {
        let Some(frame) = SyncFrame::try_encode(SyncMessageType::ArrayAck, &ack) else {
            tracing::warn!(array = %ack.array, "failed to encode ArrayAck frame; dropping");
            continue;
        };
        if let Err(e) = send_binary(sink, frame).await {
            tracing::warn!(array = %ack.array, error = %e, "ArrayAck send failed");
            return ControlFlow::Break(());
        }
        tracing::debug!(array = %ack.array, "sent ArrayAck to Origin");
    }

    ControlFlow::Continue(())
}

/// Announce collection schemas (`CollectionSchema`, opcode `0x13`) for every
/// collection with pending CRDT deltas that hasn't already been announced in
/// this session, so Origin materializes the collection before its data
/// arrives. Mirrors Origin's announce-before-shape-snapshot ordering in
/// `session_handler/announce.rs`.
pub(in crate::sync::transport) async fn push_collection_schemas<S>(
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

    let mut names: Vec<String> = pending.into_iter().map(|d| d.collection).collect();
    names.sort_unstable();
    names.dedup();

    for name in names {
        {
            let announced = client.announced_collections().lock().await;
            if announced.contains(&name) {
                continue;
            }
        }

        let Some(meta) = delegate.get_collection_meta(&name).await else {
            tracing::debug!(
                collection = %name,
                "no persisted metadata; skipping schema announce (implicit CRDT-only collection)"
            );
            continue;
        };
        let Some(descriptor) = descriptor_from_meta(&meta) else {
            // descriptor_from_meta already warned with the specific reason.
            continue;
        };

        let msg = CollectionSchemaSyncMsg {
            descriptor,
            creation_hlc: Hlc::ZERO,
        };
        let Some(frame) = SyncFrame::try_encode(SyncMessageType::CollectionSchema, &msg) else {
            tracing::error!(collection = %name, "failed to encode CollectionSchema frame; skipping");
            continue;
        };
        if let Err(e) = send_binary(sink, frame).await {
            tracing::warn!(collection = %name, error = %e, "CollectionSchema send failed");
            return ControlFlow::Break(());
        }

        client
            .announced_collections()
            .lock()
            .await
            .insert(name.clone());
        tracing::debug!(collection = %name, "announced CollectionSchema to Origin");
    }

    ControlFlow::Continue(())
}

/// Push pending CRDT deltas, respecting the flow control window.
pub(in crate::sync::transport) async fn push_crdt_deltas<S>(
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

    let mut msgs = client.build_delta_pushes(&pending).await;
    if msgs.is_empty() {
        return ControlFlow::Continue(()); // flow control window full — wait for ACKs
    }

    // Stamp each message with producer identity and a stable per-collection
    // stream seq. The seq is assigned once at first send and stored back on
    // the engine's pending delta so that reconnect re-sends reuse the same
    // seq — Origin deduplicates by seq rather than double-applying.
    let seq_by_mid: std::collections::HashMap<u64, u64> =
        pending.iter().map(|d| (d.mutation_id, d.seq)).collect();
    let producer_id = client.producer_id().await;
    let epoch = client.accepted_epoch().await;
    for msg in &mut msgs {
        let stream_id = stream_id_for(EngineKind::Crdt, &msg.collection);
        let seq = match seq_by_mid.get(&msg.mutation_id).copied() {
            Some(s) if s != 0 => s, // reuse stable seq (reconnect re-send)
            _ => {
                let s = delegate.next_stream_seq(stream_id).await;
                delegate.set_pending_delta_seq(msg.mutation_id, s).await; // persist back to engine
                s
            }
        };
        msg.producer_id = producer_id;
        msg.epoch = epoch;
        msg.seq = seq;
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
