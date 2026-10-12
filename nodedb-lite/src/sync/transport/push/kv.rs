//! KV write push.
//!
//! Sends each pending KV write as a `KvPushMsg`, in queue order. A write
//! gets its stream seq on first send, and the seq is persisted before the
//! frame goes out, so a re-send after a reconnect carries the same seq and
//! Origin applies it once.

use std::ops::ControlFlow;
use std::sync::Arc;

use futures::SinkExt;
use tokio::sync::Mutex;
use tokio_tungstenite::tungstenite::Message;

use nodedb_types::sync::wire::{EngineKind, KvPushMsg, SyncFrame, SyncMessageType, stream_id_for};

use super::send::send_binary;
use crate::sync::client::SyncClient;
use crate::sync::delegate::SyncDelegate;
use crate::sync::outbound::kv::kv_batch_id;

pub(super) async fn push<S>(
    client: &Arc<SyncClient>,
    delegate: &Arc<dyn SyncDelegate>,
    sink: &Arc<Mutex<S>>,
) -> ControlFlow<()>
where
    S: SinkExt<Message> + Unpin,
    S::Error: std::fmt::Display,
{
    let pending = match delegate.pending_kv_writes().await {
        Ok(pending) => pending,
        Err(e) => {
            tracing::error!(
                error = %e,
                "KvPush: reading the KV outbound queue failed; KV writes wait for the next tick"
            );
            return ControlFlow::Continue(());
        }
    };
    if pending.is_empty() {
        return ControlFlow::Continue(());
    }

    // The instance's own identity, not its Loro peer id: Origin dedups by
    // `lite_id`, and a peer id changes under a collision rotation.
    let lite_id = delegate.sync_identity().lite_id;
    let producer_id = client.producer_id().await;
    let epoch = client.accepted_epoch().await;

    for (durable_key, mut entry) in pending {
        let batch_id = match kv_batch_id(&durable_key) {
            Ok(batch_id) => batch_id,
            Err(e) => {
                tracing::error!(
                    collection = %entry.collection,
                    error = %e,
                    "KvPush: outbound entry has no batch id; it cannot be sent"
                );
                continue;
            }
        };
        // Announce the collection's schema before its first write, so a
        // Lite-only KV collection exists on Origin before its rows land.
        if super::control::ensure_collection_announced(client, delegate, sink, &entry.collection)
            .await
            .is_break()
        {
            return ControlFlow::Break(());
        }
        if entry.seq == 0 {
            entry.seq = delegate
                .next_stream_seq(stream_id_for(EngineKind::Kv, &entry.collection))
                .await;
            if let Err(e) = delegate.persist_kv_write_seq(&durable_key, &entry).await {
                tracing::error!(
                    collection = %entry.collection,
                    batch_id,
                    error = %e,
                    "KvPush: persisting the assigned seq failed; the write waits for the next tick"
                );
                return ControlFlow::Continue(());
            }
        }
        let msg = KvPushMsg {
            lite_id: lite_id.clone(),
            collection: entry.collection.clone(),
            key: entry.key.clone(),
            op: entry.op.to_wire(),
            batch_id,
            producer_id,
            epoch,
            seq: entry.seq,
        };
        let Some(frame) = SyncFrame::try_encode(SyncMessageType::KvPush, &msg) else {
            // Kept at the head of the queue, the write would hold back every
            // later write to Origin. It is retired the way every engine queue
            // retires an entry it cannot encode.
            tracing::error!(
                collection = %entry.collection,
                key = %String::from_utf8_lossy(&entry.key),
                batch_id,
                "KvPush: frame encode failed; retiring the write — Origin does not get it"
            );
            if let Err(e) = delegate.retire_kv_write(batch_id).await {
                tracing::error!(
                    batch_id,
                    error = %e,
                    "KvPush: retiring the unencodable write failed; it is retried next tick"
                );
                return ControlFlow::Continue(());
            }
            continue;
        };
        if let Err(e) = send_binary(sink, frame).await {
            tracing::warn!(
                collection = %entry.collection,
                batch_id,
                error = %e,
                "KvPush send failed; the write stays queued for re-send on reconnect"
            );
            return ControlFlow::Break(());
        }
        // The durable entry stays until Origin's ack names this batch id.
        delegate.mark_kv_write_in_flight(batch_id).await;
        tracing::debug!(
            collection = %entry.collection,
            batch_id,
            seq = entry.seq,
            "sent KvPush to Origin; awaiting ack"
        );
    }
    ControlFlow::Continue(())
}
