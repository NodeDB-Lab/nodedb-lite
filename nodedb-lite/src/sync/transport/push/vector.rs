//! Vector insert / delete push — delete-on-ack model.
//!
//! On successful send the durable entry is NOT deleted; instead the batch_id →
//! durable_key mapping is recorded in-flight. The durable entry is deleted only
//! when Origin sends a VectorInsertAck / VectorDeleteAck (Applied or Duplicate).
//! A send failure leaves the durable entry intact so it is re-sent on reconnect;
//! Origin's idempotent gate deduplicates re-sent entries.

use std::ops::ControlFlow;
use std::sync::Arc;

use futures::SinkExt;
use tokio::sync::Mutex;
use tokio_tungstenite::tungstenite::Message;

use nodedb_types::sync::wire::{EngineKind, SyncFrame, SyncMessageType, stream_id_for};

use super::send::send_binary;
use crate::sync::client::SyncClient;
use crate::sync::transport::delegate::SyncDelegate;

pub(super) async fn push<S>(
    client: &Arc<SyncClient>,
    delegate: &Arc<dyn SyncDelegate>,
    sink: &Arc<Mutex<S>>,
) -> ControlFlow<()>
where
    S: SinkExt<Message> + Unpin,
    S::Error: std::fmt::Display,
{
    let lite_id = format!("{}", client.peer_id());
    let producer_id = client.producer_id().await;
    let epoch = client.accepted_epoch().await;

    for (durable_key, mut entry) in delegate.pending_vector_inserts().await {
        // Announce the collection's schema before its first data frame so a
        // lite-only collection is registered on Origin before its vectors land.
        if super::control::ensure_collection_announced(client, delegate, sink, &entry.collection)
            .await
            .is_break()
        {
            return ControlFlow::Break(());
        }
        let stream_id = stream_id_for(EngineKind::Vector, &entry.collection);
        if entry.seq == 0 {
            entry.seq = delegate.next_stream_seq(stream_id).await;
            if let Err(e) = delegate
                .persist_vector_insert_seq(&durable_key, &entry)
                .await
            {
                tracing::warn!(error = %e, "failed to persist stream seq into durable entry; retaining for retry");
                continue;
            }
        }
        let seq = entry.seq;
        let msg = nodedb_types::sync::wire::VectorInsertMsg {
            lite_id: lite_id.clone(),
            collection: entry.collection.clone(),
            id: entry.id.clone(),
            vector: entry.vector.clone(),
            dim: entry.dim,
            field_name: entry.field_name.clone(),
            batch_id: entry.batch_id,
            producer_id,
            epoch,
            seq,
        };
        let Some(frame) = SyncFrame::try_encode(SyncMessageType::VectorInsert, &msg) else {
            tracing::error!(
                collection = %entry.collection, id = %entry.id, batch_id = entry.batch_id,
                "failed to encode VectorInsert frame; dropping entry"
            );
            // Delete un-encodable entries so they do not loop forever.
            delegate.acknowledge_vector_insert(durable_key).await;
            continue;
        };
        if let Err(e) = send_binary(sink, frame).await {
            tracing::warn!(
                collection = %entry.collection, id = %entry.id, batch_id = entry.batch_id, error = %e,
                "VectorInsert send failed; durable entry retained for re-send on reconnect"
            );
            return ControlFlow::Break(());
        }
        tracing::debug!(
            collection = %entry.collection, id = %entry.id, batch_id = entry.batch_id, dim = entry.dim,
            "sent VectorInsert to Origin; awaiting ack before deleting durable entry"
        );
        // Mark in-flight: durable entry survives until Origin ack.
        delegate
            .mark_vector_insert_in_flight(entry.batch_id, durable_key)
            .await;
    }

    for (durable_key, mut entry) in delegate.pending_vector_deletes().await {
        if super::control::ensure_collection_announced(client, delegate, sink, &entry.collection)
            .await
            .is_break()
        {
            return ControlFlow::Break(());
        }
        let stream_id = stream_id_for(EngineKind::Vector, &entry.collection);
        if entry.seq == 0 {
            entry.seq = delegate.next_stream_seq(stream_id).await;
            if let Err(e) = delegate
                .persist_vector_delete_seq(&durable_key, &entry)
                .await
            {
                tracing::warn!(error = %e, "failed to persist stream seq into durable entry; retaining for retry");
                continue;
            }
        }
        let seq = entry.seq;
        let msg = nodedb_types::sync::wire::VectorDeleteMsg {
            lite_id: lite_id.clone(),
            collection: entry.collection.clone(),
            id: entry.id.clone(),
            field_name: entry.field_name.clone(),
            batch_id: entry.batch_id,
            producer_id,
            epoch,
            seq,
        };
        let Some(frame) = SyncFrame::try_encode(SyncMessageType::VectorDelete, &msg) else {
            tracing::error!(
                collection = %entry.collection, id = %entry.id, batch_id = entry.batch_id,
                "failed to encode VectorDelete frame; dropping entry"
            );
            // Delete un-encodable entries so they do not loop forever.
            delegate.acknowledge_vector_delete(durable_key).await;
            continue;
        };
        if let Err(e) = send_binary(sink, frame).await {
            tracing::warn!(
                collection = %entry.collection, id = %entry.id, batch_id = entry.batch_id, error = %e,
                "VectorDelete send failed; durable entry retained for re-send on reconnect"
            );
            return ControlFlow::Break(());
        }
        tracing::debug!(
            collection = %entry.collection, id = %entry.id, batch_id = entry.batch_id,
            "sent VectorDelete to Origin; awaiting ack before deleting durable entry"
        );
        // Mark in-flight: durable entry survives until Origin ack.
        delegate
            .mark_vector_delete_in_flight(entry.batch_id, durable_key)
            .await;
    }
    ControlFlow::Continue(())
}
