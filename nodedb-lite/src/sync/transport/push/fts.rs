//! FTS index / delete push.

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

    for (durable_key, mut entry) in delegate.pending_fts_indexes().await {
        // Announce the collection's schema before its first data frame so a
        // lite-only collection is registered on Origin before its FTS docs land.
        if super::control::ensure_collection_announced(client, delegate, sink, &entry.collection)
            .await
            .is_break()
        {
            return ControlFlow::Break(());
        }
        let stream_id = stream_id_for(EngineKind::Fts, &entry.collection);
        if entry.seq == 0 {
            entry.seq = delegate.next_stream_seq(stream_id).await;
            if let Err(e) = delegate.persist_fts_index_seq(&durable_key, &entry).await {
                tracing::warn!(error = %e, "failed to persist stream seq into durable entry; retaining for retry");
                continue;
            }
        }
        let seq = entry.seq;
        let msg = nodedb_types::sync::wire::FtsIndexMsg {
            lite_id: lite_id.clone(),
            collection: entry.collection.clone(),
            doc_id: entry.doc_id.clone(),
            text: entry.text.clone(),
            batch_id: entry.batch_id,
            producer_id,
            epoch,
            seq,
        };
        let Some(frame) = SyncFrame::try_encode(SyncMessageType::FtsIndex, &msg) else {
            tracing::error!(
                collection = %entry.collection, doc_id = %entry.doc_id, batch_id = entry.batch_id,
                "failed to encode FtsIndex frame; dropping entry"
            );
            delegate.acknowledge_fts_index(durable_key).await;
            continue;
        };
        if let Err(e) = send_binary(sink, frame).await {
            tracing::warn!(
                collection = %entry.collection, doc_id = %entry.doc_id, batch_id = entry.batch_id, error = %e,
                "FtsIndex send failed; durable entry retained for re-send on reconnect"
            );
            return ControlFlow::Break(());
        }
        // Mark in-flight: durable entry survives until Origin ack.
        delegate
            .mark_fts_index_in_flight(entry.batch_id, durable_key)
            .await;
        tracing::debug!(
            collection = %entry.collection, doc_id = %entry.doc_id, batch_id = entry.batch_id,
            "sent FtsIndex to Origin; awaiting ack before deleting durable entry"
        );
    }

    for (durable_key, mut entry) in delegate.pending_fts_deletes().await {
        if super::control::ensure_collection_announced(client, delegate, sink, &entry.collection)
            .await
            .is_break()
        {
            return ControlFlow::Break(());
        }
        let stream_id = stream_id_for(EngineKind::Fts, &entry.collection);
        if entry.seq == 0 {
            entry.seq = delegate.next_stream_seq(stream_id).await;
            if let Err(e) = delegate.persist_fts_delete_seq(&durable_key, &entry).await {
                tracing::warn!(error = %e, "failed to persist stream seq into durable entry; retaining for retry");
                continue;
            }
        }
        let seq = entry.seq;
        let msg = nodedb_types::sync::wire::FtsDeleteMsg {
            lite_id: lite_id.clone(),
            collection: entry.collection.clone(),
            doc_id: entry.doc_id.clone(),
            batch_id: entry.batch_id,
            producer_id,
            epoch,
            seq,
        };
        let Some(frame) = SyncFrame::try_encode(SyncMessageType::FtsDelete, &msg) else {
            tracing::error!(
                collection = %entry.collection, doc_id = %entry.doc_id, batch_id = entry.batch_id,
                "failed to encode FtsDelete frame; dropping entry"
            );
            delegate.acknowledge_fts_delete(durable_key).await;
            continue;
        };
        if let Err(e) = send_binary(sink, frame).await {
            tracing::warn!(
                collection = %entry.collection, doc_id = %entry.doc_id, batch_id = entry.batch_id, error = %e,
                "FtsDelete send failed; durable entry retained for re-send on reconnect"
            );
            return ControlFlow::Break(());
        }
        // Mark in-flight: durable entry survives until Origin ack.
        delegate
            .mark_fts_delete_in_flight(entry.batch_id, durable_key)
            .await;
        tracing::debug!(
            collection = %entry.collection, doc_id = %entry.doc_id, batch_id = entry.batch_id,
            "sent FtsDelete to Origin; awaiting ack before deleting durable entry"
        );
    }
    ControlFlow::Continue(())
}
