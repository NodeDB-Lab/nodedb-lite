//! Vector insert / delete push.

use std::ops::ControlFlow;
use std::sync::Arc;

use futures::SinkExt;
use tokio::sync::Mutex;
use tokio_tungstenite::tungstenite::Message;

use nodedb_types::sync::wire::{SyncFrame, SyncMessageType};

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

    for entry in delegate.pending_vector_inserts() {
        let msg = nodedb_types::sync::wire::VectorInsertMsg {
            lite_id: lite_id.clone(),
            collection: entry.collection.clone(),
            id: entry.id.clone(),
            vector: entry.vector.clone(),
            dim: entry.dim,
            field_name: entry.field_name.clone(),
            batch_id: entry.batch_id,
        };
        let Some(frame) = SyncFrame::try_encode(SyncMessageType::VectorInsert, &msg) else {
            tracing::error!(
                collection = %entry.collection, id = %entry.id, batch_id = entry.batch_id,
                "failed to encode VectorInsert frame; dropping entry"
            );
            continue;
        };
        if let Err(e) = send_binary(sink, frame).await {
            tracing::warn!(
                collection = %entry.collection, id = %entry.id, batch_id = entry.batch_id, error = %e,
                "VectorInsert send failed; re-queuing"
            );
            delegate.reject_vector_insert(entry);
            return ControlFlow::Break(());
        }
        tracing::debug!(
            collection = %entry.collection, id = %entry.id, batch_id = entry.batch_id, dim = entry.dim,
            "sent VectorInsert to Origin"
        );
    }

    for entry in delegate.pending_vector_deletes() {
        let msg = nodedb_types::sync::wire::VectorDeleteMsg {
            lite_id: lite_id.clone(),
            collection: entry.collection.clone(),
            id: entry.id.clone(),
            field_name: entry.field_name.clone(),
            batch_id: entry.batch_id,
        };
        let Some(frame) = SyncFrame::try_encode(SyncMessageType::VectorDelete, &msg) else {
            tracing::error!(
                collection = %entry.collection, id = %entry.id, batch_id = entry.batch_id,
                "failed to encode VectorDelete frame; dropping entry"
            );
            continue;
        };
        if let Err(e) = send_binary(sink, frame).await {
            tracing::warn!(
                collection = %entry.collection, id = %entry.id, batch_id = entry.batch_id, error = %e,
                "VectorDelete send failed; re-queuing"
            );
            delegate.reject_vector_delete(entry);
            return ControlFlow::Break(());
        }
        tracing::debug!(
            collection = %entry.collection, id = %entry.id, batch_id = entry.batch_id,
            "sent VectorDelete to Origin"
        );
    }
    ControlFlow::Continue(())
}
