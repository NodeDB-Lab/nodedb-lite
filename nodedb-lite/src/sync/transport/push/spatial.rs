//! Spatial geometry insert / delete push.

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

    for entry in delegate.pending_spatial_inserts() {
        let msg = nodedb_types::sync::wire::SpatialInsertMsg {
            lite_id: lite_id.clone(),
            collection: entry.collection.clone(),
            field: entry.field.clone(),
            doc_id: entry.doc_id.clone(),
            geometry_bytes: entry.geometry_bytes.clone(),
            batch_id: entry.batch_id,
        };
        let Some(frame) = SyncFrame::try_encode(SyncMessageType::SpatialInsert, &msg) else {
            tracing::error!(
                collection = %entry.collection, field = %entry.field, doc_id = %entry.doc_id, batch_id = entry.batch_id,
                "failed to encode SpatialInsert frame; dropping entry"
            );
            continue;
        };
        if let Err(e) = send_binary(sink, frame).await {
            tracing::warn!(
                collection = %entry.collection, field = %entry.field, doc_id = %entry.doc_id,
                batch_id = entry.batch_id, error = %e,
                "SpatialInsert send failed; re-queuing"
            );
            delegate.reject_spatial_insert(entry);
            return ControlFlow::Break(());
        }
        tracing::debug!(
            collection = %entry.collection, field = %entry.field, doc_id = %entry.doc_id, batch_id = entry.batch_id,
            "sent SpatialInsert to Origin"
        );
    }

    for entry in delegate.pending_spatial_deletes() {
        let msg = nodedb_types::sync::wire::SpatialDeleteMsg {
            lite_id: lite_id.clone(),
            collection: entry.collection.clone(),
            field: entry.field.clone(),
            doc_id: entry.doc_id.clone(),
            batch_id: entry.batch_id,
        };
        let Some(frame) = SyncFrame::try_encode(SyncMessageType::SpatialDelete, &msg) else {
            tracing::error!(
                collection = %entry.collection, field = %entry.field, doc_id = %entry.doc_id, batch_id = entry.batch_id,
                "failed to encode SpatialDelete frame; dropping entry"
            );
            continue;
        };
        if let Err(e) = send_binary(sink, frame).await {
            tracing::warn!(
                collection = %entry.collection, field = %entry.field, doc_id = %entry.doc_id,
                batch_id = entry.batch_id, error = %e,
                "SpatialDelete send failed; re-queuing"
            );
            delegate.reject_spatial_delete(entry);
            return ControlFlow::Break(());
        }
        tracing::debug!(
            collection = %entry.collection, field = %entry.field, doc_id = %entry.doc_id, batch_id = entry.batch_id,
            "sent SpatialDelete to Origin"
        );
    }
    ControlFlow::Continue(())
}
