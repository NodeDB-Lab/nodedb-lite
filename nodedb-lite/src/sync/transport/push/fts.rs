//! FTS index / delete push.

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

    for entry in delegate.pending_fts_indexes() {
        let msg = nodedb_types::sync::wire::FtsIndexMsg {
            lite_id: lite_id.clone(),
            collection: entry.collection.clone(),
            doc_id: entry.doc_id.clone(),
            text: entry.text.clone(),
            batch_id: entry.batch_id,
        };
        let Some(frame) = SyncFrame::try_encode(SyncMessageType::FtsIndex, &msg) else {
            tracing::error!(
                collection = %entry.collection, doc_id = %entry.doc_id, batch_id = entry.batch_id,
                "failed to encode FtsIndex frame; dropping entry"
            );
            continue;
        };
        if let Err(e) = send_binary(sink, frame).await {
            tracing::warn!(
                collection = %entry.collection, doc_id = %entry.doc_id, batch_id = entry.batch_id, error = %e,
                "FtsIndex send failed; re-queuing"
            );
            delegate.reject_fts_index(entry);
            return ControlFlow::Break(());
        }
        tracing::debug!(
            collection = %entry.collection, doc_id = %entry.doc_id, batch_id = entry.batch_id,
            "sent FtsIndex to Origin"
        );
    }

    for entry in delegate.pending_fts_deletes() {
        let msg = nodedb_types::sync::wire::FtsDeleteMsg {
            lite_id: lite_id.clone(),
            collection: entry.collection.clone(),
            doc_id: entry.doc_id.clone(),
            batch_id: entry.batch_id,
        };
        let Some(frame) = SyncFrame::try_encode(SyncMessageType::FtsDelete, &msg) else {
            tracing::error!(
                collection = %entry.collection, doc_id = %entry.doc_id, batch_id = entry.batch_id,
                "failed to encode FtsDelete frame; dropping entry"
            );
            continue;
        };
        if let Err(e) = send_binary(sink, frame).await {
            tracing::warn!(
                collection = %entry.collection, doc_id = %entry.doc_id, batch_id = entry.batch_id, error = %e,
                "FtsDelete send failed; re-queuing"
            );
            delegate.reject_fts_delete(entry);
            return ControlFlow::Break(());
        }
        tracing::debug!(
            collection = %entry.collection, doc_id = %entry.doc_id, batch_id = entry.batch_id,
            "sent FtsDelete to Origin"
        );
    }
    ControlFlow::Continue(())
}
