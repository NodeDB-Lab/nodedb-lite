//! Columnar insert push.

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

    for batch in delegate.pending_columnar_batches() {
        let rows_msgpack: Vec<Vec<u8>> = batch
            .rows
            .iter()
            .filter_map(|row| zerompk::to_msgpack_vec(row).ok())
            .collect();

        let msg = nodedb_types::sync::wire::ColumnarInsertMsg {
            lite_id: lite_id.clone(),
            collection: batch.collection.clone(),
            rows: rows_msgpack,
            batch_id: batch.batch_id,
            schema_bytes: batch.schema_bytes.clone(),
        };

        let Some(frame) = SyncFrame::try_encode(SyncMessageType::ColumnarInsert, &msg) else {
            tracing::error!(
                collection = %batch.collection,
                batch_id = batch.batch_id,
                "failed to encode ColumnarInsert frame; dropping batch"
            );
            continue;
        };
        if let Err(e) = send_binary(sink, frame).await {
            tracing::warn!(
                collection = %batch.collection,
                batch_id = batch.batch_id,
                error = %e,
                "ColumnarInsert send failed; re-queuing batch"
            );
            delegate.reject_columnar_batch(batch);
            return ControlFlow::Break(());
        }
        tracing::debug!(
            collection = %batch.collection,
            batch_id = batch.batch_id,
            rows = msg.rows.len(),
            "sent ColumnarInsert to Origin"
        );
    }
    ControlFlow::Continue(())
}
