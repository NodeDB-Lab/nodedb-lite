//! Columnar insert push — delete-on-ack model.
//!
//! On successful send the durable entry is NOT deleted; instead the batch_id →
//! durable_key mapping is recorded in-flight. The durable entry is deleted only
//! when Origin sends a ColumnarInsertAck (Applied or Duplicate). A crash or
//! disconnect before ack leaves the durable entry intact so it is re-sent on
//! reconnect; Origin's idempotent gate deduplicates re-sent batches.

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

    for (durable_key, mut batch) in delegate.pending_columnar_batches().await {
        let rows_msgpack: Vec<Vec<u8>> = batch
            .rows
            .iter()
            .filter_map(|row| zerompk::to_msgpack_vec(row).ok())
            .collect();

        let stream_id = stream_id_for(EngineKind::Columnar, &batch.collection);
        if batch.seq == 0 {
            batch.seq = delegate.next_stream_seq(stream_id).await;
            if let Err(e) = delegate.persist_columnar_seq(&durable_key, &batch).await {
                tracing::warn!(error = %e, "failed to persist stream seq into durable entry; retaining for retry");
                continue;
            }
        }
        let seq = batch.seq;

        let msg = nodedb_types::sync::wire::ColumnarInsertMsg {
            lite_id: lite_id.clone(),
            collection: batch.collection.clone(),
            rows: rows_msgpack,
            batch_id: batch.batch_id,
            schema_bytes: batch.schema_bytes.clone(),
            producer_id,
            epoch,
            seq,
        };

        let Some(frame) = SyncFrame::try_encode(SyncMessageType::ColumnarInsert, &msg) else {
            tracing::error!(
                collection = %batch.collection,
                batch_id = batch.batch_id,
                "failed to encode ColumnarInsert frame; dropping batch"
            );
            // Delete from durable storage so the queue doesn't loop on an un-encodable batch.
            delegate.acknowledge_columnar_batch(durable_key).await;
            continue;
        };
        if let Err(e) = send_binary(sink, frame).await {
            tracing::warn!(
                collection = %batch.collection,
                batch_id = batch.batch_id,
                error = %e,
                "ColumnarInsert send failed; durable entry retained for re-send on reconnect"
            );
            return ControlFlow::Break(());
        }
        tracing::debug!(
            collection = %batch.collection,
            batch_id = batch.batch_id,
            rows = msg.rows.len(),
            "sent ColumnarInsert to Origin; awaiting ack before deleting durable entry"
        );
        // Mark in-flight: durable entry survives until Origin ack.
        delegate
            .mark_columnar_batch_in_flight(batch.batch_id, durable_key)
            .await;
    }
    ControlFlow::Continue(())
}
