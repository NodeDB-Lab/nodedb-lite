//! Inbound frame receive loop and dispatch table.
//!
//! `receive_loop` reads `Message::Binary` frames off the WebSocket stream,
//! decodes the `SyncFrame` envelope, and hands each frame to `dispatch_frame`,
//! which fans out to per-message-type handlers on `SyncClient` and
//! `SyncDelegate`. Pulled out of the main transport module so the giant
//! `match` over message types lives in one self-contained file instead of
//! being interleaved with the push loop.

use std::sync::Arc;

use futures::StreamExt;
use tokio_tungstenite::tungstenite::Message;

use nodedb_types::sync::wire::{SyncFrame, SyncMessageType};

use super::delegate::SyncDelegate;
use crate::error::LiteError;
use crate::sync::client::SyncClient;

/// Receive and dispatch incoming frames from Origin.
pub(super) async fn receive_loop<S>(
    client: &Arc<SyncClient>,
    delegate: &Arc<dyn SyncDelegate>,
    stream: &mut S,
) -> Result<(), LiteError>
where
    S: StreamExt<Item = Result<Message, tokio_tungstenite::tungstenite::Error>> + Unpin,
{
    while let Some(msg_result) = stream.next().await {
        let msg = msg_result.map_err(|e| LiteError::Sync {
            detail: format!("WebSocket read error: {e}"),
        })?;

        let bytes = match &msg {
            Message::Binary(b) => b.as_ref(),
            Message::Close(_) => return Ok(()),
            Message::Ping(_) | Message::Pong(_) => continue,
            _ => continue,
        };

        let Some(frame) = SyncFrame::from_bytes(bytes) else {
            tracing::warn!("received malformed frame, skipping");
            continue;
        };

        dispatch_frame(client, delegate, &frame).await;
    }

    Ok(())
}

/// Dispatch a single incoming frame to the appropriate handler.
pub(super) async fn dispatch_frame(
    client: &Arc<SyncClient>,
    delegate: &Arc<dyn SyncDelegate>,
    frame: &SyncFrame,
) {
    match frame.msg_type {
        SyncMessageType::DeltaAck => {
            if let Some(ack) = frame.decode_body::<nodedb_types::sync::wire::DeltaAckMsg>() {
                delegate.acknowledge(ack.mutation_id);
                client.handle_delta_ack(&ack).await;
            }
        }
        SyncMessageType::ResyncRequest => {
            // Origin is requesting us to re-sync. Log; the push loop re-sends
            // from the requested mutation ID on the next tick.
            if let Some(msg) = frame.decode_body::<nodedb_types::sync::wire::ResyncRequestMsg>() {
                tracing::warn!(
                    reason = ?msg.reason,
                    from_mutation_id = msg.from_mutation_id,
                    collection = %msg.collection,
                    "Origin requested re-sync"
                );
            }
        }
        SyncMessageType::DeltaReject => {
            if let Some(reject) = frame.decode_body::<nodedb_types::sync::wire::DeltaRejectMsg>() {
                // Detect auth-related rejection → pause push, trigger token refresh.
                if matches!(
                    &reject.compensation,
                    Some(nodedb_types::sync::compensation::CompensationHint::PermissionDenied)
                ) && client.config().token_provider.is_some()
                {
                    client.pause_for_auth().await;
                }

                if let Some(hint) = &reject.compensation {
                    delegate.reject_with_policy(reject.mutation_id, hint);
                } else {
                    delegate.reject(reject.mutation_id);
                }
                client.handle_delta_reject(&reject).await;
            }
        }
        SyncMessageType::TokenRefreshAck => {
            if let Some(ack) = frame.decode_body::<nodedb_types::sync::wire::TokenRefreshAckMsg>() {
                client.handle_token_refresh_ack(&ack).await;
            }
        }
        SyncMessageType::ShapeSnapshot => {
            if let Some(snapshot) =
                frame.decode_body::<nodedb_types::sync::wire::ShapeSnapshotMsg>()
            {
                if !snapshot.data.is_empty() {
                    delegate.import_remote(&snapshot.data);
                }
                client.handle_shape_snapshot(&snapshot).await;
            }
        }
        SyncMessageType::ShapeDelta => {
            if let Some(delta) = frame.decode_body::<nodedb_types::sync::wire::ShapeDeltaMsg>() {
                client.metrics().record_received();
                if let Some(resync) = client.check_sequence_gap(&delta.shape_id, delta.lsn).await {
                    tracing::warn!(
                        shape_id = %delta.shape_id,
                        "requesting re-sync due to sequence gap"
                    );
                    // Stash for the push loop to send on its next tick — the
                    // dispatch path does not own the sink.
                    client.set_pending_resync(resync).await;
                }
                if !delta.delta.is_empty() {
                    delegate.import_remote(&delta.delta);
                }
                client.handle_shape_delta(&delta).await;
            }
        }
        SyncMessageType::VectorClockSync => {
            if let Some(clock_msg) =
                frame.decode_body::<nodedb_types::sync::wire::VectorClockSyncMsg>()
            {
                client.handle_clock_sync(&clock_msg).await;
            }
        }
        SyncMessageType::DefinitionSync => {
            if let Some(msg) = frame.decode_body::<nodedb_types::sync::wire::DefinitionSyncMsg>() {
                delegate.import_definition(&msg).await;
            }
        }
        SyncMessageType::ArrayDelta => {
            if let Some(msg) = frame.decode_body::<nodedb_types::sync::wire::ArrayDeltaMsg>() {
                if let Some(ack) = delegate.handle_array_delta(&msg) {
                    client.set_pending_array_ack(ack).await;
                }
            } else {
                tracing::warn!("ArrayDelta: failed to decode frame body");
            }
        }
        SyncMessageType::ArrayDeltaBatch => {
            if let Some(msg) = frame.decode_body::<nodedb_types::sync::wire::ArrayDeltaBatchMsg>() {
                if let Some(ack) = delegate.handle_array_delta_batch(&msg) {
                    client.set_pending_array_ack(ack).await;
                }
            } else {
                tracing::warn!("ArrayDeltaBatch: failed to decode frame body");
            }
        }
        SyncMessageType::ArrayReject => {
            if let Some(msg) = frame.decode_body::<nodedb_types::sync::wire::ArrayRejectMsg>() {
                tracing::warn!(
                    array = %msg.array,
                    reason = ?msg.reason,
                    detail = %msg.detail,
                    "received ArrayReject from Origin — op removed from pending queue"
                );
                delegate.handle_array_reject(&msg);
            } else {
                tracing::warn!("ArrayReject: failed to decode frame body");
            }
        }
        SyncMessageType::ColumnarInsertAck => {
            if let Some(ack) = frame.decode_body::<nodedb_types::sync::wire::ColumnarInsertAckMsg>()
            {
                tracing::debug!(
                    collection = %ack.collection,
                    batch_id = ack.batch_id,
                    accepted = ack.accepted,
                    rejected = ack.rejected,
                    "ColumnarInsertAck received from Origin"
                );
                delegate.acknowledge_columnar_batch(ack.batch_id);
            }
        }
        SyncMessageType::VectorInsertAck => {
            if let Some(ack) = frame.decode_body::<nodedb_types::sync::wire::VectorInsertAckMsg>() {
                tracing::debug!(
                    collection = %ack.collection,
                    id = %ack.id,
                    batch_id = ack.batch_id,
                    accepted = ack.accepted,
                    "VectorInsertAck received from Origin"
                );
                if !ack.accepted {
                    tracing::warn!(
                        collection = %ack.collection,
                        id = %ack.id,
                        reason = ?ack.reject_reason,
                        "VectorInsert rejected by Origin; dropping (no retry for rejected inserts)"
                    );
                }
                // Either way the entry leaves the pending queue — accepted
                // inserts are durable on Origin; rejected ones cannot be
                // retried and would loop forever.
                delegate.acknowledge_vector_insert(ack.batch_id);
            }
        }
        SyncMessageType::VectorDeleteAck => {
            if let Some(ack) = frame.decode_body::<nodedb_types::sync::wire::VectorDeleteAckMsg>() {
                tracing::debug!(
                    collection = %ack.collection,
                    id = %ack.id,
                    batch_id = ack.batch_id,
                    accepted = ack.accepted,
                    "VectorDeleteAck received from Origin"
                );
                delegate.acknowledge_vector_delete(ack.batch_id);
            }
        }
        SyncMessageType::FtsIndexAck => {
            if let Some(ack) = frame.decode_body::<nodedb_types::sync::wire::FtsIndexAckMsg>() {
                tracing::debug!(
                    collection = %ack.collection,
                    doc_id = %ack.doc_id,
                    batch_id = ack.batch_id,
                    accepted = ack.accepted,
                    "FtsIndexAck received from Origin"
                );
                delegate.acknowledge_fts_index(ack.batch_id);
            }
        }
        SyncMessageType::FtsDeleteAck => {
            if let Some(ack) = frame.decode_body::<nodedb_types::sync::wire::FtsDeleteAckMsg>() {
                tracing::debug!(
                    collection = %ack.collection,
                    doc_id = %ack.doc_id,
                    batch_id = ack.batch_id,
                    accepted = ack.accepted,
                    "FtsDeleteAck received from Origin"
                );
                delegate.acknowledge_fts_delete(ack.batch_id);
            }
        }
        SyncMessageType::SpatialInsertAck => {
            if let Some(ack) = frame.decode_body::<nodedb_types::sync::wire::SpatialInsertAckMsg>()
            {
                tracing::debug!(
                    collection = %ack.collection,
                    field = %ack.field,
                    doc_id = %ack.doc_id,
                    batch_id = ack.batch_id,
                    accepted = ack.accepted,
                    "SpatialInsertAck received from Origin"
                );
                delegate.acknowledge_spatial_insert(ack.batch_id);
            }
        }
        SyncMessageType::SpatialDeleteAck => {
            if let Some(ack) = frame.decode_body::<nodedb_types::sync::wire::SpatialDeleteAckMsg>()
            {
                tracing::debug!(
                    collection = %ack.collection,
                    field = %ack.field,
                    doc_id = %ack.doc_id,
                    batch_id = ack.batch_id,
                    accepted = ack.accepted,
                    "SpatialDeleteAck received from Origin"
                );
                delegate.acknowledge_spatial_delete(ack.batch_id);
            }
        }
        SyncMessageType::TimeseriesAck => {
            if let Some(ack) = frame.decode_body::<nodedb_types::sync::wire::TimeseriesAckMsg>() {
                tracing::debug!(
                    collection = %ack.collection,
                    accepted = ack.accepted,
                    rejected = ack.rejected,
                    lsn = ack.lsn,
                    "TimeseriesAck received from Origin"
                );
                // Acknowledge by collection — Origin confirmed receipt for
                // the entire batch; remaining batches drain on the next push.
                delegate.acknowledge_timeseries_collection(&ack.collection);
            }
        }
        SyncMessageType::PingPong => {
            // Origin pinged. Our `ping_loop` already keeps the link alive,
            // so no response is needed here.
            tracing::trace!("received ping/pong from Origin");
        }
        _ => {
            tracing::debug!(msg_type = ?frame.msg_type, "unexpected frame type from Origin");
        }
    }
}
