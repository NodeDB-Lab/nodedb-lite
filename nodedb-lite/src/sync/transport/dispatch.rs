//! Inbound frame receive loop and dispatch table.
//!
//! `receive_loop` reads `Message::Binary` frames off the WebSocket stream,
//! decodes the `SyncFrame` envelope, and hands each frame to `dispatch_frame`,
//! which fans out to per-message-type handlers on `SyncClient` and
//! `SyncDelegate`. Pulled out of the main transport module so the giant
//! `match` over message types lives in one self-contained file instead of
//! being interleaved with the push loop.
//!
//! Engine-level ack dispatch (with AckStatus handling) lives in
//! `dispatch_acks` to keep this file within the 500-line limit.

use std::sync::Arc;

use futures::StreamExt;
use tokio_tungstenite::tungstenite::Message;

use nodedb_types::sync::wire::{AckStatus, SyncFrame, SyncMessageType};

use super::delegate::SyncDelegate;
use super::dispatch_acks;
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
                match &ack.status {
                    AckStatus::Applied | AckStatus::Duplicate => {
                        // DeltaAckMsg does not carry a collection field so we
                        // cannot derive a stream_id to advance the frontier here.
                        // The durable last_assigned from StreamSeqTracker already
                        // prevents re-sending un-acked seqs; frontier advancement
                        // for CRDT deltas is deferred until DeltaAckMsg gains a
                        // collection field.
                        delegate.acknowledge(ack.mutation_id);
                    }
                    AckStatus::Fenced => {
                        tracing::error!(
                            mutation_id = ack.mutation_id,
                            "DeltaAck: producer fenced by Origin; halting push"
                        );
                        client.set_fenced();
                        delegate.acknowledge(ack.mutation_id);
                    }
                    AckStatus::Gap { expected } => {
                        tracing::warn!(
                            mutation_id = ack.mutation_id,
                            expected,
                            "DeltaAck: sequence gap detected by Origin"
                        );
                        delegate.acknowledge(ack.mutation_id);
                    }
                }
                client.handle_delta_ack(&ack).await;
            } else {
                client.metrics().record_stale_timeouts(1);
                tracing::warn!(
                    frame_len = frame.body.len(),
                    "DeltaAck frame body failed to decode; \
                     in-flight entry will be evicted by the stale-timeout pass"
                );
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
        SyncMessageType::CollectionSchema => {
            if let Some(msg) =
                frame.decode_body::<nodedb_types::sync::wire::CollectionSchemaSyncMsg>()
            {
                delegate.import_collection_schema(&msg).await;
            } else {
                tracing::warn!("CollectionSchema: failed to decode frame body");
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
            dispatch_acks::handle_columnar_insert_ack(client, delegate, frame).await;
        }
        SyncMessageType::VectorInsertAck => {
            dispatch_acks::handle_vector_insert_ack(client, delegate, frame).await;
        }
        SyncMessageType::VectorDeleteAck => {
            dispatch_acks::handle_vector_delete_ack(client, delegate, frame).await;
        }
        SyncMessageType::FtsIndexAck => {
            dispatch_acks::handle_fts_index_ack(client, delegate, frame).await;
        }
        SyncMessageType::FtsDeleteAck => {
            dispatch_acks::handle_fts_delete_ack(client, delegate, frame).await;
        }
        SyncMessageType::SpatialInsertAck => {
            dispatch_acks::handle_spatial_insert_ack(client, delegate, frame).await;
        }
        SyncMessageType::SpatialDeleteAck => {
            dispatch_acks::handle_spatial_delete_ack(client, delegate, frame).await;
        }
        SyncMessageType::TimeseriesAck => {
            dispatch_acks::handle_timeseries_ack(client, delegate, frame).await;
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
