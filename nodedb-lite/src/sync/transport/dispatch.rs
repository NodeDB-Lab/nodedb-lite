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
//! `dispatch_acks`. Array reject and push-window pong handling lives in
//! `dispatch_array`.

use std::sync::Arc;

use futures::StreamExt;
use tokio_tungstenite::tungstenite::Message;

use nodedb_types::sync::wire::{AckStatus, SyncFrame, SyncMessageType};

use super::dispatch_acks;
use crate::error::LiteError;
use crate::sync::client::SyncClient;
use crate::sync::delegate::SyncDelegate;

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
            // Sync frames are binary. Control frames and text carry none.
            Message::Ping(_) | Message::Pong(_) | Message::Text(_) | Message::Frame(_) => continue,
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
                    AckStatus::Accepted => {
                        // Provisional: Origin admitted the delta but has not
                        // applied it. Keep it queued until a terminal status
                        // arrives.
                        tracing::trace!(
                            mutation_id = ack.mutation_id,
                            "DeltaAck: accepted, awaiting apply outcome"
                        );
                    }
                    AckStatus::Fenced => {
                        // Origin did NOT apply this delta — the producer epoch
                        // was rejected. Retiring it here would drop the write;
                        // it stays queued for re-push after the producer
                        // identity is re-established.
                        tracing::error!(
                            mutation_id = ack.mutation_id,
                            "DeltaAck: producer fenced by Origin; halting push, \
                             delta retained for re-send"
                        );
                        client.set_fenced();
                    }
                    AckStatus::Gap { expected } => {
                        // Origin did NOT apply this delta — it expected a
                        // different sequence number. The delta stays queued so
                        // the push loop re-sends it at its stable seq.
                        tracing::warn!(
                            mutation_id = ack.mutation_id,
                            expected,
                            "DeltaAck: sequence gap detected by Origin; delta \
                             retained for re-send"
                        );
                    }
                    AckStatus::Rejected { reason } => {
                        // Terminal failure: per the wire contract this delta
                        // "will never apply". Unlike `Fenced` (recoverable once
                        // the producer identity is re-established) and `Gap`
                        // (recoverable by re-sending at the right seq), there is
                        // nothing to retry — retaining it would stall the queue
                        // forever behind a message Origin has permanently
                        // refused. So it is retired, which DOES drop the write:
                        // logged at ERROR with Origin's reason so the loss is
                        // never silent.
                        tracing::error!(
                            mutation_id = ack.mutation_id,
                            reason = %reason,
                            "DeltaAck: Origin permanently rejected this delta; \
                             dropping it — the write is LOST and will not retry"
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
        SyncMessageType::RowPush => {
            // Server-originated row post-image (SQL DML on Origin, or a
            // DDL-managed system row). Applied locally; never echoed back.
            if let Some(msg) = frame.decode_body::<nodedb_types::sync::wire::RowPushMsg>() {
                // Origin's client→server writes are protected by sync_admit
                // (producer/epoch/seq); this is the symmetric gate for the
                // server→client direction, without which a re-delivered or
                // out-of-order RowPush could resurrect a deleted row.
                if client
                    .admit_row_push(msg.peer_id, &msg.collection, msg.sequence)
                    .await
                    && let Err(e) = delegate.apply_remote_row(&msg).await
                {
                    // The refusal goes back to Origin as a `RowPushReject`
                    // on the next push tick.
                    tracing::error!(
                        collection = %msg.collection,
                        doc = %msg.document_id,
                        sequence = msg.sequence,
                        error = %e,
                        "RowPush refused; row not applied, refusal queued for Origin"
                    );
                    client.queue_row_push_reject(&msg, &e).await;
                }
            } else {
                tracing::warn!(
                    frame_len = frame.body.len(),
                    "RowPush frame body failed to decode; row not applied"
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

                // A peer-id collision is the one refusal the client can act on
                // by itself, and the one it must: every later write carries the
                // same refused id, so a replica that only reports it is
                // permanently unable to sync. Rotating re-authors the local
                // documents under a new identity and re-queues this delta with
                // them, so it replaces the rollback rather than following it.
                let collision = reject
                    .compensation
                    .as_ref()
                    .is_some_and(|hint| hint.is_peer_id_collision());
                if collision {
                    delegate.rotate_peer_id().await;
                } else if let Some(hint) = &reject.compensation {
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
                // `ShapeSnapshotMsg` carries no collection of its own, so it is
                // resolved from the subscription the snapshot answers. A shape
                // with no collection (graph, array) or an unknown shape_id
                // cannot be routed to a document — importing it anywhere would
                // merge foreign state into an unrelated collection.
                if !snapshot.data.is_empty() {
                    let collection = {
                        let shapes = client.shapes().lock().await;
                        shapes
                            .get(&snapshot.shape_id)
                            .and_then(|sub| sub.definition.collection().map(str::to_string))
                    };
                    match collection {
                        Some(collection) => delegate.import_remote(&collection, &snapshot.data),
                        None => tracing::error!(
                            shape_id = %snapshot.shape_id,
                            "ShapeSnapshot has no resolvable collection — discarding payload \
                             rather than importing it into an unrelated document"
                        ),
                    }
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
                    delegate.import_remote(&delta.collection, &delta.delta);
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
                if let Some(ack) = delegate.handle_array_delta(&msg).await {
                    client.set_pending_array_ack(ack).await;
                }
            } else {
                tracing::warn!("ArrayDelta: failed to decode frame body");
            }
        }
        SyncMessageType::ArrayDeltaBatch => {
            if let Some(msg) = frame.decode_body::<nodedb_types::sync::wire::ArrayDeltaBatchMsg>() {
                if let Some(ack) = delegate.handle_array_delta_batch(&msg).await {
                    client.set_pending_array_ack(ack).await;
                }
            } else {
                tracing::warn!("ArrayDeltaBatch: failed to decode frame body");
            }
        }
        SyncMessageType::ArraySnapshot => {
            // Origin's catch-up server sends the header first, then the
            // chunks, on this ordered stream.
            if let Some(msg) = frame.decode_body::<nodedb_types::sync::wire::ArraySnapshotMsg>() {
                delegate.handle_array_snapshot(&msg).await;
            } else {
                tracing::warn!("ArraySnapshot: failed to decode frame body");
            }
        }
        SyncMessageType::ArraySnapshotChunk => {
            if let Some(msg) =
                frame.decode_body::<nodedb_types::sync::wire::ArraySnapshotChunkMsg>()
            {
                // The ack for the last chunk carries the snapshot HLC, which
                // advances Origin's GC frontier for this replica.
                if let Some(ack) = delegate.handle_array_snapshot_chunk(&msg).await {
                    client.set_pending_array_ack(ack).await;
                }
            } else {
                tracing::warn!("ArraySnapshotChunk: failed to decode frame body");
            }
        }
        SyncMessageType::ArrayReject => {
            super::dispatch_array::handle_array_reject(client, delegate, frame).await;
        }
        SyncMessageType::ArrayAdvisoryRefused => {
            super::dispatch_array::handle_array_advisory_refused(frame);
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
        SyncMessageType::KvPushAck => {
            super::dispatch_kv::handle_kv_push_ack(client, delegate, frame).await;
        }
        SyncMessageType::PingPong => {
            super::dispatch_array::handle_ping_pong(client, delegate, frame).await;
        }
        // Types Lite has no handler for: client-to-server frames, a second
        // handshake ack, and server frames Lite does not act on. Origin logs
        // an unhandled type at warn and keeps the session, so Lite does too.
        SyncMessageType::Handshake
        | SyncMessageType::HandshakeAck
        | SyncMessageType::DeltaPush
        | SyncMessageType::CollectionPurged
        | SyncMessageType::RowPushReject
        | SyncMessageType::ShapeSubscribe
        | SyncMessageType::ShapeUnsubscribe
        | SyncMessageType::TimeseriesPush
        | SyncMessageType::Throttle
        | SyncMessageType::TokenRefresh
        | SyncMessageType::PresenceUpdate
        | SyncMessageType::PresenceBroadcast
        | SyncMessageType::PresenceLeave
        | SyncMessageType::ArraySchema
        | SyncMessageType::ArrayAck
        | SyncMessageType::ArrayCatchupRequest
        | SyncMessageType::ArrayOpResults
        | SyncMessageType::ColumnarInsert
        | SyncMessageType::VectorInsert
        | SyncMessageType::VectorDelete
        | SyncMessageType::FtsIndex
        | SyncMessageType::FtsDelete
        | SyncMessageType::SpatialInsert
        | SyncMessageType::SpatialDelete
        | SyncMessageType::KvPush => warn_unhandled(frame),
        // `SyncMessageType` is `#[non_exhaustive]`, so a match outside its
        // crate needs this arm. A type added later is unhandled the same way.
        _ => warn_unhandled(frame),
    }
}

/// Log a frame of a type Lite does not handle. The session stays open.
fn warn_unhandled(frame: &SyncFrame) {
    tracing::warn!(
        msg_type = ?frame.msg_type,
        msg_type_code = frame.msg_type as u8,
        frame_len = frame.body.len(),
        "unhandled sync message type from Origin; frame ignored"
    );
}
