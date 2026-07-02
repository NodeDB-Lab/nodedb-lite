//! Engine-level ack dispatch helpers — called from `dispatch_frame`.
//!
//! Each function handles one ack message type, applies `AckStatus` handling
//! (frontier-advance on Applied/Duplicate, fenced-flag on Fenced, warn on Gap),
//! then calls the appropriate `delegate.acknowledge_*` method.

use std::sync::Arc;

use nodedb_types::sync::wire::{AckStatus, EngineKind, SyncFrame, stream_id_for};

use super::delegate::SyncDelegate;
use crate::sync::client::SyncClient;

pub(super) async fn handle_columnar_insert_ack(
    client: &Arc<SyncClient>,
    delegate: &Arc<dyn SyncDelegate>,
    frame: &SyncFrame,
) {
    let Some(ack) = frame.decode_body::<nodedb_types::sync::wire::ColumnarInsertAckMsg>() else {
        return;
    };
    tracing::debug!(
        collection = %ack.collection,
        batch_id = ack.batch_id,
        accepted = ack.accepted,
        rejected = ack.rejected,
        "ColumnarInsertAck received from Origin"
    );
    match &ack.status {
        AckStatus::Applied | AckStatus::Duplicate => {
            let stream_id = stream_id_for(EngineKind::Columnar, &ack.collection);
            delegate.record_stream_ack(stream_id, ack.applied_seq).await;
            // Delete-on-ack: remove the in-flight record and delete the durable entry.
            delegate.ack_columnar_batch_in_flight(ack.batch_id).await;
        }
        AckStatus::Fenced => {
            tracing::error!(
                collection = %ack.collection,
                batch_id = ack.batch_id,
                "ColumnarInsertAck: producer fenced by Origin; halting push"
            );
            client.set_fenced();
        }
        AckStatus::Gap { expected } => {
            tracing::warn!(
                collection = %ack.collection,
                batch_id = ack.batch_id,
                expected,
                applied_seq = ack.applied_seq,
                "ColumnarInsertAck: sequence gap detected by Origin; re-draining un-acked entries from expected"
            );
            // Re-drain by clearing all in-flight maps: the push loop will
            // re-send every durable un-acked entry starting from the oldest,
            // which is ≤ expected. Origin deduplicates already-applied entries
            // via its idempotent gate.
            delegate.clear_engine_in_flight().await;
        }
    }
}

pub(super) async fn handle_vector_insert_ack(
    client: &Arc<SyncClient>,
    delegate: &Arc<dyn SyncDelegate>,
    frame: &SyncFrame,
) {
    let Some(ack) = frame.decode_body::<nodedb_types::sync::wire::VectorInsertAckMsg>() else {
        return;
    };
    tracing::debug!(
        collection = %ack.collection,
        id = %ack.id,
        batch_id = ack.batch_id,
        accepted = ack.accepted,
        "VectorInsertAck received from Origin"
    );
    match &ack.status {
        AckStatus::Applied | AckStatus::Duplicate => {
            let stream_id = stream_id_for(EngineKind::Vector, &ack.collection);
            delegate.record_stream_ack(stream_id, ack.applied_seq).await;
            // Delete-on-ack: remove the in-flight record and delete the durable entry.
            delegate.ack_vector_insert_in_flight(ack.batch_id).await;
        }
        AckStatus::Fenced => {
            tracing::error!(
                collection = %ack.collection,
                batch_id = ack.batch_id,
                "VectorInsertAck: producer fenced by Origin; halting push"
            );
            client.set_fenced();
        }
        AckStatus::Gap { expected } => {
            tracing::warn!(
                collection = %ack.collection,
                batch_id = ack.batch_id,
                expected,
                applied_seq = ack.applied_seq,
                "VectorInsertAck: sequence gap detected by Origin; re-draining un-acked entries from expected"
            );
            delegate.clear_engine_in_flight().await;
        }
    }
    if !ack.accepted {
        tracing::warn!(
            collection = %ack.collection,
            id = %ack.id,
            reason = ?ack.reject_reason,
            "VectorInsert rejected by Origin"
        );
    }
}

pub(super) async fn handle_vector_delete_ack(
    client: &Arc<SyncClient>,
    delegate: &Arc<dyn SyncDelegate>,
    frame: &SyncFrame,
) {
    let Some(ack) = frame.decode_body::<nodedb_types::sync::wire::VectorDeleteAckMsg>() else {
        return;
    };
    tracing::debug!(
        collection = %ack.collection,
        id = %ack.id,
        batch_id = ack.batch_id,
        accepted = ack.accepted,
        "VectorDeleteAck received from Origin"
    );
    match &ack.status {
        AckStatus::Applied | AckStatus::Duplicate => {
            let stream_id = stream_id_for(EngineKind::Vector, &ack.collection);
            delegate.record_stream_ack(stream_id, ack.applied_seq).await;
            // Delete-on-ack: remove the in-flight record and delete the durable entry.
            delegate.ack_vector_delete_in_flight(ack.batch_id).await;
        }
        AckStatus::Fenced => {
            tracing::error!(
                collection = %ack.collection,
                batch_id = ack.batch_id,
                "VectorDeleteAck: producer fenced by Origin; halting push"
            );
            client.set_fenced();
        }
        AckStatus::Gap { expected } => {
            tracing::warn!(
                collection = %ack.collection,
                batch_id = ack.batch_id,
                expected,
                applied_seq = ack.applied_seq,
                "VectorDeleteAck: sequence gap detected by Origin; re-draining un-acked entries from expected"
            );
            delegate.clear_engine_in_flight().await;
        }
    }
}

pub(super) async fn handle_fts_index_ack(
    client: &Arc<SyncClient>,
    delegate: &Arc<dyn SyncDelegate>,
    frame: &SyncFrame,
) {
    let Some(ack) = frame.decode_body::<nodedb_types::sync::wire::FtsIndexAckMsg>() else {
        return;
    };
    tracing::debug!(
        collection = %ack.collection,
        doc_id = %ack.doc_id,
        batch_id = ack.batch_id,
        accepted = ack.accepted,
        "FtsIndexAck received from Origin"
    );
    match &ack.status {
        AckStatus::Applied | AckStatus::Duplicate => {
            let stream_id = stream_id_for(EngineKind::Fts, &ack.collection);
            delegate.record_stream_ack(stream_id, ack.applied_seq).await;
            // Delete-on-ack: remove the in-flight record and delete the durable entry.
            delegate.ack_fts_index_in_flight(ack.batch_id).await;
        }
        AckStatus::Fenced => {
            tracing::error!(
                collection = %ack.collection,
                batch_id = ack.batch_id,
                "FtsIndexAck: producer fenced by Origin; halting push"
            );
            client.set_fenced();
        }
        AckStatus::Gap { expected } => {
            tracing::warn!(
                collection = %ack.collection,
                batch_id = ack.batch_id,
                expected,
                applied_seq = ack.applied_seq,
                "FtsIndexAck: sequence gap detected by Origin; re-draining un-acked entries from expected"
            );
            delegate.clear_engine_in_flight().await;
        }
    }
}

pub(super) async fn handle_fts_delete_ack(
    client: &Arc<SyncClient>,
    delegate: &Arc<dyn SyncDelegate>,
    frame: &SyncFrame,
) {
    let Some(ack) = frame.decode_body::<nodedb_types::sync::wire::FtsDeleteAckMsg>() else {
        return;
    };
    tracing::debug!(
        collection = %ack.collection,
        doc_id = %ack.doc_id,
        batch_id = ack.batch_id,
        accepted = ack.accepted,
        "FtsDeleteAck received from Origin"
    );
    match &ack.status {
        AckStatus::Applied | AckStatus::Duplicate => {
            let stream_id = stream_id_for(EngineKind::Fts, &ack.collection);
            delegate.record_stream_ack(stream_id, ack.applied_seq).await;
            // Delete-on-ack: remove the in-flight record and delete the durable entry.
            delegate.ack_fts_delete_in_flight(ack.batch_id).await;
        }
        AckStatus::Fenced => {
            tracing::error!(
                collection = %ack.collection,
                batch_id = ack.batch_id,
                "FtsDeleteAck: producer fenced by Origin; halting push"
            );
            client.set_fenced();
        }
        AckStatus::Gap { expected } => {
            tracing::warn!(
                collection = %ack.collection,
                batch_id = ack.batch_id,
                expected,
                applied_seq = ack.applied_seq,
                "FtsDeleteAck: sequence gap detected by Origin; re-draining un-acked entries from expected"
            );
            delegate.clear_engine_in_flight().await;
        }
    }
}

pub(super) async fn handle_spatial_insert_ack(
    client: &Arc<SyncClient>,
    delegate: &Arc<dyn SyncDelegate>,
    frame: &SyncFrame,
) {
    let Some(ack) = frame.decode_body::<nodedb_types::sync::wire::SpatialInsertAckMsg>() else {
        return;
    };
    tracing::debug!(
        collection = %ack.collection,
        field = %ack.field,
        doc_id = %ack.doc_id,
        batch_id = ack.batch_id,
        accepted = ack.accepted,
        "SpatialInsertAck received from Origin"
    );
    match &ack.status {
        AckStatus::Applied | AckStatus::Duplicate => {
            let stream_id = stream_id_for(EngineKind::Spatial, &ack.collection);
            delegate.record_stream_ack(stream_id, ack.applied_seq).await;
            // Delete-on-ack: remove the in-flight record and delete the durable entry.
            delegate.ack_spatial_insert_in_flight(ack.batch_id).await;
        }
        AckStatus::Fenced => {
            tracing::error!(
                collection = %ack.collection,
                batch_id = ack.batch_id,
                "SpatialInsertAck: producer fenced by Origin; halting push"
            );
            client.set_fenced();
        }
        AckStatus::Gap { expected } => {
            tracing::warn!(
                collection = %ack.collection,
                batch_id = ack.batch_id,
                expected,
                applied_seq = ack.applied_seq,
                "SpatialInsertAck: sequence gap detected by Origin; re-draining un-acked entries from expected"
            );
            delegate.clear_engine_in_flight().await;
        }
    }
}

pub(super) async fn handle_spatial_delete_ack(
    client: &Arc<SyncClient>,
    delegate: &Arc<dyn SyncDelegate>,
    frame: &SyncFrame,
) {
    let Some(ack) = frame.decode_body::<nodedb_types::sync::wire::SpatialDeleteAckMsg>() else {
        return;
    };
    tracing::debug!(
        collection = %ack.collection,
        field = %ack.field,
        doc_id = %ack.doc_id,
        batch_id = ack.batch_id,
        accepted = ack.accepted,
        "SpatialDeleteAck received from Origin"
    );
    match &ack.status {
        AckStatus::Applied | AckStatus::Duplicate => {
            let stream_id = stream_id_for(EngineKind::Spatial, &ack.collection);
            delegate.record_stream_ack(stream_id, ack.applied_seq).await;
            // Delete-on-ack: remove the in-flight record and delete the durable entry.
            delegate.ack_spatial_delete_in_flight(ack.batch_id).await;
        }
        AckStatus::Fenced => {
            tracing::error!(
                collection = %ack.collection,
                batch_id = ack.batch_id,
                "SpatialDeleteAck: producer fenced by Origin; halting push"
            );
            client.set_fenced();
        }
        AckStatus::Gap { expected } => {
            tracing::warn!(
                collection = %ack.collection,
                batch_id = ack.batch_id,
                expected,
                applied_seq = ack.applied_seq,
                "SpatialDeleteAck: sequence gap detected by Origin; re-draining un-acked entries from expected"
            );
            delegate.clear_engine_in_flight().await;
        }
    }
}

pub(super) async fn handle_timeseries_ack(
    client: &Arc<SyncClient>,
    delegate: &Arc<dyn SyncDelegate>,
    frame: &SyncFrame,
) {
    let Some(ack) = frame.decode_body::<nodedb_types::sync::wire::TimeseriesAckMsg>() else {
        return;
    };
    tracing::debug!(
        collection = %ack.collection,
        accepted = ack.accepted,
        rejected = ack.rejected,
        lsn = ack.lsn,
        "TimeseriesAck received from Origin"
    );
    match &ack.status {
        AckStatus::Applied | AckStatus::Duplicate => {
            let stream_id = stream_id_for(EngineKind::Timeseries, &ack.collection);
            delegate.record_stream_ack(stream_id, ack.applied_seq).await;
            // Delete-on-ack: delete all durable entries whose seq ≤ applied_seq.
            delegate
                .ack_timeseries_batches_through_seq(ack.applied_seq)
                .await;
        }
        AckStatus::Fenced => {
            tracing::error!(
                collection = %ack.collection,
                "TimeseriesAck: producer fenced by Origin; halting push"
            );
            client.set_fenced();
        }
        AckStatus::Gap { expected } => {
            tracing::warn!(
                collection = %ack.collection,
                expected,
                applied_seq = ack.applied_seq,
                "TimeseriesAck: sequence gap detected by Origin; re-draining un-acked entries from expected"
            );
            delegate.clear_engine_in_flight().await;
        }
    }
}
