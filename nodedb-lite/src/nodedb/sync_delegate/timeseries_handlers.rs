//! Free functions extracted from the timeseries-related `SyncDelegate` methods.
//!
//! These are called from the thin delegation methods in `delegate_impl.rs` to
//! keep the `impl SyncDelegate` block concise.

use crate::nodedb::core::NodeDbLite;
use crate::storage::engine::StorageEngine;

pub(super) async fn pending_timeseries_batches_impl<S: StorageEngine>(
    db: &NodeDbLite<S>,
) -> Vec<(
    Vec<u8>,
    crate::sync::outbound::timeseries::PendingTimeseriesBatch,
)> {
    match &db.timeseries_outbound {
        Some(q) => q
            .drain_batch(crate::sync::PUSH_DRAIN_LIMIT)
            .await
            .unwrap_or_default(),
        None => Vec::new(),
    }
}

pub(super) async fn mark_timeseries_batch_in_flight_impl<S: StorageEngine>(
    db: &NodeDbLite<S>,
    stream_seq: u64,
    batch_id: u64,
    durable_key: Vec<u8>,
) {
    if let Some(q) = &db.timeseries_outbound {
        q.mark_in_flight_by_seq(stream_seq, batch_id, durable_key)
            .await;
    }
}

/// Retire exactly the batch Origin terminally rejected.
///
/// Separate from the `applied_seq` sweep because a rejected batch never
/// advances the frontier — see `TimeseriesOutbound::ack_in_flight_by_batch_id`.
pub(super) async fn ack_timeseries_batch_by_id_impl<S: StorageEngine>(
    db: &NodeDbLite<S>,
    batch_id: u64,
) {
    if let Some(q) = &db.timeseries_outbound
        && let Some(key) = q.ack_in_flight_by_batch_id(batch_id).await
        && let Err(e) = q.ack_keys(&[key]).await
    {
        tracing::warn!(batch_id, error = %e, "timeseries rejected-batch ack_keys failed");
    }
}

pub(super) async fn ack_timeseries_batches_through_seq_impl<S: StorageEngine>(
    db: &NodeDbLite<S>,
    applied_seq: u64,
) {
    if let Some(q) = &db.timeseries_outbound {
        let keys = q.ack_in_flight_through_seq(applied_seq).await;
        for key in keys {
            if let Err(e) = q.ack_keys(&[key]).await {
                tracing::warn!(applied_seq, error = %e, "timeseries in-flight ack_keys failed");
            }
        }
    }
}

pub(super) async fn acknowledge_timeseries_batch_impl<S: StorageEngine>(
    db: &NodeDbLite<S>,
    durable_key: Vec<u8>,
) {
    if let Some(q) = &db.timeseries_outbound
        && let Err(e) = q.ack_keys(&[durable_key]).await
    {
        tracing::warn!(error = %e, "timeseries outbound ack_keys failed");
    }
}

pub(super) async fn persist_timeseries_seq_impl<S: StorageEngine>(
    db: &NodeDbLite<S>,
    key: &[u8],
    batch: &crate::sync::outbound::timeseries::PendingTimeseriesBatch,
) -> Result<(), crate::error::LiteError> {
    match &db.timeseries_outbound {
        Some(q) => q.update_entry(key, batch).await,
        None => Ok(()),
    }
}
