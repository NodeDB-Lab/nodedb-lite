//! Free functions extracted from the columnar-related `SyncDelegate` methods.
//!
//! These are called from the thin delegation methods in `delegate_impl.rs` to
//! keep the `impl SyncDelegate` block concise.

use crate::nodedb::core::NodeDbLite;
use crate::storage::engine::StorageEngine;

pub(super) async fn pending_columnar_batches_impl<S: StorageEngine>(
    db: &NodeDbLite<S>,
) -> Vec<(
    Vec<u8>,
    crate::sync::outbound::columnar::PendingColumnarBatch,
)> {
    match &db.columnar_outbound {
        Some(q) => q
            .drain_batch(crate::sync::PUSH_DRAIN_LIMIT)
            .await
            .unwrap_or_default(),
        None => Vec::new(),
    }
}

pub(super) async fn mark_columnar_batch_in_flight_impl<S: StorageEngine>(
    db: &NodeDbLite<S>,
    batch_id: u64,
    durable_key: Vec<u8>,
) {
    if let Some(q) = &db.columnar_outbound {
        q.mark_in_flight(batch_id, durable_key).await;
    }
}

pub(super) async fn ack_columnar_batch_in_flight_impl<S: StorageEngine>(
    db: &NodeDbLite<S>,
    batch_id: u64,
) {
    if let Some(q) = &db.columnar_outbound
        && let Some(key) = q.ack_in_flight(batch_id).await
        && let Err(e) = q.ack_keys(&[key]).await
    {
        tracing::warn!(batch_id, error = %e, "columnar in-flight ack_keys failed");
    }
}

pub(super) async fn acknowledge_columnar_batch_impl<S: StorageEngine>(
    db: &NodeDbLite<S>,
    durable_key: Vec<u8>,
) {
    if let Some(q) = &db.columnar_outbound
        && let Err(e) = q.ack_keys(&[durable_key]).await
    {
        tracing::warn!(error = %e, "columnar outbound ack_keys failed");
    }
}

pub(super) async fn persist_columnar_seq_impl<S: StorageEngine>(
    db: &NodeDbLite<S>,
    key: &[u8],
    batch: &crate::sync::outbound::columnar::PendingColumnarBatch,
) -> Result<(), crate::error::LiteError> {
    match &db.columnar_outbound {
        Some(q) => q.update_entry(key, batch).await,
        None => Ok(()),
    }
}
