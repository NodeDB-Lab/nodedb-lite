//! Free functions extracted from the FTS-related `SyncDelegate` methods.
//!
//! These are called from the thin delegation methods in `delegate_impl.rs` to
//! keep the `impl SyncDelegate` block concise.

use crate::nodedb::core::NodeDbLite;
use crate::storage::engine::StorageEngine;

pub(super) async fn pending_fts_indexes_impl<S: StorageEngine>(
    db: &NodeDbLite<S>,
) -> Vec<(Vec<u8>, crate::sync::outbound::fts::PendingFtsIndex)> {
    match &db.fts_outbound {
        Some(q) => q
            .drain_indexes(crate::sync::PUSH_DRAIN_LIMIT)
            .await
            .unwrap_or_default(),
        None => Vec::new(),
    }
}

pub(super) async fn mark_fts_index_in_flight_impl<S: StorageEngine>(
    db: &NodeDbLite<S>,
    batch_id: u64,
    durable_key: Vec<u8>,
) {
    if let Some(q) = &db.fts_outbound {
        q.mark_index_in_flight(batch_id, durable_key).await;
    }
}

pub(super) async fn ack_fts_index_in_flight_impl<S: StorageEngine>(
    db: &NodeDbLite<S>,
    batch_id: u64,
) {
    if let Some(q) = &db.fts_outbound
        && let Some(key) = q.ack_index_in_flight(batch_id).await
        && let Err(e) = q.ack_index_keys(&[key]).await
    {
        tracing::warn!(batch_id, error = %e, "fts index in-flight ack_keys failed");
    }
}

pub(super) async fn acknowledge_fts_index_impl<S: StorageEngine>(
    db: &NodeDbLite<S>,
    durable_key: Vec<u8>,
) {
    if let Some(q) = &db.fts_outbound
        && let Err(e) = q.ack_index_keys(&[durable_key]).await
    {
        tracing::warn!(error = %e, "fts index outbound ack_keys failed");
    }
}

pub(super) async fn pending_fts_deletes_impl<S: StorageEngine>(
    db: &NodeDbLite<S>,
) -> Vec<(Vec<u8>, crate::sync::outbound::fts::PendingFtsDelete)> {
    match &db.fts_outbound {
        Some(q) => q
            .drain_deletes(crate::sync::PUSH_DRAIN_LIMIT)
            .await
            .unwrap_or_default(),
        None => Vec::new(),
    }
}

pub(super) async fn mark_fts_delete_in_flight_impl<S: StorageEngine>(
    db: &NodeDbLite<S>,
    batch_id: u64,
    durable_key: Vec<u8>,
) {
    if let Some(q) = &db.fts_outbound {
        q.mark_delete_in_flight(batch_id, durable_key).await;
    }
}

pub(super) async fn ack_fts_delete_in_flight_impl<S: StorageEngine>(
    db: &NodeDbLite<S>,
    batch_id: u64,
) {
    if let Some(q) = &db.fts_outbound
        && let Some(key) = q.ack_delete_in_flight(batch_id).await
        && let Err(e) = q.ack_delete_keys(&[key]).await
    {
        tracing::warn!(batch_id, error = %e, "fts delete in-flight ack_keys failed");
    }
}

pub(super) async fn acknowledge_fts_delete_impl<S: StorageEngine>(
    db: &NodeDbLite<S>,
    durable_key: Vec<u8>,
) {
    if let Some(q) = &db.fts_outbound
        && let Err(e) = q.ack_delete_keys(&[durable_key]).await
    {
        tracing::warn!(error = %e, "fts delete outbound ack_keys failed");
    }
}

pub(super) async fn persist_fts_index_seq_impl<S: StorageEngine>(
    db: &NodeDbLite<S>,
    key: &[u8],
    entry: &crate::sync::outbound::fts::PendingFtsIndex,
) -> Result<(), crate::error::LiteError> {
    match &db.fts_outbound {
        Some(q) => q.update_index_entry(key, entry).await,
        None => Ok(()),
    }
}

pub(super) async fn persist_fts_delete_seq_impl<S: StorageEngine>(
    db: &NodeDbLite<S>,
    key: &[u8],
    entry: &crate::sync::outbound::fts::PendingFtsDelete,
) -> Result<(), crate::error::LiteError> {
    match &db.fts_outbound {
        Some(q) => q.update_delete_entry(key, entry).await,
        None => Ok(()),
    }
}
