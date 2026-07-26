//! Free functions extracted from the spatial-related `SyncDelegate` methods.
//!
//! These are called from the thin delegation methods in `delegate_impl.rs` to
//! keep the `impl SyncDelegate` block concise.

use crate::nodedb::core::NodeDbLite;
use crate::storage::engine::StorageEngine;

pub(super) async fn pending_spatial_inserts_impl<S: StorageEngine>(
    db: &NodeDbLite<S>,
) -> Vec<(
    Vec<u8>,
    crate::sync::outbound::spatial::PendingSpatialInsert,
)> {
    match &db.spatial_outbound {
        Some(q) => q
            .drain_inserts(crate::sync::PUSH_DRAIN_LIMIT)
            .await
            .unwrap_or_default(),
        None => Vec::new(),
    }
}

pub(super) async fn mark_spatial_insert_in_flight_impl<S: StorageEngine>(
    db: &NodeDbLite<S>,
    batch_id: u64,
    durable_key: Vec<u8>,
) {
    if let Some(q) = &db.spatial_outbound {
        q.mark_insert_in_flight(batch_id, durable_key).await;
    }
}

pub(super) async fn ack_spatial_insert_in_flight_impl<S: StorageEngine>(
    db: &NodeDbLite<S>,
    batch_id: u64,
) {
    if let Some(q) = &db.spatial_outbound
        && let Some(key) = q.ack_insert_in_flight(batch_id).await
        && let Err(e) = q.ack_insert_keys(&[key]).await
    {
        tracing::warn!(batch_id, error = %e, "spatial insert in-flight ack_keys failed");
    }
}

pub(super) async fn acknowledge_spatial_insert_impl<S: StorageEngine>(
    db: &NodeDbLite<S>,
    durable_key: Vec<u8>,
) {
    if let Some(q) = &db.spatial_outbound
        && let Err(e) = q.ack_insert_keys(&[durable_key]).await
    {
        tracing::warn!(error = %e, "spatial insert outbound ack_keys failed");
    }
}

pub(super) async fn pending_spatial_deletes_impl<S: StorageEngine>(
    db: &NodeDbLite<S>,
) -> Vec<(
    Vec<u8>,
    crate::sync::outbound::spatial::PendingSpatialDelete,
)> {
    match &db.spatial_outbound {
        Some(q) => q
            .drain_deletes(crate::sync::PUSH_DRAIN_LIMIT)
            .await
            .unwrap_or_default(),
        None => Vec::new(),
    }
}

pub(super) async fn mark_spatial_delete_in_flight_impl<S: StorageEngine>(
    db: &NodeDbLite<S>,
    batch_id: u64,
    durable_key: Vec<u8>,
) {
    if let Some(q) = &db.spatial_outbound {
        q.mark_delete_in_flight(batch_id, durable_key).await;
    }
}

pub(super) async fn ack_spatial_delete_in_flight_impl<S: StorageEngine>(
    db: &NodeDbLite<S>,
    batch_id: u64,
) {
    if let Some(q) = &db.spatial_outbound
        && let Some(key) = q.ack_delete_in_flight(batch_id).await
        && let Err(e) = q.ack_delete_keys(&[key]).await
    {
        tracing::warn!(batch_id, error = %e, "spatial delete in-flight ack_keys failed");
    }
}

pub(super) async fn acknowledge_spatial_delete_impl<S: StorageEngine>(
    db: &NodeDbLite<S>,
    durable_key: Vec<u8>,
) {
    if let Some(q) = &db.spatial_outbound
        && let Err(e) = q.ack_delete_keys(&[durable_key]).await
    {
        tracing::warn!(error = %e, "spatial delete outbound ack_keys failed");
    }
}

pub(super) async fn persist_spatial_insert_seq_impl<S: StorageEngine>(
    db: &NodeDbLite<S>,
    key: &[u8],
    insert: &crate::sync::outbound::spatial::PendingSpatialInsert,
) -> Result<(), crate::error::LiteError> {
    match &db.spatial_outbound {
        Some(q) => q.update_insert_entry(key, insert).await,
        None => Ok(()),
    }
}

pub(super) async fn persist_spatial_delete_seq_impl<S: StorageEngine>(
    db: &NodeDbLite<S>,
    key: &[u8],
    delete: &crate::sync::outbound::spatial::PendingSpatialDelete,
) -> Result<(), crate::error::LiteError> {
    match &db.spatial_outbound {
        Some(q) => q.update_delete_entry(key, delete).await,
        None => Ok(()),
    }
}
