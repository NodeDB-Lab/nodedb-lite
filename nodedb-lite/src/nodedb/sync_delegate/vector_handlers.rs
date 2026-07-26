//! Free functions extracted from the vector-related `SyncDelegate` methods.
//!
//! These are called from the thin delegation methods in `delegate_impl.rs` to
//! keep the `impl SyncDelegate` block concise.

use crate::nodedb::core::NodeDbLite;
use crate::storage::engine::StorageEngine;

pub(super) async fn pending_vector_inserts_impl<S: StorageEngine>(
    db: &NodeDbLite<S>,
) -> Vec<(Vec<u8>, crate::sync::outbound::vector::PendingVectorInsert)> {
    match &db.vector_outbound {
        Some(q) => q
            .drain_inserts(crate::sync::PUSH_DRAIN_LIMIT)
            .await
            .unwrap_or_default(),
        None => Vec::new(),
    }
}

pub(super) async fn mark_vector_insert_in_flight_impl<S: StorageEngine>(
    db: &NodeDbLite<S>,
    batch_id: u64,
    durable_key: Vec<u8>,
) {
    if let Some(q) = &db.vector_outbound {
        q.mark_insert_in_flight(batch_id, durable_key).await;
    }
}

pub(super) async fn ack_vector_insert_in_flight_impl<S: StorageEngine>(
    db: &NodeDbLite<S>,
    batch_id: u64,
) {
    if let Some(q) = &db.vector_outbound
        && let Some(key) = q.ack_insert_in_flight(batch_id).await
        && let Err(e) = q.ack_insert_keys(&[key]).await
    {
        tracing::warn!(batch_id, error = %e, "vector insert in-flight ack_keys failed");
    }
}

pub(super) async fn acknowledge_vector_insert_impl<S: StorageEngine>(
    db: &NodeDbLite<S>,
    durable_key: Vec<u8>,
) {
    if let Some(q) = &db.vector_outbound
        && let Err(e) = q.ack_insert_keys(&[durable_key]).await
    {
        tracing::warn!(error = %e, "vector insert outbound ack_keys failed");
    }
}

pub(super) async fn pending_vector_deletes_impl<S: StorageEngine>(
    db: &NodeDbLite<S>,
) -> Vec<(Vec<u8>, crate::sync::outbound::vector::PendingVectorDelete)> {
    match &db.vector_outbound {
        Some(q) => q
            .drain_deletes(crate::sync::PUSH_DRAIN_LIMIT)
            .await
            .unwrap_or_default(),
        None => Vec::new(),
    }
}

pub(super) async fn mark_vector_delete_in_flight_impl<S: StorageEngine>(
    db: &NodeDbLite<S>,
    batch_id: u64,
    durable_key: Vec<u8>,
) {
    if let Some(q) = &db.vector_outbound {
        q.mark_delete_in_flight(batch_id, durable_key).await;
    }
}

pub(super) async fn ack_vector_delete_in_flight_impl<S: StorageEngine>(
    db: &NodeDbLite<S>,
    batch_id: u64,
) {
    if let Some(q) = &db.vector_outbound
        && let Some(key) = q.ack_delete_in_flight(batch_id).await
        && let Err(e) = q.ack_delete_keys(&[key]).await
    {
        tracing::warn!(batch_id, error = %e, "vector delete in-flight ack_keys failed");
    }
}

pub(super) async fn acknowledge_vector_delete_impl<S: StorageEngine>(
    db: &NodeDbLite<S>,
    durable_key: Vec<u8>,
) {
    if let Some(q) = &db.vector_outbound
        && let Err(e) = q.ack_delete_keys(&[durable_key]).await
    {
        tracing::warn!(error = %e, "vector delete outbound ack_keys failed");
    }
}

pub(super) async fn persist_vector_insert_seq_impl<S: StorageEngine>(
    db: &NodeDbLite<S>,
    key: &[u8],
    insert: &crate::sync::outbound::vector::PendingVectorInsert,
) -> Result<(), crate::error::LiteError> {
    match &db.vector_outbound {
        Some(q) => q.update_insert_entry(key, insert).await,
        None => Ok(()),
    }
}

pub(super) async fn persist_vector_delete_seq_impl<S: StorageEngine>(
    db: &NodeDbLite<S>,
    key: &[u8],
    delete: &crate::sync::outbound::vector::PendingVectorDelete,
) -> Result<(), crate::error::LiteError> {
    match &db.vector_outbound {
        Some(q) => q.update_delete_entry(key, delete).await,
        None => Ok(()),
    }
}
