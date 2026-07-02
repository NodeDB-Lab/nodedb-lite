//! Vector insert / delete outbound queue for Lite sync.
//!
//! When `NodeDbLite::vector_insert_impl` or `vector_delete_impl` is called,
//! the operation is durably enqueued here. The sync transport drains this queue
//! on every tick and sends `VectorInsert` (0xA2) / `VectorDelete` (0xA4) wire
//! frames to Origin.
//!
//! # Durability
//!
//! Inserts are backed by [`DurableOutboundQueue`] in
//! [`Namespace::VectorInsertPending`]; deletes use
//! [`Namespace::VectorDeletePending`]. Each entry is persisted before the
//! enqueue call returns. Entries survive process restarts; un-acked entries are
//! re-drained on reconnect (at-least-once delivery).
//!
//! # Backpressure
//!
//! `enqueue_insert` / `enqueue_delete` return [`LiteError::Backpressure`] when
//! their respective queue is at cap, propagating to the write caller so the
//! device can pause until the sync transport drains the backlog.
//!
//! # Design: two queues, two namespaces
//!
//! Inserts and deletes are kept in separate queues so the push transport can
//! drain and ack them independently. A shared queue with a 1-byte op tag would
//! require the push path to branch on type after decoding; two queues keep each
//! drain path homogeneous and mirror the columnar/timeseries pattern exactly.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use nodedb_types::Namespace;
use tokio::sync::Mutex;

use super::durable_queue::DurableOutboundQueue;
use crate::error::LiteError;
use crate::storage::engine::StorageEngine;

/// A single pending vector insert awaiting sync to Origin.
#[derive(
    Debug,
    Clone,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct PendingVectorInsert {
    /// Monotonic batch ID for ACK correlation.
    pub batch_id: u64,
    /// Collection name.
    pub collection: String,
    /// Document ID.
    pub id: String,
    /// Raw embedding values.
    pub vector: Vec<f32>,
    /// Embedding dimension.
    pub dim: usize,
    /// Named vector field; empty string = default (no named field).
    pub field_name: String,
    /// Stable idempotent-producer seq for this entry. 0 = not yet assigned;
    /// assigned at first drain and persisted so re-sends after reconnect reuse
    /// the same seq (Origin dedups instead of double-applying).
    #[serde(default)]
    pub seq: u64,
}

/// A single pending vector delete awaiting sync to Origin.
#[derive(
    Debug,
    Clone,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct PendingVectorDelete {
    /// Monotonic batch ID for ACK correlation.
    pub batch_id: u64,
    /// Collection name.
    pub collection: String,
    /// Document ID.
    pub id: String,
    /// Named vector field; empty string = default (no named field).
    pub field_name: String,
    /// Stable idempotent-producer seq for this entry. 0 = not yet assigned;
    /// assigned at first drain and persisted so re-sends after reconnect reuse
    /// the same seq (Origin dedups instead of double-applying).
    #[serde(default)]
    pub seq: u64,
}

/// Durable outbound queue for vector insert and delete sync.
///
/// Held as `Arc<VectorOutbound<S>>` by `NodeDbLite` and shared with the sync
/// transport. The inner storage is accessed only for `enqueue_insert` /
/// `enqueue_delete` (from the write path) and `drain_inserts` /
/// `drain_deletes` / `ack_*` (from the async sync transport task).
pub struct VectorOutbound<S: StorageEngine> {
    inserts: DurableOutboundQueue<S>,
    deletes: DurableOutboundQueue<S>,
    ids: AtomicU64,
    /// batch_id → durable_key for in-flight insert entries.
    in_flight_inserts: Mutex<HashMap<u64, Vec<u8>>>,
    /// batch_id → durable_key for in-flight delete entries.
    in_flight_deletes: Mutex<HashMap<u64, Vec<u8>>>,
}

impl<S: StorageEngine> VectorOutbound<S> {
    /// Open durable queues backed by [`Namespace::VectorInsertPending`] and
    /// [`Namespace::VectorDeletePending`].
    pub async fn open(storage: Arc<S>) -> Result<Self, LiteError> {
        Self::open_with_cap(storage, DurableOutboundQueue::<S>::DEFAULT_CAP).await
    }

    /// Open with a custom cap (useful for tests with small limits).
    pub async fn open_with_cap(storage: Arc<S>, cap: usize) -> Result<Self, LiteError> {
        let inserts = DurableOutboundQueue::open_with_cap(
            Arc::clone(&storage),
            Namespace::VectorInsertPending,
            cap,
        )
        .await?;
        let deletes = DurableOutboundQueue::open_with_cap(
            Arc::clone(&storage),
            Namespace::VectorDeletePending,
            cap,
        )
        .await?;
        Ok(Self {
            inserts,
            deletes,
            ids: AtomicU64::new(1),
            in_flight_inserts: Mutex::new(HashMap::new()),
            in_flight_deletes: Mutex::new(HashMap::new()),
        })
    }

    /// Durably enqueue a vector insert.
    ///
    /// Returns [`LiteError::Backpressure`] when the insert queue is at cap.
    pub async fn enqueue_insert(
        &self,
        collection: &str,
        id: &str,
        vector: Vec<f32>,
        dim: usize,
        field_name: &str,
    ) -> Result<(), LiteError> {
        let batch_id = self.ids.fetch_add(1, Ordering::Relaxed);
        let entry = PendingVectorInsert {
            batch_id,
            collection: collection.to_string(),
            id: id.to_string(),
            vector,
            dim,
            field_name: field_name.to_string(),
            seq: 0,
        };
        let payload = zerompk::to_msgpack_vec(&entry).map_err(|e| LiteError::Serialization {
            detail: format!("vector insert outbound encode: {e}"),
        })?;
        self.inserts.enqueue(&payload).await
    }

    /// Durably enqueue a vector delete.
    ///
    /// Returns [`LiteError::Backpressure`] when the delete queue is at cap.
    pub async fn enqueue_delete(
        &self,
        collection: &str,
        id: &str,
        field_name: &str,
    ) -> Result<(), LiteError> {
        let batch_id = self.ids.fetch_add(1, Ordering::Relaxed);
        let entry = PendingVectorDelete {
            batch_id,
            collection: collection.to_string(),
            id: id.to_string(),
            field_name: field_name.to_string(),
            seq: 0,
        };
        let payload = zerompk::to_msgpack_vec(&entry).map_err(|e| LiteError::Serialization {
            detail: format!("vector delete outbound encode: {e}"),
        })?;
        self.deletes.enqueue(&payload).await
    }

    /// Drain up to `limit` pending inserts in FIFO order, skipping in-flight entries.
    ///
    /// Returns `(durable_key, insert)` pairs. On send success, call
    /// [`mark_insert_in_flight`]. The durable entry is deleted only on Origin
    /// ack via [`ack_insert_in_flight`].
    ///
    /// Does **not** remove entries from storage.
    pub async fn drain_inserts(
        &self,
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, PendingVectorInsert)>, LiteError> {
        let in_flight = self.in_flight_inserts.lock().await;
        let pairs = self.inserts.drain_batch(limit).await?;
        let mut out = Vec::with_capacity(pairs.len());
        for (key, payload) in pairs {
            if in_flight.values().any(|k| k == &key) {
                continue;
            }
            let entry: PendingVectorInsert =
                zerompk::from_msgpack(&payload).map_err(|e| LiteError::Serialization {
                    detail: format!("vector insert outbound decode: {e}"),
                })?;
            out.push((key, entry));
        }
        Ok(out)
    }

    /// Drain up to `limit` pending deletes in FIFO order, skipping in-flight entries.
    ///
    /// Returns `(durable_key, delete)` pairs. On send success, call
    /// [`mark_delete_in_flight`]. The durable entry is deleted only on Origin
    /// ack via [`ack_delete_in_flight`].
    ///
    /// Does **not** remove entries from storage.
    pub async fn drain_deletes(
        &self,
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, PendingVectorDelete)>, LiteError> {
        let in_flight = self.in_flight_deletes.lock().await;
        let pairs = self.deletes.drain_batch(limit).await?;
        let mut out = Vec::with_capacity(pairs.len());
        for (key, payload) in pairs {
            if in_flight.values().any(|k| k == &key) {
                continue;
            }
            let entry: PendingVectorDelete =
                zerompk::from_msgpack(&payload).map_err(|e| LiteError::Serialization {
                    detail: format!("vector delete outbound decode: {e}"),
                })?;
            out.push((key, entry));
        }
        Ok(out)
    }

    /// Record that an insert has been sent to Origin and is awaiting its ack.
    pub async fn mark_insert_in_flight(&self, batch_id: u64, durable_key: Vec<u8>) {
        self.in_flight_inserts
            .lock()
            .await
            .insert(batch_id, durable_key);
    }

    /// Remove the in-flight record for an insert and return its durable key.
    pub async fn ack_insert_in_flight(&self, batch_id: u64) -> Option<Vec<u8>> {
        self.in_flight_inserts.lock().await.remove(&batch_id)
    }

    /// Record that a delete has been sent to Origin and is awaiting its ack.
    pub async fn mark_delete_in_flight(&self, batch_id: u64, durable_key: Vec<u8>) {
        self.in_flight_deletes
            .lock()
            .await
            .insert(batch_id, durable_key);
    }

    /// Remove the in-flight record for a delete and return its durable key.
    pub async fn ack_delete_in_flight(&self, batch_id: u64) -> Option<Vec<u8>> {
        self.in_flight_deletes.lock().await.remove(&batch_id)
    }

    /// Clear all in-flight records on reconnect so entries are re-drained.
    pub async fn clear_in_flight(&self) {
        self.in_flight_inserts.lock().await.clear();
        self.in_flight_deletes.lock().await.clear();
    }

    /// Delete the durable insert entries identified by `keys` (Origin ack path).
    pub async fn ack_insert_keys(&self, keys: &[Vec<u8>]) -> Result<(), LiteError> {
        self.inserts.ack_keys(keys).await
    }

    /// Delete the durable delete entries identified by `keys` (Origin ack path).
    pub async fn ack_delete_keys(&self, keys: &[Vec<u8>]) -> Result<(), LiteError> {
        self.deletes.ack_keys(keys).await
    }

    /// Update the durable insert payload for `key` with the new seq.
    pub async fn update_insert_entry(
        &self,
        key: &[u8],
        insert: &PendingVectorInsert,
    ) -> Result<(), LiteError> {
        let payload = zerompk::to_msgpack_vec(insert).map_err(|e| LiteError::Serialization {
            detail: format!("vector insert outbound update encode: {e}"),
        })?;
        self.inserts.update_entry(key, &payload).await
    }

    /// Update the durable delete payload for `key` with the new seq.
    pub async fn update_delete_entry(
        &self,
        key: &[u8],
        delete: &PendingVectorDelete,
    ) -> Result<(), LiteError> {
        let payload = zerompk::to_msgpack_vec(delete).map_err(|e| LiteError::Serialization {
            detail: format!("vector delete outbound update encode: {e}"),
        })?;
        self.deletes.update_entry(key, &payload).await
    }

    /// Number of pending insert entries in durable storage.
    pub async fn pending_insert_count(&self) -> Result<u64, LiteError> {
        self.inserts.len().await
    }

    /// Number of pending delete entries in durable storage.
    pub async fn pending_delete_count(&self) -> Result<u64, LiteError> {
        self.deletes.len().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::pagedb_storage::PagedbStorageMem;

    async fn make_queue() -> VectorOutbound<PagedbStorageMem> {
        let storage = Arc::new(PagedbStorageMem::open_in_memory().await.unwrap());
        VectorOutbound::open(storage).await.unwrap()
    }

    #[tokio::test]
    async fn enqueue_and_drain_inserts() {
        let q = make_queue().await;
        q.enqueue_insert("vecs", "v1", vec![1.0, 0.0], 2, "")
            .await
            .unwrap();
        q.enqueue_insert("vecs", "v2", vec![0.0, 1.0], 2, "")
            .await
            .unwrap();

        let pairs = q.drain_inserts(usize::MAX).await.unwrap();
        assert_eq!(pairs.len(), 2);
        assert_eq!(pairs[0].1.id, "v1");
        assert_eq!(pairs[1].1.id, "v2");
    }

    #[tokio::test]
    async fn enqueue_and_drain_deletes() {
        let q = make_queue().await;
        q.enqueue_delete("vecs", "v1", "").await.unwrap();

        let pairs = q.drain_deletes(usize::MAX).await.unwrap();
        assert_eq!(pairs.len(), 1);
        assert_eq!(pairs[0].1.id, "v1");
    }

    #[tokio::test]
    async fn ack_insert_keys_removes_entries() {
        let q = make_queue().await;
        q.enqueue_insert("vecs", "v1", vec![1.0], 1, "")
            .await
            .unwrap();
        q.enqueue_insert("vecs", "v2", vec![2.0], 1, "")
            .await
            .unwrap();

        let pairs = q.drain_inserts(1).await.unwrap();
        assert_eq!(pairs.len(), 1);
        let keys: Vec<Vec<u8>> = pairs.into_iter().map(|(k, _)| k).collect();
        q.ack_insert_keys(&keys).await.unwrap();

        assert_eq!(q.pending_insert_count().await.unwrap(), 1);
    }

    #[tokio::test]
    async fn ack_delete_keys_removes_entries() {
        let q = make_queue().await;
        q.enqueue_delete("vecs", "v1", "").await.unwrap();
        q.enqueue_delete("vecs", "v2", "").await.unwrap();

        let pairs = q.drain_deletes(1).await.unwrap();
        assert_eq!(pairs.len(), 1);
        let keys: Vec<Vec<u8>> = pairs.into_iter().map(|(k, _)| k).collect();
        q.ack_delete_keys(&keys).await.unwrap();

        assert_eq!(q.pending_delete_count().await.unwrap(), 1);
    }

    #[tokio::test]
    async fn insert_cap_returns_backpressure() {
        let storage = Arc::new(PagedbStorageMem::open_in_memory().await.unwrap());
        let q = VectorOutbound::open_with_cap(storage, 2).await.unwrap();
        q.enqueue_insert("vecs", "a", vec![1.0], 1, "")
            .await
            .unwrap();
        q.enqueue_insert("vecs", "b", vec![2.0], 1, "")
            .await
            .unwrap();
        let err = q
            .enqueue_insert("vecs", "c", vec![3.0], 1, "")
            .await
            .unwrap_err();
        assert!(matches!(err, LiteError::Backpressure { .. }));
    }

    #[tokio::test]
    async fn delete_cap_returns_backpressure() {
        let storage = Arc::new(PagedbStorageMem::open_in_memory().await.unwrap());
        let q = VectorOutbound::open_with_cap(storage, 2).await.unwrap();
        q.enqueue_delete("vecs", "a", "").await.unwrap();
        q.enqueue_delete("vecs", "b", "").await.unwrap();
        let err = q.enqueue_delete("vecs", "c", "").await.unwrap_err();
        assert!(matches!(err, LiteError::Backpressure { .. }));
    }

    #[tokio::test]
    async fn batch_ids_are_unique() {
        let q = make_queue().await;
        q.enqueue_insert("vecs", "a", vec![1.0], 1, "")
            .await
            .unwrap();
        q.enqueue_delete("vecs", "b", "").await.unwrap();
        q.enqueue_insert("vecs", "c", vec![2.0], 1, "")
            .await
            .unwrap();

        let inserts = q.drain_inserts(usize::MAX).await.unwrap();
        let deletes = q.drain_deletes(usize::MAX).await.unwrap();

        let mut all_ids: Vec<u64> = inserts.iter().map(|(_, e)| e.batch_id).collect();
        all_ids.extend(deletes.iter().map(|(_, e)| e.batch_id));
        let mut sorted = all_ids.clone();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(sorted.len(), all_ids.len(), "batch_ids must be unique");
        assert!(all_ids.iter().all(|&id| id > 0));
    }

    #[tokio::test]
    async fn survives_reload() {
        let storage = Arc::new(PagedbStorageMem::open_in_memory().await.unwrap());
        {
            let q = VectorOutbound::open(Arc::clone(&storage)).await.unwrap();
            q.enqueue_insert("vecs", "v1", vec![1.0], 1, "")
                .await
                .unwrap();
        }
        let q = VectorOutbound::open(Arc::clone(&storage)).await.unwrap();
        assert_eq!(q.pending_insert_count().await.unwrap(), 1);
        // new entry must not overwrite the existing key
        q.enqueue_insert("vecs", "v2", vec![2.0], 1, "")
            .await
            .unwrap();
        assert_eq!(q.pending_insert_count().await.unwrap(), 2);
    }
}
