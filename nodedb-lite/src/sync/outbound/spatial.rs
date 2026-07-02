//! Spatial geometry insert/delete outbound queue for Lite sync.
//!
//! # Durability
//!
//! Two [`DurableOutboundQueue`]s (one per op-kind) back this outbound:
//! [`Namespace::SpatialInsertPending`] and [`Namespace::SpatialDeletePending`].
//!
//! # Staging + spill model (no `block_on`)
//!
//! `spatial_insert` and `spatial_delete` are sync methods on `NodeDbLite`.
//! They cannot `.await` a storage write. Instead they push to a bounded
//! in-memory [`PendingQueue`] (staging buffer). The async `flush()` path
//! drains the staging buffer and spills entries to the durable queues.
//!
//! If the staging buffer reaches `STAGING_CAP` entries before the next flush,
//! new enqueues are dropped with a `warn!` — the geometry is already durable
//! in the local R-tree; Origin will see it on the next full catch-up.
//!
//! The push transport calls [`drain_inserts`] / [`drain_deletes`] which reads
//! from the durable queues. [`ack_insert_keys`] / [`ack_delete_keys`] delete
//! confirmed entries by key.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use nodedb_types::Namespace;
use tokio::sync::Mutex;

use super::durable_queue::DurableOutboundQueue;
use super::queue::PendingQueue;
use crate::error::LiteError;
use crate::storage::engine::StorageEngine;

/// Maximum number of entries that may accumulate in the in-memory staging
/// buffer between flush cycles.
const STAGING_CAP: usize = 4_096;

/// A single pending spatial insert operation awaiting sync to Origin.
#[derive(
    Debug,
    Clone,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct PendingSpatialInsert {
    /// Monotonic batch ID for ACK correlation.
    pub batch_id: u64,
    /// Collection name.
    pub collection: String,
    /// Geometry field name.
    pub field: String,
    /// Document ID.
    pub doc_id: String,
    /// MessagePack-serialised `nodedb_types::geometry::Geometry`.
    pub geometry_bytes: Vec<u8>,
    /// Stable idempotent-producer seq for this entry. 0 = not yet assigned;
    /// assigned at first drain and persisted so re-sends after reconnect reuse
    /// the same seq (Origin dedups instead of double-applying).
    #[serde(default)]
    pub seq: u64,
}

/// A single pending spatial delete operation awaiting sync to Origin.
#[derive(
    Debug,
    Clone,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct PendingSpatialDelete {
    /// Monotonic batch ID for ACK correlation.
    pub batch_id: u64,
    /// Collection name.
    pub collection: String,
    /// Geometry field name.
    pub field: String,
    /// Document ID.
    pub doc_id: String,
    /// Stable idempotent-producer seq for this entry. 0 = not yet assigned;
    /// assigned at first drain and persisted so re-sends after reconnect reuse
    /// the same seq (Origin dedups instead of double-applying).
    #[serde(default)]
    pub seq: u64,
}

/// Durable outbound queue for spatial insert and delete sync.
///
/// Held as `Arc<SpatialOutbound<S>>` by `NodeDbLite` and shared with the sync
/// transport. The inner storage is accessed only for `flush_staging()` (from
/// the auto-flush timer) and `drain_inserts` / `drain_deletes` / `ack_*`
/// (from the async sync transport task).
pub struct SpatialOutbound<S: StorageEngine> {
    /// In-memory staging buffer for insert ops (filled synchronously).
    staging_inserts: PendingQueue<PendingSpatialInsert>,
    /// In-memory staging buffer for delete ops (filled synchronously).
    staging_deletes: PendingQueue<PendingSpatialDelete>,
    /// Durable FIFO queue for insert ops.
    durable_inserts: DurableOutboundQueue<S>,
    /// Durable FIFO queue for delete ops.
    durable_deletes: DurableOutboundQueue<S>,
    /// Shared monotonic ID generator.
    ids: AtomicU64,
    /// batch_id → durable_key for in-flight insert entries.
    in_flight_inserts: Mutex<HashMap<u64, Vec<u8>>>,
    /// batch_id → durable_key for in-flight delete entries.
    in_flight_deletes_map: Mutex<HashMap<u64, Vec<u8>>>,
}

impl<S: StorageEngine> SpatialOutbound<S> {
    /// Open durable queues backed by [`Namespace::SpatialInsertPending`] and
    /// [`Namespace::SpatialDeletePending`].
    pub async fn open(storage: Arc<S>) -> Result<Self, LiteError> {
        Self::open_with_cap(storage, DurableOutboundQueue::<S>::DEFAULT_CAP).await
    }

    /// Open with a custom cap (useful for tests with small limits).
    pub async fn open_with_cap(storage: Arc<S>, cap: usize) -> Result<Self, LiteError> {
        let durable_inserts = DurableOutboundQueue::open_with_cap(
            Arc::clone(&storage),
            Namespace::SpatialInsertPending,
            cap,
        )
        .await?;
        let durable_deletes = DurableOutboundQueue::open_with_cap(
            Arc::clone(&storage),
            Namespace::SpatialDeletePending,
            cap,
        )
        .await?;
        Ok(Self {
            staging_inserts: PendingQueue::new(),
            staging_deletes: PendingQueue::new(),
            durable_inserts,
            durable_deletes,
            ids: AtomicU64::new(1),
            in_flight_inserts: Mutex::new(HashMap::new()),
            in_flight_deletes_map: Mutex::new(HashMap::new()),
        })
    }

    /// Synchronously stage a spatial insert entry.
    ///
    /// Serialises the geometry to MessagePack; drops the entry with a `warn!`
    /// if serialisation fails or the staging buffer is at [`STAGING_CAP`].
    pub fn stage_insert(
        &self,
        collection: &str,
        field: &str,
        doc_id: &str,
        geometry: &nodedb_types::geometry::Geometry,
    ) {
        if self.staging_inserts.len() >= STAGING_CAP {
            tracing::warn!(
                collection,
                field,
                doc_id,
                "spatial_outbound: staging buffer full; dropping insert sync entry \
                 (will re-sync on next catch-up)"
            );
            return;
        }
        let geometry_bytes = match zerompk::to_msgpack_vec(geometry) {
            Ok(b) => b,
            Err(e) => {
                tracing::warn!(
                    collection,
                    field,
                    doc_id,
                    error = %e,
                    "spatial_outbound: failed to serialise geometry; skipping sync enqueue"
                );
                return;
            }
        };
        let batch_id = self.ids.fetch_add(1, Ordering::Relaxed);
        self.staging_inserts.push(PendingSpatialInsert {
            batch_id,
            collection: collection.to_string(),
            field: field.to_string(),
            doc_id: doc_id.to_string(),
            geometry_bytes,
            seq: 0,
        });
    }

    /// Synchronously stage a spatial delete entry.
    ///
    /// Drops the entry with a `warn!` if the staging buffer is at
    /// [`STAGING_CAP`].
    pub fn stage_delete(&self, collection: &str, field: &str, doc_id: &str) {
        if self.staging_deletes.len() >= STAGING_CAP {
            tracing::warn!(
                collection,
                field,
                doc_id,
                "spatial_outbound: staging buffer full; dropping delete sync entry \
                 (will re-sync on next catch-up)"
            );
            return;
        }
        let batch_id = self.ids.fetch_add(1, Ordering::Relaxed);
        self.staging_deletes.push(PendingSpatialDelete {
            batch_id,
            collection: collection.to_string(),
            field: field.to_string(),
            doc_id: doc_id.to_string(),
            seq: 0,
        });
    }

    /// Spill staged entries to durable storage.
    ///
    /// Called from the async `flush()` path (~every second). Drains the
    /// in-memory staging buffers and appends each entry to the durable queues.
    /// Returns [`LiteError::Backpressure`] on the first entry that cannot be
    /// enqueued (durable queue at cap); remaining staged entries are re-queued
    /// at the head of the staging buffer.
    pub async fn flush_staging(&self) -> Result<(), LiteError> {
        // Spill insert staging.
        let staged_inserts = self.staging_inserts.drain();
        let mut failed_inserts: Vec<PendingSpatialInsert> = Vec::new();
        let mut backpressure: Option<LiteError> = None;
        for entry in staged_inserts {
            if backpressure.is_some() {
                failed_inserts.push(entry);
                continue;
            }
            let payload =
                zerompk::to_msgpack_vec(&entry).map_err(|e| LiteError::Serialization {
                    detail: format!("spatial insert outbound encode: {e}"),
                })?;
            match self.durable_inserts.enqueue(&payload).await {
                Ok(()) => {}
                Err(e @ LiteError::Backpressure { .. }) => {
                    failed_inserts.push(entry);
                    backpressure = Some(e);
                }
                Err(e) => return Err(e),
            }
        }
        for entry in failed_inserts.into_iter().rev() {
            self.staging_inserts.requeue(entry);
        }

        // Spill delete staging.
        let staged_deletes = self.staging_deletes.drain();
        let mut failed_deletes: Vec<PendingSpatialDelete> = Vec::new();
        for entry in staged_deletes {
            if backpressure.is_some() {
                failed_deletes.push(entry);
                continue;
            }
            let payload =
                zerompk::to_msgpack_vec(&entry).map_err(|e| LiteError::Serialization {
                    detail: format!("spatial delete outbound encode: {e}"),
                })?;
            match self.durable_deletes.enqueue(&payload).await {
                Ok(()) => {}
                Err(e @ LiteError::Backpressure { .. }) => {
                    failed_deletes.push(entry);
                    backpressure = Some(e);
                }
                Err(e) => return Err(e),
            }
        }
        for entry in failed_deletes.into_iter().rev() {
            self.staging_deletes.requeue(entry);
        }

        match backpressure {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    /// Drain up to `limit` pending inserts in FIFO order, skipping in-flight
    /// entries (sent but not yet acked by Origin).
    ///
    /// Returns `(durable_key, insert)` pairs. On send success, call
    /// [`mark_insert_in_flight`]. The durable entry is deleted only on Origin
    /// ack via [`ack_insert_in_flight`].
    pub async fn drain_inserts(
        &self,
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, PendingSpatialInsert)>, LiteError> {
        // Spill staged entries every drain tick so replication doesn't depend on
        // the auto-flush timer (absent in direct library/test usage).
        if let Err(e) = self.flush_staging().await {
            tracing::debug!(error = %e, "spatial_outbound: staging spill backpressured");
        }
        let in_flight = self.in_flight_inserts.lock().await;
        let pairs = self.durable_inserts.drain_batch(limit).await?;
        let mut out = Vec::with_capacity(pairs.len());
        for (key, payload) in pairs {
            if in_flight.values().any(|k| k == &key) {
                continue;
            }
            let entry: PendingSpatialInsert =
                zerompk::from_msgpack(&payload).map_err(|e| LiteError::Serialization {
                    detail: format!("spatial insert outbound decode: {e}"),
                })?;
            out.push((key, entry));
        }
        Ok(out)
    }

    /// Drain up to `limit` pending deletes in FIFO order, skipping in-flight
    /// entries.
    ///
    /// Returns `(durable_key, delete)` pairs. On send success, call
    /// [`mark_delete_in_flight`]. The durable entry is deleted only on Origin
    /// ack via [`ack_delete_in_flight`].
    pub async fn drain_deletes(
        &self,
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, PendingSpatialDelete)>, LiteError> {
        if let Err(e) = self.flush_staging().await {
            tracing::debug!(error = %e, "spatial_outbound: staging spill backpressured");
        }
        let in_flight = self.in_flight_deletes_map.lock().await;
        let pairs = self.durable_deletes.drain_batch(limit).await?;
        let mut out = Vec::with_capacity(pairs.len());
        for (key, payload) in pairs {
            if in_flight.values().any(|k| k == &key) {
                continue;
            }
            let entry: PendingSpatialDelete =
                zerompk::from_msgpack(&payload).map_err(|e| LiteError::Serialization {
                    detail: format!("spatial delete outbound decode: {e}"),
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
        self.in_flight_deletes_map
            .lock()
            .await
            .insert(batch_id, durable_key);
    }

    /// Remove the in-flight record for a delete and return its durable key.
    pub async fn ack_delete_in_flight(&self, batch_id: u64) -> Option<Vec<u8>> {
        self.in_flight_deletes_map.lock().await.remove(&batch_id)
    }

    /// Clear all in-flight records on reconnect so entries are re-drained.
    pub async fn clear_in_flight(&self) {
        self.in_flight_inserts.lock().await.clear();
        self.in_flight_deletes_map.lock().await.clear();
    }

    /// Delete the durable insert entries identified by `keys` (Origin ack path).
    pub async fn ack_insert_keys(&self, keys: &[Vec<u8>]) -> Result<(), LiteError> {
        self.durable_inserts.ack_keys(keys).await
    }

    /// Delete the durable delete entries identified by `keys` (Origin ack path).
    pub async fn ack_delete_keys(&self, keys: &[Vec<u8>]) -> Result<(), LiteError> {
        self.durable_deletes.ack_keys(keys).await
    }

    /// Update the durable insert payload for `key` with the new seq.
    pub async fn update_insert_entry(
        &self,
        key: &[u8],
        insert: &PendingSpatialInsert,
    ) -> Result<(), LiteError> {
        let payload = zerompk::to_msgpack_vec(insert).map_err(|e| LiteError::Serialization {
            detail: format!("spatial insert outbound update encode: {e}"),
        })?;
        self.durable_inserts.update_entry(key, &payload).await
    }

    /// Update the durable delete payload for `key` with the new seq.
    pub async fn update_delete_entry(
        &self,
        key: &[u8],
        delete: &PendingSpatialDelete,
    ) -> Result<(), LiteError> {
        let payload = zerompk::to_msgpack_vec(delete).map_err(|e| LiteError::Serialization {
            detail: format!("spatial delete outbound update encode: {e}"),
        })?;
        self.durable_deletes.update_entry(key, &payload).await
    }

    /// Number of pending insert entries in durable storage.
    pub async fn pending_insert_count(&self) -> Result<u64, LiteError> {
        self.durable_inserts.len().await
    }

    /// Number of pending delete entries in durable storage.
    pub async fn pending_delete_count(&self) -> Result<u64, LiteError> {
        self.durable_deletes.len().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::pagedb_storage::PagedbStorageMem;

    fn point_geometry() -> nodedb_types::geometry::Geometry {
        nodedb_types::geometry::Geometry::point(1.0, 2.0)
    }

    async fn make_queue() -> SpatialOutbound<PagedbStorageMem> {
        let storage = Arc::new(PagedbStorageMem::open_in_memory().await.unwrap());
        SpatialOutbound::open(storage).await.unwrap()
    }

    #[tokio::test]
    async fn stage_and_flush_inserts() {
        let q = make_queue().await;
        q.stage_insert("places", "loc", "doc1", &point_geometry());
        q.stage_insert("places", "loc", "doc2", &point_geometry());

        assert_eq!(q.pending_insert_count().await.unwrap(), 0);

        q.flush_staging().await.unwrap();

        let pairs = q.drain_inserts(usize::MAX).await.unwrap();
        assert_eq!(pairs.len(), 2);
        assert_eq!(pairs[0].1.doc_id, "doc1");
        assert_eq!(pairs[1].1.doc_id, "doc2");
    }

    #[tokio::test]
    async fn stage_and_flush_deletes() {
        let q = make_queue().await;
        q.stage_delete("places", "loc", "doc1");

        q.flush_staging().await.unwrap();

        let pairs = q.drain_deletes(usize::MAX).await.unwrap();
        assert_eq!(pairs.len(), 1);
        assert_eq!(pairs[0].1.doc_id, "doc1");
    }

    #[tokio::test]
    async fn ack_insert_keys_removes_entries() {
        let q = make_queue().await;
        q.stage_insert("places", "loc", "doc1", &point_geometry());
        q.stage_insert("places", "loc", "doc2", &point_geometry());
        q.flush_staging().await.unwrap();

        let pairs = q.drain_inserts(1).await.unwrap();
        assert_eq!(pairs.len(), 1);
        let keys: Vec<Vec<u8>> = pairs.into_iter().map(|(k, _)| k).collect();
        q.ack_insert_keys(&keys).await.unwrap();

        assert_eq!(q.pending_insert_count().await.unwrap(), 1);
    }

    #[tokio::test]
    async fn ack_delete_keys_removes_entries() {
        let q = make_queue().await;
        q.stage_delete("places", "loc", "doc1");
        q.stage_delete("places", "loc", "doc2");
        q.flush_staging().await.unwrap();

        let pairs = q.drain_deletes(1).await.unwrap();
        assert_eq!(pairs.len(), 1);
        let keys: Vec<Vec<u8>> = pairs.into_iter().map(|(k, _)| k).collect();
        q.ack_delete_keys(&keys).await.unwrap();

        assert_eq!(q.pending_delete_count().await.unwrap(), 1);
    }

    #[tokio::test]
    async fn geometry_bytes_round_trip() {
        let geom = point_geometry();
        let q = make_queue().await;
        q.stage_insert("places", "loc", "doc1", &geom);
        q.flush_staging().await.unwrap();
        let pairs = q.drain_inserts(usize::MAX).await.unwrap();
        assert!(!pairs[0].1.geometry_bytes.is_empty());
        let restored: nodedb_types::geometry::Geometry =
            zerompk::from_msgpack(&pairs[0].1.geometry_bytes)
                .expect("geometry round-trip deserialise");
        assert_eq!(geom, restored);
    }

    #[tokio::test]
    async fn durable_cap_returns_backpressure() {
        let storage = Arc::new(PagedbStorageMem::open_in_memory().await.unwrap());
        let q = SpatialOutbound::open_with_cap(storage, 2).await.unwrap();
        q.stage_insert("places", "loc", "a", &point_geometry());
        q.stage_insert("places", "loc", "b", &point_geometry());
        q.stage_insert("places", "loc", "c", &point_geometry());
        let err = q.flush_staging().await.unwrap_err();
        assert!(matches!(err, LiteError::Backpressure { .. }));
        assert_eq!(q.staging_inserts.len(), 1);
    }

    #[tokio::test]
    async fn survives_reload() {
        let storage = Arc::new(PagedbStorageMem::open_in_memory().await.unwrap());
        {
            let q = SpatialOutbound::open(Arc::clone(&storage)).await.unwrap();
            q.stage_insert("places", "loc", "v1", &point_geometry());
            q.flush_staging().await.unwrap();
        }
        let q = SpatialOutbound::open(Arc::clone(&storage)).await.unwrap();
        assert_eq!(q.pending_insert_count().await.unwrap(), 1);
        q.stage_insert("places", "loc", "v2", &point_geometry());
        q.flush_staging().await.unwrap();
        assert_eq!(q.pending_insert_count().await.unwrap(), 2);
    }
}
