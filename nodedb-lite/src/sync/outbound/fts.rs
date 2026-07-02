//! FTS index/delete outbound queue for Lite sync.
//!
//! # Durability
//!
//! Two [`DurableOutboundQueue`]s (one per op-kind) back this outbound:
//! [`Namespace::FtsIndexPending`] and [`Namespace::FtsDeletePending`].
//!
//! # Staging + spill model (no `block_on`)
//!
//! `index_document_text` and `remove_document_text` are sync methods called
//! from the document write path. They cannot `.await` a storage write.
//! Instead they push to a bounded in-memory [`PendingQueue`] (staging buffer).
//! The async `flush()` method (called by the auto-flush timer ~every second)
//! drains the staging buffer and spills entries to the durable queues.
//!
//! If the staging buffer reaches `STAGING_CAP` entries before the next flush,
//! new enqueues are dropped with a `warn!` — the geometry is already durable
//! in the local FTS index; Origin will see it on the next full catch-up.
//!
//! The push transport calls [`drain_indexes`] / [`drain_deletes`] which reads
//! from the durable queues. [`ack_index_keys`] / [`ack_delete_keys`] delete
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
/// buffer between flush cycles. Chosen to be small enough that memory impact
/// is negligible while large enough to absorb one auto-flush interval (~1 s)
/// at any reasonable write rate.
const STAGING_CAP: usize = 4_096;

/// A single pending FTS index operation awaiting sync to Origin.
#[derive(
    Debug,
    Clone,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct PendingFtsIndex {
    /// Monotonic batch ID for ACK correlation.
    pub batch_id: u64,
    /// Collection name.
    pub collection: String,
    /// Document ID.
    pub doc_id: String,
    /// Concatenated text to index (all string fields joined by space).
    pub text: String,
    /// Stable idempotent-producer seq for this entry. 0 = not yet assigned;
    /// assigned at first drain and persisted so re-sends after reconnect reuse
    /// the same seq (Origin dedups instead of double-applying).
    #[serde(default)]
    pub seq: u64,
}

/// A single pending FTS delete operation awaiting sync to Origin.
#[derive(
    Debug,
    Clone,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct PendingFtsDelete {
    /// Monotonic batch ID for ACK correlation.
    pub batch_id: u64,
    /// Collection name.
    pub collection: String,
    /// Document ID.
    pub doc_id: String,
    /// Stable idempotent-producer seq for this entry. 0 = not yet assigned;
    /// assigned at first drain and persisted so re-sends after reconnect reuse
    /// the same seq (Origin dedups instead of double-applying).
    #[serde(default)]
    pub seq: u64,
}

/// Durable outbound queue for FTS index and delete sync.
///
/// Held as `Arc<FtsOutbound<S>>` by `NodeDbLite` and shared with the sync
/// transport. The inner storage is accessed only for `flush()` (from the
/// auto-flush timer) and `drain_indexes` / `drain_deletes` / `ack_*` (from
/// the async sync transport task).
pub struct FtsOutbound<S: StorageEngine> {
    /// In-memory staging buffer for index ops (filled synchronously).
    staging_indexes: PendingQueue<PendingFtsIndex>,
    /// In-memory staging buffer for delete ops (filled synchronously).
    staging_deletes: PendingQueue<PendingFtsDelete>,
    /// Durable FIFO queue for index ops.
    durable_indexes: DurableOutboundQueue<S>,
    /// Durable FIFO queue for delete ops.
    durable_deletes: DurableOutboundQueue<S>,
    /// Shared monotonic ID generator.
    ids: AtomicU64,
    /// batch_id → durable_key for in-flight index entries.
    in_flight_indexes: Mutex<HashMap<u64, Vec<u8>>>,
    /// batch_id → durable_key for in-flight delete entries.
    in_flight_deletes_map: Mutex<HashMap<u64, Vec<u8>>>,
}

impl<S: StorageEngine> FtsOutbound<S> {
    /// Open durable queues backed by [`Namespace::FtsIndexPending`] and
    /// [`Namespace::FtsDeletePending`].
    pub async fn open(storage: Arc<S>) -> Result<Self, LiteError> {
        Self::open_with_cap(storage, DurableOutboundQueue::<S>::DEFAULT_CAP).await
    }

    /// Open with a custom cap (useful for tests with small limits).
    pub async fn open_with_cap(storage: Arc<S>, cap: usize) -> Result<Self, LiteError> {
        let durable_indexes = DurableOutboundQueue::open_with_cap(
            Arc::clone(&storage),
            Namespace::FtsIndexPending,
            cap,
        )
        .await?;
        let durable_deletes = DurableOutboundQueue::open_with_cap(
            Arc::clone(&storage),
            Namespace::FtsDeletePending,
            cap,
        )
        .await?;
        Ok(Self {
            staging_indexes: PendingQueue::new(),
            staging_deletes: PendingQueue::new(),
            durable_indexes,
            durable_deletes,
            ids: AtomicU64::new(1),
            in_flight_indexes: Mutex::new(HashMap::new()),
            in_flight_deletes_map: Mutex::new(HashMap::new()),
        })
    }

    /// Synchronously stage an FTS index entry.
    ///
    /// Drops the entry with a `warn!` if the staging buffer is at
    /// [`STAGING_CAP`]. The entry is already durable in the local FTS index;
    /// Origin sees it on the next catch-up.
    pub fn stage_index(&self, collection: &str, doc_id: &str, text: String) {
        if self.staging_indexes.len() >= STAGING_CAP {
            tracing::warn!(
                collection,
                doc_id,
                "fts_outbound: staging buffer full; dropping index sync entry \
                 (will re-sync on next catch-up)"
            );
            return;
        }
        let batch_id = self.ids.fetch_add(1, Ordering::Relaxed);
        self.staging_indexes.push(PendingFtsIndex {
            batch_id,
            collection: collection.to_string(),
            doc_id: doc_id.to_string(),
            text,
            seq: 0,
        });
    }

    /// Synchronously stage an FTS delete entry.
    ///
    /// Drops the entry with a `warn!` if the staging buffer is at
    /// [`STAGING_CAP`].
    pub fn stage_delete(&self, collection: &str, doc_id: &str) {
        if self.staging_deletes.len() >= STAGING_CAP {
            tracing::warn!(
                collection,
                doc_id,
                "fts_outbound: staging buffer full; dropping delete sync entry \
                 (will re-sync on next catch-up)"
            );
            return;
        }
        let batch_id = self.ids.fetch_add(1, Ordering::Relaxed);
        self.staging_deletes.push(PendingFtsDelete {
            batch_id,
            collection: collection.to_string(),
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
        // Spill index staging.
        let staged_indexes = self.staging_indexes.drain();
        let mut failed_indexes: Vec<PendingFtsIndex> = Vec::new();
        let mut backpressure: Option<LiteError> = None;
        for entry in staged_indexes {
            if backpressure.is_some() {
                failed_indexes.push(entry);
                continue;
            }
            let payload =
                zerompk::to_msgpack_vec(&entry).map_err(|e| LiteError::Serialization {
                    detail: format!("fts index outbound encode: {e}"),
                })?;
            match self.durable_indexes.enqueue(&payload).await {
                Ok(()) => {}
                Err(e @ LiteError::Backpressure { .. }) => {
                    failed_indexes.push(entry);
                    backpressure = Some(e);
                }
                Err(e) => return Err(e),
            }
        }
        // Re-stage failed entries at the front so they are retried next flush.
        for entry in failed_indexes.into_iter().rev() {
            self.staging_indexes.requeue(entry);
        }

        // Spill delete staging.
        let staged_deletes = self.staging_deletes.drain();
        let mut failed_deletes: Vec<PendingFtsDelete> = Vec::new();
        for entry in staged_deletes {
            if backpressure.is_some() {
                failed_deletes.push(entry);
                continue;
            }
            let payload =
                zerompk::to_msgpack_vec(&entry).map_err(|e| LiteError::Serialization {
                    detail: format!("fts delete outbound encode: {e}"),
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

    /// Drain up to `limit` pending index entries in FIFO order, skipping
    /// in-flight entries (sent but not yet acked by Origin).
    ///
    /// Returns `(durable_key, entry)` pairs. On send success, call
    /// [`mark_index_in_flight`]. The durable entry is deleted only on Origin
    /// ack via [`ack_index_in_flight`].
    pub async fn drain_indexes(
        &self,
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, PendingFtsIndex)>, LiteError> {
        // Spill staged entries to durable storage every drain tick so
        // replication does not depend on the auto-flush timer (which is absent
        // in direct library/test usage). Backpressure is non-fatal here.
        if let Err(e) = self.flush_staging().await {
            tracing::debug!(error = %e, "fts_outbound: staging spill backpressured");
        }
        let in_flight = self.in_flight_indexes.lock().await;
        let pairs = self.durable_indexes.drain_batch(limit).await?;
        let mut out = Vec::with_capacity(pairs.len());
        for (key, payload) in pairs {
            if in_flight.values().any(|k| k == &key) {
                continue;
            }
            let entry: PendingFtsIndex =
                zerompk::from_msgpack(&payload).map_err(|e| LiteError::Serialization {
                    detail: format!("fts index outbound decode: {e}"),
                })?;
            out.push((key, entry));
        }
        Ok(out)
    }

    /// Drain up to `limit` pending delete entries in FIFO order, skipping
    /// in-flight entries.
    ///
    /// Returns `(durable_key, entry)` pairs. On send success, call
    /// [`mark_delete_in_flight`]. The durable entry is deleted only on Origin
    /// ack via [`ack_delete_in_flight`].
    pub async fn drain_deletes(
        &self,
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, PendingFtsDelete)>, LiteError> {
        if let Err(e) = self.flush_staging().await {
            tracing::debug!(error = %e, "fts_outbound: staging spill backpressured");
        }
        let in_flight = self.in_flight_deletes_map.lock().await;
        let pairs = self.durable_deletes.drain_batch(limit).await?;
        let mut out = Vec::with_capacity(pairs.len());
        for (key, payload) in pairs {
            if in_flight.values().any(|k| k == &key) {
                continue;
            }
            let entry: PendingFtsDelete =
                zerompk::from_msgpack(&payload).map_err(|e| LiteError::Serialization {
                    detail: format!("fts delete outbound decode: {e}"),
                })?;
            out.push((key, entry));
        }
        Ok(out)
    }

    /// Record that an index entry has been sent to Origin and is awaiting its ack.
    pub async fn mark_index_in_flight(&self, batch_id: u64, durable_key: Vec<u8>) {
        self.in_flight_indexes
            .lock()
            .await
            .insert(batch_id, durable_key);
    }

    /// Remove the in-flight record for an index entry and return its durable key.
    pub async fn ack_index_in_flight(&self, batch_id: u64) -> Option<Vec<u8>> {
        self.in_flight_indexes.lock().await.remove(&batch_id)
    }

    /// Record that a delete entry has been sent to Origin and is awaiting its ack.
    pub async fn mark_delete_in_flight(&self, batch_id: u64, durable_key: Vec<u8>) {
        self.in_flight_deletes_map
            .lock()
            .await
            .insert(batch_id, durable_key);
    }

    /// Remove the in-flight record for a delete entry and return its durable key.
    pub async fn ack_delete_in_flight(&self, batch_id: u64) -> Option<Vec<u8>> {
        self.in_flight_deletes_map.lock().await.remove(&batch_id)
    }

    /// Clear all in-flight records on reconnect so entries are re-drained.
    pub async fn clear_in_flight(&self) {
        self.in_flight_indexes.lock().await.clear();
        self.in_flight_deletes_map.lock().await.clear();
    }

    /// Delete the durable index entries identified by `keys` (Origin ack path).
    pub async fn ack_index_keys(&self, keys: &[Vec<u8>]) -> Result<(), LiteError> {
        self.durable_indexes.ack_keys(keys).await
    }

    /// Delete the durable delete entries identified by `keys` (Origin ack path).
    pub async fn ack_delete_keys(&self, keys: &[Vec<u8>]) -> Result<(), LiteError> {
        self.durable_deletes.ack_keys(keys).await
    }

    /// Update the durable index payload for `key` with the new seq.
    pub async fn update_index_entry(
        &self,
        key: &[u8],
        entry: &PendingFtsIndex,
    ) -> Result<(), LiteError> {
        let payload = zerompk::to_msgpack_vec(entry).map_err(|e| LiteError::Serialization {
            detail: format!("fts index outbound update encode: {e}"),
        })?;
        self.durable_indexes.update_entry(key, &payload).await
    }

    /// Update the durable delete payload for `key` with the new seq.
    pub async fn update_delete_entry(
        &self,
        key: &[u8],
        entry: &PendingFtsDelete,
    ) -> Result<(), LiteError> {
        let payload = zerompk::to_msgpack_vec(entry).map_err(|e| LiteError::Serialization {
            detail: format!("fts delete outbound update encode: {e}"),
        })?;
        self.durable_deletes.update_entry(key, &payload).await
    }

    /// Number of pending index entries in durable storage.
    pub async fn pending_index_count(&self) -> Result<u64, LiteError> {
        self.durable_indexes.len().await
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

    async fn make_queue() -> FtsOutbound<PagedbStorageMem> {
        let storage = Arc::new(PagedbStorageMem::open_in_memory().await.unwrap());
        FtsOutbound::open(storage).await.unwrap()
    }

    #[tokio::test]
    async fn stage_and_flush_indexes() {
        let q = make_queue().await;
        q.stage_index("docs", "d1", "hello world".to_string());
        q.stage_index("docs", "d2", "rust rocks".to_string());

        // Before flush, durable queue is empty.
        assert_eq!(q.pending_index_count().await.unwrap(), 0);

        q.flush_staging().await.unwrap();

        let pairs = q.drain_indexes(usize::MAX).await.unwrap();
        assert_eq!(pairs.len(), 2);
        assert_eq!(pairs[0].1.doc_id, "d1");
        assert_eq!(pairs[1].1.doc_id, "d2");
    }

    #[tokio::test]
    async fn stage_and_flush_deletes() {
        let q = make_queue().await;
        q.stage_delete("docs", "d1");

        q.flush_staging().await.unwrap();

        let pairs = q.drain_deletes(usize::MAX).await.unwrap();
        assert_eq!(pairs.len(), 1);
        assert_eq!(pairs[0].1.doc_id, "d1");
    }

    #[tokio::test]
    async fn ack_index_keys_removes_entries() {
        let q = make_queue().await;
        q.stage_index("docs", "d1", "text".to_string());
        q.stage_index("docs", "d2", "text2".to_string());
        q.flush_staging().await.unwrap();

        let pairs = q.drain_indexes(1).await.unwrap();
        assert_eq!(pairs.len(), 1);
        let keys: Vec<Vec<u8>> = pairs.into_iter().map(|(k, _)| k).collect();
        q.ack_index_keys(&keys).await.unwrap();

        assert_eq!(q.pending_index_count().await.unwrap(), 1);
    }

    #[tokio::test]
    async fn ack_delete_keys_removes_entries() {
        let q = make_queue().await;
        q.stage_delete("docs", "d1");
        q.stage_delete("docs", "d2");
        q.flush_staging().await.unwrap();

        let pairs = q.drain_deletes(1).await.unwrap();
        assert_eq!(pairs.len(), 1);
        let keys: Vec<Vec<u8>> = pairs.into_iter().map(|(k, _)| k).collect();
        q.ack_delete_keys(&keys).await.unwrap();

        assert_eq!(q.pending_delete_count().await.unwrap(), 1);
    }

    #[tokio::test]
    async fn durable_cap_returns_backpressure() {
        let storage = Arc::new(PagedbStorageMem::open_in_memory().await.unwrap());
        let q = FtsOutbound::open_with_cap(storage, 2).await.unwrap();
        q.stage_index("docs", "a", "foo".to_string());
        q.stage_index("docs", "b", "bar".to_string());
        q.stage_index("docs", "c", "baz".to_string());
        // First flush drains a and b into durable (cap=2), c stays staged.
        let err = q.flush_staging().await.unwrap_err();
        assert!(matches!(err, LiteError::Backpressure { .. }));
        // c must have been re-staged.
        assert_eq!(q.staging_indexes.len(), 1);
    }

    #[tokio::test]
    async fn survives_reload() {
        let storage = Arc::new(PagedbStorageMem::open_in_memory().await.unwrap());
        {
            let q = FtsOutbound::open(Arc::clone(&storage)).await.unwrap();
            q.stage_index("docs", "v1", "text".to_string());
            q.flush_staging().await.unwrap();
        }
        let q = FtsOutbound::open(Arc::clone(&storage)).await.unwrap();
        assert_eq!(q.pending_index_count().await.unwrap(), 1);
        q.stage_index("docs", "v2", "text2".to_string());
        q.flush_staging().await.unwrap();
        assert_eq!(q.pending_index_count().await.unwrap(), 2);
    }
}
