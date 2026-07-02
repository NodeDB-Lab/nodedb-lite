//! Columnar insert outbound queue for Lite sync.
//!
//! When a columnar row is inserted on Lite, it is durably enqueued here.
//! The sync transport drains this queue and sends `ColumnarInsert` wire
//! frames to Origin. Each drained entry carries a monotonic durable key used
//! to delete it from storage once Origin acknowledges receipt.
//!
//! # Durability
//!
//! The queue is backed by [`DurableOutboundQueue`], which persists every
//! entry in [`Namespace::ColumnarPending`] before returning from `enqueue`.
//! Entries survive process restarts; a crash between send and ack causes
//! at-most-one retry (at-least-once delivery to Origin).
//!
//! # Backpressure
//!
//! When the queue reaches its cap, `enqueue` returns
//! [`LiteError::Backpressure`], propagating to the caller so writes pause
//! until the sync transport drains the backlog.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use nodedb_types::Namespace;
use nodedb_types::value::Value;
use tokio::sync::Mutex;

use super::durable_queue::DurableOutboundQueue;
use crate::error::LiteError;
use crate::storage::engine::StorageEngine;

/// A single pending batch of columnar rows awaiting sync to Origin.
#[derive(
    Debug,
    Clone,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct PendingColumnarBatch {
    /// Monotonic batch ID (per-collection, Lite-assigned) for ACK correlation.
    pub batch_id: u64,
    /// Collection name.
    pub collection: String,
    /// Rows in schema column order, one `Vec<Value>` per row.
    pub rows: Vec<Vec<Value>>,
    /// MessagePack-serialized `ColumnarSchema` hint. May be empty.
    pub schema_bytes: Vec<u8>,
    /// Stable idempotent-producer seq for this entry. 0 = not yet assigned;
    /// assigned at first drain and persisted so re-sends after reconnect reuse
    /// the same seq (Origin dedups instead of double-applying).
    #[serde(default)]
    pub seq: u64,
}

/// Durable outbound queue for columnar inserts.
///
/// Held as `Arc<ColumnarOutbound<S>>` by `NodeDbLite` and shared with
/// `ColumnarEngine` via `Arc`. The inner storage is accessed only for
/// `enqueue` (from the sync insert path) and `drain_batch`/`ack_keys`
/// (from the async sync transport path).
pub struct ColumnarOutbound<S: StorageEngine> {
    queue: DurableOutboundQueue<S>,
    ids: AtomicU64,
    /// batch_id → durable_key for entries that have been sent but not yet
    /// acked by Origin. Cleared on reconnect so entries are re-drained.
    in_flight: Mutex<HashMap<u64, Vec<u8>>>,
}

impl<S: StorageEngine> ColumnarOutbound<S> {
    /// Open the durable queue backed by [`Namespace::ColumnarPending`].
    pub async fn open(storage: Arc<S>) -> Result<Self, LiteError> {
        Self::open_with_cap(storage, DurableOutboundQueue::<S>::DEFAULT_CAP).await
    }

    /// Open with a custom cap.
    pub async fn open_with_cap(storage: Arc<S>, cap: usize) -> Result<Self, LiteError> {
        let queue =
            DurableOutboundQueue::open_with_cap(storage, Namespace::ColumnarPending, cap).await?;
        Ok(Self {
            queue,
            ids: AtomicU64::new(1),
            in_flight: Mutex::new(HashMap::new()),
        })
    }

    /// Durably enqueue a single row for a collection.
    ///
    /// Rows for the same collection are **not** coalesced here — each call
    /// produces one durable entry. The sync transport batches by collection
    /// when building wire frames.
    ///
    /// Returns [`LiteError::Backpressure`] when the queue is at cap.
    pub async fn enqueue_row(
        &self,
        collection: &str,
        row: Vec<Value>,
        schema_bytes: Vec<u8>,
    ) -> Result<(), LiteError> {
        let batch_id = self.ids.fetch_add(1, Ordering::Relaxed);
        let batch = PendingColumnarBatch {
            batch_id,
            collection: collection.to_string(),
            rows: vec![row],
            schema_bytes,
            seq: 0,
        };
        let payload = zerompk::to_msgpack_vec(&batch).map_err(|e| LiteError::Serialization {
            detail: format!("columnar outbound encode: {e}"),
        })?;
        self.queue.enqueue(&payload).await
    }

    /// Drain up to `limit` pending batches in FIFO order, skipping any entries
    /// currently in-flight (sent but not yet acked by Origin).
    ///
    /// Returns `(durable_key, batch)` pairs. On send success, call
    /// [`mark_in_flight`] with the batch_id and key. The durable entry is
    /// deleted only when Origin's ack arrives via [`ack_in_flight`].
    ///
    /// Does **not** remove entries from storage.
    pub async fn drain_batch(
        &self,
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, PendingColumnarBatch)>, LiteError> {
        let in_flight = self.in_flight.lock().await;
        let pairs = self.queue.drain_batch(limit).await?;
        let mut out = Vec::with_capacity(pairs.len());
        for (key, payload) in pairs {
            if in_flight.values().any(|k| k == &key) {
                continue;
            }
            let batch: PendingColumnarBatch =
                zerompk::from_msgpack(&payload).map_err(|e| LiteError::Serialization {
                    detail: format!("columnar outbound decode: {e}"),
                })?;
            out.push((key, batch));
        }
        Ok(out)
    }

    /// Record that a batch has been sent to Origin and is awaiting its ack.
    ///
    /// The durable entry is kept in storage until [`ack_in_flight`] is called.
    pub async fn mark_in_flight(&self, batch_id: u64, durable_key: Vec<u8>) {
        self.in_flight.lock().await.insert(batch_id, durable_key);
    }

    /// Remove the in-flight record for `batch_id` and return its durable key.
    ///
    /// Returns `Some(key)` if the entry was in-flight; `None` if already acked
    /// or not tracked (e.g. the entry was un-encodable and dropped at send time).
    pub async fn ack_in_flight(&self, batch_id: u64) -> Option<Vec<u8>> {
        self.in_flight.lock().await.remove(&batch_id)
    }

    /// Clear all in-flight records on reconnect.
    ///
    /// The durable entries are still in storage and will be re-drained on the
    /// next push tick. Origin's idempotent gate deduplicates re-sent batches.
    pub async fn clear_in_flight(&self) {
        self.in_flight.lock().await.clear();
    }

    /// Delete the durable entries identified by `keys` (Origin ack path).
    pub async fn ack_keys(&self, keys: &[Vec<u8>]) -> Result<(), LiteError> {
        self.queue.ack_keys(keys).await
    }

    /// Update the durable payload for `key` with the new seq stamped into `batch`.
    pub async fn update_entry(
        &self,
        key: &[u8],
        batch: &PendingColumnarBatch,
    ) -> Result<(), LiteError> {
        let payload = zerompk::to_msgpack_vec(batch).map_err(|e| LiteError::Serialization {
            detail: format!("columnar outbound update encode: {e}"),
        })?;
        self.queue.update_entry(key, &payload).await
    }

    /// Number of pending entries in durable storage.
    pub async fn len(&self) -> Result<u64, LiteError> {
        self.queue.len().await
    }

    /// Returns `true` if no pending entries remain.
    pub async fn is_empty(&self) -> Result<bool, LiteError> {
        self.queue.is_empty().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::pagedb_storage::PagedbStorageMem;

    async fn make_queue() -> ColumnarOutbound<PagedbStorageMem> {
        let storage = Arc::new(PagedbStorageMem::open_in_memory().await.unwrap());
        ColumnarOutbound::open(storage).await.unwrap()
    }

    #[tokio::test]
    async fn enqueue_and_drain() {
        let q = make_queue().await;
        q.enqueue_row("metrics", vec![Value::Integer(1)], Vec::new())
            .await
            .unwrap();
        q.enqueue_row("metrics", vec![Value::Integer(2)], Vec::new())
            .await
            .unwrap();

        let pairs = q.drain_batch(usize::MAX).await.unwrap();
        assert_eq!(pairs.len(), 2);
        assert_eq!(pairs[0].1.collection, "metrics");
        assert_eq!(pairs[0].1.rows[0][0], Value::Integer(1));
        assert_eq!(pairs[1].1.rows[0][0], Value::Integer(2));
    }

    #[tokio::test]
    async fn ack_keys_removes_entries() {
        let q = make_queue().await;
        q.enqueue_row("m", vec![Value::Integer(1)], Vec::new())
            .await
            .unwrap();
        q.enqueue_row("m", vec![Value::Integer(2)], Vec::new())
            .await
            .unwrap();

        let pairs = q.drain_batch(1).await.unwrap();
        assert_eq!(pairs.len(), 1);
        let keys: Vec<Vec<u8>> = pairs.into_iter().map(|(k, _)| k).collect();
        q.ack_keys(&keys).await.unwrap();

        assert_eq!(q.len().await.unwrap(), 1);
    }

    #[tokio::test]
    async fn cap_returns_backpressure() {
        let storage = Arc::new(PagedbStorageMem::open_in_memory().await.unwrap());
        let q = ColumnarOutbound::open_with_cap(storage, 2).await.unwrap();
        q.enqueue_row("m", vec![Value::Integer(1)], Vec::new())
            .await
            .unwrap();
        q.enqueue_row("m", vec![Value::Integer(2)], Vec::new())
            .await
            .unwrap();
        let err = q
            .enqueue_row("m", vec![Value::Integer(3)], Vec::new())
            .await
            .unwrap_err();
        assert!(matches!(err, LiteError::Backpressure { .. }));
    }
}
