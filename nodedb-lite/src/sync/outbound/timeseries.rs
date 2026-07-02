//! Timeseries insert outbound queue for Lite sync.
//!
//! When a timeseries-profile columnar collection is written on Lite, rows are
//! durably enqueued here. The sync transport drains this queue and sends
//! `TimeseriesPush` (0x40) wire frames to Origin.
//!
//! # Durability
//!
//! Backed by [`DurableOutboundQueue`] in [`Namespace::TimeseriesPending`].
//! Entries persist across restarts; un-acked entries are re-sent after
//! reconnect (at-least-once delivery).
//!
//! # Backpressure
//!
//! `enqueue_row` returns [`LiteError::Backpressure`] when the queue is full,
//! propagating to the write caller so the device can apply back-pressure.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use nodedb_types::Namespace;
use nodedb_types::value::Value;
use tokio::sync::Mutex;

use super::durable_queue::DurableOutboundQueue;
use crate::error::LiteError;
use crate::storage::engine::StorageEngine;

/// One pending row batch for a timeseries collection.
#[derive(
    Debug,
    Clone,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct PendingTimeseriesBatch {
    /// Monotonic batch ID for ACK correlation.
    pub batch_id: u64,
    /// Collection name.
    pub collection: String,
    /// Column names in schema order (mirrors `ColumnarSchema::columns`).
    pub column_names: Vec<String>,
    /// Rows in schema column order.
    pub rows: Vec<Vec<Value>>,
    /// Stable idempotent-producer seq for this entry. 0 = not yet assigned;
    /// assigned at first drain and persisted so re-sends after reconnect reuse
    /// the same seq (Origin dedups instead of double-applying).
    #[serde(default)]
    pub seq: u64,
}

/// Durable outbound queue for timeseries-profile columnar inserts.
pub struct TimeseriesOutbound<S: StorageEngine> {
    queue: DurableOutboundQueue<S>,
    ids: AtomicU64,
    /// stream_seq → durable_key for entries sent but not yet acked by Origin.
    ///
    /// Keyed by stream_seq rather than batch_id because `TimeseriesAckMsg`
    /// carries `applied_seq` but not `batch_id`; on ack we clear all entries
    /// whose seq ≤ applied_seq.
    in_flight: Mutex<HashMap<u64, Vec<u8>>>,
}

impl<S: StorageEngine> TimeseriesOutbound<S> {
    /// Open the durable queue backed by [`Namespace::TimeseriesPending`].
    pub async fn open(storage: Arc<S>) -> Result<Self, LiteError> {
        Self::open_with_cap(storage, DurableOutboundQueue::<S>::DEFAULT_CAP).await
    }

    /// Open with a custom cap.
    pub async fn open_with_cap(storage: Arc<S>, cap: usize) -> Result<Self, LiteError> {
        let queue =
            DurableOutboundQueue::open_with_cap(storage, Namespace::TimeseriesPending, cap).await?;
        Ok(Self {
            queue,
            ids: AtomicU64::new(1),
            in_flight: Mutex::new(HashMap::new()),
        })
    }

    /// Durably enqueue a single row.
    ///
    /// Returns [`LiteError::Backpressure`] when the queue is at cap.
    pub async fn enqueue_row(
        &self,
        collection: &str,
        column_names: Vec<String>,
        row: Vec<Value>,
    ) -> Result<(), LiteError> {
        let batch_id = self.ids.fetch_add(1, Ordering::Relaxed);
        let batch = PendingTimeseriesBatch {
            batch_id,
            collection: collection.to_string(),
            column_names,
            rows: vec![row],
            seq: 0,
        };
        let payload = zerompk::to_msgpack_vec(&batch).map_err(|e| LiteError::Serialization {
            detail: format!("timeseries outbound encode: {e}"),
        })?;
        self.queue.enqueue(&payload).await
    }

    /// Drain up to `limit` pending batches in FIFO order, skipping any entries
    /// currently in-flight (sent but not yet acked by Origin).
    ///
    /// Returns `(durable_key, batch)` pairs. On send success, call
    /// [`mark_in_flight`]. The durable entry is deleted only on Origin ack via
    /// [`ack_in_flight`].
    pub async fn drain_batch(
        &self,
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, PendingTimeseriesBatch)>, LiteError> {
        let in_flight = self.in_flight.lock().await;
        let pairs = self.queue.drain_batch(limit).await?;
        let mut out = Vec::with_capacity(pairs.len());
        for (key, payload) in pairs {
            if in_flight.values().any(|k| k == &key) {
                continue;
            }
            let batch: PendingTimeseriesBatch =
                zerompk::from_msgpack(&payload).map_err(|e| LiteError::Serialization {
                    detail: format!("timeseries outbound decode: {e}"),
                })?;
            out.push((key, batch));
        }
        Ok(out)
    }

    /// Record that a batch has been sent to Origin and is awaiting its ack.
    ///
    /// Keyed by `stream_seq` because `TimeseriesAckMsg` echoes `applied_seq`
    /// but not `batch_id`.
    pub async fn mark_in_flight_by_seq(&self, stream_seq: u64, durable_key: Vec<u8>) {
        self.in_flight.lock().await.insert(stream_seq, durable_key);
    }

    /// Remove all in-flight records with seq ≤ `applied_seq` and return their
    /// durable keys so they can be deleted from storage.
    pub async fn ack_in_flight_through_seq(&self, applied_seq: u64) -> Vec<Vec<u8>> {
        let mut guard = self.in_flight.lock().await;
        let to_ack: Vec<u64> = guard
            .keys()
            .copied()
            .filter(|&seq| seq <= applied_seq)
            .collect();
        to_ack
            .into_iter()
            .filter_map(|seq| guard.remove(&seq))
            .collect()
    }

    /// Clear all in-flight records on reconnect so entries are re-drained.
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
        batch: &PendingTimeseriesBatch,
    ) -> Result<(), LiteError> {
        let payload = zerompk::to_msgpack_vec(batch).map_err(|e| LiteError::Serialization {
            detail: format!("timeseries outbound update encode: {e}"),
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

    async fn make_queue() -> TimeseriesOutbound<PagedbStorageMem> {
        let storage = Arc::new(PagedbStorageMem::open_in_memory().await.unwrap());
        TimeseriesOutbound::open(storage).await.unwrap()
    }

    #[tokio::test]
    async fn enqueue_and_drain() {
        let q = make_queue().await;
        q.enqueue_row(
            "metrics",
            vec!["time".into(), "value".into()],
            vec![Value::Integer(1000), Value::Float(1.0)],
        )
        .await
        .unwrap();
        q.enqueue_row(
            "metrics",
            vec!["time".into(), "value".into()],
            vec![Value::Integer(2000), Value::Float(2.0)],
        )
        .await
        .unwrap();

        let pairs = q.drain_batch(usize::MAX).await.unwrap();
        assert_eq!(pairs.len(), 2);
        assert_eq!(pairs[0].1.collection, "metrics");
        assert_eq!(pairs[1].1.rows[0][0], Value::Integer(2000));
    }

    #[tokio::test]
    async fn ack_keys_removes_entries() {
        let q = make_queue().await;
        q.enqueue_row(
            "m",
            vec!["time".into(), "value".into()],
            vec![Value::Integer(1000), Value::Float(1.0)],
        )
        .await
        .unwrap();
        q.enqueue_row(
            "m",
            vec!["time".into(), "value".into()],
            vec![Value::Integer(2000), Value::Float(2.0)],
        )
        .await
        .unwrap();

        let pairs = q.drain_batch(1).await.unwrap();
        let keys: Vec<Vec<u8>> = pairs.into_iter().map(|(k, _)| k).collect();
        q.ack_keys(&keys).await.unwrap();

        assert_eq!(q.len().await.unwrap(), 1);
    }

    #[tokio::test]
    async fn cap_returns_backpressure() {
        let storage = Arc::new(PagedbStorageMem::open_in_memory().await.unwrap());
        let q = TimeseriesOutbound::open_with_cap(storage, 2).await.unwrap();
        q.enqueue_row("m", vec!["t".into()], vec![Value::Integer(1)])
            .await
            .unwrap();
        q.enqueue_row("m", vec!["t".into()], vec![Value::Integer(2)])
            .await
            .unwrap();
        let err = q
            .enqueue_row("m", vec!["t".into()], vec![Value::Integer(3)])
            .await
            .unwrap_err();
        assert!(matches!(err, LiteError::Backpressure { .. }));
    }
}
