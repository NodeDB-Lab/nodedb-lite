//! Durable FIFO queue backed by a [`StorageEngine`] namespace.
//!
//! Used by the columnar and timeseries outbound sync paths to persist pending
//! row batches across restarts. Keys are big-endian monotonic `u64` IDs
//! (`[u8; 8]`), giving natural FIFO order because byte-comparable big-endian
//! integers sort in ascending insertion order.
//!
//! # Backpressure
//!
//! When [`DurableOutboundQueue::len`] reaches the configured cap,
//! [`DurableOutboundQueue::enqueue`] returns [`LiteError::Backpressure`]
//! instead of writing to storage. RAM is bounded regardless of cap; the items
//! themselves always live on disk.
//!
//! # Drain and acknowledge
//!
//! [`drain_batch`] reads up to `limit` entries in FIFO order **without
//! removing them**. The caller sends the payloads to Origin, then calls
//! [`ack_keys`] with the confirmed keys to delete them. Un-acked entries
//! survive a crash and are re-drained on the next connect.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use nodedb_types::Namespace;

use crate::error::LiteError;
use crate::storage::engine::{StorageEngine, WriteOp};

/// Durable FIFO outbound queue backed by a [`StorageEngine`] namespace.
pub struct DurableOutboundQueue<S: StorageEngine> {
    storage: Arc<S>,
    namespace: Namespace,
    cap: usize,
    /// Monotonic counter for the next key to assign.
    ///
    /// Initialised at `open()` time from the maximum existing key so the
    /// counter survives restarts and never regresses.
    next_id: AtomicU64,
}

impl<S: StorageEngine> DurableOutboundQueue<S> {
    /// Default maximum number of pending batches before backpressure kicks in.
    pub const DEFAULT_CAP: usize = 100_000;

    /// Open a queue over the given namespace, deriving the next-ID from the
    /// highest key currently in storage (zero on an empty namespace).
    pub async fn open(storage: Arc<S>, namespace: Namespace) -> Result<Self, LiteError> {
        Self::open_with_cap(storage, namespace, Self::DEFAULT_CAP).await
    }

    /// Open with a custom cap (useful for tests with small limits).
    pub async fn open_with_cap(
        storage: Arc<S>,
        namespace: Namespace,
        cap: usize,
    ) -> Result<Self, LiteError> {
        // Derive next ID from the highest existing key so restarts are seamless.
        let next_id = Self::recover_next_id(&storage, namespace).await?;
        Ok(Self {
            storage,
            namespace,
            cap,
            next_id: AtomicU64::new(next_id),
        })
    }

    /// Read the max key in the namespace and return `max + 1` (or 1 if empty).
    async fn recover_next_id(storage: &Arc<S>, namespace: Namespace) -> Result<u64, LiteError> {
        // scan_range with empty start and limit = usize::MAX reads all keys.
        // We only need the last key so we use a single scan and take the tail.
        // This is called once at startup, not on the hot path.
        let pairs = storage.scan_range(namespace, &[], usize::MAX).await?;
        let max = pairs.last().and_then(|(key, _)| {
            if key.len() == 8 {
                Some(u64::from_be_bytes(key[..8].try_into().ok()?))
            } else {
                None
            }
        });
        Ok(max.map(|m| m.saturating_add(1)).unwrap_or(1))
    }

    /// Enqueue a pre-encoded payload.
    ///
    /// Returns [`LiteError::Backpressure`] when `len() >= cap`.
    pub async fn enqueue(&self, payload: &[u8]) -> Result<(), LiteError> {
        let current = self.len().await?;
        if current >= self.cap as u64 {
            return Err(LiteError::Backpressure {
                detail: format!(
                    "outbound pending queue full ({current} >= {}); writes paused until \
                     Origin sync drains the queue",
                    self.cap
                ),
            });
        }
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let key = id.to_be_bytes().to_vec();
        self.storage.put(self.namespace, &key, payload).await
    }

    /// Return up to `limit` entries in FIFO order (lowest key first).
    ///
    /// Does **not** remove the returned entries. Call [`ack_keys`] after
    /// Origin confirms delivery to remove them.
    pub async fn drain_batch(&self, limit: usize) -> Result<Vec<(Vec<u8>, Vec<u8>)>, LiteError> {
        self.storage.scan_range(self.namespace, &[], limit).await
    }

    /// Delete the entries identified by `keys` from storage.
    ///
    /// Called after Origin acknowledges the corresponding batches. Deleting
    /// only confirmed keys means un-acked entries survive a crash and will be
    /// re-sent on reconnect (at-least-once delivery).
    pub async fn ack_keys(&self, keys: &[Vec<u8>]) -> Result<(), LiteError> {
        if keys.is_empty() {
            return Ok(());
        }
        let ops: Vec<WriteOp> = keys
            .iter()
            .map(|key| WriteOp::Delete {
                ns: self.namespace,
                key: key.clone(),
            })
            .collect();
        self.storage.batch_write(&ops).await
    }

    /// Update the payload stored under an existing `key` in-place.
    ///
    /// Used by the push paths to persist an assigned stream seq back into a
    /// durable entry before sending, so reconnects reuse the same seq.
    pub async fn update_entry(&self, key: &[u8], payload: &[u8]) -> Result<(), LiteError> {
        self.storage.put(self.namespace, key, payload).await
    }

    /// Total number of pending entries in storage.
    pub async fn len(&self) -> Result<u64, LiteError> {
        self.storage.count(self.namespace).await
    }

    /// Returns `true` if the queue contains no pending entries.
    pub async fn is_empty(&self) -> Result<bool, LiteError> {
        self.len().await.map(|n| n == 0)
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::pagedb_storage::PagedbStorageMem;

    async fn make_queue() -> DurableOutboundQueue<PagedbStorageMem> {
        let storage = Arc::new(PagedbStorageMem::open_in_memory().await.unwrap());
        DurableOutboundQueue::open(storage, Namespace::ColumnarPending)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn enqueue_persists_and_drain_fifo() {
        let q = make_queue().await;
        q.enqueue(b"payload-a").await.unwrap();
        q.enqueue(b"payload-b").await.unwrap();
        q.enqueue(b"payload-c").await.unwrap();

        let pairs = q.drain_batch(usize::MAX).await.unwrap();
        assert_eq!(pairs.len(), 3);
        let payloads: Vec<&[u8]> = pairs.iter().map(|(_, v)| v.as_slice()).collect();
        assert_eq!(payloads, vec![b"payload-a", b"payload-b", b"payload-c"]);
    }

    #[tokio::test]
    async fn drain_batch_respects_limit() {
        let q = make_queue().await;
        q.enqueue(b"a").await.unwrap();
        q.enqueue(b"b").await.unwrap();
        q.enqueue(b"c").await.unwrap();

        let pairs = q.drain_batch(2).await.unwrap();
        assert_eq!(pairs.len(), 2);
    }

    #[tokio::test]
    async fn ack_keys_deletes_entries() {
        let q = make_queue().await;
        q.enqueue(b"x").await.unwrap();
        q.enqueue(b"y").await.unwrap();
        q.enqueue(b"z").await.unwrap();

        let pairs = q.drain_batch(2).await.unwrap();
        assert_eq!(pairs.len(), 2);
        let keys: Vec<Vec<u8>> = pairs.into_iter().map(|(k, _)| k).collect();
        q.ack_keys(&keys).await.unwrap();

        assert_eq!(q.len().await.unwrap(), 1);
        let remaining = q.drain_batch(usize::MAX).await.unwrap();
        assert_eq!(remaining[0].1, b"z");
    }

    #[tokio::test]
    async fn len_and_is_empty() {
        let q = make_queue().await;
        assert!(q.is_empty().await.unwrap());
        q.enqueue(b"data").await.unwrap();
        assert_eq!(q.len().await.unwrap(), 1);
        assert!(!q.is_empty().await.unwrap());
    }

    #[tokio::test]
    async fn cap_returns_backpressure() {
        let storage = Arc::new(PagedbStorageMem::open_in_memory().await.unwrap());
        let q = DurableOutboundQueue::open_with_cap(storage, Namespace::ColumnarPending, 2)
            .await
            .unwrap();

        q.enqueue(b"a").await.unwrap();
        q.enqueue(b"b").await.unwrap();
        let err = q.enqueue(b"c").await.unwrap_err();
        assert!(
            matches!(err, LiteError::Backpressure { .. }),
            "expected Backpressure, got: {err:?}"
        );
    }

    #[tokio::test]
    async fn update_entry_persists_and_re_drain_reuses_payload() {
        // Models the stable-seq fix: a push assigns a seq, persists it into the
        // durable entry via update_entry, and a later re-drain (reconnect) must
        // return the SAME updated payload — so the re-sent frame carries the
        // same seq and Origin dedups it instead of double-applying.
        let q = make_queue().await;
        q.enqueue(b"seq=0").await.unwrap();

        let pairs = q.drain_batch(usize::MAX).await.unwrap();
        assert_eq!(pairs.len(), 1);
        let key = pairs[0].0.clone();
        assert_eq!(pairs[0].1, b"seq=0");

        // First drain assigns + persists the seq back into the entry.
        q.update_entry(&key, b"seq=7").await.unwrap();

        // Re-drain (e.g. after reconnect clears in-flight) reuses the persisted
        // payload — same key, same (now-assigned) seq — and does NOT duplicate.
        let re = q.drain_batch(usize::MAX).await.unwrap();
        assert_eq!(re.len(), 1, "update_entry must not create a new entry");
        assert_eq!(re[0].0, key, "key is stable across update");
        assert_eq!(
            re[0].1, b"seq=7",
            "re-drain returns the persisted seq payload"
        );
        assert_eq!(q.len().await.unwrap(), 1);
    }

    #[tokio::test]
    async fn survives_reload() {
        let storage = Arc::new(PagedbStorageMem::open_in_memory().await.unwrap());
        {
            let q = DurableOutboundQueue::open(Arc::clone(&storage), Namespace::ColumnarPending)
                .await
                .unwrap();
            q.enqueue(b"first").await.unwrap();
            q.enqueue(b"second").await.unwrap();
        }
        // Re-open over the same storage — counter resumes after max existing key.
        let q = DurableOutboundQueue::open(Arc::clone(&storage), Namespace::ColumnarPending)
            .await
            .unwrap();
        assert_eq!(q.len().await.unwrap(), 2);
        // New entry must not overwrite existing keys.
        q.enqueue(b"third").await.unwrap();
        assert_eq!(q.len().await.unwrap(), 3);

        let pairs = q.drain_batch(usize::MAX).await.unwrap();
        let payloads: Vec<&[u8]> = pairs.iter().map(|(_, v)| v.as_slice()).collect();
        assert_eq!(
            payloads,
            vec![
                b"first".as_slice(),
                b"second".as_slice(),
                b"third".as_slice()
            ]
        );
    }
}
