//! Per-stream monotonic sequence frontier — durable state for outbound frame
//! stamping and inbound ack tracking.
//!
//! # Persistence
//!
//! Each per-stream frontier is persisted under `Namespace::Meta` with key
//! `"sync.stream_seq:{stream_id:016x}"`. The 16-byte value encodes
//! `last_assigned || last_acked` as two big-endian u64s.
//!
//! [`StreamSeqTracker::load`] restores all entries on startup so the first
//! outbound frame after a restart continues from where it left off (the
//! persist-before-send invariant is maintained across process restarts).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use nodedb_types::Namespace;

use crate::error::LiteError;
use crate::storage::engine::StorageEngine;

/// Storage key prefix for per-stream sequence frontier entries.
const STREAM_SEQ_PREFIX: &str = "sync.stream_seq:";

/// Storage key for a given stream_id.
fn stream_seq_key(stream_id: u64) -> Vec<u8> {
    format!("{STREAM_SEQ_PREFIX}{stream_id:016x}").into_bytes()
}

/// In-memory frontier for a single stream.
#[derive(Debug, Clone, Copy, Default)]
struct Frontier {
    /// Highest sequence number assigned (and persisted) for outbound frames.
    last_assigned: u64,
    /// Highest sequence number acknowledged by Origin.
    last_acked: u64,
}

/// Tracks the per-stream monotonic sequence frontier for outbound frame
/// stamping and ack recording.
///
/// Thread-safe via an internal [`Mutex`]. Persist-before-send is enforced
/// in [`next_seq`]: storage is flushed before the sequence number is returned
/// to the caller.
pub struct StreamSeqTracker<S: StorageEngine> {
    storage: Arc<S>,
    state: Mutex<HashMap<u64, Frontier>>,
}

impl<S: StorageEngine> StreamSeqTracker<S> {
    /// Load all persisted stream sequence entries from `Namespace::Meta`.
    ///
    /// Scans keys starting with `b"sync.stream_seq:"` and parses each
    /// 16-byte value as `last_assigned || last_acked` (two big-endian u64s).
    /// Returns an empty tracker if no entries are stored yet.
    pub async fn load(storage: Arc<S>) -> Result<Self, LiteError> {
        let prefix = STREAM_SEQ_PREFIX.as_bytes();
        let pairs = storage
            .scan_range(Namespace::Meta, prefix, usize::MAX)
            .await?;

        let mut state = HashMap::new();
        for (key, value) in pairs {
            if !key.starts_with(prefix) {
                break;
            }
            let id_bytes = &key[prefix.len()..];
            let id_str = std::str::from_utf8(id_bytes).map_err(|e| LiteError::Storage {
                detail: format!("stream_seq_tracker: non-UTF8 stream_id in storage: {e}"),
            })?;
            let stream_id = u64::from_str_radix(id_str, 16).map_err(|e| LiteError::Storage {
                detail: format!("stream_seq_tracker: invalid hex stream_id '{id_str}': {e}"),
            })?;

            let arr: [u8; 16] = value.try_into().map_err(|v: Vec<u8>| LiteError::Storage {
                detail: format!(
                    "stream_seq_tracker: frontier wrong length ({}) for stream {stream_id:016x}",
                    v.len()
                ),
            })?;
            let last_assigned = u64::from_be_bytes(arr[..8].try_into().unwrap_or([0; 8]));
            let last_acked = u64::from_be_bytes(arr[8..].try_into().unwrap_or([0; 8]));
            state.insert(
                stream_id,
                Frontier {
                    last_assigned,
                    last_acked,
                },
            );
        }

        Ok(Self {
            storage,
            state: Mutex::new(state),
        })
    }

    /// Assign the next sequence number for `stream_id`.
    ///
    /// Computes `next = last_assigned + 1`, persists the updated frontier to
    /// `Namespace::Meta` BEFORE returning (persist-before-send invariant), then
    /// returns `next`.
    ///
    /// The in-memory lock is dropped before the storage await to avoid holding
    /// it across async I/O.
    pub async fn next_seq(&self, stream_id: u64) -> Result<u64, LiteError> {
        let (next, encoded) = {
            let mut state = self.state.lock().map_err(|_| LiteError::LockPoisoned)?;
            let entry = state.entry(stream_id).or_default();
            let next = entry.last_assigned + 1;
            entry.last_assigned = next;
            let encoded = encode_frontier(*entry);
            (next, encoded)
        };

        self.storage
            .put(Namespace::Meta, &stream_seq_key(stream_id), &encoded)
            .await?;

        Ok(next)
    }

    /// Record that Origin has applied `applied_seq` for `stream_id`.
    ///
    /// Advances `last_acked` (and `last_assigned` if needed to stay consistent),
    /// then persists. No-op if `applied_seq <= current last_acked`.
    pub async fn record_ack(&self, stream_id: u64, applied_seq: u64) -> Result<(), LiteError> {
        let (updated, encoded) = {
            let mut state = self.state.lock().map_err(|_| LiteError::LockPoisoned)?;
            let entry = state.entry(stream_id).or_default();
            if applied_seq <= entry.last_acked {
                return Ok(());
            }
            entry.last_acked = applied_seq;
            entry.last_assigned = entry.last_assigned.max(applied_seq);
            let encoded = encode_frontier(*entry);
            (true, encoded)
        };

        if updated {
            self.storage
                .put(Namespace::Meta, &stream_seq_key(stream_id), &encoded)
                .await?;
        }

        Ok(())
    }

    /// Return the highest acknowledged sequence for `stream_id`, or 0 if unknown.
    pub async fn last_acked(&self, stream_id: u64) -> u64 {
        self.state
            .lock()
            .ok()
            .and_then(|s| s.get(&stream_id).map(|f| f.last_acked))
            .unwrap_or(0)
    }

    /// Return the highest assigned sequence for `stream_id`, or 0 if unknown.
    pub async fn last_assigned(&self, stream_id: u64) -> u64 {
        self.state
            .lock()
            .ok()
            .and_then(|s| s.get(&stream_id).map(|f| f.last_assigned))
            .unwrap_or(0)
    }
}

/// Encode a `Frontier` as 16 bytes: `last_assigned || last_acked` big-endian.
fn encode_frontier(f: Frontier) -> Vec<u8> {
    let mut buf = Vec::with_capacity(16);
    buf.extend_from_slice(&f.last_assigned.to_be_bytes());
    buf.extend_from_slice(&f.last_acked.to_be_bytes());
    buf
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::pagedb_storage::{PagedbStorageDefault, PagedbStorageMem};

    async fn make_tracker() -> StreamSeqTracker<PagedbStorageMem> {
        let storage = Arc::new(PagedbStorageMem::open_in_memory().await.unwrap());
        StreamSeqTracker::load(storage).await.unwrap()
    }

    #[tokio::test]
    async fn next_seq_starts_at_one() {
        let tracker = make_tracker().await;
        let seq = tracker.next_seq(1).await.unwrap();
        assert_eq!(seq, 1);
    }

    #[tokio::test]
    async fn next_seq_is_monotonic() {
        let tracker = make_tracker().await;
        let s1 = tracker.next_seq(42).await.unwrap();
        let s2 = tracker.next_seq(42).await.unwrap();
        let s3 = tracker.next_seq(42).await.unwrap();
        assert_eq!(s1, 1);
        assert_eq!(s2, 2);
        assert_eq!(s3, 3);
    }

    #[tokio::test]
    async fn next_seq_survives_reload() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("stream_seq_test.pagedb");

        {
            let storage = Arc::new(
                PagedbStorageDefault::open(
                    &path,
                    crate::storage::encryption::Encryption::Plaintext,
                )
                .await
                .unwrap(),
            );
            let tracker = StreamSeqTracker::load(Arc::clone(&storage)).await.unwrap();
            assert_eq!(tracker.next_seq(7).await.unwrap(), 1);
            assert_eq!(tracker.next_seq(7).await.unwrap(), 2);
        }

        {
            let storage = Arc::new(
                PagedbStorageDefault::open(
                    &path,
                    crate::storage::encryption::Encryption::Plaintext,
                )
                .await
                .unwrap(),
            );
            let tracker = StreamSeqTracker::load(storage).await.unwrap();
            // Must resume from 2, not restart from 0.
            assert_eq!(
                tracker.next_seq(7).await.unwrap(),
                3,
                "seq must resume from durable last_assigned on reload"
            );
        }
    }

    #[tokio::test]
    async fn record_ack_advances_and_survives_reload() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("stream_seq_ack_test.pagedb");

        {
            let storage = Arc::new(
                PagedbStorageDefault::open(
                    &path,
                    crate::storage::encryption::Encryption::Plaintext,
                )
                .await
                .unwrap(),
            );
            let tracker = StreamSeqTracker::load(Arc::clone(&storage)).await.unwrap();
            tracker.next_seq(5).await.unwrap(); // assigns 1
            tracker.next_seq(5).await.unwrap(); // assigns 2
            tracker.record_ack(5, 2).await.unwrap();
            assert_eq!(tracker.last_acked(5).await, 2);
        }

        {
            let storage = Arc::new(
                PagedbStorageDefault::open(
                    &path,
                    crate::storage::encryption::Encryption::Plaintext,
                )
                .await
                .unwrap(),
            );
            let tracker = StreamSeqTracker::load(storage).await.unwrap();
            assert_eq!(
                tracker.last_acked(5).await,
                2,
                "last_acked must survive storage restart"
            );
        }
    }

    #[tokio::test]
    async fn record_ack_is_noop_if_not_advancing() {
        let tracker = make_tracker().await;
        tracker.next_seq(3).await.unwrap();
        tracker.record_ack(3, 1).await.unwrap();
        assert_eq!(tracker.last_acked(3).await, 1);
        // Recording same or lower is a no-op.
        tracker.record_ack(3, 1).await.unwrap();
        tracker.record_ack(3, 0).await.unwrap();
        assert_eq!(tracker.last_acked(3).await, 1);
    }

    #[tokio::test]
    async fn two_stream_ids_are_independent() {
        let tracker = make_tracker().await;
        assert_eq!(tracker.next_seq(10).await.unwrap(), 1);
        assert_eq!(tracker.next_seq(10).await.unwrap(), 2);
        // Stream 20 starts independently at 1.
        assert_eq!(tracker.next_seq(20).await.unwrap(), 1);
        assert_eq!(tracker.next_seq(20).await.unwrap(), 2);
        // Stream 10 continues from where it left off.
        assert_eq!(tracker.next_seq(10).await.unwrap(), 3);

        tracker.record_ack(10, 2).await.unwrap();
        assert_eq!(tracker.last_acked(10).await, 2);
        // Stream 20's ack is unaffected.
        assert_eq!(tracker.last_acked(20).await, 0);
    }
}
