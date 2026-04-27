//! Durable FIFO queue of outbound array ops waiting for transport to Origin.
//!
//! Backed by [`Namespace::ArrayDelta`]. Each entry's key is the 18-byte
//! big-endian HLC encoding from [`Hlc::to_bytes`], giving natural FIFO order
//! because HLCs are strictly monotonic per replica and `to_bytes` is
//! byte-comparable.
//!
//! # Backpressure
//!
//! When [`PendingQueue::len`] reaches [`PendingQueue::DEFAULT_CAP`] (or the
//! cap supplied to [`PendingQueue::with_cap`]), [`PendingQueue::enqueue`]
//! returns [`LiteError::Backpressure`] instead of writing to storage.

use std::sync::Arc;

use nodedb_array::sync::hlc::Hlc;
use nodedb_array::sync::op::ArrayOp;
use nodedb_array::sync::op_codec;
use nodedb_types::Namespace;

use crate::error::LiteError;
use crate::storage::engine::{StorageEngineSync, WriteOp};

/// Durable outbound pending-op queue backed by [`Namespace::ArrayDelta`].
pub struct PendingQueue<S: StorageEngineSync> {
    storage: Arc<S>,
    cap: usize,
}

impl<S: StorageEngineSync> PendingQueue<S> {
    /// Default maximum number of pending ops before backpressure kicks in.
    pub const DEFAULT_CAP: usize = 100_000;

    /// Create a queue with [`DEFAULT_CAP`].
    pub fn new(storage: Arc<S>) -> Self {
        Self {
            storage,
            cap: Self::DEFAULT_CAP,
        }
    }

    /// Create a queue with a custom cap (useful for tests with small limits).
    pub fn with_cap(storage: Arc<S>, cap: usize) -> Self {
        Self { storage, cap }
    }

    /// Enqueue a single op.
    ///
    /// Returns [`LiteError::Backpressure`] when `len() >= cap` to prevent
    /// unbounded queue growth during prolonged offline operation.
    pub fn enqueue(&self, op: &ArrayOp) -> Result<(), LiteError> {
        let current = self.len()?;
        if current >= self.cap as u64 {
            return Err(LiteError::Backpressure {
                detail: format!(
                    "array pending queue full ({current} >= {}); local writes paused until Origin sync drains the queue",
                    self.cap
                ),
            });
        }
        let key = op.header.hlc.to_bytes().to_vec();
        let value = op_codec::encode_op(op).map_err(|e| LiteError::Storage {
            detail: format!("pending queue encode: {e}"),
        })?;
        self.storage.put_sync(Namespace::ArrayDelta, &key, &value)
    }

    /// Return up to `limit` ops in FIFO order (lowest HLC first).
    ///
    /// Does not remove the returned ops — call [`ack_through`] after they are
    /// confirmed by Origin.
    pub fn drain_batch(&self, limit: usize) -> Result<Vec<ArrayOp>, LiteError> {
        let pairs = self
            .storage
            .scan_range_sync(Namespace::ArrayDelta, &[], limit)?;

        let mut ops = Vec::with_capacity(pairs.len());
        for (_key, value) in pairs {
            let op = op_codec::decode_op(&value).map_err(|e| LiteError::Storage {
                detail: format!("pending queue decode: {e}"),
            })?;
            ops.push(op);
        }
        Ok(ops)
    }

    /// Remove all ops whose `header.hlc <= ack_hlc`.
    ///
    /// Called after Origin acknowledges a batch up to `ack_hlc`. Returns the
    /// number of ops removed.
    pub fn ack_through(&self, ack_hlc: Hlc) -> Result<u64, LiteError> {
        // HLC bytes are byte-comparable; scan from the start and stop when we
        // hit an entry with a key > ack_hlc bytes.
        let ack_key = ack_hlc.to_bytes();
        let pairs = self
            .storage
            .scan_range_sync(Namespace::ArrayDelta, &[], usize::MAX)?;

        let to_delete: Vec<WriteOp> = pairs
            .into_iter()
            .take_while(|(key, _)| {
                if key.len() != 18 {
                    return false;
                }
                // key <= ack_key (bytes are big-endian comparable)
                key.as_slice() <= ack_key.as_slice()
            })
            .map(|(key, _)| WriteOp::Delete {
                ns: Namespace::ArrayDelta,
                key,
            })
            .collect();

        let count = to_delete.len() as u64;
        if !to_delete.is_empty() {
            self.storage.batch_write_sync(&to_delete)?;
        }
        Ok(count)
    }

    /// Total count of pending ops in storage.
    pub fn len(&self) -> Result<u64, LiteError> {
        self.storage.count_sync(Namespace::ArrayDelta)
    }

    /// Returns `true` if the queue contains no pending ops.
    pub fn is_empty(&self) -> Result<bool, LiteError> {
        self.len().map(|n| n == 0)
    }

    /// Remove a single op identified by `hlc` from the queue.
    ///
    /// Returns `true` if the op existed and was deleted, `false` if it was
    /// not present (idempotent — safe to call on already-acked ops).
    ///
    /// Used by the inbound reject handler to roll back a single optimistic
    /// local write without touching the rest of the queue.
    pub fn remove(&self, hlc: Hlc) -> Result<bool, LiteError> {
        let key = hlc.to_bytes().to_vec();
        let exists = self
            .storage
            .get_sync(Namespace::ArrayDelta, &key)?
            .is_some();
        if exists {
            self.storage.delete_sync(Namespace::ArrayDelta, &key)?;
        }
        Ok(exists)
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::redb_storage::RedbStorage;
    use nodedb_array::sync::op::{ArrayOpHeader, ArrayOpKind};
    use nodedb_array::sync::replica_id::ReplicaId;
    use nodedb_array::types::cell_value::value::CellValue;
    use nodedb_array::types::coord::value::CoordValue;

    fn replica() -> ReplicaId {
        ReplicaId::new(1)
    }

    fn hlc(ms: u64) -> Hlc {
        Hlc::new(ms, 0, replica()).unwrap()
    }

    fn make_op(ms: u64) -> ArrayOp {
        ArrayOp {
            header: ArrayOpHeader {
                array: "arr".into(),
                hlc: hlc(ms),
                schema_hlc: hlc(1),
                valid_from_ms: 0,
                valid_until_ms: -1,
                system_from_ms: ms as i64,
            },
            kind: ArrayOpKind::Put,
            coord: vec![CoordValue::Int64(ms as i64)],
            attrs: Some(vec![CellValue::Null]),
        }
    }

    fn make_queue() -> PendingQueue<RedbStorage> {
        let storage = Arc::new(RedbStorage::open_in_memory().unwrap());
        PendingQueue::new(storage)
    }

    #[test]
    fn enqueue_drain_fifo_order() {
        let q = make_queue();
        q.enqueue(&make_op(10)).unwrap();
        q.enqueue(&make_op(20)).unwrap();
        q.enqueue(&make_op(30)).unwrap();

        let ops = q.drain_batch(usize::MAX).unwrap();
        assert_eq!(ops.len(), 3);
        let ms: Vec<u64> = ops.iter().map(|o| o.header.hlc.physical_ms).collect();
        assert_eq!(ms, vec![10, 20, 30], "must be FIFO (ascending HLC) order");
    }

    #[test]
    fn enqueue_full_returns_backpressure() {
        let storage = Arc::new(RedbStorage::open_in_memory().unwrap());
        let q = PendingQueue::with_cap(Arc::clone(&storage), 2);

        q.enqueue(&make_op(1)).unwrap();
        q.enqueue(&make_op(2)).unwrap();

        let err = q.enqueue(&make_op(3)).unwrap_err();
        assert!(
            matches!(err, LiteError::Backpressure { .. }),
            "expected Backpressure, got: {err:?}"
        );
    }

    #[test]
    fn ack_through_removes_lower() {
        let q = make_queue();
        q.enqueue(&make_op(10)).unwrap();
        q.enqueue(&make_op(20)).unwrap();
        q.enqueue(&make_op(30)).unwrap();

        // Ack through ms=20 (inclusive).
        let removed = q.ack_through(hlc(20)).unwrap();
        assert_eq!(removed, 2);
        assert_eq!(q.len().unwrap(), 1);

        let remaining = q.drain_batch(usize::MAX).unwrap();
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].header.hlc.physical_ms, 30);
    }

    #[test]
    fn remove_existing_returns_true() {
        let q = make_queue();
        q.enqueue(&make_op(10)).unwrap();
        q.enqueue(&make_op(20)).unwrap();

        let removed = q.remove(hlc(10)).unwrap();
        assert!(removed, "remove of existing op must return true");
        assert_eq!(q.len().unwrap(), 1);

        let remaining = q.drain_batch(usize::MAX).unwrap();
        assert_eq!(remaining[0].header.hlc.physical_ms, 20);
    }

    #[test]
    fn remove_missing_returns_false() {
        let q = make_queue();
        q.enqueue(&make_op(10)).unwrap();

        let removed = q.remove(hlc(99)).unwrap();
        assert!(!removed, "remove of absent op must return false");
        assert_eq!(q.len().unwrap(), 1, "queue length must be unchanged");
    }

    #[test]
    fn survives_storage_restart() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("pending_test.redb");

        {
            let storage = Arc::new(RedbStorage::open(&path).unwrap());
            let q = PendingQueue::new(storage);
            q.enqueue(&make_op(5)).unwrap();
            q.enqueue(&make_op(15)).unwrap();
        }

        {
            let storage = Arc::new(RedbStorage::open(&path).unwrap());
            let q = PendingQueue::new(storage);
            assert_eq!(q.len().unwrap(), 2);
            let ops = q.drain_batch(usize::MAX).unwrap();
            let ms: Vec<u64> = ops.iter().map(|o| o.header.hlc.physical_ms).collect();
            assert_eq!(ms, vec![5, 15]);
        }
    }
}
