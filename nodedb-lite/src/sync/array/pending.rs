//! Durable FIFO queue of outbound array ops waiting for transport to Origin.
//!
//! Backed by [`Namespace::ArrayDelta`]. Each entry's key is the 18-byte
//! big-endian HLC encoding from [`Hlc::to_bytes`], giving natural FIFO order
//! because HLCs are strictly monotonic per replica and `to_bytes` is
//! byte-comparable.
//!
//! Each value holds the encoded op and its stream seq. The seq is `0` until
//! the push loop first sends the op. The push loop persists the seq before
//! that send, so a re-send after a reconnect carries the same seq.
//!
//! An op leaves the queue when Origin confirms it ([`PendingQueue::retire`])
//! or refuses it permanently ([`PendingQueue::remove`]).
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
use crate::storage::engine::{StorageEngine, WriteOp};

/// Value stored per queued op.
#[derive(zerompk::ToMessagePack, zerompk::FromMessagePack)]
struct StoredOp {
    /// Stream seq, `0` until the op is first sent.
    seq: u64,
    /// `op_codec` encoding of the op.
    op: Vec<u8>,
}

/// One queued op, in the shape the push loop sends it.
#[derive(Debug, Clone, PartialEq)]
pub struct PendingArrayOp {
    /// Target array.
    pub array: String,
    /// The op's HLC. It is also the op's queue key.
    pub hlc: Hlc,
    /// The `op_codec` encoding, sent verbatim as the delta payload.
    pub op_payload: Vec<u8>,
    /// Stream seq, `0` until the op is first sent.
    pub seq: u64,
}

fn decode_stored(value: &[u8]) -> Result<StoredOp, LiteError> {
    zerompk::from_msgpack(value).map_err(|e| LiteError::Storage {
        detail: format!("pending queue entry decode: {e}"),
    })
}

fn encode_stored(stored: &StoredOp) -> Result<Vec<u8>, LiteError> {
    zerompk::to_msgpack_vec(stored).map_err(|e| LiteError::Serialization {
        detail: format!("pending queue entry encode: {e}"),
    })
}

fn decode_payload(payload: &[u8]) -> Result<ArrayOp, LiteError> {
    op_codec::decode_op(payload).map_err(|e| LiteError::Storage {
        detail: format!("pending queue decode: {e}"),
    })
}

/// The smallest key strictly greater than `hlc`'s key.
///
/// Every key is exactly 18 bytes, so appending one zero byte orders directly
/// after `hlc` and before every larger 18-byte key.
fn key_after(hlc: Hlc) -> Vec<u8> {
    let mut key = hlc.to_bytes().to_vec();
    key.push(0);
    key
}

/// Durable outbound pending-op queue backed by [`Namespace::ArrayDelta`].
pub struct PendingQueue<S: StorageEngine> {
    storage: Arc<S>,
    cap: usize,
}

impl<S: StorageEngine> PendingQueue<S> {
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
    pub async fn enqueue(&self, op: &ArrayOp) -> Result<(), LiteError> {
        let current = self.len().await?;
        if current >= self.cap as u64 {
            return Err(LiteError::Backpressure {
                detail: format!(
                    "array pending queue full ({current} >= {}); local writes paused until Origin sync drains the queue",
                    self.cap
                ),
            });
        }
        let key = op.header.hlc.to_bytes().to_vec();
        let payload = op_codec::encode_op(op).map_err(|e| LiteError::Storage {
            detail: format!("pending queue encode: {e}"),
        })?;
        let value = encode_stored(&StoredOp {
            seq: 0,
            op: payload,
        })?;
        self.storage.put(Namespace::ArrayDelta, &key, &value).await
    }

    /// Return up to `limit` queued ops with an HLC strictly greater than
    /// `after`, in FIFO order. `None` starts at the head of the queue.
    pub async fn scan_after(
        &self,
        after: Option<Hlc>,
        limit: usize,
    ) -> Result<Vec<PendingArrayOp>, LiteError> {
        let start = after.map(key_after).unwrap_or_default();
        let pairs = self
            .storage
            .scan_range(Namespace::ArrayDelta, &start, limit)
            .await?;

        let mut ops = Vec::with_capacity(pairs.len());
        for (_key, value) in pairs {
            let stored = decode_stored(&value)?;
            let op = decode_payload(&stored.op)?;
            ops.push(PendingArrayOp {
                array: op.header.array,
                hlc: op.header.hlc,
                op_payload: stored.op,
                seq: stored.seq,
            });
        }
        Ok(ops)
    }

    /// Persist `seq` as the stream seq of the op queued under `hlc`.
    ///
    /// Returns `false` when the op is no longer queued. The push loop calls
    /// this only for an op it has not sent yet, so no ack or reject can
    /// remove the op between the read and the write.
    pub async fn set_seq(&self, hlc: Hlc, seq: u64) -> Result<bool, LiteError> {
        let key = hlc.to_bytes();
        let Some(value) = self.storage.get(Namespace::ArrayDelta, &key).await? else {
            return Ok(false);
        };
        let mut stored = decode_stored(&value)?;
        stored.seq = seq;
        self.storage
            .put(Namespace::ArrayDelta, &key, &encode_stored(&stored)?)
            .await?;
        Ok(true)
    }

    /// Remove every op named in `hlcs` in one atomic write.
    ///
    /// Ops no longer queued are skipped. Returns the number removed.
    pub async fn retire(&self, hlcs: &[Hlc]) -> Result<u64, LiteError> {
        let mut deletes = Vec::with_capacity(hlcs.len());
        for hlc in hlcs {
            let key = hlc.to_bytes().to_vec();
            if self
                .storage
                .get(Namespace::ArrayDelta, &key)
                .await?
                .is_some()
            {
                deletes.push(WriteOp::Delete {
                    ns: Namespace::ArrayDelta,
                    key,
                });
            }
        }
        let count = deletes.len() as u64;
        if !deletes.is_empty() {
            self.storage.batch_write(&deletes).await?;
        }
        Ok(count)
    }

    /// Return up to `limit` ops in FIFO order (lowest HLC first).
    ///
    /// Does not remove the returned ops — call [`ack_through`] after they are
    /// confirmed by Origin.
    pub async fn drain_batch(&self, limit: usize) -> Result<Vec<ArrayOp>, LiteError> {
        let pairs = self
            .storage
            .scan_range(Namespace::ArrayDelta, &[], limit)
            .await?;

        let mut ops = Vec::with_capacity(pairs.len());
        for (_key, value) in pairs {
            let stored = decode_stored(&value)?;
            ops.push(decode_payload(&stored.op)?);
        }
        Ok(ops)
    }

    /// Remove all ops whose `header.hlc <= ack_hlc`.
    ///
    /// Called after Origin acknowledges a batch up to `ack_hlc`. Returns the
    /// number of ops removed.
    pub async fn ack_through(&self, ack_hlc: Hlc) -> Result<u64, LiteError> {
        // HLC bytes are byte-comparable; scan from the start and stop when we
        // hit an entry with a key > ack_hlc bytes.
        let ack_key = ack_hlc.to_bytes();
        let pairs = self
            .storage
            .scan_range(Namespace::ArrayDelta, &[], usize::MAX)
            .await?;

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
            self.storage.batch_write(&to_delete).await?;
        }
        Ok(count)
    }

    /// Total count of pending ops in storage.
    pub async fn len(&self) -> Result<u64, LiteError> {
        self.storage.count(Namespace::ArrayDelta).await
    }

    /// Returns `true` if the queue contains no pending ops.
    pub async fn is_empty(&self) -> Result<bool, LiteError> {
        self.len().await.map(|n| n == 0)
    }

    /// Remove a single op identified by `hlc` from the queue.
    ///
    /// Returns `true` if the op existed and was deleted, `false` if it was
    /// not present (idempotent — safe to call on already-acked ops).
    ///
    /// Used by the inbound reject handler to roll back a single optimistic
    /// local write without touching the rest of the queue.
    pub async fn remove(&self, hlc: Hlc) -> Result<bool, LiteError> {
        let key = hlc.to_bytes().to_vec();
        let exists = self
            .storage
            .get(Namespace::ArrayDelta, &key)
            .await?
            .is_some();
        if exists {
            self.storage.delete(Namespace::ArrayDelta, &key).await?;
        }
        Ok(exists)
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::pagedb_storage::{PagedbStorageDefault, PagedbStorageMem};
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

    async fn make_queue() -> PendingQueue<PagedbStorageMem> {
        let storage = Arc::new(PagedbStorageMem::open_in_memory().await.unwrap());
        PendingQueue::new(storage)
    }

    #[tokio::test]
    async fn enqueue_drain_fifo_order() {
        let q = make_queue().await;
        q.enqueue(&make_op(10)).await.unwrap();
        q.enqueue(&make_op(20)).await.unwrap();
        q.enqueue(&make_op(30)).await.unwrap();

        let ops = q.drain_batch(usize::MAX).await.unwrap();
        assert_eq!(ops.len(), 3);
        let ms: Vec<u64> = ops.iter().map(|o| o.header.hlc.physical_ms).collect();
        assert_eq!(ms, vec![10, 20, 30], "must be FIFO (ascending HLC) order");
    }

    #[tokio::test]
    async fn enqueue_full_returns_backpressure() {
        let storage = Arc::new(PagedbStorageMem::open_in_memory().await.unwrap());
        let q = PendingQueue::with_cap(Arc::clone(&storage), 2);

        q.enqueue(&make_op(1)).await.unwrap();
        q.enqueue(&make_op(2)).await.unwrap();

        let err = q.enqueue(&make_op(3)).await.unwrap_err();
        assert!(
            matches!(err, LiteError::Backpressure { .. }),
            "expected Backpressure, got: {err:?}"
        );
    }

    #[tokio::test]
    async fn ack_through_removes_lower() {
        let q = make_queue().await;
        q.enqueue(&make_op(10)).await.unwrap();
        q.enqueue(&make_op(20)).await.unwrap();
        q.enqueue(&make_op(30)).await.unwrap();

        // Ack through ms=20 (inclusive).
        let removed = q.ack_through(hlc(20)).await.unwrap();
        assert_eq!(removed, 2);
        assert_eq!(q.len().await.unwrap(), 1);

        let remaining = q.drain_batch(usize::MAX).await.unwrap();
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].header.hlc.physical_ms, 30);
    }

    #[tokio::test]
    async fn remove_existing_returns_true() {
        let q = make_queue().await;
        q.enqueue(&make_op(10)).await.unwrap();
        q.enqueue(&make_op(20)).await.unwrap();

        let removed = q.remove(hlc(10)).await.unwrap();
        assert!(removed, "remove of existing op must return true");
        assert_eq!(q.len().await.unwrap(), 1);

        let remaining = q.drain_batch(usize::MAX).await.unwrap();
        assert_eq!(remaining[0].header.hlc.physical_ms, 20);
    }

    #[tokio::test]
    async fn remove_missing_returns_false() {
        let q = make_queue().await;
        q.enqueue(&make_op(10)).await.unwrap();

        let removed = q.remove(hlc(99)).await.unwrap();
        assert!(!removed, "remove of absent op must return false");
        assert_eq!(q.len().await.unwrap(), 1, "queue length must be unchanged");
    }

    #[tokio::test]
    async fn survives_storage_restart() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("pending_test.pagedb");

        {
            let storage = Arc::new(
                PagedbStorageDefault::open(
                    &path,
                    crate::storage::encryption::Encryption::Plaintext,
                )
                .await
                .unwrap(),
            );
            let q = PendingQueue::new(storage);
            q.enqueue(&make_op(5)).await.unwrap();
            q.enqueue(&make_op(15)).await.unwrap();
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
            let q = PendingQueue::new(storage);
            assert_eq!(q.len().await.unwrap(), 2);
            let ops = q.drain_batch(usize::MAX).await.unwrap();
            let ms: Vec<u64> = ops.iter().map(|o| o.header.hlc.physical_ms).collect();
            assert_eq!(ms, vec![5, 15]);
        }
    }

    fn physical(ops: &[PendingArrayOp]) -> Vec<u64> {
        ops.iter().map(|o| o.hlc.physical_ms).collect()
    }

    #[tokio::test]
    async fn scan_after_starts_strictly_after_the_cursor() {
        let q = make_queue().await;
        for ms in [10, 20, 30] {
            q.enqueue(&make_op(ms)).await.unwrap();
        }

        let all = q.scan_after(None, usize::MAX).await.unwrap();
        assert_eq!(physical(&all), vec![10, 20, 30]);
        assert!(all.iter().all(|o| o.seq == 0 && o.array == "arr"));
        let decoded = op_codec::decode_op(&all[0].op_payload).unwrap();
        assert_eq!(decoded.header.hlc, hlc(10), "payload is the encoded op");

        let tail = q.scan_after(Some(hlc(10)), usize::MAX).await.unwrap();
        assert_eq!(physical(&tail), vec![20, 30]);
        let bounded = q.scan_after(None, 2).await.unwrap();
        assert_eq!(physical(&bounded), vec![10, 20]);
    }

    #[tokio::test]
    async fn set_seq_persists_the_seq_and_reports_absent_ops() {
        let q = make_queue().await;
        q.enqueue(&make_op(10)).await.unwrap();

        assert!(q.set_seq(hlc(10), 7).await.unwrap());
        assert!(!q.set_seq(hlc(99), 7).await.unwrap());

        let ops = q.scan_after(None, usize::MAX).await.unwrap();
        assert_eq!(ops[0].seq, 7);
        let drained = q.drain_batch(usize::MAX).await.unwrap();
        assert_eq!(
            drained[0].header.hlc,
            hlc(10),
            "the op survives the seq write"
        );
    }

    #[tokio::test]
    async fn retire_removes_exactly_the_named_ops() {
        let q = make_queue().await;
        for ms in [10, 20, 30] {
            q.enqueue(&make_op(ms)).await.unwrap();
        }

        let removed = q.retire(&[hlc(10), hlc(30), hlc(99)]).await.unwrap();
        assert_eq!(removed, 2, "an absent op is skipped");
        let remaining = q.scan_after(None, usize::MAX).await.unwrap();
        assert_eq!(physical(&remaining), vec![20]);
    }
}
