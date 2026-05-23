//! Storage-backed [`OpLog`] implementation for the array CRDT sync subsystem.
//!
//! # Storage layout
//!
//! One entry per op, stored under [`Namespace::ArrayOpLog`].
//!
//! **Composite key** = `[array_name_len: u8] [array_name_bytes] [hlc_bytes: 18]`
//!
//! The leading length byte allows:
//! - Per-array prefix scans (build `[len, name_bytes]` as prefix).
//! - Range queries for `scan_range`: iterate from prefix start, break when
//!   the prefix no longer matches.
//!
//! **Name length constraint**: `u8` accommodates names up to 255 bytes.
//! The array schema validator enforces this upper bound. If a name longer
//! than 255 bytes arrives at [`RedbOpLog::append`], the method returns
//! [`ArrayError::SegmentCorruption`] rather than silently truncating.
//!
//! # Runtime requirement
//!
//! The `OpLog` trait has synchronous methods. This implementation bridges into
//! async storage via `tokio::task::block_in_place`, which requires the
//! multi-thread Tokio runtime. The array sync subsystem is
//! `#[cfg(not(target_arch = "wasm32"))]` only, and the embedder runtimes
//! (FFI, CLI) are all multi-thread. Tests in this module use
//! `#[tokio::test(flavor = "multi_thread")]` for the same reason.

use std::future::Future;
use std::sync::Arc;

use nodedb_array::error::{ArrayError, ArrayResult};
use nodedb_array::sync::hlc::Hlc;
use nodedb_array::sync::op::ArrayOp;
use nodedb_array::sync::op_codec;
use nodedb_array::sync::op_log::{OpIter, OpLog};
use nodedb_types::Namespace;

use crate::error::LiteError;
use crate::storage::engine::{StorageEngine, WriteOp};

/// Storage-backed append-only operation log for the array CRDT sync subsystem.
///
/// Generic over any [`StorageEngine`] implementation. Bridges the sync
/// `OpLog` trait into async storage via `tokio::task::block_in_place`.
///
/// See the module-level documentation for the composite key layout.
pub struct RedbOpLog<S: StorageEngine> {
    storage: Arc<S>,
}

impl<S: StorageEngine> RedbOpLog<S> {
    /// Wrap an existing storage backend.
    pub fn new(storage: Arc<S>) -> Self {
        Self { storage }
    }
}

// ─── Key helpers ──────────────────────────────────────────────────────────────

/// Build the full composite key `[name_len: u8][name_bytes][hlc: 18]`.
///
/// Returns [`ArrayError::SegmentCorruption`] if the array name is empty or
/// exceeds 255 bytes.
fn make_key(array: &str, hlc: Hlc) -> ArrayResult<Vec<u8>> {
    let name = array.as_bytes();
    if name.is_empty() {
        return Err(ArrayError::SegmentCorruption {
            detail: "op_log: array name must not be empty".into(),
        });
    }
    if name.len() > 255 {
        return Err(ArrayError::SegmentCorruption {
            detail: "op_log: array name exceeds 255 bytes".into(),
        });
    }
    let mut key = Vec::with_capacity(1 + name.len() + 18);
    key.push(name.len() as u8);
    key.extend_from_slice(name);
    key.extend_from_slice(&hlc.to_bytes());
    Ok(key)
}

/// Build the array prefix `[name_len: u8][name_bytes]` used for prefix scans.
///
/// Returns [`ArrayError::SegmentCorruption`] on invalid name (same rules as
/// [`make_key`]).
fn make_array_prefix(array: &str) -> ArrayResult<Vec<u8>> {
    let name = array.as_bytes();
    if name.is_empty() {
        return Err(ArrayError::SegmentCorruption {
            detail: "op_log: array name must not be empty".into(),
        });
    }
    if name.len() > 255 {
        return Err(ArrayError::SegmentCorruption {
            detail: "op_log: array name exceeds 255 bytes".into(),
        });
    }
    let mut prefix = Vec::with_capacity(1 + name.len());
    prefix.push(name.len() as u8);
    prefix.extend_from_slice(name);
    Ok(prefix)
}

/// Compute the exclusive upper-bound prefix for a range scan.
///
/// Increments the last byte of `prefix`, wrapping if necessary. Returns
/// `None` if `prefix` is all-`0xFF` bytes (no upper bound possible; caller
/// should scan to the end of namespace).
#[allow(dead_code)]
fn next_prefix(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut upper = prefix.to_vec();
    for byte in upper.iter_mut().rev() {
        if *byte < 0xFF {
            *byte += 1;
            return Some(upper);
        }
        *byte = 0x00;
    }
    None
}

/// Parse a composite key back into `(array_name, Hlc)`.
///
/// Returns `None` on malformed input (too short, name_len truncated, etc.)
/// rather than panicking.
fn split_key(key: &[u8]) -> Option<(String, Hlc)> {
    if key.is_empty() {
        return None;
    }
    let name_len = key[0] as usize;
    let name_end = 1 + name_len;
    if key.len() < name_end + 18 {
        return None;
    }
    let name = std::str::from_utf8(&key[1..name_end]).ok()?;
    let hlc_bytes: &[u8; 18] = key[name_end..name_end + 18].try_into().ok()?;
    Some((name.to_owned(), Hlc::from_bytes(hlc_bytes)))
}

/// Map a [`LiteError`] to [`ArrayError::SegmentCorruption`].
fn lite_err_to_array(e: LiteError) -> ArrayError {
    ArrayError::SegmentCorruption {
        detail: format!("op_log: {e}"),
    }
}

// ─── OpLog impl ───────────────────────────────────────────────────────────────
//
// The `OpLog` trait has synchronous methods. We bridge into async storage via
// `tokio::task::block_in_place`, which runs the future on the current thread
// without yielding the multi-thread runtime. Requires multi-thread flavor.

/// Run an async closure synchronously via `block_in_place`.
///
/// Panics if called outside a multi-thread Tokio runtime (which is guaranteed
/// by the array sync subsystem's runtime contract).
fn block<F, T>(f: F) -> T
where
    F: Future<Output = T>,
{
    tokio::task::block_in_place(|| tokio::runtime::Handle::current().block_on(f))
}

impl<S: StorageEngine> OpLog for RedbOpLog<S> {
    /// Append an operation to the log.
    ///
    /// Idempotent: re-appending the same `(array, hlc)` overwrites the stored
    /// bytes with identical bytes — a semantic no-op.
    fn append(&self, op: &ArrayOp) -> ArrayResult<()> {
        let key = make_key(&op.header.array, op.header.hlc)?;
        let value = op_codec::encode_op(op)?;
        block(self.storage.put(Namespace::ArrayOpLog, &key, &value)).map_err(lite_err_to_array)
    }

    /// Return all ops with `hlc >= from`, across all arrays, in composite-key
    /// order (array name, then HLC).
    ///
    /// All matching entries are collected upfront so the storage handle is not
    /// held across the returned iterator lifetime.
    fn scan_from<'a>(&'a self, from: Hlc) -> ArrayResult<OpIter<'a>> {
        let pairs = block(
            self.storage
                .scan_range(Namespace::ArrayOpLog, &[], usize::MAX),
        )
        .map_err(lite_err_to_array)?;

        let ops: Vec<ArrayResult<ArrayOp>> = pairs
            .into_iter()
            .filter_map(|(key, value)| {
                let (_, hlc) = split_key(&key)?;
                if hlc < from {
                    return None;
                }
                Some(op_codec::decode_op(&value))
            })
            .collect();

        Ok(Box::new(ops.into_iter()))
    }

    /// Return ops for `array` with `from <= hlc <= to`, in HLC order.
    ///
    /// Uses a prefix scan on `[name_len][name_bytes]` to avoid decoding ops
    /// from other arrays.
    fn scan_range<'a>(&'a self, array: &str, from: Hlc, to: Hlc) -> ArrayResult<OpIter<'a>> {
        let prefix = make_array_prefix(array)?;

        let pairs = block(
            self.storage
                .scan_range(Namespace::ArrayOpLog, &prefix, usize::MAX),
        )
        .map_err(lite_err_to_array)?;

        let ops: Vec<ArrayResult<ArrayOp>> = pairs
            .into_iter()
            .take_while(|(key, _)| key.starts_with(&prefix))
            .filter_map(|(key, value)| {
                let (_, hlc) = split_key(&key)?;
                if hlc < from || hlc > to {
                    return None;
                }
                Some(op_codec::decode_op(&value))
            })
            .collect();

        Ok(Box::new(ops.into_iter()))
    }

    /// Return the total number of ops across all arrays.
    fn len(&self) -> ArrayResult<u64> {
        block(self.storage.count(Namespace::ArrayOpLog)).map_err(lite_err_to_array)
    }

    /// Delete all ops with `hlc < hlc` and return the count deleted.
    fn drop_below(&self, hlc: Hlc) -> ArrayResult<u64> {
        let pairs = block(
            self.storage
                .scan_range(Namespace::ArrayOpLog, &[], usize::MAX),
        )
        .map_err(lite_err_to_array)?;

        let to_delete: Vec<WriteOp> = pairs
            .into_iter()
            .filter_map(|(key, _)| {
                let (_, entry_hlc) = split_key(&key)?;
                if entry_hlc < hlc {
                    Some(WriteOp::Delete {
                        ns: Namespace::ArrayOpLog,
                        key,
                    })
                } else {
                    None
                }
            })
            .collect();

        let count = to_delete.len() as u64;
        if !to_delete.is_empty() {
            block(self.storage.batch_write(&to_delete)).map_err(lite_err_to_array)?;
        }
        Ok(count)
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_array::sync::op::{ArrayOpHeader, ArrayOpKind};
    use nodedb_array::sync::replica_id::ReplicaId;
    use nodedb_array::types::cell_value::value::CellValue;
    use nodedb_array::types::coord::value::CoordValue;

    use crate::storage::pagedb_storage::{PagedbStorageDefault, PagedbStorageMem};

    fn replica() -> ReplicaId {
        ReplicaId::new(1)
    }

    fn hlc(ms: u64, logical: u16) -> Hlc {
        Hlc::new(ms, logical, replica()).unwrap()
    }

    fn make_op(array: &str, ms: u64) -> ArrayOp {
        ArrayOp {
            header: ArrayOpHeader {
                array: array.into(),
                hlc: hlc(ms, 0),
                schema_hlc: hlc(1, 0),
                valid_from_ms: 0,
                valid_until_ms: -1,
                system_from_ms: ms as i64,
            },
            kind: ArrayOpKind::Put,
            coord: vec![CoordValue::Int64(ms as i64)],
            attrs: Some(vec![CellValue::Null]),
        }
    }

    async fn make_log() -> RedbOpLog<PagedbStorageMem> {
        let storage = Arc::new(PagedbStorageMem::open_in_memory().await.unwrap());
        RedbOpLog::new(storage)
    }

    // All tests that exercise RedbOpLog methods must run on the multi-thread
    // Tokio runtime because block_in_place requires it.

    #[tokio::test(flavor = "multi_thread")]
    async fn append_then_scan_returns_op() {
        let log = make_log().await;
        log.append(&make_op("arr", 10)).unwrap();
        log.append(&make_op("arr", 20)).unwrap();

        let ops: Vec<_> = log
            .scan_from(Hlc::ZERO)
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        assert_eq!(ops.len(), 2);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn scan_from_filters_below() {
        let log = make_log().await;
        log.append(&make_op("arr", 10)).unwrap();
        log.append(&make_op("arr", 20)).unwrap();
        log.append(&make_op("arr", 30)).unwrap();

        let ops: Vec<_> = log
            .scan_from(hlc(20, 0))
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        assert_eq!(ops.len(), 2);
        assert!(ops.iter().all(|op| op.header.hlc.physical_ms >= 20));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn scan_range_filters_array() {
        let log = make_log().await;
        log.append(&make_op("a", 10)).unwrap();
        log.append(&make_op("b", 20)).unwrap();
        log.append(&make_op("a", 30)).unwrap();

        let ops: Vec<_> = log
            .scan_range("a", Hlc::ZERO, hlc(u64::MAX >> 16, 0))
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        assert_eq!(ops.len(), 2);
        assert!(ops.iter().all(|op| op.header.array == "a"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn scan_range_filters_inclusive_bounds() {
        let log = make_log().await;
        log.append(&make_op("arr", 10)).unwrap();
        log.append(&make_op("arr", 20)).unwrap();
        log.append(&make_op("arr", 30)).unwrap();

        // Both bounds are inclusive.
        let ops: Vec<_> = log
            .scan_range("arr", hlc(10, 0), hlc(20, 0))
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        assert_eq!(ops.len(), 2);
        let ms: Vec<u64> = ops.iter().map(|op| op.header.hlc.physical_ms).collect();
        assert!(ms.contains(&10));
        assert!(ms.contains(&20));
        assert!(!ms.contains(&30));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn drop_below_drops_correctly() {
        let log = make_log().await;
        log.append(&make_op("arr", 10)).unwrap();
        log.append(&make_op("arr", 20)).unwrap();
        log.append(&make_op("arr", 30)).unwrap();

        // drop_below(20) drops only ms=10 (strict less-than).
        let dropped = log.drop_below(hlc(20, 0)).unwrap();
        assert_eq!(dropped, 1);
        assert_eq!(log.len().unwrap(), 2);

        let ops: Vec<_> = log
            .scan_from(Hlc::ZERO)
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        assert!(ops.iter().all(|op| op.header.hlc.physical_ms >= 20));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn len_counts_correctly() {
        let log = make_log().await;
        assert_eq!(log.len().unwrap(), 0);
        log.append(&make_op("x", 1)).unwrap();
        log.append(&make_op("y", 2)).unwrap();
        assert_eq!(log.len().unwrap(), 2);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn idempotent_append() {
        let log = make_log().await;
        log.append(&make_op("arr", 10)).unwrap();
        log.append(&make_op("arr", 10)).unwrap();
        assert_eq!(log.len().unwrap(), 1);
    }

    #[test]
    fn array_name_too_long_errors() {
        // make_key fails before touching storage, so no runtime needed.
        let long_name = "a".repeat(256);
        let name_bytes = long_name.as_bytes();
        assert!(name_bytes.len() > 255);
        let err = make_key(&long_name, Hlc::ZERO).unwrap_err();
        assert!(
            matches!(err, ArrayError::SegmentCorruption { ref detail } if detail.contains("255")),
            "unexpected error: {err:?}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn decode_corruption_propagates() {
        let storage = Arc::new(PagedbStorageMem::open_in_memory().await.unwrap());
        let log = RedbOpLog::new(Arc::clone(&storage));

        // Write a valid key with garbage value directly via storage.
        let valid_key = make_key("arr", hlc(99, 0)).unwrap();
        storage
            .put(Namespace::ArrayOpLog, &valid_key, b"\xff\xfe garbage")
            .await
            .unwrap();

        let results: Vec<_> = log.scan_from(Hlc::ZERO).unwrap().collect();
        assert_eq!(results.len(), 1);
        assert!(results[0].is_err(), "expected decode error, got Ok");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn survives_storage_restart() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("op_log_test.pagedb");

        {
            let storage = Arc::new(PagedbStorageDefault::open(&path).await.unwrap());
            let log = RedbOpLog::new(Arc::clone(&storage));
            log.append(&make_op("arr", 10)).unwrap();
            log.append(&make_op("arr", 20)).unwrap();
        }

        // Reopen the same file.
        {
            let storage = Arc::new(PagedbStorageDefault::open(&path).await.unwrap());
            let log = RedbOpLog::new(storage);
            assert_eq!(log.len().unwrap(), 2);
            let ops: Vec<_> = log
                .scan_from(Hlc::ZERO)
                .unwrap()
                .map(|r| r.unwrap())
                .collect();
            assert_eq!(ops.len(), 2);
        }
    }

    #[test]
    fn next_prefix_increments_last_byte() {
        assert_eq!(next_prefix(&[0x01, 0x61]), Some(vec![0x01, 0x62]));
        assert_eq!(next_prefix(&[0x01, 0xFF]), Some(vec![0x02, 0x00]));
        assert_eq!(next_prefix(&[0xFF, 0xFF]), None);
    }
}
