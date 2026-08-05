//! `StorageEngine` trait: the async key-value blob interface.
//!
//! All persistent storage on the edge goes through this trait. pagedb is the
//! backend on every target — native via platform async I/O and WASM via the
//! OPFS worker. The engines above (HNSW, CSR, Loro) serialize their data to
//! opaque blobs and store them here. The storage layer never interprets the
//! data.

use std::time::{Duration, Instant};

use async_trait::async_trait;

use crate::error::LiteError;
use nodedb_types::Namespace;

/// Key-value pair returned by scan operations (`scan_prefix`, `scan_range`).
///
/// First element is the key (without namespace prefix), second is the value.
/// Defined here (not in `nodedb-types`) because it's specific to the
/// `StorageEngine` trait's scan interface.
pub type KvPair = (Vec<u8>, Vec<u8>);

/// Summary of what a [`StorageEngine::compact`] call reclaimed.
///
/// Lite-owned (not a pagedb type) so the trait doesn't force pagedb types on
/// non-pagedb impls. The pagedb-backed engine maps `pagedb::CompactStats` into
/// this; other engines return the `Default` (all-zero) value from the trait's
/// default no-op `compact`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CompactionOutcome {
    /// Number of underlying pages reclaimed (moved to free-list or freed by
    /// repacking). Zero for engines with nothing to compact.
    pub reclaimed_pages: u64,
    /// Number of segment files repacked.
    pub segments_repacked: u32,
    /// Bytes truncated from the backing file by lowering the high-water mark.
    pub file_bytes_freed: u64,
    /// Retired segment files reclaimed.
    ///
    /// A segment replacement normally reclaims its predecessor as part of the
    /// commit, so this counts the remainder: segments whose retirement was
    /// deferred because a reader still pinned them, and files left behind by a
    /// process that died mid-retirement.
    pub reclaimed_segments: u64,
    /// Bytes freed by deleting tombstoned segment files.
    pub segment_bytes_freed: u64,
}

/// Timings and operation count for one atomic batch write.
///
/// Engines that cannot expose internal stages retain the total wall duration
/// from the default [`StorageEngine::batch_write_profiled`] implementation.
#[derive(Debug, Clone, Copy, Default)]
pub struct StorageWriteProfile {
    pub total: Duration,
    pub begin: Duration,
    pub prepare: Duration,
    pub apply: Duration,
    pub commit: Duration,
    pub operations: u64,
}

/// A write operation for batch writes.
#[derive(Debug, Clone)]
pub enum WriteOp {
    /// Insert or update a key-value pair.
    Put {
        ns: Namespace,
        key: Vec<u8>,
        value: Vec<u8>,
    },
    /// Delete a key.
    Delete { ns: Namespace, key: Vec<u8> },
}

/// Async key-value blob storage backend.
///
/// Implementations must be `Send + Sync + 'static` to be shareable across
/// async tasks and engine threads.
///
/// All operations are keyed by `(Namespace, key)`. Values are opaque byte
/// slices — the storage layer never interprets them.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub trait StorageEngine: Send + Sync + 'static {
    /// Get a value by namespace and key.
    ///
    /// Returns `None` if the key does not exist.
    async fn get(&self, ns: Namespace, key: &[u8]) -> Result<Option<Vec<u8>>, LiteError>;

    /// Put (insert or overwrite) a value.
    async fn put(&self, ns: Namespace, key: &[u8], value: &[u8]) -> Result<(), LiteError>;

    /// Delete a key. No-op if the key does not exist.
    async fn delete(&self, ns: Namespace, key: &[u8]) -> Result<(), LiteError>;

    /// Scan all keys with a given prefix in a namespace.
    ///
    /// Returns `(key, value)` pairs ordered by key. The prefix match is
    /// bytewise: `key.starts_with(prefix)`.
    ///
    /// If `prefix` is empty, returns all entries in the namespace.
    async fn scan_prefix(&self, ns: Namespace, prefix: &[u8]) -> Result<Vec<KvPair>, LiteError>;

    /// Atomically apply a batch of writes.
    ///
    /// All operations in the batch succeed or fail together (transaction).
    /// This is the primary write path for engines that need to persist
    /// multiple related blobs atomically (e.g., HNSW node + metadata).
    async fn batch_write(&self, ops: &[WriteOp]) -> Result<(), LiteError>;

    /// Atomically apply a batch and report its wall-clock cost.
    ///
    /// Backends without stage-level visibility use the normal write path and
    /// report only the total duration and operation count.
    async fn batch_write_profiled(
        &self,
        ops: &[WriteOp],
    ) -> Result<StorageWriteProfile, LiteError> {
        let started = Instant::now();
        self.batch_write(ops).await?;
        Ok(StorageWriteProfile {
            total: started.elapsed(),
            operations: ops.len() as u64,
            ..StorageWriteProfile::default()
        })
    }

    /// Atomically bulk-load one strictly ordered, put-only stream.
    ///
    /// The default is deliberately unsupported: a backend must opt in only
    /// when it can retain the stream's bounded-memory and all-or-nothing
    /// semantics rather than silently collecting it into a normal batch.
    #[cfg(feature = "graphalytics-runner")]
    async fn bulk_load_sorted_unique(
        &self,
        _ops: &mut (dyn Iterator<Item = Result<WriteOp, LiteError>> + Send),
        _profile: bool,
    ) -> Result<StorageWriteProfile, LiteError> {
        Err(LiteError::Unsupported {
            detail: "sorted bulk loading is not supported by this storage engine".to_string(),
        })
    }

    /// Count the number of entries in a namespace.
    ///
    /// Useful for cold-start progress reporting and memory governor decisions.
    async fn count(&self, ns: Namespace) -> Result<u64, LiteError>;

    /// Compact the backing store, reclaiming dead pages and (when possible)
    /// truncating the file to bound on-disk growth.
    ///
    /// The default implementation is a no-op returning a zero
    /// [`CompactionOutcome`], so engines with nothing to compact (in-memory
    /// stores, test doubles) need not override it. The pagedb-backed engine
    /// overrides this to drain the deferred-free list, truncate `main.db`, AND
    /// delete tombstoned segment files.
    ///
    /// Reclaiming tombstones belongs here because compaction is the only
    /// "reclaim disk" operation callers know to run, and tombstones are by far
    /// the largest reclaimable thing: a segment replacement retires its
    /// predecessor and nothing else ever deletes it.
    async fn compact(&self) -> Result<CompactionOutcome, LiteError> {
        Ok(CompactionOutcome::default())
    }

    /// Range scan: return up to `limit` entries where key >= `start`.
    ///
    /// Results are ordered by key (lexicographic byte order).
    async fn scan_range(
        &self,
        ns: Namespace,
        start: &[u8],
        limit: usize,
    ) -> Result<Vec<KvPair>, LiteError>;

    /// Bounded range scan: return entries where `start <= key < end`.
    ///
    /// - `start = None` means the beginning of the namespace.
    /// - `end = None` means the end of the namespace.
    /// - `limit = None` means no cap.
    ///
    /// Results are ordered by key (lexicographic byte order).
    async fn scan_range_bounded(
        &self,
        ns: Namespace,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        limit: Option<usize>,
    ) -> Result<Vec<KvPair>, LiteError>;

    /// Return this engine's vector segment operations interface if supported.
    ///
    /// `PagedbStorage` returns `Some(self)`. Test doubles return `None`,
    /// falling back to the legacy blob checkpoint path.
    ///
    /// Only available on non-WASM targets (mmap is required).
    #[cfg(not(target_arch = "wasm32"))]
    fn as_vector_segment_ext(
        &self,
    ) -> Option<&dyn crate::storage::vector_segment_ext::VectorSegmentExt> {
        None
    }

    /// Return this engine's array segment operations interface if supported.
    ///
    /// `PagedbStorage` returns `Some(self)`. Test doubles return `None`,
    /// falling back to the KV blob path.
    ///
    /// Only available on non-WASM targets.
    #[cfg(not(target_arch = "wasm32"))]
    fn as_array_segment_ext(
        &self,
    ) -> Option<&dyn crate::storage::array_segment_ext::ArraySegmentExt> {
        None
    }

    /// Return this engine's FTS segment operations interface if supported.
    ///
    /// `PagedbStorage` returns `Some(self)`. Test doubles return `None`,
    /// falling back to the KV blob path where each term's postings are stored
    /// as a separate B+ tree entry.
    ///
    /// Only available on non-WASM targets.
    #[cfg(not(target_arch = "wasm32"))]
    fn as_fts_segment_ext(&self) -> Option<&dyn crate::storage::fts_segment_ext::FtsSegmentExt> {
        None
    }

    /// Return this engine's columnar segment operations interface if supported.
    ///
    /// `PagedbStorage` returns `Some(self)`. Test doubles return `None`,
    /// falling back to the KV blob path for large segment bytes.
    ///
    /// Only available on non-WASM targets.
    #[cfg(not(target_arch = "wasm32"))]
    fn as_columnar_segment_ext(
        &self,
    ) -> Option<&dyn crate::storage::columnar_segment_ext::ColumnarSegmentExt> {
        None
    }

    /// Return this engine's graph segment operations interface if supported.
    ///
    /// `PagedbStorage` returns `Some(self)`. Test doubles return `None`,
    /// falling back to the legacy `Namespace::Graph` KV blob path for CSR
    /// adjacency checkpoints.
    ///
    /// Only available on non-WASM targets.
    #[cfg(not(target_arch = "wasm32"))]
    fn as_graph_segment_ext(
        &self,
    ) -> Option<&dyn crate::storage::graph_segment_ext::GraphSegmentExt> {
        None
    }

    /// Return this engine's spatial segment operations interface if supported.
    ///
    /// `PagedbStorage` returns `Some(self)`. Test doubles return `None`,
    /// falling back to the legacy `Namespace::Spatial` KV blob path for R-tree
    /// checkpoint blobs.
    ///
    /// Only available on non-WASM targets.
    #[cfg(not(target_arch = "wasm32"))]
    fn as_spatial_segment_ext(
        &self,
    ) -> Option<&dyn crate::storage::spatial_segment_ext::SpatialSegmentExt> {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_op_debug() {
        let op = WriteOp::Put {
            ns: Namespace::Vector,
            key: vec![1, 2],
            value: vec![3, 4],
        };
        let dbg = format!("{op:?}");
        assert!(dbg.contains("Put"));
        assert!(dbg.contains("Vector"));
    }
}
