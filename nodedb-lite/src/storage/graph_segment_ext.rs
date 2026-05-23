// SPDX-License-Identifier: Apache-2.0

//! `GraphSegmentExt` — pagedb segment operations for CSR adjacency data.
//!
//! The `StorageEngine` trait handles sparse, sorted, key-value state.  For CSR
//! adjacency blobs (one per graph collection, large, sequentially read on
//! cold-open restore) pagedb segments are the appropriate backing.  A single
//! segment per collection reduces B+ tree pressure from O(edge_count) entries
//! to O(1) per collection.
//!
//! `GraphHistory` rows (bitemporal edge/node history) remain on the B+ tree
//! (`Namespace::GraphHistory`) — they are genuinely sparse and sorted by
//! (entity_id, valid_time).
//!
//! Only compiled on non-WASM targets.  WASM stays on the KV blob path.

use crate::error::LiteError;

/// Extension trait: write, open, and delete CSR adjacency segments backed
/// by pagedb encrypted segment files.
///
/// One segment per collection (e.g. `"social_graph"`).  The segment contains
/// the serialized CSR checkpoint bytes exactly as produced by
/// `CsrIndex::checkpoint_to_bytes()` — no additional framing beyond the
/// 8-byte length-prefix envelope used to survive pagedb's page-boundary
/// zero-padding.
///
/// This trait is object-safe so `StorageEngine` implementations can return
/// `Option<&dyn GraphSegmentExt>` via `as_graph_segment_ext()`.
#[async_trait::async_trait]
pub trait GraphSegmentExt: Send + Sync {
    /// Write the CSR checkpoint blob for `collection`.
    ///
    /// Chunks the length-prefixed payload into 4 KiB pagedb pages, creates a
    /// new encrypted segment, and links it under `graph/csr/{collection}`.
    /// If a segment already exists under that name it is atomically replaced
    /// (old segment is tombstoned and reaped by the next `Db::gc_now` call).
    async fn write_graph_segment(&self, collection: &str, bytes: &[u8]) -> Result<(), LiteError>;

    /// Open a previously written CSR segment for `collection`.
    ///
    /// Returns the raw CSR checkpoint bytes in a `Box<[u8]>`, identical to
    /// the bytes passed to `write_graph_segment`.
    ///
    /// Returns `None` if no segment exists under `graph/csr/{collection}`.
    async fn open_graph_segment(&self, collection: &str) -> Result<Option<Box<[u8]>>, LiteError>;

    /// Delete the CSR segment for `collection`.
    ///
    /// No-op if no segment exists.  The segment file is tombstoned and reaped
    /// by the next `Db::gc_now` cycle.
    async fn delete_graph_segment(&self, collection: &str) -> Result<(), LiteError>;
}
