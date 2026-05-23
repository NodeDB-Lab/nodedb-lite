// SPDX-License-Identifier: Apache-2.0

//! `SpatialSegmentExt` — pagedb segment operations for R-tree checkpoint data.
//!
//! The `StorageEngine` trait handles sparse, sorted, key-value state. For R-tree
//! checkpoint blobs (one per (collection, field) pair, potentially large,
//! sequentially read on cold-open restore) pagedb segments are the appropriate
//! backing. A single segment per (collection, field) reduces B+ tree pressure from
//! O(rtree_node_count) entries to O(1) per index.
//!
//! The following keys remain on the B+ tree (`Namespace::Spatial`):
//! - `spatial:_collections` — `Vec<(String, String)>` catalog (collection, field)
//! - `spatial:_next_id` — `u64` next entry ID
//! - `spatial:{collection}:{field}:docmap` — doc_id → entry_id mapping
//!
//! Only the `spatial:{collection}:{field}:rtree` blob (CRC32C-wrapped R-tree
//! checkpoint bytes) moves to pagedb segments.
//!
//! Only compiled on non-WASM targets. WASM stays on the KV blob path.

use crate::error::LiteError;

/// Extension trait: write, open, and delete R-tree checkpoint segments backed
/// by pagedb encrypted segment files.
///
/// One segment per (collection, field) pair (e.g. `"orders"`, `"location"`).
/// The segment contains the CRC32C-wrapped R-tree checkpoint bytes exactly as
/// produced by `crate::storage::checksum::wrap` — no additional framing beyond
/// the 8-byte length-prefix envelope used to survive pagedb's page-boundary
/// zero-padding.
///
/// This trait is object-safe so `StorageEngine` implementations can return
/// `Option<&dyn SpatialSegmentExt>` via `as_spatial_segment_ext()`.
#[async_trait::async_trait]
pub trait SpatialSegmentExt: Send + Sync {
    /// Write the CRC32C-wrapped R-tree checkpoint blob for `(collection, field)`.
    ///
    /// Chunks the length-prefixed payload into 4 KiB pagedb pages, creates a new
    /// encrypted segment, and links it under `spatial/rtree/{collection}/{field}`.
    /// If a segment already exists under that name it is atomically replaced (old
    /// segment is tombstoned and reaped by the next `Db::gc_now` call).
    async fn write_spatial_segment(
        &self,
        collection: &str,
        field: &str,
        bytes: &[u8],
    ) -> Result<(), LiteError>;

    /// Open a previously written R-tree segment for `(collection, field)`.
    ///
    /// Returns the CRC32C-wrapped R-tree checkpoint bytes in a `Box<[u8]>`,
    /// identical to the bytes passed to `write_spatial_segment`.
    ///
    /// Returns `None` if no segment exists under `spatial/rtree/{collection}/{field}`.
    async fn open_spatial_segment(
        &self,
        collection: &str,
        field: &str,
    ) -> Result<Option<Box<[u8]>>, LiteError>;

    /// Delete the R-tree segment for `(collection, field)`.
    ///
    /// No-op if no segment exists. The segment file is tombstoned and reaped by
    /// the next `Db::gc_now` cycle.
    async fn delete_spatial_segment(&self, collection: &str, field: &str) -> Result<(), LiteError>;
}
