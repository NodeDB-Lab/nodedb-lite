// SPDX-License-Identifier: Apache-2.0

//! `ColumnarSegmentExt` — pagedb segment operations for columnar segment data.
//!
//! The `StorageEngine` trait handles sparse, sorted, key-value state.  For
//! columnar segment bytes (potentially large per-segment blobs, sequentially
//! read on cold-open restore) pagedb segments are the appropriate backing.
//! Moving large compressed-column data off the B+ tree reduces write
//! amplification and allows pagedb's AEAD encryption to be applied at the
//! segment granularity rather than as opaque blobs.
//!
//! The split:
//! - **pagedb segment**: `{collection}:seg:{id}` bytes produced by
//!   `nodedb_columnar::SegmentWriter`.  These are potentially large and
//!   immutable once written.
//! - **B+ tree** (`Namespace::Columnar`): segment metadata list
//!   (`{collection}:meta`), delete bitmaps (`{collection}:del:{id}`), and
//!   schema entries.  All small and point-lookup hot.
//!
//! Segment metadata already carries the full list of segment IDs, so no
//! auxiliary sentinel index is needed (unlike the FTS path which lacks an
//! equivalent enumeration index).
//!
//! Only compiled on non-WASM targets.  WASM stays on the KV blob path.

use crate::error::LiteError;

/// Extension trait: write, open, and delete columnar segment data backed by
/// pagedb encrypted segment files.
///
/// One segment per `(collection, segment_id)` pair.  The segment contains the
/// serialized columnar bytes exactly as produced by
/// `nodedb_columnar::SegmentWriter` — no additional framing beyond the 8-byte
/// length prefix envelope used to survive pagedb's page-boundary padding.
///
/// Segment metadata (the ordered list of segment IDs with row counts),
/// delete bitmaps, and collection schemas remain on the B+ tree
/// (`Namespace::Columnar` and `Namespace::Meta`) — they are small and
/// point-lookup hot.
///
/// This trait is object-safe so `StorageEngine` implementations can return
/// `Option<&dyn ColumnarSegmentExt>` via `as_columnar_segment_ext()`.
#[async_trait::async_trait]
pub trait ColumnarSegmentExt: Send + Sync {
    /// Write the segment bytes for `(collection, segment_id)`.
    ///
    /// Chunks the length-prefixed payload into 4 KiB pagedb pages, creates a
    /// new encrypted segment, and links it under
    /// `col/seg/{collection}/{segment_id}`.  If a segment already exists under
    /// that name it is atomically replaced (old segment is tombstoned and
    /// reaped by the next `Db::gc_now` call).
    async fn write_columnar_segment(
        &self,
        collection: &str,
        segment_id: u32,
        bytes: &[u8],
    ) -> Result<(), LiteError>;

    /// Open a previously written columnar segment for `(collection, segment_id)`.
    ///
    /// Returns the raw segment bytes in a `Box<[u8]>`, identical to the bytes
    /// passed to `write_columnar_segment`.
    ///
    /// Returns `None` if no segment exists under `col/seg/{collection}/{segment_id}`.
    async fn open_columnar_segment(
        &self,
        collection: &str,
        segment_id: u32,
    ) -> Result<Option<Box<[u8]>>, LiteError>;

    /// Delete the columnar segment for `(collection, segment_id)`.
    ///
    /// No-op if no segment exists.  The segment file is tombstoned and reaped
    /// by the next `Db::gc_now` cycle.
    async fn delete_columnar_segment(
        &self,
        collection: &str,
        segment_id: u32,
    ) -> Result<(), LiteError>;
}
