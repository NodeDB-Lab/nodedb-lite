// SPDX-License-Identifier: Apache-2.0

//! `ArraySegmentExt` — pagedb segment operations for array tile data.
//!
//! The `StorageEngine` trait handles sparse, sorted, key-value state.  For
//! array tile data (large binary payloads, sequentially read per segment,
//! encrypted at rest) pagedb segments are the appropriate backing.  This
//! trait exposes exactly the operations the array persistence layer needs
//! without leaking pagedb types into caller code.
//!
//! Only compiled on non-WASM targets.  WASM stays on the KV blob path.

use crate::error::LiteError;

/// Extension trait: write, open, and delete array tile segments backed by
/// pagedb encrypted segment files.
///
/// Implementors translate between the generic array segment byte stream
/// (produced by `nodedb_array::SegmentWriter`) and the pagedb segment page
/// layout.
///
/// This trait is object-safe so `StorageEngine` implementations can return
/// `Option<&dyn ArraySegmentExt>` via `as_array_segment_ext()`.
#[async_trait::async_trait]
pub trait ArraySegmentExt: Send + Sync {
    /// Write an array segment for `array_name` / `seg_id`.
    ///
    /// Chunks the raw segment bytes into 4 KiB pagedb pages, creates a new
    /// encrypted segment, and links it under `arr/tile/{array_name}/{seg_id}`.
    /// If a segment already exists under that name it is atomically replaced
    /// (old segment is tombstoned and reaped by the next `Db::gc_now` call).
    async fn write_array_segment(
        &self,
        array_name: &str,
        seg_id: u64,
        bytes: &[u8],
    ) -> Result<(), LiteError>;

    /// Open a previously written array segment for `array_name` / `seg_id`.
    ///
    /// Returns the raw segment bytes in a `Box<[u8]>`, identical to the bytes
    /// passed to `write_array_segment`.  `SegmentReader::open` can parse them
    /// directly.
    ///
    /// Returns `None` if no segment exists under that name.
    async fn open_array_segment(
        &self,
        array_name: &str,
        seg_id: u64,
    ) -> Result<Option<Box<[u8]>>, LiteError>;

    /// Delete the array segment for `array_name` / `seg_id`.
    ///
    /// No-op if no segment exists.  The segment file is tombstoned and reaped
    /// by the next `Db::gc_now` cycle.
    async fn delete_array_segment(&self, array_name: &str, seg_id: u64) -> Result<(), LiteError>;
}
