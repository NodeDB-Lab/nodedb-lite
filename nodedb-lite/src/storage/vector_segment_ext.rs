// SPDX-License-Identifier: Apache-2.0

//! `VectorSegmentExt` — pagedb segment operations for HNSW vector data.
//!
//! The `StorageEngine` trait handles sparse, sorted, key-value state.  For
//! HNSW vector data (large, sequentially accessed, zero-copy win) pagedb
//! segments are more appropriate.  This trait exposes exactly the three
//! operations the HNSW persistence layer needs without leaking pagedb types
//! into caller code.
//!
//! Only compiled on non-WASM targets because `pagedb::MmapView` (used by
//! `PagedbBacking`) requires mmap.  WASM stays on the blob path.

use crate::engine::vector::pagedb_backing::PagedbBacking;
use crate::error::LiteError;

/// Extension trait: write, open, and delete HNSW vector segments backed by
/// pagedb encrypted segment files.
///
/// Implementors are responsible for translating between the generic NDVS byte
/// stream and the pagedb segment page layout.
///
/// This trait is object-safe so `StorageEngine` implementations can return
/// `Option<&dyn VectorSegmentExt>` via `as_vector_segment_ext()`.
#[async_trait::async_trait]
pub trait VectorSegmentExt: Send + Sync {
    /// Write a vector segment for `collection_name`.
    ///
    /// Serialises the NDVS v2 format in memory, chunks it into 4 KiB pagedb
    /// pages, creates a new encrypted segment, and links it under
    /// `vec/hnsw/{collection_name}`.  If a segment already exists under that
    /// name it is atomically replaced (old segment is tombstoned and will be
    /// reaped by the next `Db::gc_now` call).
    ///
    /// `vectors[i]` corresponds to node `i`; `surrogate_ids` must either be
    /// empty or have the same length as `vectors`.
    async fn write_vector_segment(
        &self,
        collection_name: &str,
        dim: usize,
        vectors: &[Vec<f32>],
        surrogate_ids: &[u64],
    ) -> Result<(), LiteError>;

    /// Open a previously written vector segment for `collection_name`.
    ///
    /// Returns `None` if no segment exists under `vec/hnsw/{collection_name}`.
    async fn open_vector_segment(
        &self,
        collection_name: &str,
    ) -> Result<Option<PagedbBacking>, LiteError>;

    /// Delete the vector segment for `collection_name`.
    ///
    /// No-op if no segment exists.  The segment file is tombstoned and will be
    /// removed by the next `Db::gc_now` cycle.
    async fn delete_vector_segment(&self, collection_name: &str) -> Result<(), LiteError>;
}
