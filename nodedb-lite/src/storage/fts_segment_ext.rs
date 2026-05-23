// SPDX-License-Identifier: Apache-2.0

//! `FtsSegmentExt` — pagedb segment operations for FTS memtable posting data.
//!
//! The `StorageEngine` trait handles sparse, sorted, key-value state. For FTS
//! posting data (potentially large per-index blobs, sequentially read on
//! cold-open restore) pagedb segments are the appropriate backing.  Bundling
//! all term postings for one index key into a single segment reduces B+ tree
//! pressure from O(vocab_size) entries to O(1) per index key.
//!
//! Term dictionary entries, doc-length tables, surrogate maps, and collection
//! metadata remain on the B+ tree (`Namespace::Fts`) — they are genuinely
//! sparse and sorted.
//!
//! Only compiled on non-WASM targets.  WASM stays on the KV blob path.

use crate::error::LiteError;

/// Extension trait: write, open, delete, and list FTS posting segments backed
/// by pagedb encrypted segment files.
///
/// One segment per FTS index key (e.g. `"articles:_doc"`).  The segment
/// contains the serialized posting blob exactly as produced by the checkpoint
/// layer — no additional framing beyond the 8-byte length prefix envelope used
/// to survive pagedb's page-boundary padding.
///
/// This trait is object-safe so `StorageEngine` implementations can return
/// `Option<&dyn FtsSegmentExt>` via `as_fts_segment_ext()`.
#[async_trait::async_trait]
pub trait FtsSegmentExt: Send + Sync {
    /// Write the posting blob for `index_key`.
    ///
    /// Chunks the length-prefixed payload into 4 KiB pagedb pages, creates a
    /// new encrypted segment, and links it under `fts/seg/{index_key}`.  If a
    /// segment already exists under that name it is atomically replaced (old
    /// segment is tombstoned and reaped by the next `Db::gc_now` call).
    async fn write_fts_segment(&self, index_key: &str, bytes: &[u8]) -> Result<(), LiteError>;

    /// Open a previously written FTS posting segment for `index_key`.
    ///
    /// Returns the raw posting bytes in a `Box<[u8]>`, identical to the bytes
    /// passed to `write_fts_segment`.
    ///
    /// Returns `None` if no segment exists under `fts/seg/{index_key}`.
    async fn open_fts_segment(&self, index_key: &str) -> Result<Option<Box<[u8]>>, LiteError>;

    /// Delete the FTS posting segment for `index_key`.
    ///
    /// No-op if no segment exists.  The segment file is tombstoned and reaped
    /// by the next `Db::gc_now` cycle.
    async fn delete_fts_segment(&self, index_key: &str) -> Result<(), LiteError>;

    /// List all FTS segment names whose pagedb segment name starts with
    /// `fts/seg/{prefix}`.
    ///
    /// Returns the bare `index_key` strings (segment name minus the
    /// `"fts/seg/"` prefix) so callers can iterate segments without
    /// knowing the full pagedb name scheme.
    async fn list_fts_segments(&self, prefix: &str) -> Result<Vec<String>, LiteError>;
}
