//! Per-collection in-memory FTS manager for Lite.
//!
//! Wraps `nodedb_fts::FtsIndex<MemoryBackend>` with per-collection management:
//! - Incremental insert/remove on document put/delete
//! - Multi-field per-collection keying (`collection:field`)
//! - BM25 search delegated directly to nodedb-fts (BMW, analyzers, fuzzy)
//! - Rebuilt from CRDT state on cold start (no persistence — in-RAM only)
//!
//! This is the canonical FTS implementation for Lite. Origin uses
//! `FtsIndex<RedbBackend>` in `engine/sparse/fts_redb/` for persistence.

use std::collections::HashMap;

use tracing;

use nodedb_fts::FtsIndex;
use nodedb_fts::backend::memory::MemoryBackend;
use nodedb_fts::posting::{QueryMode as FtsQueryMode, TextSearchResult};
use nodedb_types::text_search::{QueryMode, TextSearchParams};

/// Manages per-collection (and per-field) in-memory full-text search indexes.
///
/// Each `(collection, field)` pair gets its own `FtsIndex<MemoryBackend>`.
/// A special `collection:_doc` key is used for whole-document text indexing
/// (all string fields concatenated) used by the `text_search` API.
pub struct FtsCollectionManager {
    /// Key: `"{collection}:{field}"` → FTS index.
    /// Whole-document index uses key `"{collection}:_doc"`.
    indices: HashMap<String, FtsIndex<MemoryBackend>>,
}

impl FtsCollectionManager {
    pub fn new() -> Self {
        Self {
            indices: HashMap::new(),
        }
    }

    /// Returns true if no collections are indexed.
    pub fn is_empty(&self) -> bool {
        self.indices.is_empty()
    }

    // ── Whole-document indexing (used by `document_put` / `text_search`) ─────

    /// Index all string field values from a document as a single text blob.
    ///
    /// The document is stored under the `"{collection}:_doc"` key.
    /// Calling again with the same `doc_id` replaces the previous entry.
    pub fn index_document(&mut self, collection: &str, doc_id: &str, text: &str) {
        if text.is_empty() {
            return;
        }
        let key = format!("{collection}:_doc");
        let idx = self
            .indices
            .entry(key.clone())
            .or_insert_with(|| FtsIndex::new(MemoryBackend::new()));
        // Remove old entry first (upsert semantics).
        let _ = idx.remove_document(&key, doc_id);
        let _ = idx.index_document(&key, doc_id, text);
    }

    /// Remove a document from the whole-document index.
    pub fn remove_document(&mut self, collection: &str, doc_id: &str) {
        let key = format!("{collection}:_doc");
        if let Some(idx) = self.indices.get_mut(&key) {
            let _ = idx.remove_document(&key, doc_id);
        }
    }

    /// Search the whole-document index for a collection.
    ///
    /// All query knobs are passed via [`TextSearchParams`]: boolean mode (OR/AND),
    /// fuzzy matching, and BM25 scoring parameters (k1, b).
    pub fn search(
        &self,
        collection: &str,
        query: &str,
        top_k: usize,
        params: &TextSearchParams,
    ) -> Vec<TextSearchResult> {
        let key = format!("{collection}:_doc");
        let Some(idx) = self.indices.get(&key) else {
            return Vec::new();
        };
        let mode = match params.mode {
            QueryMode::Or => FtsQueryMode::Or,
            QueryMode::And => FtsQueryMode::And,
        };
        idx.search_with_mode(&key, query, top_k, params.fuzzy, mode)
            .inspect_err(|e| tracing::warn!(collection, error = %e, "fts search failed"))
            .unwrap_or_default()
    }

    // ── Per-field indexing (used by strict collections via index_integration) ─

    /// Index a single field value for a document.
    ///
    /// Key is `"{collection}:{field}"`. Calling again with the same `doc_id`
    /// replaces the previous entry (upsert semantics).
    pub fn index_field(&mut self, collection: &str, field: &str, doc_id: &str, text: &str) {
        if text.is_empty() {
            return;
        }
        let key = format!("{collection}:{field}");
        let idx = self
            .indices
            .entry(key.clone())
            .or_insert_with(|| FtsIndex::new(MemoryBackend::new()));
        let _ = idx.remove_document(&key, doc_id);
        let _ = idx.index_document(&key, doc_id, text);
    }

    /// Remove all field entries for a document across all fields in a collection.
    pub fn remove_field(&mut self, collection: &str, field: &str, doc_id: &str) {
        let key = format!("{collection}:{field}");
        if let Some(idx) = self.indices.get_mut(&key) {
            let _ = idx.remove_document(&key, doc_id);
        }
    }

    /// Number of distinct collection prefixes with active indexes.
    pub fn collection_count(&self) -> usize {
        self.indices
            .keys()
            .map(|k| k.split(':').next().unwrap_or(k.as_str()))
            .collect::<std::collections::HashSet<_>>()
            .len()
    }

    /// Drop all FTS indexes for a collection (called on collection drop/truncate).
    pub fn drop_collection(&mut self, collection: &str) {
        let prefix = format!("{collection}:");
        self.indices.retain(|k, _| !k.starts_with(&prefix));
    }
}

impl Default for FtsCollectionManager {
    fn default() -> Self {
        Self::new()
    }
}
