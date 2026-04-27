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
use nodedb_fts::posting::QueryMode as FtsQueryMode;
use nodedb_types::Surrogate;
use nodedb_types::text_search::{QueryMode, TextSearchParams};

/// A resolved FTS result with the original string doc_id restored.
pub struct FtsResult {
    pub doc_id: String,
    pub score: f32,
    pub fuzzy: bool,
}

/// Manages per-collection (and per-field) in-memory full-text search indexes.
///
/// Each `(collection, field)` pair gets its own `FtsIndex<MemoryBackend>`.
/// A special `collection:_doc` key is used for whole-document text indexing
/// (all string fields concatenated) used by the `text_search` API.
pub struct FtsCollectionManager {
    /// Key: `"{collection}:{field}"` → FTS index.
    /// Whole-document index uses key `"{collection}:_doc"`.
    indices: HashMap<String, FtsIndex<MemoryBackend>>,
    /// Forward map: original string doc_id → dense u32 surrogate.
    ///
    /// Surrogates **must** be dense (0, 1, 2, …) because `nodedb_fts::Memtable`
    /// uses them as direct indices into a `Vec<u8>` fieldnorm array
    /// (`record_doc` calls `vec.resize(surrogate + 1, 0)`). Hashing strings
    /// into the u32 space produced sparse surrogates near `u32::MAX`, which
    /// allocated multi-gigabyte zero-filled vectors per insert and made
    /// indexing effectively hang.
    id_to_surrogate: HashMap<String, u32>,
    /// Reverse map: surrogate u32 → original string doc_id.
    surrogate_to_id: HashMap<u32, String>,
    /// Next surrogate to assign on first sighting of a doc_id.
    next_surrogate: u32,
}

impl FtsCollectionManager {
    pub fn new() -> Self {
        Self {
            indices: HashMap::new(),
            id_to_surrogate: HashMap::new(),
            surrogate_to_id: HashMap::new(),
            next_surrogate: 0,
        }
    }

    /// Look up or allocate a dense surrogate for a string `doc_id`.
    ///
    /// Returns the existing surrogate if `doc_id` has been indexed before,
    /// otherwise assigns the next sequential u32 and records the mapping
    /// in both directions.
    fn surrogate_for(&mut self, doc_id: &str) -> Surrogate {
        if let Some(&s) = self.id_to_surrogate.get(doc_id) {
            return Surrogate(s);
        }
        let s = self.next_surrogate;
        self.next_surrogate = self
            .next_surrogate
            .checked_add(1)
            .expect("FTS surrogate counter overflowed u32");
        self.id_to_surrogate.insert(doc_id.to_owned(), s);
        self.surrogate_to_id.insert(s, doc_id.to_owned());
        Surrogate(s)
    }

    /// Look up an existing surrogate without allocating one.
    fn lookup_surrogate(&self, doc_id: &str) -> Option<Surrogate> {
        self.id_to_surrogate.get(doc_id).copied().map(Surrogate)
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
        let surrogate = self.surrogate_for(doc_id);
        let key = format!("{collection}:_doc");
        let idx = self
            .indices
            .entry(key.clone())
            .or_insert_with(|| FtsIndex::new(MemoryBackend::new()));
        // Remove old entry first (upsert semantics).
        let _ = idx.remove_document(0, &key, surrogate);
        let _ = idx.index_document(0, &key, surrogate, text);
    }

    /// Remove a document from the whole-document index.
    pub fn remove_document(&mut self, collection: &str, doc_id: &str) {
        let Some(surrogate) = self.lookup_surrogate(doc_id) else {
            return;
        };
        let key = format!("{collection}:_doc");
        if let Some(idx) = self.indices.get_mut(&key) {
            let _ = idx.remove_document(0, &key, surrogate);
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
    ) -> Vec<FtsResult> {
        let key = format!("{collection}:_doc");
        let Some(idx) = self.indices.get(&key) else {
            return Vec::new();
        };
        let mode = match params.mode {
            QueryMode::Or => FtsQueryMode::Or,
            QueryMode::And => FtsQueryMode::And,
        };
        let raw = idx
            .search_with_mode(0, &key, query, top_k, params.fuzzy, mode, None)
            .inspect_err(|e| tracing::warn!(collection, error = %e, "fts search failed"))
            .unwrap_or_default();
        raw.into_iter()
            .filter_map(|r| {
                let doc_id = self.surrogate_to_id.get(&r.doc_id.0)?.clone();
                Some(FtsResult {
                    doc_id,
                    score: r.score,
                    fuzzy: r.fuzzy,
                })
            })
            .collect()
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
        let surrogate = self.surrogate_for(doc_id);
        let key = format!("{collection}:{field}");
        let idx = self
            .indices
            .entry(key.clone())
            .or_insert_with(|| FtsIndex::new(MemoryBackend::new()));
        let _ = idx.remove_document(0, &key, surrogate);
        let _ = idx.index_document(0, &key, surrogate, text);
    }

    /// Remove all field entries for a document across all fields in a collection.
    pub fn remove_field(&mut self, collection: &str, field: &str, doc_id: &str) {
        let Some(surrogate) = self.lookup_surrogate(doc_id) else {
            return;
        };
        let key = format!("{collection}:{field}");
        if let Some(idx) = self.indices.get_mut(&key) {
            let _ = idx.remove_document(0, &key, surrogate);
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
