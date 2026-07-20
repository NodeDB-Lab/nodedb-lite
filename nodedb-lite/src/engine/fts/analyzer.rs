// SPDX-License-Identifier: Apache-2.0

//! Per-collection analyzer binding for Lite's FTS indexes.
//!
//! Mirrors Origin's `TextOp::SetAnalyzer`: the analyzer name is persisted into
//! each index's backend metadata via `FtsIndex::set_collection_analyzer`, so
//! `analyze_for_collection` resolves it for every later tokenization of that
//! collection's text — indexing and query-time scoring alike.
//!
//! Lite shards one collection across several `FtsIndex` instances — a
//! whole-document index keyed `"{collection}:_doc"` plus one per indexed field
//! keyed `"{collection}:{field}"` — and passes that composite key as the
//! `collection` argument to nodedb-fts. The analyzer therefore has to be bound
//! on every one of a collection's indexes under its own key, including indexes
//! that do not exist yet: DDL normally sets the analyzer before any document is
//! written, so the name is also retained in `collection_analyzers` and applied
//! to each index at creation time.

use super::manager::FtsCollectionManager;
use crate::engine::fts::{LiteFtsIndex, MemoryBackend};
use nodedb_fts::FtsIndex;

impl FtsCollectionManager {
    /// Bind `analyzer_name` to every index belonging to `collection`, and
    /// retain it so indexes created later inherit the same analyzer.
    ///
    /// Unrecognized names fall back to the standard analyzer inside
    /// nodedb-fts at resolve time, matching Origin's behavior.
    pub fn set_collection_analyzer(&mut self, collection: &str, analyzer_name: &str) {
        self.collection_analyzers
            .insert(collection.to_string(), analyzer_name.to_string());

        let prefix = format!("{collection}:");
        for (key, idx) in self.indices.iter_mut() {
            if key.starts_with(&prefix) {
                let _ = idx.set_collection_analyzer(0, 0, key, analyzer_name);
            }
        }
    }

    /// Analyzer bound to the collection owning `key`, if any.
    ///
    /// `key` is the composite `"{collection}:{field}"` index key; the
    /// collection is the portion before the last `:`, so field names
    /// containing `:` do not split incorrectly.
    pub(crate) fn analyzer_for_key(&self, key: &str) -> Option<&str> {
        let collection = key.rsplit_once(':').map(|(c, _)| c)?;
        self.collection_analyzers
            .get(collection)
            .map(String::as_str)
    }

    /// Create an index for `key`, applying the collection's bound analyzer.
    ///
    /// Used at every index-creation site so an analyzer bound before the first
    /// write is not silently lost for indexes materialized afterwards.
    pub(crate) fn new_index_for(&self, key: &str) -> LiteFtsIndex {
        let idx = FtsIndex::new(MemoryBackend::new());
        if let Some(name) = self.analyzer_for_key(key) {
            let _ = idx.set_collection_analyzer(0, 0, key, name);
        }
        idx
    }
}
