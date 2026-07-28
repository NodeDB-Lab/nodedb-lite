// SPDX-License-Identifier: Apache-2.0

//! Per-collection text configuration for Lite's FTS indexes.
//!
//! Mirrors Origin's `TextOp::SetTextConfig`: the analyzer name and the default
//! fuzzy-matching flag are persisted into each index's backend metadata via
//! `FtsIndex::set_collection_analyzer` / `FtsIndex::set_collection_fuzzy`, so
//! `analyze_for_collection` resolves the analyzer for every later tokenization
//! of that collection's text — indexing and query-time scoring alike — and
//! `FtsIndex::search` falls back to fuzzy matching for the collection even when
//! the query did not ask for it.
//!
//! Lite shards one collection across several `FtsIndex` instances — a
//! whole-document index keyed `"{collection}:_doc"` plus one per indexed field
//! keyed `"{collection}:{field}"` — and passes that composite key as the
//! `collection` argument to nodedb-fts. Each setting therefore has to be bound
//! on every one of a collection's indexes under its own key, including indexes
//! that do not exist yet: DDL normally runs before any document is written, so
//! the values are also retained in `collection_analyzers` /
//! `collection_fuzzy_defaults` and applied to each index at creation time.

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

    /// Bind the default fuzzy-matching flag to every index belonging to
    /// `collection`, and retain it so indexes created later inherit it.
    pub fn set_collection_fuzzy(&mut self, collection: &str, fuzzy: bool) {
        self.collection_fuzzy_defaults
            .insert(collection.to_string(), fuzzy);

        let prefix = format!("{collection}:");
        for (key, idx) in self.indices.iter_mut() {
            if key.starts_with(&prefix) {
                let _ = idx.set_collection_fuzzy(0, 0, key, fuzzy);
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

    /// Default fuzzy-matching flag bound to the collection owning `key`, if any.
    ///
    /// Same composite-key split as [`Self::analyzer_for_key`].
    pub(crate) fn fuzzy_for_key(&self, key: &str) -> Option<bool> {
        let collection = key.rsplit_once(':').map(|(c, _)| c)?;
        self.collection_fuzzy_defaults.get(collection).copied()
    }

    /// Create an index for `key`, applying the collection's bound text config.
    ///
    /// Used at every index-creation site so an analyzer or fuzzy default bound
    /// before the first write is not silently lost for indexes materialized
    /// afterwards.
    pub(crate) fn new_index_for(&self, key: &str) -> LiteFtsIndex {
        let idx = FtsIndex::new(MemoryBackend::new());
        if let Some(name) = self.analyzer_for_key(key) {
            let _ = idx.set_collection_analyzer(0, 0, key, name);
        }
        if let Some(fuzzy) = self.fuzzy_for_key(key) {
            let _ = idx.set_collection_fuzzy(0, 0, key, fuzzy);
        }
        idx
    }
}

#[cfg(test)]
mod tests {
    use super::FtsCollectionManager;

    const DOC_KEY: &str = "col:_doc";

    /// Read back the analyzer and fuzzy default persisted on `col:_doc`.
    fn doc_index_config(mgr: &FtsCollectionManager) -> (Option<String>, bool) {
        let idx = mgr
            .indices
            .get(DOC_KEY)
            .expect("whole-document index must exist");
        (
            idx.get_collection_analyzer(0, 0, DOC_KEY)
                .expect("meta read must succeed"),
            idx.get_collection_fuzzy(0, 0, DOC_KEY)
                .expect("meta read must succeed"),
        )
    }

    #[test]
    fn setting_analyzer_leaves_fuzzy_default_unchanged() {
        let mut mgr = FtsCollectionManager::new();
        mgr.index_document("col", "doc1", "the quick brown fox");

        mgr.set_collection_fuzzy("col", true);
        mgr.set_collection_analyzer("col", "german");

        let (analyzer, fuzzy) = doc_index_config(&mgr);
        assert_eq!(analyzer.as_deref(), Some("german"));
        assert!(
            fuzzy,
            "binding the analyzer must not clear the fuzzy default"
        );
        assert_eq!(mgr.fuzzy_for_key(DOC_KEY), Some(true));
    }

    #[test]
    fn setting_fuzzy_default_leaves_analyzer_unchanged() {
        let mut mgr = FtsCollectionManager::new();
        mgr.index_document("col", "doc1", "the quick brown fox");

        mgr.set_collection_analyzer("col", "german");
        mgr.set_collection_fuzzy("col", true);

        let (analyzer, fuzzy) = doc_index_config(&mgr);
        assert_eq!(
            analyzer.as_deref(),
            Some("german"),
            "binding the fuzzy default must not clear the analyzer"
        );
        assert!(fuzzy);
        assert_eq!(mgr.analyzer_for_key(DOC_KEY), Some("german"));
    }

    #[test]
    fn config_bound_before_any_index_is_inherited_by_later_indexes() {
        let mut mgr = FtsCollectionManager::new();
        // DDL order: config first, documents afterwards — no index exists yet.
        mgr.set_collection_analyzer("col", "german");
        mgr.set_collection_fuzzy("col", true);
        assert!(mgr.indices.is_empty());

        mgr.index_document("col", "doc1", "der schnelle braune fuchs");

        let (analyzer, fuzzy) = doc_index_config(&mgr);
        assert_eq!(analyzer.as_deref(), Some("german"));
        assert!(fuzzy);
    }

    #[test]
    fn config_bound_before_any_index_is_inherited_by_later_field_indexes() {
        let mut mgr = FtsCollectionManager::new();
        mgr.set_collection_analyzer("col", "german");
        mgr.set_collection_fuzzy("col", true);

        mgr.index_field("col", "title", "doc1", "der schnelle braune fuchs");

        let key = "col:title";
        let idx = mgr.indices.get(key).expect("field index must exist");
        assert_eq!(
            idx.get_collection_analyzer(0, 0, key)
                .expect("meta read must succeed")
                .as_deref(),
            Some("german")
        );
        assert!(
            idx.get_collection_fuzzy(0, 0, key)
                .expect("meta read must succeed")
        );
    }

    #[test]
    fn binding_nothing_leaves_the_collection_at_its_defaults() {
        // The `SetTextConfig { analyzer_name: None, fuzzy_default: None }`
        // case: neither setter runs, so nothing is retained or persisted.
        let mut mgr = FtsCollectionManager::new();
        mgr.index_document("col", "doc1", "the quick brown fox");

        let (analyzer, fuzzy) = doc_index_config(&mgr);
        assert_eq!(analyzer, None);
        assert!(!fuzzy);
        assert_eq!(mgr.analyzer_for_key(DOC_KEY), None);
        assert_eq!(mgr.fuzzy_for_key(DOC_KEY), None);
    }

    #[test]
    fn collection_names_containing_colons_split_at_the_last_separator() {
        // Key `"a:b:field"` belongs to collection `"a:b"`, not `"a"` — both
        // lookups split at the last `:` so they agree on the owner.
        let mut mgr = FtsCollectionManager::new();
        mgr.set_collection_analyzer("a:b", "german");
        mgr.set_collection_fuzzy("a:b", true);

        assert_eq!(mgr.analyzer_for_key("a:b:field"), Some("german"));
        assert_eq!(mgr.fuzzy_for_key("a:b:field"), Some(true));
        assert_eq!(mgr.analyzer_for_key("a:field"), None);
        assert_eq!(mgr.fuzzy_for_key("a:field"), None);
    }
}
