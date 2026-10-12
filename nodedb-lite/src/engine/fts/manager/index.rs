// SPDX-License-Identifier: Apache-2.0

//! Write side of [`FtsCollectionManager`]: whole-document, per-field, and
//! schemaless-document indexing, removal, and the Origin surrogate map.

use std::collections::{HashMap, HashSet};

use nodedb_fts::IndexScope;
use nodedb_fts::backend::FtsBackend;
use nodedb_types::Surrogate;
use nodedb_types::value::Value;

use super::registry::{FtsCollectionManager, fts_err, index_key};
use crate::error::LiteError;

/// Every top-level string field of a document, joined by spaces: the text
/// of the whole-document index.
pub fn whole_document_text(fields: &HashMap<String, Value>) -> String {
    super::fields::joined_text(&super::fields::selected_texts(fields, None))
}

impl FtsCollectionManager {
    // ── Schemaless documents ──────────────────────────────────────────────────

    /// Index a schemaless document: its whole-document text under the bare
    /// `"{collection}"` key and each top-level string field under
    /// `"{collection}:{field}"`.
    ///
    /// Replaces whatever the document held before. The document is retracted
    /// from every other field index of the collection, so a field it no longer
    /// carries as a string stops matching. A field with an empty name has no
    /// key of its own and is searchable through the whole-document index only.
    ///
    /// Returns the indexed `(field, text)` pairs sorted by field name, without
    /// empty field names. The list is empty when the document holds no
    /// indexable string field.
    pub fn index_document_fields(
        &mut self,
        collection: &str,
        doc_id: &str,
        fields: &HashMap<String, Value>,
    ) -> Result<Vec<(String, String)>, LiteError> {
        let texts = super::fields::selected_texts(fields, self.declaration_for(collection));
        let text = super::fields::joined_text(&texts);
        self.index_document(collection, doc_id, &text)?;

        let whole = index_key(collection, "");
        let mut indexed: HashSet<String> = HashSet::new();
        let mut pairs = Vec::with_capacity(texts.len());
        for (field, value) in texts {
            if field.is_empty() {
                continue;
            }
            self.index_field(collection, field, doc_id, value)?;
            indexed.insert(index_key(collection, field));
            pairs.push((field.to_owned(), value.to_owned()));
        }

        let system = index_key(collection, "_geohash");
        for key in self.collection_keys(collection) {
            if key != whole && key != system && !indexed.contains(&key) {
                self.retract(collection, &key, doc_id)?;
            }
        }
        Ok(pairs)
    }

    /// Remove a document from every index of `collection`: the
    /// whole-document index and each per-field index.
    pub fn remove_document_fields(&self, collection: &str, doc_id: &str) -> Result<(), LiteError> {
        for key in self.collection_keys(collection) {
            self.retract(collection, &key, doc_id)?;
        }
        Ok(())
    }

    // ── Whole-document index ──────────────────────────────────────────────────

    /// Index `text` as a document's whole-document entry.
    ///
    /// The document is stored under the bare `"{collection}"` key.
    /// Calling again with the same `doc_id` replaces the previous entry.
    ///
    /// Empty text is a removal, not a no-op: a document updated until it has
    /// no indexable words must stop matching the words it used to contain.
    pub fn index_document(
        &mut self,
        collection: &str,
        doc_id: &str,
        text: &str,
    ) -> Result<(), LiteError> {
        self.index_text(collection, &index_key(collection, ""), doc_id, text)
    }

    /// Remove a document from the whole-document index.
    pub fn remove_document(&self, collection: &str, doc_id: &str) -> Result<(), LiteError> {
        self.retract(collection, &index_key(collection, ""), doc_id)
    }

    // ── Per-field index ───────────────────────────────────────────────────────

    /// Index a single field value for a document.
    ///
    /// Key is `"{collection}:{field}"`. Calling again with the same `doc_id`
    /// replaces the previous entry (upsert semantics).
    ///
    /// Empty text is a removal, not a no-op: clearing a field must stop the
    /// document matching the words that field used to contain.
    pub fn index_field(
        &mut self,
        collection: &str,
        field: &str,
        doc_id: &str,
        text: &str,
    ) -> Result<(), LiteError> {
        self.index_text(collection, &index_key(collection, field), doc_id, text)
    }

    /// Remove a document's entry from the `field` index of a collection.
    pub fn remove_field(
        &self,
        collection: &str,
        field: &str,
        doc_id: &str,
    ) -> Result<(), LiteError> {
        self.retract(collection, &index_key(collection, field), doc_id)
    }

    // ── Shared primitives ─────────────────────────────────────────────────────

    /// Replace `doc_id`'s entry in the index under `key` with `text`,
    /// creating the index on first use. Empty text removes the entry.
    fn index_text(
        &mut self,
        collection: &str,
        key: &str,
        doc_id: &str,
        text: &str,
    ) -> Result<(), LiteError> {
        if text.is_empty() {
            return self.retract(collection, key, doc_id);
        }
        let surrogate = self.surrogate_for(collection, doc_id)?;
        if !self.indices.contains_key(key) {
            let mut retained = super::retained::RetainedIndex::default();
            retained.reserve_base(std::sync::Arc::clone(&self.governor), key)?;
            let fresh = self.new_index_for(collection, key)?;
            self.indices.insert(key.to_owned(), fresh);
            self.retained.insert(key.to_owned(), retained);
        }
        let tokens = self
            .indices
            .get(key)
            .ok_or_else(|| {
                fts_err(
                    collection,
                    format!("index '{key}' vanished before analysis"),
                )
            })?
            .analyze_for_collection(0, 0, key, text)
            .map_err(|error| fts_err(collection, error))?;
        self.retained
            .get_mut(key)
            .ok_or_else(|| fts_err(collection, format!("index lease '{key}' is absent")))?
            .reserve_write(
                std::sync::Arc::clone(&self.governor),
                key,
                surrogate,
                &tokens,
            )?;
        self.retract(collection, key, doc_id)?;
        let idx = self
            .indices
            .get(key)
            .ok_or_else(|| fts_err(collection, format!("index '{key}' vanished mid-write")))?;
        idx.index_analyzed_document(0, 0, key, surrogate, &tokens)
            .map_err(|e| fts_err(collection, e))
    }

    /// Remove `doc_id` from the index under `key`, if the index holds it.
    ///
    /// An index holds a document exactly when it records the document's
    /// length, so absent documents skip the posting scan a removal costs.
    fn retract(&self, collection: &str, key: &str, doc_id: &str) -> Result<(), LiteError> {
        let Some(surrogate) = self.lookup_surrogate(doc_id) else {
            return Ok(());
        };
        let Some(idx) = self.indices.get(key) else {
            return Ok(());
        };
        let held = idx
            .backend()
            .read_doc_length(0, 0, IndexScope::document(key), surrogate)
            .map_err(|e| fts_err(collection, e))?
            .is_some();
        if held {
            idx.remove_document(0, 0, key, surrogate)
                .map_err(|e| fts_err(collection, e))?;
        }
        Ok(())
    }

    // ── Origin-surrogate reverse map (for FtsIndexDoc / FtsDeleteDoc sync) ────

    /// Register an association between an Origin global surrogate and the
    /// Lite string `doc_id`. Called from the `FtsIndexDoc` execution arm so
    /// `FtsDeleteDoc` can later resolve the Origin surrogate to a string doc_id
    /// and call the proper single-doc removal instead of dropping the collection.
    pub fn register_origin_surrogate(&mut self, origin_surrogate: Surrogate, doc_id: &str) {
        self.origin_surrogate_to_doc_id
            .insert(origin_surrogate.0, doc_id.to_owned());
    }

    /// Remove a single document identified by its Origin-assigned surrogate.
    ///
    /// Returns the removed doc_id, or `None` if the surrogate has no known
    /// Lite mapping (e.g. it was never indexed via this Lite instance).
    pub fn remove_by_origin_surrogate(
        &mut self,
        collection: &str,
        origin_surrogate: Surrogate,
    ) -> Result<Option<String>, LiteError> {
        let Some(doc_id) = self.origin_surrogate_to_doc_id.remove(&origin_surrogate.0) else {
            tracing::debug!(
                collection,
                sur = origin_surrogate.0,
                "FtsDeleteDoc: no Lite mapping for Origin surrogate — document was never indexed here"
            );
            return Ok(None);
        };
        self.remove_document(collection, &doc_id)?;
        Ok(Some(doc_id))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use nodedb_types::Surrogate;
    use nodedb_types::text_search::TextSearchParams;
    use nodedb_types::value::Value;

    use super::super::registry::test_governor;
    use super::FtsCollectionManager;

    fn doc(pairs: &[(&str, &str)]) -> HashMap<String, Value> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_owned(), Value::String((*v).to_owned())))
            .collect()
    }

    fn hits(mgr: &FtsCollectionManager, field: &str, query: &str) -> Vec<String> {
        mgr.search("col", field, query, 10, &TextSearchParams::default())
            .expect("search must succeed")
            .into_iter()
            .map(|r| r.doc_id)
            .collect()
    }

    #[test]
    fn clearing_a_document_removes_it_from_the_index() {
        let mut mgr = FtsCollectionManager::new(test_governor());
        mgr.index_document("col", "doc1", "the quick brown fox")
            .expect("index update must succeed");
        assert_eq!(hits(&mgr, "", "quick"), vec!["doc1"]);

        // An update that strips every indexable word is a removal, not a no-op:
        // the document must stop matching the words it used to contain.
        mgr.index_document("col", "doc1", "")
            .expect("index update must succeed");
        assert!(
            hits(&mgr, "", "quick").is_empty(),
            "cleared document must not keep matching its prior terms"
        );
    }

    #[test]
    fn clearing_a_field_removes_it_from_the_field_index() {
        let mut mgr = FtsCollectionManager::new(test_governor());
        mgr.index_field("col", "title", "doc1", "the quick brown fox")
            .expect("index update must succeed");
        assert!(
            !mgr.indices
                .get("col:title")
                .expect("field index exists")
                .memtable()
                .is_empty(),
            "field index must hold the document's postings"
        );

        mgr.index_field("col", "title", "doc1", "")
            .expect("index update must succeed");
        assert!(
            mgr.indices
                .get("col:title")
                .expect("field index exists")
                .memtable()
                .is_empty(),
            "cleared field must not keep its prior postings"
        );
    }

    #[test]
    fn schemaless_document_is_indexed_per_field_and_whole() {
        let mut mgr = FtsCollectionManager::new(test_governor());
        mgr.index_document_fields("col", "d1", &doc(&[("title", "rust"), ("body", "python")]))
            .expect("index update must succeed");

        assert_eq!(hits(&mgr, "title", "rust"), vec!["d1"]);
        assert!(hits(&mgr, "title", "python").is_empty());
        assert_eq!(hits(&mgr, "body", "python"), vec!["d1"]);
        assert_eq!(hits(&mgr, "", "python"), vec!["d1"]);
    }

    #[test]
    fn rewriting_without_a_field_retracts_it_from_that_field_index() {
        let mut mgr = FtsCollectionManager::new(test_governor());
        mgr.index_document_fields("col", "d1", &doc(&[("title", "rust"), ("body", "python")]))
            .expect("index update must succeed");
        mgr.index_document_fields("col", "d1", &doc(&[("title", "rust")]))
            .expect("index update must succeed");

        assert!(hits(&mgr, "body", "python").is_empty());
        assert!(hits(&mgr, "", "python").is_empty());
        assert_eq!(hits(&mgr, "title", "rust"), vec!["d1"]);
    }

    #[test]
    fn removing_a_document_clears_every_index_of_the_collection() {
        let mut mgr = FtsCollectionManager::new(test_governor());
        mgr.index_document_fields("col", "d1", &doc(&[("title", "rust"), ("body", "python")]))
            .expect("index update must succeed");
        mgr.remove_document_fields("col", "d1")
            .expect("removal must succeed");

        assert!(hits(&mgr, "title", "rust").is_empty());
        assert!(hits(&mgr, "body", "python").is_empty());
        assert!(hits(&mgr, "", "rust").is_empty());
    }

    #[test]
    fn a_field_named_doc_is_its_own_index() {
        let mut mgr = FtsCollectionManager::new(test_governor());
        mgr.index_document_fields("col", "d1", &doc(&[("_doc", "alpha"), ("title", "beta")]))
            .expect("index update must succeed");

        assert_eq!(hits(&mgr, "_doc", "alpha"), vec!["d1"]);
        assert!(
            hits(&mgr, "_doc", "beta").is_empty(),
            "`_doc` holds only its own field"
        );
        assert_eq!(
            hits(&mgr, "", "beta"),
            vec!["d1"],
            "the whole document holds both"
        );
    }

    #[test]
    fn fts_delete_doc_removes_only_targeted_doc() {
        let mut mgr = FtsCollectionManager::new(test_governor());
        mgr.index_document("col", "doc1", "rust programming language")
            .expect("index update must succeed");
        mgr.index_document("col", "doc2", "rust is fast and safe")
            .expect("index update must succeed");
        mgr.index_document("col", "doc3", "python is also great")
            .expect("index update must succeed");

        // Register origin surrogate for doc2 (as if FtsIndexDoc was dispatched).
        mgr.register_origin_surrogate(Surrogate(42), "doc2");

        let removed = mgr
            .remove_by_origin_surrogate("col", Surrogate(42))
            .expect("removal must succeed");
        assert!(removed.is_some(), "doc2 must be found and removed");

        let ids = hits(&mgr, "", "rust");
        assert!(
            ids.contains(&"doc1".to_owned()),
            "doc1 must still be present"
        );
        assert!(!ids.contains(&"doc2".to_owned()), "doc2 must be removed");
    }

    #[test]
    fn fts_delete_doc_unknown_surrogate_returns_none() {
        let mut mgr = FtsCollectionManager::new(test_governor());
        mgr.index_document("col", "doc1", "hello world")
            .expect("index update must succeed");

        let removed = mgr
            .remove_by_origin_surrogate("col", Surrogate(99))
            .expect("removal must succeed");
        assert!(removed.is_none(), "unknown surrogate must return None");
        assert_eq!(hits(&mgr, "", "hello"), vec!["doc1"]);
    }
}
