// SPDX-License-Identifier: Apache-2.0

//! Per-`(collection, field)` sparse inverted index manager for Lite.
//!
//! Mirrors the FTS manager's shape: one index per named field, keyed
//! `"{collection}:{field}"`, maintained incrementally on document write and
//! delete, and checkpointed to storage on flush so a reopen is free.

use std::collections::HashMap;

use nodedb_types::SparseVector;
use nodedb_types::Value;

use super::index::{SparseHit, SparseInvertedIndex};

/// Index key used when a caller does not name a field.
const DEFAULT_FIELD: &str = "_sparse";

/// Manages sparse inverted indexes for every `(collection, field)` pair.
pub struct SparseVectorManager {
    /// Key: `"{collection}:{field}"` → inverted index.
    indices: HashMap<String, SparseInvertedIndex>,
}

impl Default for SparseVectorManager {
    fn default() -> Self {
        Self::new()
    }
}

impl SparseVectorManager {
    /// Create an empty manager.
    pub fn new() -> Self {
        Self {
            indices: HashMap::new(),
        }
    }

    /// Build the `"{collection}:{field}"` index key.
    ///
    /// An empty `field` maps to the default field name so callers that do not
    /// name a field still address a stable index.
    pub fn index_key(collection: &str, field: &str) -> String {
        if field.is_empty() {
            format!("{collection}:{DEFAULT_FIELD}")
        } else {
            format!("{collection}:{field}")
        }
    }

    /// Whether no collection has any sparse index.
    pub fn is_empty(&self) -> bool {
        self.indices.values().all(SparseInvertedIndex::is_empty)
    }

    /// Number of live indexes.
    pub fn index_count(&self) -> usize {
        self.indices.len()
    }

    /// Insert or replace `doc_id`'s sparse vector for `(collection, field)`.
    pub fn index_document(
        &mut self,
        collection: &str,
        field: &str,
        doc_id: &str,
        vector: &SparseVector,
    ) {
        let key = Self::index_key(collection, field);
        self.indices.entry(key).or_default().insert(doc_id, vector);
    }

    /// Remove `doc_id` from one `(collection, field)` index.
    ///
    /// Returns `true` when the document was present.
    pub fn remove_document(&mut self, collection: &str, field: &str, doc_id: &str) -> bool {
        let key = Self::index_key(collection, field);
        match self.indices.get_mut(&key) {
            Some(index) => index.delete(doc_id),
            None => false,
        }
    }

    /// Remove `doc_id` from every sparse index belonging to `collection`.
    ///
    /// Used by the document-delete path, which knows the collection and the
    /// document ID but not which fields carried sparse vectors.
    pub fn remove_document_all_fields(&mut self, collection: &str, doc_id: &str) -> usize {
        let prefix = format!("{collection}:");
        let mut removed = 0usize;
        for (key, index) in self.indices.iter_mut() {
            if key.starts_with(&prefix) && index.delete(doc_id) {
                removed += 1;
            }
        }
        removed
    }

    /// Reconcile every sparse index of `collection` against a document's fields.
    ///
    /// String fields that parse as a sparse-vector literal (`'{12: 0.5}'`) are
    /// indexed under their own field name. A field that does not parse is not
    /// an error — it is simply not a sparse column — but the document is then
    /// removed from that field's index so a column that stops holding a sparse
    /// vector cannot leave stale postings behind.
    pub fn index_document_fields(
        &mut self,
        collection: &str,
        doc_id: &str,
        fields: &HashMap<String, Value>,
    ) {
        let mut indexed_fields: Vec<String> = Vec::new();

        for (field, value) in fields {
            let Value::String(literal) = value else {
                continue;
            };
            let Ok(vector) = SparseVector::parse_literal(literal) else {
                continue;
            };
            self.index_document(collection, field, doc_id, &vector);
            indexed_fields.push(Self::index_key(collection, field));
        }

        // Drop the document from any other sparse index of this collection.
        let prefix = format!("{collection}:");
        for (key, index) in self.indices.iter_mut() {
            if key.starts_with(&prefix) && !indexed_fields.iter().any(|k| k == key) {
                index.delete(doc_id);
            }
        }
    }

    /// Top-`k` documents for `(collection, field)` by dot product, descending.
    ///
    /// An absent index yields no hits rather than an error — a collection that
    /// has never been written simply has nothing to match.
    pub fn search(
        &self,
        collection: &str,
        field: &str,
        query: &SparseVector,
        top_k: usize,
    ) -> Vec<SparseHit> {
        let key = Self::index_key(collection, field);
        match self.indices.get(&key) {
            Some(index) => index.search(query, top_k),
            None => Vec::new(),
        }
    }

    /// Borrow every index for checkpoint serialization.
    pub fn checkpoint_data(&self) -> &HashMap<String, SparseInvertedIndex> {
        &self.indices
    }

    /// Install indexes restored from a checkpoint, replacing current state.
    pub fn load_checkpoint(&mut self, indices: HashMap<String, SparseInvertedIndex>) {
        self.indices = indices;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sv(entries: &[(u32, f32)]) -> SparseVector {
        SparseVector::from_entries(entries.to_vec()).expect("valid sparse vector")
    }

    #[test]
    fn fields_are_isolated_per_index() {
        let mut mgr = SparseVectorManager::new();
        mgr.index_document("docs", "title_sparse", "d1", &sv(&[(1, 1.0)]));
        mgr.index_document("docs", "body_sparse", "d1", &sv(&[(2, 1.0)]));

        assert_eq!(mgr.index_count(), 2);
        assert!(
            mgr.search("docs", "title_sparse", &sv(&[(2, 1.0)]), 10)
                .is_empty()
        );
        assert_eq!(
            mgr.search("docs", "body_sparse", &sv(&[(2, 1.0)]), 10)
                .len(),
            1
        );
    }

    #[test]
    fn collections_are_isolated() {
        let mut mgr = SparseVectorManager::new();
        mgr.index_document("a", "f", "d1", &sv(&[(1, 1.0)]));
        mgr.index_document("b", "f", "d1", &sv(&[(1, 1.0)]));

        mgr.remove_document_all_fields("a", "d1");
        assert!(mgr.search("a", "f", &sv(&[(1, 1.0)]), 10).is_empty());
        assert_eq!(mgr.search("b", "f", &sv(&[(1, 1.0)]), 10).len(), 1);
    }

    #[test]
    fn missing_index_returns_no_hits() {
        let mgr = SparseVectorManager::new();
        assert!(mgr.search("nope", "f", &sv(&[(1, 1.0)]), 10).is_empty());
    }

    #[test]
    fn non_sparse_string_fields_are_skipped() {
        let mut mgr = SparseVectorManager::new();
        let mut fields = HashMap::new();
        fields.insert("title".to_string(), Value::String("hello world".into()));
        fields.insert("n".to_string(), Value::Integer(3));
        fields.insert("emb".to_string(), Value::String("{1: 0.5}".into()));

        mgr.index_document_fields("docs", "d1", &fields);

        assert_eq!(mgr.index_count(), 1);
        assert_eq!(mgr.search("docs", "emb", &sv(&[(1, 1.0)]), 10).len(), 1);
    }

    #[test]
    fn field_that_stops_being_sparse_is_unindexed() {
        let mut mgr = SparseVectorManager::new();
        let mut fields = HashMap::new();
        fields.insert("emb".to_string(), Value::String("{1: 0.5}".into()));
        mgr.index_document_fields("docs", "d1", &fields);

        fields.insert("emb".to_string(), Value::String("plain text".into()));
        mgr.index_document_fields("docs", "d1", &fields);

        assert!(mgr.search("docs", "emb", &sv(&[(1, 1.0)]), 10).is_empty());
    }

    #[test]
    fn remove_document_reports_presence() {
        let mut mgr = SparseVectorManager::new();
        mgr.index_document("docs", "emb", "d1", &sv(&[(1, 1.0)]));
        assert!(mgr.remove_document("docs", "emb", "d1"));
        assert!(!mgr.remove_document("docs", "emb", "d1"));
    }
}
