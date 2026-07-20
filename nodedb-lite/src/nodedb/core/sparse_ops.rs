// SPDX-License-Identifier: Apache-2.0

//! Sparse-vector index maintenance and the public sparse search API.
//!
//! Write-path hooks (`index_document_sparse` / `remove_document_sparse`) are
//! invoked from the same points as their full-text counterparts, so every
//! document write keeps the sparse inverted index in step with document state.

use nodedb_types::SparseVector;
use nodedb_types::error::{NodeDbError, NodeDbResult};

use crate::nodedb::lock_ext::LockExt;
use crate::storage::engine::StorageEngine;

use super::types::NodeDbLite;

impl<S: StorageEngine> NodeDbLite<S> {
    /// Update the sparse-vector inverted index after a document write.
    ///
    /// Every string field is offered to the sparse-vector literal parser; the
    /// ones that parse (`'{12: 0.5, 88: 0.3}'`) are indexed under their own
    /// field name. Fields that do not parse are simply not sparse columns and
    /// are skipped silently.
    pub(crate) fn index_document_sparse(
        &self,
        collection: &str,
        doc_id: &str,
        fields: &std::collections::HashMap<String, nodedb_types::Value>,
    ) {
        self.sparse_state
            .manager
            .lock_or_recover()
            .index_document_fields(collection, doc_id, fields);
    }

    /// Remove a document from every sparse index of `collection`.
    pub(crate) fn remove_document_sparse(&self, collection: &str, doc_id: &str) {
        self.sparse_state
            .manager
            .lock_or_recover()
            .remove_document_all_fields(collection, doc_id);
    }

    /// Index a sparse vector under `(collection, field)`.
    ///
    /// `entries` are `(dimension, weight)` pairs; they are sorted, deduplicated
    /// and validated by [`SparseVector`]. Re-indexing the same `doc_id`
    /// replaces its postings rather than adding to them.
    pub fn sparse_insert(
        &self,
        collection: &str,
        field: &str,
        doc_id: &str,
        entries: &[(u32, f32)],
    ) -> NodeDbResult<()> {
        let vector =
            SparseVector::from_entries(entries.to_vec()).map_err(NodeDbError::bad_request)?;
        self.sparse_state
            .manager
            .lock_or_recover()
            .index_document(collection, field, doc_id, &vector);
        Ok(())
    }

    /// Remove a document from one `(collection, field)` sparse index.
    ///
    /// Returns `true` when the document was indexed.
    pub fn sparse_delete(&self, collection: &str, field: &str, doc_id: &str) -> bool {
        self.sparse_state
            .manager
            .lock_or_recover()
            .remove_document(collection, field, doc_id)
    }

    /// Top-`k` documents in `(collection, field)` by sparse dot product.
    ///
    /// Results are `(doc_id, score)` ordered by score descending, ties broken
    /// by ascending `doc_id`. Documents sharing no dimension with the query
    /// score zero and are excluded. A collection with no sparse index yields
    /// no hits rather than an error.
    pub fn sparse_search(
        &self,
        collection: &str,
        field: &str,
        query_entries: &[(u32, f32)],
        top_k: usize,
    ) -> NodeDbResult<Vec<(String, f32)>> {
        let query =
            SparseVector::from_entries(query_entries.to_vec()).map_err(NodeDbError::bad_request)?;
        let hits = self
            .sparse_state
            .manager
            .lock_or_recover()
            .search(collection, field, &query, top_k);
        Ok(hits.into_iter().map(|h| (h.doc_id, h.score)).collect())
    }
}
