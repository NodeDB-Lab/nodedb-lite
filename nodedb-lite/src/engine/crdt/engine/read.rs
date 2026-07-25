// SPDX-License-Identifier: BUSL-1.1

//! Read paths, collection-name registry, and conflict-policy binding.

use loro::LoroValue;

use super::types::CrdtEngine;

impl CrdtEngine {
    /// Read a single field from a row without cloning the entire row.
    ///
    /// Fast path for KV reads: avoids `get_deep_value()` and returns
    /// only the requested field.
    pub fn read_field(&self, collection: &str, doc_id: &str, field: &str) -> Option<LoroValue> {
        self.state.read_field(collection, doc_id, field)
    }

    // ─── Reads ───────────────────────────────────────────────────────

    /// Read a document's fields.
    pub fn read(&self, collection: &str, doc_id: &str) -> Option<LoroValue> {
        self.state.read_row(collection, doc_id)
    }

    /// Check if a document exists.
    pub fn exists(&self, collection: &str, doc_id: &str) -> bool {
        self.state.row_exists(collection, doc_id)
    }

    /// List all document IDs in a collection.
    pub fn list_ids(&self, collection: &str) -> Vec<String> {
        self.state.row_ids(collection)
    }
    /// Register a collection name so it appears in `collection_names()` even
    /// before any document has been inserted into it.
    ///
    /// This is needed for bitemporal document collections created via DDL: the
    /// bitemporal flag is persisted to `Namespace::Meta`, but the Loro root map
    /// has no entry for the collection until the first `upsert`.  Calling this
    /// method ensures the SQL catalog can resolve the collection name immediately
    /// after `CREATE COLLECTION … WITH (bitemporal=true)`.
    pub fn register_collection(&mut self, name: &str) {
        self.registered_collections.insert(name.to_owned());
    }

    /// List all known collection names.
    ///
    /// Merges names that appear as top-level keys in the Loro document (i.e.
    /// collections that have at least one row) with names that were explicitly
    /// registered via `register_collection` (i.e. collections created via DDL
    /// but not yet populated).
    pub fn collection_names(&self) -> Vec<String> {
        let mut names: std::collections::HashSet<String> =
            self.state.collection_names().into_iter().collect();
        names.extend(self.registered_collections.iter().cloned());
        names.into_iter().collect()
    }

    /// Set conflict resolution policy for a collection.
    pub fn set_policy(&mut self, collection: &str, policy: nodedb_crdt::CollectionPolicy) {
        self.policies.set(collection, policy);
    }

    /// Get the policy registry (for sync conflict resolution).
    pub fn policies(&self) -> &nodedb_crdt::PolicyRegistry {
        &self.policies
    }
}
