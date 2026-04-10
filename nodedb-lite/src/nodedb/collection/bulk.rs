//! Bulk update and delete by predicate (ScanFilter-based).
//!
//! Holds CRDT lock across scan+write to prevent concurrent modification
//! between the filter evaluation and the mutation application.

use std::collections::HashMap;

use nodedb_types::error::{NodeDbError, NodeDbResult};
use nodedb_types::value::Value;

use super::super::convert::value_to_loro;
use super::super::{LockExt, NodeDbLite};
use crate::storage::engine::StorageEngine;

impl<S: StorageEngine> NodeDbLite<S> {
    /// Bulk update documents matching a predicate.
    ///
    /// Scans all documents, evaluates `ScanFilter` predicates, and applies
    /// `updates` to matching documents — all under a single CRDT lock to
    /// prevent concurrent writes from being lost between scan and update.
    ///
    /// Returns the number of documents updated.
    pub fn bulk_update(
        &self,
        collection: &str,
        filters: &[nodedb_query::ScanFilter],
        updates: &HashMap<String, Value>,
    ) -> NodeDbResult<u64> {
        // Single lock for scan + write: no gap for concurrent modifications.
        let mut crdt = self.crdt.lock_or_recover();
        let ids = crdt.list_ids(collection);

        let mut matching_ids = Vec::new();
        for id in &ids {
            if let Some(loro_val) = crdt.read(collection, id) {
                let doc = crate::nodedb::convert::loro_value_to_document(id, &loro_val);
                let json = serde_json::to_value(&doc.fields).unwrap_or_default();
                let msgpack = nodedb_types::json_msgpack::json_to_msgpack_or_empty(&json);
                if filters.is_empty() || filters.iter().all(|f| f.matches_binary(&msgpack)) {
                    matching_ids.push(id.clone());
                }
            }
        }

        let update_fields: Vec<(&str, loro::LoroValue)> = updates
            .iter()
            .map(|(k, v)| (k.as_str(), value_to_loro(v)))
            .collect();

        let mut count = 0u64;
        for id in &matching_ids {
            crdt.upsert(collection, id, &update_fields)
                .map_err(NodeDbError::storage)?;
            count += 1;
        }
        drop(crdt);

        // Update text index outside the CRDT lock (text index has its own lock).
        for id in &matching_ids {
            let crdt = self.crdt.lock_or_recover();
            if let Some(loro_val) = crdt.read(collection, id) {
                let doc = crate::nodedb::convert::loro_value_to_document(id, &loro_val);
                drop(crdt);
                self.index_document_text(collection, id, &doc.fields);
            }
        }

        Ok(count)
    }

    /// Bulk delete documents matching a predicate.
    ///
    /// Same single-lock pattern as `bulk_update`.
    /// Returns the number of documents deleted.
    pub fn bulk_delete(
        &self,
        collection: &str,
        filters: &[nodedb_query::ScanFilter],
    ) -> NodeDbResult<u64> {
        let mut crdt = self.crdt.lock_or_recover();
        let ids = crdt.list_ids(collection);

        let mut matching_ids = Vec::new();
        for id in &ids {
            if let Some(loro_val) = crdt.read(collection, id) {
                let doc = crate::nodedb::convert::loro_value_to_document(id, &loro_val);
                let json = serde_json::to_value(&doc.fields).unwrap_or_default();
                let msgpack = nodedb_types::json_msgpack::json_to_msgpack_or_empty(&json);
                if filters.is_empty() || filters.iter().all(|f| f.matches_binary(&msgpack)) {
                    matching_ids.push(id.clone());
                }
            }
        }

        let mut count = 0u64;
        for id in &matching_ids {
            crdt.delete(collection, id).map_err(NodeDbError::storage)?;
            count += 1;
        }
        drop(crdt);

        for id in &matching_ids {
            self.remove_document_text(collection, id);
        }

        Ok(count)
    }
}
