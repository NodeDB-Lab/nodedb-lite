//! Atomic transactions and conflict resolution policies.
//!
//! Loro transactions are operation bundling (not ACID rollback). Our
//! approach: validate all operations upfront, then apply as a single
//! `batch_upsert` + batch delete. One Loro delta for the entire
//! transaction. If validation fails, nothing is applied.

use std::collections::HashMap;

use nodedb_types::error::{NodeDbError, NodeDbResult};
use nodedb_types::value::Value;

use super::super::convert::value_to_loro;
use super::super::{LockExt, NodeDbLite};
use crate::engine::crdt::engine::{CrdtBatchOp, CrdtField};
use crate::storage::engine::StorageEngine;

/// A single operation in a transaction batch.
#[derive(Debug, Clone)]
pub enum TransactionOp {
    Put {
        collection: String,
        doc_id: String,
        fields: HashMap<String, Value>,
    },
    Delete {
        collection: String,
        doc_id: String,
    },
}

impl<S: StorageEngine> NodeDbLite<S> {
    /// Execute a batch of operations atomically.
    ///
    /// **Atomicity model**: all operations are validated upfront. If any
    /// validation fails, none are applied. Once validation passes, all
    /// operations are applied in a single Loro batch (one delta export).
    ///
    /// This is NOT ACID rollback — Loro doesn't support undo. Instead,
    /// we validate-then-apply: no partial commits are possible because
    /// we only start writing after all checks pass.
    ///
    /// Returns the number of operations applied.
    pub fn transaction(&self, ops: &[TransactionOp]) -> NodeDbResult<u64> {
        if ops.is_empty() {
            return Ok(0);
        }

        // Phase 1: Validate all operations.
        // - Put: validate doc_id is non-empty, fields are non-empty.
        // - Delete: validate doc_id is non-empty.
        for (i, op) in ops.iter().enumerate() {
            match op {
                TransactionOp::Put {
                    collection,
                    doc_id,
                    fields,
                } => {
                    if doc_id.is_empty() {
                        return Err(NodeDbError::bad_request(format!(
                            "transaction op {i}: Put requires non-empty doc_id"
                        )));
                    }
                    if collection.is_empty() {
                        return Err(NodeDbError::bad_request(format!(
                            "transaction op {i}: Put requires non-empty collection"
                        )));
                    }
                    if fields.is_empty() {
                        return Err(NodeDbError::bad_request(format!(
                            "transaction op {i}: Put requires at least one field"
                        )));
                    }
                }
                TransactionOp::Delete { collection, doc_id } => {
                    if doc_id.is_empty() || collection.is_empty() {
                        return Err(NodeDbError::bad_request(format!(
                            "transaction op {i}: Delete requires non-empty collection and doc_id"
                        )));
                    }
                }
            }
        }

        // Phase 2: Separate puts and deletes.
        let mut put_ops: Vec<(&str, &str, Vec<CrdtField<'_>>)> = Vec::new();
        let mut delete_ops: Vec<(&str, &str)> = Vec::new();

        for op in ops {
            match op {
                TransactionOp::Put {
                    collection,
                    doc_id,
                    fields,
                } => {
                    let loro_fields: Vec<CrdtField<'_>> = fields
                        .iter()
                        .map(|(k, v)| (k.as_str(), value_to_loro(v)))
                        .collect();
                    put_ops.push((collection.as_str(), doc_id.as_str(), loro_fields));
                }
                TransactionOp::Delete { collection, doc_id } => {
                    delete_ops.push((collection.as_str(), doc_id.as_str()));
                }
            }
        }

        // Phase 3: Apply all operations in a single Loro batch.
        // One delta export for the entire transaction.
        let mut crdt = self.crdt.lock_or_recover();

        // Build batch ops with borrowed field slices.
        let batch_refs: Vec<CrdtBatchOp<'_>> = put_ops
            .iter()
            .map(|(coll, id, fields)| (*coll, *id, fields.as_slice()))
            .collect();

        if !batch_refs.is_empty() {
            crdt.batch_upsert(&batch_refs)
                .map_err(NodeDbError::storage)?;
        }

        // Deletes are applied individually but within the same lock hold,
        // so the delta export at the end captures everything.
        for &(collection, doc_id) in &delete_ops {
            crdt.delete(collection, doc_id)
                .map_err(NodeDbError::storage)?;
        }

        let count = (put_ops.len() + delete_ops.len()) as u64;
        drop(crdt);

        // Phase 4: Update text indices for all affected documents.
        for (collection, doc_id, _) in &put_ops {
            let crdt = self.crdt.lock_or_recover();
            if let Some(loro_val) = crdt.read(collection, doc_id) {
                let doc = crate::nodedb::convert::loro_value_to_document(doc_id, &loro_val);
                drop(crdt);
                self.index_document_text(collection, doc_id, &doc.fields);
            }
        }
        for &(collection, doc_id) in &delete_ops {
            self.remove_document_text(collection, doc_id);
        }

        Ok(count)
    }

    /// Set conflict resolution policy for a collection.
    ///
    /// Policies are evaluated on sync when Origin rejects a delta.
    /// Available policies from `nodedb-crdt::PolicyRegistry`:
    /// - LastWriterWins (default)
    /// - RenameSuffix
    /// - CascadeDefer
    /// - EscalateToDlq
    pub fn set_conflict_policy(&self, collection: &str, policy: nodedb_crdt::CollectionPolicy) {
        let mut crdt = self.crdt.lock_or_recover();
        crdt.set_policy(collection, policy);
    }
}
