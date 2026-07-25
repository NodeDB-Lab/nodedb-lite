// SPDX-License-Identifier: BUSL-1.1

//! Write paths: upsert, set-fields, delete, batching, deferred
//! accumulation, and the shared delta-capture envelope.

use std::sync::atomic::Ordering;

use loro::LoroValue;
use nodedb_crdt::CrdtState;

use crate::error::LiteError;

use super::types::{CrdtBatchOp, CrdtEngine, PendingDelta};

impl CrdtEngine {
    // ─── Mutations ───────────────────────────────────────────────────

    /// Insert or update a document (used by document_put, vector_insert metadata, etc.).
    ///
    /// Generates a Loro delta and accumulates it as a pending sync item.
    pub fn upsert(
        &mut self,
        collection: &str,
        doc_id: &str,
        fields: &[(&str, LoroValue)],
    ) -> Result<u64, LiteError> {
        // Snapshot before mutation for delta extraction.
        let version_before = self.state.oplog_version_vector();

        self.state
            .upsert(collection, doc_id, fields)
            .map_err(|e| LiteError::Storage {
                detail: format!("CRDT upsert failed: {e}"),
            })?;

        // Extract the delta (operations since version_before).
        let delta_bytes = self
            .state
            .export_updates_since(&version_before)
            .map_err(|e| LiteError::Storage {
                detail: format!("delta export failed: {e}"),
            })?;

        let mutation_id = self.next_mutation_id.fetch_add(1, Ordering::Relaxed);
        self.pending_deltas.push(PendingDelta {
            mutation_id,
            collection: collection.to_string(),
            document_id: doc_id.to_string(),
            delta_bytes,
            seq: 0,
        });

        Ok(mutation_id)
    }

    /// Partial-merge write: set exactly the provided scalar fields on a row,
    /// leaving untouched keys intact.
    ///
    /// This is `upsert` without its full-projection prune — the UPDATE SET
    /// semantic behind `CrdtOp::DocUpsert { partial: true }`. Delta export and
    /// pending-sync accounting are identical to `upsert`.
    pub fn set_fields(
        &mut self,
        collection: &str,
        doc_id: &str,
        fields: &[(&str, LoroValue)],
    ) -> Result<u64, LiteError> {
        let version_before = self.state.oplog_version_vector();

        self.state
            .set_fields(collection, doc_id, fields)
            .map_err(|e| LiteError::Storage {
                detail: format!("CRDT set_fields failed: {e}"),
            })?;

        let delta_bytes = self
            .state
            .export_updates_since(&version_before)
            .map_err(|e| LiteError::Storage {
                detail: format!("delta export failed: {e}"),
            })?;

        let mutation_id = self.next_mutation_id.fetch_add(1, Ordering::Relaxed);
        self.pending_deltas.push(PendingDelta {
            mutation_id,
            collection: collection.to_string(),
            document_id: doc_id.to_string(),
            delta_bytes,
            seq: 0,
        });

        Ok(mutation_id)
    }

    /// Delete a document/row.
    pub fn delete(&mut self, collection: &str, doc_id: &str) -> Result<u64, LiteError> {
        let version_before = self.state.oplog_version_vector();

        self.state
            .delete(collection, doc_id)
            .map_err(|e| LiteError::Storage {
                detail: format!("CRDT delete failed: {e}"),
            })?;

        let delta_bytes = self
            .state
            .export_updates_since(&version_before)
            .map_err(|e| LiteError::Storage {
                detail: format!("delta export failed: {e}"),
            })?;

        let mutation_id = self.next_mutation_id.fetch_add(1, Ordering::Relaxed);
        self.pending_deltas.push(PendingDelta {
            mutation_id,
            collection: collection.to_string(),
            document_id: doc_id.to_string(),
            delta_bytes,
            seq: 0,
        });

        Ok(mutation_id)
    }

    /// Batch upsert: apply N mutations with a single delta export.
    ///
    /// This is O(1) Loro exports instead of O(N). Use for bulk inserts
    /// (cold-start hydration, batch vector insert, graph edge loading).
    pub fn batch_upsert(&mut self, ops: &[CrdtBatchOp<'_>]) -> Result<u64, LiteError> {
        if ops.is_empty() {
            return Ok(0);
        }

        let version_before = self.state.oplog_version_vector();

        for &(collection, doc_id, fields) in ops {
            self.state
                .upsert(collection, doc_id, fields)
                .map_err(|e| LiteError::Storage {
                    detail: format!("CRDT batch upsert failed: {e}"),
                })?;
        }

        let delta_bytes = self
            .state
            .export_updates_since(&version_before)
            .map_err(|e| LiteError::Storage {
                detail: format!("batch delta export failed: {e}"),
            })?;

        // Use the collection from the first op. If ops span multiple collections,
        // label it "mixed" to avoid misleading a single-collection name.
        let collection_name = {
            let first = ops[0].0;
            if ops.iter().all(|&(c, _, _)| c == first) {
                first.to_string()
            } else {
                "mixed".to_string()
            }
        };

        let mutation_id = self.next_mutation_id.fetch_add(1, Ordering::Relaxed);
        self.pending_deltas.push(PendingDelta {
            mutation_id,
            collection: collection_name,
            document_id: format!("{}_ops", ops.len()),
            delta_bytes,
            seq: 0,
        });

        Ok(mutation_id)
    }

    /// Upsert without generating a delta. Use `flush_deltas()` later
    /// to batch-export all accumulated mutations as a single delta.
    ///
    /// This is the fast path for local-only writes (KV put, bulk insert)
    /// where per-operation delta export is prohibitively expensive.
    pub fn upsert_deferred(
        &mut self,
        collection: &str,
        doc_id: &str,
        fields: &[(&str, LoroValue)],
    ) -> Result<(), LiteError> {
        // Capture version before if this is the first deferred op.
        if self.deferred_version.is_none() {
            self.deferred_version = Some(self.state.oplog_version_vector());
        }

        self.state
            .upsert(collection, doc_id, fields)
            .map_err(|e| LiteError::Storage {
                detail: format!("CRDT upsert failed: {e}"),
            })?;
        self.deferred_count += 1;
        Ok(())
    }

    /// Delete without generating a delta. Use `flush_deltas()` later.
    pub fn delete_deferred(&mut self, collection: &str, doc_id: &str) -> Result<(), LiteError> {
        if self.deferred_version.is_none() {
            self.deferred_version = Some(self.state.oplog_version_vector());
        }

        self.state
            .delete(collection, doc_id)
            .map_err(|e| LiteError::Storage {
                detail: format!("CRDT delete failed: {e}"),
            })?;
        self.deferred_count += 1;
        Ok(())
    }

    /// Export a single delta covering all deferred mutations since the last
    /// flush. Returns the number of operations included, or 0 if none.
    ///
    /// Call this after a batch of `upsert_deferred` / `delete_deferred`
    /// calls to produce the sync delta.
    pub fn flush_deltas(&mut self) -> Result<usize, LiteError> {
        let count = self.deferred_count;
        if count == 0 {
            return Ok(0);
        }

        let version_before = self
            .deferred_version
            .take()
            .expect("deferred_version must be set when deferred_count > 0");

        let delta_bytes = self
            .state
            .export_updates_since(&version_before)
            .map_err(|e| LiteError::Storage {
                detail: format!("flush delta export failed: {e}"),
            })?;

        let mutation_id = self.next_mutation_id.fetch_add(1, Ordering::Relaxed);
        self.pending_deltas.push(PendingDelta {
            mutation_id,
            // "deferred" reflects that this delta covers multiple collections
            // accumulated via upsert_deferred/delete_deferred calls.
            collection: "deferred".to_string(),
            document_id: format!("{count}_ops"),
            delta_bytes,
            seq: 0,
        });

        self.deferred_count = 0;
        Ok(count)
    }
    /// Delete all documents in a collection in a single batch.
    /// Returns the number of documents deleted. Generates one delta.
    pub fn clear_collection(&mut self, collection: &str) -> Result<usize, LiteError> {
        let version_before = self.state.oplog_version_vector();

        let count = self
            .state
            .clear_collection(collection)
            .map_err(|e| LiteError::Storage {
                detail: format!("clear collection: {e}"),
            })?;

        if count > 0 {
            let delta_bytes = self
                .state
                .export_updates_since(&version_before)
                .map_err(|e| LiteError::Storage {
                    detail: format!("delta export after clear: {e}"),
                })?;

            let mutation_id = self.next_mutation_id.fetch_add(1, Ordering::Relaxed);
            self.pending_deltas.push(PendingDelta {
                mutation_id,
                collection: collection.to_string(),
                document_id: "*".to_string(),
                delta_bytes,
                seq: 0,
            });
        }

        Ok(count)
    }
    // ─── LoroMovableList Operations ──────────────────────────────────

    /// Run `body` against the doc, capture the resulting Loro delta against
    /// the pre-mutation version vector, and push it onto the pending-deltas
    /// queue tagged with a fresh mutation id. Used to factor the
    /// "snapshot → mutate → export delta → enqueue" envelope shared by all
    /// LoroMovableList helpers.
    pub(super) fn with_delta_capture<F>(
        &mut self,
        collection: &str,
        document_id: &str,
        op_name: &str,
        body: F,
    ) -> Result<(), LiteError>
    where
        F: FnOnce(&CrdtState) -> Result<(), LiteError>,
    {
        let version_before = self.state.oplog_version_vector();
        body(&self.state)?;
        let delta_bytes = self
            .state
            .export_updates_since(&version_before)
            .map_err(|e| LiteError::Storage {
                detail: format!("{op_name} delta export: {e}"),
            })?;
        let mutation_id = self.next_mutation_id.fetch_add(1, Ordering::Relaxed);
        self.pending_deltas.push(PendingDelta {
            mutation_id,
            collection: collection.to_string(),
            document_id: document_id.to_string(),
            delta_bytes,
            seq: 0,
        });
        Ok(())
    }
}
