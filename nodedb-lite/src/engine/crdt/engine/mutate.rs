// SPDX-License-Identifier: BUSL-1.1

//! Write paths: upsert, set-fields, delete, batching, deferred
//! accumulation, and the shared delta-capture envelope.

use std::sync::atomic::Ordering;

use loro::LoroValue;
use nodedb_crdt::CrdtState;

use crate::error::LiteError;

use super::types::{CrdtBatchOp, CrdtEngine, DeferredOp, PendingDelta};

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
        let (_, mutation_id) = self.with_delta_capture(collection, doc_id, "upsert", |state| {
            state
                .upsert(collection, doc_id, fields)
                .map_err(|e| LiteError::Storage {
                    detail: format!("CRDT upsert failed: {e}"),
                })
        })?;
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
        let (_, mutation_id) =
            self.with_delta_capture(collection, doc_id, "set_fields", |state| {
                state
                    .set_fields(collection, doc_id, fields)
                    .map_err(|e| LiteError::Storage {
                        detail: format!("CRDT set_fields failed: {e}"),
                    })
            })?;
        Ok(mutation_id)
    }

    /// Delete a document/row.
    pub fn delete(&mut self, collection: &str, doc_id: &str) -> Result<u64, LiteError> {
        let (_, mutation_id) = self.with_delta_capture(collection, doc_id, "delete", |state| {
            state
                .delete(collection, doc_id)
                .map_err(|e| LiteError::Storage {
                    detail: format!("CRDT delete failed: {e}"),
                })
        })?;
        Ok(mutation_id)
    }

    /// Batch upsert: apply N mutations, emitting one delta per row.
    ///
    /// Ops may span collections. Each row gets its own `PendingDelta` tagged
    /// with its real collection and document ID — a delta covering several
    /// rows (or several collections) is not independently applicable by the
    /// receiver, which commits per row and stores documents per collection.
    ///
    /// Returns the mutation ID of the last delta enqueued, or 0 if `ops` is
    /// empty.
    pub fn batch_upsert(&mut self, ops: &[CrdtBatchOp<'_>]) -> Result<u64, LiteError> {
        let mut last_mutation_id = 0;
        for &(collection, doc_id, fields) in ops {
            let (_, mutation_id) =
                self.with_delta_capture(collection, doc_id, "batch upsert", |state| {
                    state
                        .upsert(collection, doc_id, fields)
                        .map_err(|e| LiteError::Storage {
                            detail: format!("CRDT batch upsert failed: {e}"),
                        })
                })?;
            if mutation_id != 0 {
                last_mutation_id = mutation_id;
            }
        }
        Ok(last_mutation_id)
    }

    /// Upsert without generating a delta. Use `flush_deltas()` later
    /// to export the accumulated mutations.
    ///
    /// This is the fast path for local-only writes (KV put, bulk insert)
    /// where per-operation delta export is prohibitively expensive.
    pub fn upsert_deferred(
        &mut self,
        collection: &str,
        doc_id: &str,
        fields: &[(&str, LoroValue)],
    ) -> Result<(), LiteError> {
        self.defer(collection, doc_id, |state| {
            state
                .upsert(collection, doc_id, fields)
                .map_err(|e| LiteError::Storage {
                    detail: format!("CRDT upsert failed: {e}"),
                })
        })
    }

    /// Delete without generating a delta. Use `flush_deltas()` later.
    pub fn delete_deferred(&mut self, collection: &str, doc_id: &str) -> Result<(), LiteError> {
        self.defer(collection, doc_id, |state| {
            state
                .delete(collection, doc_id)
                .map_err(|e| LiteError::Storage {
                    detail: format!("CRDT delete failed: {e}"),
                })
        })
    }

    /// Apply `body` to the collection's document and record the counter range
    /// its operations occupy, so `flush_deltas` can export exactly that row
    /// later.
    fn defer<F>(&mut self, collection: &str, document_id: &str, body: F) -> Result<(), LiteError>
    where
        F: FnOnce(&CrdtState) -> Result<(), LiteError>,
    {
        let (from_counter, to_counter) = {
            let state = self.state_mut(collection)?;
            let from_counter = state.local_op_counter();
            body(state)?;
            (from_counter, state.local_op_counter())
        };
        self.deferred.push(DeferredOp {
            collection: collection.to_string(),
            document_id: document_id.to_string(),
            from_counter,
            to_counter,
        });
        Ok(())
    }

    /// Export one delta per deferred mutation since the last flush. Returns
    /// the number of deferred operations processed, or 0 if none.
    ///
    /// Call this after a batch of `upsert_deferred` / `delete_deferred`
    /// calls to produce the sync deltas. Each deferred write is exported over
    /// its own recorded counter range so the resulting delta is applicable on
    /// its own — a single coalesced delta spanning rows and collections is not.
    pub fn flush_deltas(&mut self) -> Result<usize, LiteError> {
        let deferred = std::mem::take(&mut self.deferred);
        let count = deferred.len();

        for op in deferred {
            let Some(state) = self.states.get(&op.collection) else {
                continue;
            };
            let delta_bytes = state
                .export_local_range(op.from_counter, op.to_counter)
                .map_err(|e| LiteError::Storage {
                    detail: format!("flush delta export for '{}': {e}", op.collection),
                })?;
            // An empty range exports no bytes, and an empty blob is not
            // importable — never enqueue one.
            if delta_bytes.is_empty() {
                continue;
            }

            let mutation_id = self.next_mutation_id.fetch_add(1, Ordering::Relaxed);
            self.pending_deltas.push(PendingDelta {
                mutation_id,
                collection: op.collection,
                document_id: op.document_id,
                delta_bytes,
                seq: 0,
            });
            self.mark_delta_unpersisted(mutation_id);
        }

        Ok(count)
    }

    /// Delete all documents in a collection in a single batch.
    /// Returns the number of documents deleted. Generates one delta.
    pub fn clear_collection(&mut self, collection: &str) -> Result<usize, LiteError> {
        if !self.states.contains_key(collection) {
            return Ok(0);
        }
        let (count, _) = self.with_delta_capture(collection, "*", "clear collection", |state| {
            state
                .clear_collection(collection)
                .map_err(|e| LiteError::Storage {
                    detail: format!("clear collection: {e}"),
                })
        })?;
        Ok(count)
    }

    // ─── Shared Delta-Capture Envelope ───────────────────────────────

    /// Run `body` against the collection's document, capture the resulting
    /// Loro delta against the pre-mutation version vector, and push it onto
    /// the pending-deltas queue tagged with a fresh mutation ID.
    ///
    /// Returns `body`'s value alongside the assigned mutation ID; the ID is 0
    /// when the mutation produced no operations and nothing was enqueued (an
    /// empty delta blob is not importable by the receiver).
    pub(super) fn with_delta_capture<F, T>(
        &mut self,
        collection: &str,
        document_id: &str,
        op_name: &str,
        body: F,
    ) -> Result<(T, u64), LiteError>
    where
        F: FnOnce(&CrdtState) -> Result<T, LiteError>,
    {
        let (value, delta_bytes) = {
            let state = self.state_mut(collection)?;
            let version_before = state.oplog_version_vector();
            let counter_before = state.local_op_counter();
            let value = body(state)?;
            // A body that authored nothing (deleting an absent row, clearing an
            // empty collection) still exports a non-empty Loro header. Enqueuing
            // that would send the receiver a delta carrying no operations.
            if state.local_op_counter() == counter_before {
                return Ok((value, 0));
            }
            let delta_bytes =
                state
                    .export_updates_since(&version_before)
                    .map_err(|e| LiteError::Storage {
                        detail: format!("{op_name} delta export: {e}"),
                    })?;
            (value, delta_bytes)
        };

        if delta_bytes.is_empty() {
            return Ok((value, 0));
        }

        let mutation_id = self.next_mutation_id.fetch_add(1, Ordering::Relaxed);
        self.pending_deltas.push(PendingDelta {
            mutation_id,
            collection: collection.to_string(),
            document_id: document_id.to_string(),
            delta_bytes,
            seq: 0,
        });
        self.mark_delta_unpersisted(mutation_id);
        Ok((value, mutation_id))
    }
}
