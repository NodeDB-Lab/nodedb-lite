// SPDX-License-Identifier: BUSL-1.1

//! Immediate row mutations and the shared delta-capture envelope.

use std::sync::atomic::Ordering;

use loro::LoroValue;
use nodedb_crdt::CrdtState;

use crate::error::LiteError;

use super::live_ids::AffectedRows;
use super::rollback::{Rollback, run_with_rollback};
use super::types::{CrdtBatchOp, CrdtEngine, CrdtRowOp, CrdtRowWrite, PendingDelta};

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
        self.check_unique_writes(&[(CrdtRowWrite::Upsert, collection, doc_id, fields)])?;
        let (_, mutation_id) =
            self.with_delta_capture(collection, doc_id, "upsert", Rollback::RowImage, |state| {
                state
                    .upsert(collection, doc_id, fields)
                    .map_err(|e| LiteError::crdt("CRDT upsert", e))
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
        self.check_unique_writes(&[(CrdtRowWrite::SetFields, collection, doc_id, fields)])?;
        let (_, mutation_id) = self.with_delta_capture(
            collection,
            doc_id,
            "set_fields",
            Rollback::RowImage,
            |state| {
                state
                    .set_fields(collection, doc_id, fields)
                    .map_err(|e| LiteError::crdt("CRDT set_fields", e))
            },
        )?;
        Ok(mutation_id)
    }

    /// Delete the named scalar fields from a row, leaving every other key
    /// intact. An absent row or field authors nothing.
    ///
    /// Returns `(fields removed, mutation id)`. The mutation ID is 0 when
    /// nothing was removed and no delta was enqueued.
    pub fn remove_fields(
        &mut self,
        collection: &str,
        doc_id: &str,
        fields: &[&str],
    ) -> Result<(usize, u64), LiteError> {
        self.with_delta_capture(
            collection,
            doc_id,
            "remove_fields",
            Rollback::RowImage,
            |state| {
                state
                    .remove_fields(collection, doc_id, fields)
                    .map_err(|e| LiteError::crdt("CRDT remove_fields", e))
            },
        )
    }

    /// Delete a document/row.
    pub fn delete(&mut self, collection: &str, doc_id: &str) -> Result<u64, LiteError> {
        let (_, mutation_id) =
            self.with_delta_capture(collection, doc_id, "delete", Rollback::Keep, |state| {
                state
                    .delete(collection, doc_id)
                    .map_err(|e| LiteError::crdt("CRDT delete", e))
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
        let rows: Vec<CrdtRowOp<'_>> = ops
            .iter()
            .map(|&(collection, doc_id, fields)| (CrdtRowWrite::Upsert, collection, doc_id, fields))
            .collect();
        self.check_unique_writes(&rows)?;
        let mut last_mutation_id = 0;
        for &(collection, doc_id, fields) in ops {
            let (_, mutation_id) = self.with_delta_capture(
                collection,
                doc_id,
                "batch upsert",
                Rollback::RowImage,
                |state| {
                    state
                        .upsert(collection, doc_id, fields)
                        .map_err(|e| LiteError::crdt("CRDT batch upsert", e))
                },
            )?;
            if mutation_id != 0 {
                last_mutation_id = mutation_id;
            }
        }
        Ok(last_mutation_id)
    }

    /// Batch of mixed row writes: each op is a full-row `upsert` or a
    /// field-merging `set_fields`, applied in order under one engine borrow.
    ///
    /// Emits one delta per row, as [`Self::batch_upsert`] does. Returns the
    /// mutation ID of every delta enqueued, in op order. A row write that
    /// authored nothing enqueues no delta and contributes no ID.
    pub fn batch_write(&mut self, ops: &[CrdtRowOp<'_>]) -> Result<Vec<u64>, LiteError> {
        self.check_unique_writes(ops)?;
        let mut mutation_ids = Vec::with_capacity(ops.len());
        for &(mode, collection, doc_id, fields) in ops {
            let (_, mutation_id) = self.with_delta_capture(
                collection,
                doc_id,
                "batch write",
                Rollback::RowImage,
                |state| {
                    match mode {
                        CrdtRowWrite::Upsert => state.upsert(collection, doc_id, fields),
                        CrdtRowWrite::SetFields => state.set_fields(collection, doc_id, fields),
                    }
                    .map_err(|e| LiteError::crdt("CRDT batch write", e))
                },
            )?;
            if mutation_id != 0 {
                mutation_ids.push(mutation_id);
            }
        }
        Ok(mutation_ids)
    }

    /// Delete all documents in a collection in a single batch.
    /// Returns the number of documents deleted. Generates one delta.
    pub fn clear_collection(&mut self, collection: &str) -> Result<usize, LiteError> {
        if !self.states.contains_key(collection) {
            return Ok(0);
        }
        let (count, _) = self.capture_affected(
            collection,
            "*",
            AffectedRows::Collection,
            Rollback::Keep,
            "clear collection",
            |state| {
                state
                    .clear_collection(collection)
                    .map_err(|e| LiteError::crdt("clear collection", e))
            },
        )?;
        self.clear_index_entries(collection);
        Ok(count)
    }

    // ─── Shared Delta-Capture Envelope ───────────────────────────────

    /// Run `body` against the collection's document, capture the resulting
    /// Loro delta against the pre-mutation version vector, and push it onto
    /// the pending-deltas queue tagged with a fresh mutation ID.
    ///
    /// A failed `body` is undone as `rollback` says. The operations it
    /// committed, and the compensating ones, are still enqueued as one delta
    /// before its error returns: see [`super::rollback`].
    ///
    /// Returns `body`'s value alongside the assigned mutation ID; the ID is 0
    /// when the mutation produced no operations and nothing was enqueued (an
    /// empty delta blob is not importable by the receiver).
    pub(super) fn with_delta_capture<F, T>(
        &mut self,
        collection: &str,
        document_id: &str,
        op_name: &str,
        rollback: Rollback,
        body: F,
    ) -> Result<(T, u64), LiteError>
    where
        F: FnOnce(&CrdtState) -> Result<T, LiteError>,
    {
        self.capture_affected(
            collection,
            document_id,
            AffectedRows::One(document_id),
            rollback,
            op_name,
            body,
        )
    }

    /// `rollback` applies to a single affected row. A collection-wide body
    /// has no row image and keeps what it committed.
    fn capture_affected<F, T>(
        &mut self,
        collection: &str,
        document_id: &str,
        affected: AffectedRows<'_>,
        rollback: Rollback,
        op_name: &str,
        body: F,
    ) -> Result<(T, u64), LiteError>
    where
        F: FnOnce(&CrdtState) -> Result<T, LiteError>,
    {
        let image_row = match (rollback, &affected) {
            (Rollback::RowImage, AffectedRows::One(row)) => Some(*row),
            _ => None,
        };
        let result: Result<_, LiteError> = (|| {
            let state = self.state_mut(collection)?;
            let version_before = state.oplog_version_vector();
            let counter_before = state.local_op_counter();
            let outcome = run_with_rollback(state, collection, image_row, body);
            // A body that authored nothing (deleting an absent row, clearing an
            // empty collection, a write refused before its first operation)
            // still exports a non-empty Loro header. Enqueuing that would send
            // the receiver a delta carrying no operations.
            if state.local_op_counter() == counter_before {
                return Ok((outcome, Vec::new()));
            }
            let delta_bytes = state
                .export_updates_since(&version_before)
                .map_err(|e| LiteError::crdt(&format!("{op_name} delta export"), e))?;
            Ok((outcome, delta_bytes))
        })();
        self.reconcile_affected(collection, affected);
        let (outcome, delta_bytes) = result?;

        let mutation_id = if delta_bytes.is_empty() {
            0
        } else {
            self.enqueue_delta(collection, document_id, delta_bytes)
        };
        Ok((outcome?, mutation_id))
    }

    /// Push `delta_bytes` onto the pending-deltas queue under a fresh
    /// mutation ID, and return the ID.
    fn enqueue_delta(&mut self, collection: &str, document_id: &str, delta_bytes: Vec<u8>) -> u64 {
        let mutation_id = self.next_mutation_id.fetch_add(1, Ordering::Relaxed);
        self.pending_deltas.push(PendingDelta {
            mutation_id,
            collection: collection.to_string(),
            document_id: document_id.to_string(),
            delta_bytes,
            seq: 0,
        });
        self.mark_delta_unpersisted(mutation_id);
        mutation_id
    }
}

#[cfg(test)]
mod tests {
    use super::{AffectedRows, CrdtEngine, Rollback};
    use crate::error::LiteError;
    use loro::LoroValue;

    #[test]
    fn collection_operation_error_reconciles_remaining_rows() {
        let mut engine = CrdtEngine::new(1).unwrap();
        for id in ["*", "b"] {
            engine
                .upsert("docs", id, &[("value", LoroValue::I64(1))])
                .unwrap();
        }
        let result: Result<((), u64), LiteError> = engine.capture_affected(
            "docs",
            "*",
            AffectedRows::Collection,
            Rollback::Keep,
            "clear collection",
            |state| {
                state.delete("docs", "b").unwrap();
                Err(LiteError::Storage {
                    detail: "injected collection error".into(),
                })
            },
        );
        assert!(
            matches!(result, Err(LiteError::Storage { detail }) if detail == "injected collection error")
        );
        assert_eq!(engine.live_ids_page("docs", None, 10, 10).unwrap(), ["*"]);
        assert_eq!(
            engine.pending_count(),
            3,
            "the delete committed before the error reaches the sync queue"
        );
    }

    fn string(value: &str) -> LoroValue {
        LoroValue::String(value.to_string().into())
    }

    /// A page row with scalar field `a` and a block list under `blocks`.
    fn engine_with_block_page() -> CrdtEngine {
        let mut engine = CrdtEngine::new(1).unwrap();
        engine.upsert("pages", "p", &[("a", string("x"))]).unwrap();
        let block: sonic_rs::Value = sonic_rs::from_str(r#"{"t":"b0"}"#).unwrap();
        engine
            .list_insert("pages", "p", "blocks", 0, &block)
            .unwrap();
        engine
    }

    /// Replay every pending delta into a fresh peer and read the page row.
    fn replayed_page(engine: &CrdtEngine) -> Option<LoroValue> {
        let peer = nodedb_crdt::CrdtState::new(CrdtEngine::collection_peer_id(2, "pages")).unwrap();
        for delta in engine.pending_deltas() {
            peer.import(&delta.delta_bytes).unwrap();
        }
        peer.read_row("pages", "p")
    }

    fn assert_refused_write_leaves_row_unchanged(
        write: impl FnOnce(&mut CrdtEngine) -> Result<u64, LiteError>,
    ) {
        let mut engine = engine_with_block_page();
        let before = engine.read("pages", "p");
        let pending_before = engine.pending_count();

        let err = write(&mut engine).unwrap_err();

        assert!(
            matches!(&err, LiteError::TypeMismatch { collection, .. } if collection == "pages"),
            "{err:?}"
        );
        assert_eq!(engine.read("pages", "p"), before);
        assert_eq!(
            engine.pending_count(),
            pending_before + 1,
            "the partial write and its undo reach the sync queue as one delta"
        );
        assert_eq!(replayed_page(&engine), before);
    }

    #[test]
    fn a_refused_set_fields_leaves_the_row_unchanged_on_every_replica() {
        assert_refused_write_leaves_row_unchanged(|engine| {
            engine.set_fields("pages", "p", &[("a", string("y")), ("blocks", string("z"))])
        });
    }

    #[test]
    fn a_refused_upsert_leaves_the_row_unchanged_on_every_replica() {
        assert_refused_write_leaves_row_unchanged(|engine| {
            engine.upsert(
                "pages",
                "p",
                &[("n", string("new")), ("blocks", string("z"))],
            )
        });
    }

    #[test]
    fn a_write_refused_before_its_first_operation_enqueues_nothing() {
        let mut engine = engine_with_block_page();
        let pending_before = engine.pending_count();
        let err = engine
            .set_fields("pages", "p", &[("blocks", string("z"))])
            .unwrap_err();
        assert!(matches!(err, LiteError::TypeMismatch { .. }), "{err:?}");
        assert_eq!(engine.pending_count(), pending_before);
    }

    #[test]
    fn a_successful_write_after_a_refused_one_enqueues_one_delta() {
        let mut engine = engine_with_block_page();
        engine
            .set_fields("pages", "p", &[("a", string("y")), ("blocks", string("z"))])
            .unwrap_err();
        let pending_before = engine.pending_count();

        let mutation_id = engine
            .set_fields("pages", "p", &[("a", string("y"))])
            .unwrap();

        assert_ne!(mutation_id, 0);
        assert_eq!(engine.pending_count(), pending_before + 1);
        assert_eq!(
            replayed_page(&engine),
            engine.read("pages", "p"),
            "a fresh peer converges on the written row"
        );
    }

    #[test]
    fn upsert_generates_delta() {
        let mut engine = CrdtEngine::new(1).unwrap();
        let mid = engine
            .upsert(
                "users",
                "u1",
                &[("name", LoroValue::String("Alice".into()))],
            )
            .unwrap();

        assert_eq!(mid, 1);
        assert_eq!(engine.pending_count(), 1);
        assert!(!engine.pending_deltas()[0].delta_bytes.is_empty());
    }

    #[test]
    fn delete_generates_delta() {
        let mut engine = CrdtEngine::new(1).unwrap();
        engine
            .upsert("users", "u1", &[("name", LoroValue::String("X".into()))])
            .unwrap();
        let mid = engine.delete("users", "u1").unwrap();

        assert_eq!(mid, 2); // Second mutation.
        assert_eq!(engine.pending_count(), 2);
        assert!(!engine.exists("users", "u1"));
    }

    /// A delta must be applicable on its own. The receiver stores documents per
    /// collection, so a delta for `probe` whose causal predecessors were written
    /// to `signals` can never be applied there — those predecessors never arrive
    /// and the row is silently lost. Writing the collections interleaved and
    /// replaying only `probe`'s deltas into a fresh document reproduces exactly
    /// that: under a single shared oplog the second delta is causally incomplete
    /// and row "b" never materializes.
    #[test]
    fn interleaved_collection_writes_export_self_contained_deltas() {
        const PEER: u64 = 7;

        let mut engine = CrdtEngine::new(PEER).unwrap();
        engine
            .upsert("probe", "a", &[("v", LoroValue::I64(1))])
            .unwrap();
        engine
            .upsert("signals", "s1", &[("v", LoroValue::I64(2))])
            .unwrap();
        engine
            .upsert("probe", "b", &[("v", LoroValue::I64(3))])
            .unwrap();

        let probe_deltas: Vec<Vec<u8>> = engine
            .pending_deltas()
            .iter()
            .filter(|d| d.collection == "probe")
            .map(|d| d.delta_bytes.clone())
            .collect();
        assert_eq!(probe_deltas.len(), 2, "one delta per probe row");

        // The receiver only ever sees this collection's deltas.
        let receiver =
            nodedb_crdt::CrdtState::new(CrdtEngine::collection_peer_id(PEER, "probe")).unwrap();
        for bytes in &probe_deltas {
            receiver.import(bytes).unwrap();
        }

        assert!(receiver.row_exists("probe", "a"));
        assert!(
            receiver.row_exists("probe", "b"),
            "second probe delta was causally incomplete; row 'b' was lost"
        );
    }
}
