// SPDX-License-Identifier: BUSL-1.1

//! Deferred row mutations and per-row delta export.

use std::sync::atomic::Ordering;

use loro::LoroValue;
use nodedb_crdt::CrdtState;

use crate::error::LiteError;

use super::rollback::{Rollback, run_with_rollback};
use super::types::{CrdtEngine, CrdtRowWrite, DeferredOp, PendingDelta};

impl CrdtEngine {
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
        self.check_unique_writes(&[(CrdtRowWrite::Upsert, collection, doc_id, fields)])?;
        self.defer(collection, doc_id, Rollback::RowImage, |state| {
            state
                .upsert(collection, doc_id, fields)
                .map_err(|e| LiteError::crdt("CRDT upsert", e))
        })
    }

    /// Field merge without generating a delta, as `set_fields` merges. Use
    /// `flush_deltas()` later.
    pub fn set_fields_deferred(
        &mut self,
        collection: &str,
        doc_id: &str,
        fields: &[(&str, LoroValue)],
    ) -> Result<(), LiteError> {
        self.check_unique_writes(&[(CrdtRowWrite::SetFields, collection, doc_id, fields)])?;
        self.defer(collection, doc_id, Rollback::RowImage, |state| {
            state
                .set_fields(collection, doc_id, fields)
                .map_err(|e| LiteError::crdt("CRDT set_fields", e))
        })
    }

    /// Delete without generating a delta. Use `flush_deltas()` later.
    pub fn delete_deferred(&mut self, collection: &str, doc_id: &str) -> Result<(), LiteError> {
        self.defer(collection, doc_id, Rollback::Keep, |state| {
            state
                .delete(collection, doc_id)
                .map_err(|e| LiteError::crdt("CRDT delete", e))
        })
    }

    /// Apply `body` to the collection's document and record the counter range
    /// its operations occupy, so `flush_deltas` can export exactly that row
    /// later.
    ///
    /// A failed `body` is undone as `rollback` says. The operations it
    /// committed, and the compensating ones, are still recorded for
    /// `flush_deltas` before its error returns: see [`super::rollback`].
    fn defer<F>(
        &mut self,
        collection: &str,
        document_id: &str,
        rollback: Rollback,
        body: F,
    ) -> Result<(), LiteError>
    where
        F: FnOnce(&CrdtState) -> Result<(), LiteError>,
    {
        let image_row = (rollback == Rollback::RowImage).then_some(document_id);
        let result: Result<_, LiteError> = (|| {
            let state = self.state_mut(collection)?;
            let from_counter = state.local_op_counter();
            let outcome = run_with_rollback(state, collection, image_row, body);
            Ok((outcome, from_counter, state.local_op_counter()))
        })();
        self.sync_indexes(collection, [document_id]);
        let (outcome, from_counter, to_counter) = result?;
        if outcome.is_ok() || to_counter != from_counter {
            self.deferred.push(DeferredOp {
                collection: collection.to_string(),
                document_id: document_id.to_string(),
                from_counter,
                to_counter,
            });
        }
        outcome
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
                .map_err(|e| {
                    LiteError::crdt(&format!("flush delta export for '{}'", op.collection), e)
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
}

#[cfg(test)]
mod tests {
    use super::{CrdtEngine, LiteError, Rollback};
    use loro::LoroValue;

    /// Deferred writes must flush as one self-contained delta per row, tagged with
    /// the row's real collection and document ID — not a single coalesced blob.
    #[test]
    fn flush_deltas_emits_one_delta_per_deferred_row() {
        const PEER: u64 = 9;

        let mut engine = CrdtEngine::new(PEER).unwrap();
        engine
            .upsert_deferred("probe", "a", &[("v", LoroValue::I64(1))])
            .unwrap();
        engine
            .upsert_deferred("signals", "s1", &[("v", LoroValue::I64(2))])
            .unwrap();
        engine
            .upsert_deferred("probe", "b", &[("v", LoroValue::I64(3))])
            .unwrap();

        assert_eq!(engine.pending_count(), 0, "deferred writes export nothing");
        assert_eq!(engine.flush_deltas().unwrap(), 3);
        assert_eq!(engine.pending_count(), 3);

        let tags: Vec<(String, String)> = engine
            .pending_deltas()
            .iter()
            .map(|d| (d.collection.clone(), d.document_id.clone()))
            .collect();
        assert_eq!(
            tags,
            vec![
                ("probe".to_string(), "a".to_string()),
                ("signals".to_string(), "s1".to_string()),
                ("probe".to_string(), "b".to_string()),
            ]
        );
        assert!(
            engine
                .pending_deltas()
                .iter()
                .all(|d| !d.delta_bytes.is_empty())
        );

        let receiver =
            nodedb_crdt::CrdtState::new(CrdtEngine::collection_peer_id(PEER, "probe")).unwrap();
        for delta in engine
            .pending_deltas()
            .iter()
            .filter(|d| d.collection == "probe")
        {
            receiver.import(&delta.delta_bytes).unwrap();
        }
        assert!(receiver.row_exists("probe", "a"));
        assert!(receiver.row_exists("probe", "b"));

        // A second flush with nothing deferred is a no-op.
        assert_eq!(engine.flush_deltas().unwrap(), 0);
        assert_eq!(engine.pending_count(), 3);
    }

    #[test]
    fn deferred_operation_error_reconciles_applied_rows() {
        let mut engine = CrdtEngine::new(1).unwrap();
        let result = engine.defer("docs", "a", Rollback::Keep, |state| {
            state
                .upsert("docs", "a", &[("value", LoroValue::I64(1))])
                .unwrap();
            Err(LiteError::Storage {
                detail: "injected deferred error".into(),
            })
        });
        assert!(
            matches!(result, Err(LiteError::Storage { detail }) if detail == "injected deferred error")
        );
        assert_eq!(engine.live_ids_page("docs", None, 1, 10).unwrap(), ["a"]);
        assert_eq!(
            engine.flush_deltas().unwrap(),
            1,
            "the write committed before the error reaches the sync queue"
        );
        assert_eq!(engine.pending_count(), 1);
    }

    #[test]
    fn a_refused_deferred_set_fields_leaves_the_row_unchanged_on_every_replica() {
        const PEER: u64 = 3;
        let text = |value: &str| LoroValue::String(value.to_string().into());

        let mut engine = CrdtEngine::new(PEER).unwrap();
        engine.upsert("pages", "p", &[("a", text("x"))]).unwrap();
        let block: sonic_rs::Value = sonic_rs::from_str(r#"{"t":"b0"}"#).unwrap();
        engine
            .list_insert("pages", "p", "blocks", 0, &block)
            .unwrap();
        let before = engine.read("pages", "p");

        let err = engine
            .set_fields_deferred("pages", "p", &[("a", text("y")), ("blocks", text("z"))])
            .unwrap_err();

        assert!(matches!(err, LiteError::TypeMismatch { .. }), "{err:?}");
        assert_eq!(engine.read("pages", "p"), before);
        assert_eq!(engine.flush_deltas().unwrap(), 1);
        let peer =
            nodedb_crdt::CrdtState::new(CrdtEngine::collection_peer_id(PEER + 1, "pages")).unwrap();
        for delta in engine.pending_deltas() {
            peer.import(&delta.delta_bytes).unwrap();
        }
        assert_eq!(peer.read_row("pages", "p"), before);
    }
}
