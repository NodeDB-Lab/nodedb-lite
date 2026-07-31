// SPDX-License-Identifier: BUSL-1.1

//! Adopting a new base peer id across every collection.

use std::sync::atomic::Ordering;

use crate::error::LiteError;

use super::types::{CrdtEngine, PendingDelta};

impl CrdtEngine {
    /// Re-author every collection under `new_peer_id`.
    ///
    /// Origin refuses deltas whose Loro peer id belongs to another replica,
    /// and the CRDT merge discards writes that share a peer id with the
    /// replica that already owns it. Recovering therefore means more than
    /// changing which id future writes carry: every operation already in these
    /// documents is attributed to the refused id, so the documents themselves
    /// are rebuilt under the new identity (see `CrdtState::rekey`).
    ///
    /// All collections rotate together. A collection's Loro peer id is derived
    /// from this base id, so two replicas' derived ids collide exactly when
    /// their base ids do — a collision reported for one collection is a
    /// collision in all of them.
    ///
    /// The rebuilt state is unknown to Origin: its version vector shares
    /// nothing with the one Origin has seen. Every row is therefore queued for
    /// re-push and the acked-version watermarks are dropped, so the rotation
    /// resyncs the replica rather than resuming a stream Origin cannot follow.
    ///
    /// Either every collection rotates or none does. A partial rotation would
    /// leave some collections authoring under an id Origin refuses while
    /// others moved on, with no record of which is which.
    pub fn rotate_peer_id(&mut self, new_peer_id: u64) -> Result<(), LiteError> {
        if new_peer_id == self.peer_id {
            return Err(LiteError::Storage {
                detail: "peer-id rotation was handed the id it is replacing".to_string(),
            });
        }

        // Deferred writes are applied to the current documents but not yet
        // exported. Exporting them first keeps their counter ranges meaningful;
        // after the rebuild those ranges refer to a document that no longer
        // exists.
        self.flush_deltas()?;

        let mut rotated = std::collections::BTreeMap::new();
        let mut pending = Vec::new();
        let mut next_mutation_id = self.next_mutation_id.load(Ordering::Relaxed);

        for (collection, state) in &self.states {
            let peer_id = Self::collection_peer_id(new_peer_id, collection);
            let rekeyed = state
                .rekey(collection, peer_id)
                .map_err(|e| LiteError::Storage {
                    detail: format!("peer-id rotation of '{collection}' failed: {e}"),
                })?;

            for row in &rekeyed.rows {
                let delta_bytes = rekeyed
                    .state
                    .export_local_range(row.from_counter, row.to_counter)
                    .map_err(|e| LiteError::Storage {
                        detail: format!(
                            "peer-id rotation of '{collection}' could not export row \
                             '{}': {e}",
                            row.row_id
                        ),
                    })?;
                if delta_bytes.is_empty() {
                    continue;
                }
                pending.push(PendingDelta {
                    mutation_id: next_mutation_id,
                    collection: collection.clone(),
                    document_id: row.row_id.clone(),
                    delta_bytes,
                    // A fresh stream seq: the old one identified this row in a
                    // stream Origin can no longer follow.
                    seq: 0,
                });
                next_mutation_id += 1;
            }

            rotated.insert(collection.clone(), rekeyed.state);
        }

        // Nothing above mutated `self`, so a failure returned before this point
        // leaves the engine exactly as it was.
        self.states = rotated;
        self.pending_deltas = pending;
        self.acked_versions.clear();
        self.next_mutation_id
            .store(next_mutation_id, Ordering::Relaxed);
        self.peer_id = new_peer_id;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use loro::LoroValue;

    use super::*;

    fn engine_with_rows() -> CrdtEngine {
        let mut engine = CrdtEngine::new(11).expect("engine");
        engine
            .upsert(
                "users",
                "u1",
                &[("name", LoroValue::String("alice".into()))],
            )
            .expect("u1");
        engine
            .upsert("users", "u2", &[("name", LoroValue::String("bob".into()))])
            .expect("u2");
        engine
            .upsert("orders", "o1", &[("total", LoroValue::I64(10))])
            .expect("o1");
        engine
    }

    #[test]
    fn rotation_adopts_the_new_base_id_everywhere() {
        let mut engine = engine_with_rows();

        engine.rotate_peer_id(22).expect("rotate");

        assert_eq!(engine.peer_id(), 22);
        for collection in ["users", "orders"] {
            let expected = CrdtEngine::collection_peer_id(22, collection);
            assert_eq!(
                engine.state_peer_id(collection),
                Some(expected),
                "'{collection}' must author under the rotated identity"
            );
        }
    }

    #[test]
    fn rotation_preserves_every_row() {
        let mut engine = engine_with_rows();

        engine.rotate_peer_id(22).expect("rotate");

        assert_eq!(
            engine.read_field("users", "u1", "name"),
            Some(LoroValue::String("alice".into()))
        );
        assert_eq!(
            engine.read_field("users", "u2", "name"),
            Some(LoroValue::String("bob".into()))
        );
        assert_eq!(
            engine.read_field("orders", "o1", "total"),
            Some(LoroValue::I64(10))
        );
    }

    #[test]
    fn rotation_queues_every_row_for_repush() {
        let mut engine = engine_with_rows();
        engine.clear_pending_deltas();

        engine.rotate_peer_id(22).expect("rotate");

        let pending = engine.pending_deltas();
        assert_eq!(
            pending.len(),
            3,
            "Origin has never seen the rebuilt document; every row must be re-pushed"
        );
        for (collection, document_id) in [("users", "u1"), ("users", "u2"), ("orders", "o1")] {
            assert!(
                pending
                    .iter()
                    .any(|d| d.collection == collection && d.document_id == document_id),
                "{collection}/{document_id} was not queued"
            );
        }
        assert!(
            pending.iter().all(|d| d.seq == 0),
            "a rotated delta must take a fresh stream seq"
        );
    }

    #[test]
    fn rotation_drops_acked_watermarks() {
        let mut engine = engine_with_rows();
        engine.set_acked_version("users", 42);

        engine.rotate_peer_id(22).expect("rotate");

        assert_eq!(
            engine.acked_version("users"),
            0,
            "the rebuilt document shares no version history with what Origin acked"
        );
    }

    #[test]
    fn rotation_exports_deferred_writes_before_rebuilding() {
        let mut engine = CrdtEngine::new(11).expect("engine");
        engine
            .upsert_deferred(
                "users",
                "u1",
                &[("name", LoroValue::String("alice".into()))],
            )
            .expect("deferred");

        engine.rotate_peer_id(22).expect("rotate");

        assert_eq!(
            engine.read_field("users", "u1", "name"),
            Some(LoroValue::String("alice".into())),
            "a deferred write must survive the rotation"
        );
        assert!(
            engine
                .pending_deltas()
                .iter()
                .any(|d| d.document_id == "u1"),
            "a deferred write must be queued for push after the rotation"
        );
    }

    #[test]
    fn rotating_to_the_same_id_is_refused() {
        let mut engine = engine_with_rows();
        assert!(
            engine.rotate_peer_id(11).is_err(),
            "a rotation that changes nothing must not report success"
        );
    }
}
