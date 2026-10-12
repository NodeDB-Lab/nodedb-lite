// SPDX-License-Identifier: Apache-2.0

//! Ordered applied-row enumeration and retained-memory accounting.

use std::collections::BTreeSet;
use std::ops::Bound::{Excluded, Unbounded};

use super::types::CrdtEngine;
use crate::error::LiteError;

// Charge a conservative tree node per entry, including links and unused slots.
pub(super) const TREE_ENTRY_BYTES: usize = 128;

pub(super) enum AffectedRows<'a> {
    One(&'a str),
    Collection,
}

impl CrdtEngine {
    /// Read IDs after `exclusive_after_id`, bounded by count and UTF-8 bytes.
    /// Continue from the last retained ID until an empty page.
    /// A short byte-limited page does not indicate completion.
    pub(crate) fn live_ids_page(
        &self,
        collection: &str,
        exclusive_after_id: Option<&str>,
        count_limit: usize,
        byte_limit: usize,
    ) -> Result<Vec<String>, LiteError> {
        if count_limit == 0 {
            return Ok(Vec::new());
        }
        let Some(ids) = self.live_ids.get(collection) else {
            return Ok(Vec::new());
        };
        let after = exclusive_after_id.map_or(Unbounded, Excluded);
        let mut page = Vec::new();
        let mut bytes = 0usize;
        for id in ids.range::<str, _>((after, Unbounded)).take(count_limit) {
            if id.len() > byte_limit.saturating_sub(bytes) {
                if page.is_empty() {
                    return Err(LiteError::Backpressure {
                        detail: format!(
                            "live ID '{id}' in '{collection}' exceeds byte budget {byte_limit}: increase the page byte budget"
                        ),
                    });
                }
                break;
            }
            bytes += id.len();
            page.push(id.clone());
        }
        Ok(page)
    }

    pub(super) fn reconcile_live_id(&mut self, collection: &str, id: &str) {
        let exists = self
            .states
            .get(collection)
            .is_some_and(|state| state.row_exists(collection, id));
        if exists {
            if !self.live_ids.contains_key(collection) {
                let key = collection.to_owned();
                self.live_id_bytes = self
                    .live_id_bytes
                    .saturating_add(key.capacity())
                    .saturating_add(TREE_ENTRY_BYTES);
                self.live_ids.insert(key, BTreeSet::new());
            }
            let Some(ids) = self.live_ids.get_mut(collection) else {
                return;
            };
            if !ids.contains(id) {
                let owned = id.to_owned();
                self.live_id_bytes = self
                    .live_id_bytes
                    .saturating_add(owned.capacity())
                    .saturating_add(TREE_ENTRY_BYTES);
                ids.insert(owned);
            }
        } else if let Some(ids) = self.live_ids.get_mut(collection) {
            if let Some(removed) = ids.take(id) {
                self.live_id_bytes = self
                    .live_id_bytes
                    .saturating_sub(removed.capacity().saturating_add(TREE_ENTRY_BYTES));
            }
            if ids.is_empty() {
                drop(self.remove_live_collection(collection));
            }
        }
    }

    fn remove_live_collection(&mut self, collection: &str) -> Option<BTreeSet<String>> {
        if let Some((key, ids)) = self.live_ids.remove_entry(collection) {
            let bytes = ids.iter().fold(
                key.capacity().saturating_add(TREE_ENTRY_BYTES),
                |bytes, id| {
                    bytes
                        .saturating_add(id.capacity())
                        .saturating_add(TREE_ENTRY_BYTES)
                },
            );
            self.live_id_bytes = self.live_id_bytes.saturating_sub(bytes);
            Some(ids)
        } else {
            None
        }
    }

    /// Cold restore and exceptional partial imports enumerate once.
    /// Startup retains O(collection size) transient row-ID allocation.
    pub(super) fn reconcile_live_collection(&mut self, collection: &str) {
        let ids = self
            .states
            .get(collection)
            .map(|state| state.row_ids(collection))
            .unwrap_or_default();
        let previous = self.remove_live_collection(collection).unwrap_or_default();
        if let Some(catalog) = &self.indexes {
            catalog.resync(
                collection,
                previous.iter().chain(ids.iter()).map(String::as_str),
                |id| {
                    self.read(collection, id)
                        .map(|row| crate::index::document::row_value(&row))
                },
            );
        }
        for id in ids {
            // Consume the existing enumeration instead of cloning it again.
            if self
                .states
                .get(collection)
                .is_some_and(|state| state.row_exists(collection, &id))
            {
                if !self.live_ids.contains_key(collection) {
                    let key = collection.to_owned();
                    self.live_id_bytes = self
                        .live_id_bytes
                        .saturating_add(key.capacity())
                        .saturating_add(TREE_ENTRY_BYTES);
                    self.live_ids.insert(key, BTreeSet::new());
                }
                if let Some(retained) = self.live_ids.get_mut(collection) {
                    self.live_id_bytes = self
                        .live_id_bytes
                        .saturating_add(id.capacity())
                        .saturating_add(TREE_ENTRY_BYTES);
                    retained.insert(id);
                }
            }
        }
    }

    pub(super) fn reconcile_affected(&mut self, collection: &str, affected: AffectedRows<'_>) {
        match affected {
            AffectedRows::One(id) => self.sync_indexes(collection, [id]),
            AffectedRows::Collection => self.reconcile_live_collection(collection),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::CrdtEngine;
    use crate::engine::crdt::engine::rollback::Rollback;
    use crate::engine::crdt::engine::types::CrdtRowWrite;
    use crate::error::LiteError;
    use loro::LoroValue;

    fn fields() -> Vec<(&'static str, LoroValue)> {
        vec![("value", LoroValue::I64(1))]
    }

    fn ids(engine: &CrdtEngine) -> Vec<String> {
        engine.live_ids_page("docs", None, 100, 1000).unwrap()
    }

    #[test]
    fn live_ids_follow_scalar_deferred_and_mixed_writes() {
        let mut engine = CrdtEngine::new(1).unwrap();
        let fields = fields();
        engine.upsert("docs", "*", &fields).unwrap();
        engine.set_fields("docs", "*", &fields).unwrap();
        engine.upsert_deferred("docs", "b", &fields).unwrap();
        engine.set_fields_deferred("docs", "c", &fields).unwrap();
        engine
            .batch_write(&[
                (CrdtRowWrite::Upsert, "docs", "d", &fields),
                (CrdtRowWrite::SetFields, "docs", "e", &fields),
            ])
            .unwrap();
        assert_eq!(ids(&engine), ["*", "b", "c", "d", "e"]);
        engine.delete_deferred("docs", "b").unwrap();
        engine.delete("docs", "*").unwrap();
        engine.flush_deltas().unwrap();
        assert_eq!(ids(&engine), ["c", "d", "e"]);
        engine.clear_collection("docs").unwrap();
        assert!(ids(&engine).is_empty());
        assert_eq!(engine.live_id_bytes, 0);
    }

    #[test]
    fn live_ids_reconcile_state_after_operation_error() {
        let mut engine = CrdtEngine::new(1).unwrap();
        let result: Result<((), u64), LiteError> =
            engine.with_delta_capture("docs", "*", "write", Rollback::Keep, |state| {
                state.upsert("docs", "*", &fields()).unwrap();
                Err(LiteError::Storage {
                    detail: "injected operation error".into(),
                })
            });
        assert!(
            matches!(result, Err(LiteError::Storage { detail }) if detail == "injected operation error")
        );
        assert_eq!(ids(&engine), ["*"]);
        assert_eq!(
            engine.pending_count(),
            1,
            "the write committed before the error reaches the sync queue"
        );
        assert!(engine.live_id_bytes > 0);
        engine.delete("docs", "*").unwrap();
        assert_eq!(engine.live_id_bytes, 0);
    }

    #[test]
    fn live_ids_page_obeys_count_bytes_and_exclusive_cursor() {
        let mut engine = CrdtEngine::new(1).unwrap();
        for id in ["a", "bb", "ccc", "dddd"] {
            engine.upsert("docs", id, &fields()).unwrap();
        }
        engine.upsert("other", "isolated", &fields()).unwrap();
        assert!(engine.live_ids_page("docs", None, 0, 0).unwrap().is_empty());
        assert_eq!(
            engine.live_ids_page("docs", None, 2, 100).unwrap(),
            ["a", "bb"]
        );
        assert_eq!(
            engine.live_ids_page("docs", None, 100, 3).unwrap(),
            ["a", "bb"]
        );
        assert_eq!(
            engine.live_ids_page("docs", Some("bb"), 100, 3).unwrap(),
            ["ccc"]
        );
        assert_eq!(
            engine.live_ids_page("docs", Some("ccc"), 1, 4).unwrap(),
            ["dddd"]
        );
        assert!(
            engine
                .live_ids_page("docs", Some("dddd"), 1, 4)
                .unwrap()
                .is_empty()
        );
        assert!(matches!(
            engine.live_ids_page("docs", Some("bb"), 1, 2),
            Err(LiteError::Backpressure { .. })
        ));
        assert!(
            engine
                .live_ids_page("absent", None, 10, 0)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn live_ids_restore_and_follow_tracked_imports_and_rotation() {
        let mut source = CrdtEngine::new(1).unwrap();
        source.upsert("docs", "a", &fields()).unwrap();
        let snapshot = source.export_snapshot("docs").unwrap();
        let mut restored = CrdtEngine::from_snapshot(2, "docs", &snapshot).unwrap();
        assert_eq!(ids(&restored), ["a"]);
        source.upsert("docs", "b", &fields()).unwrap();
        let delta = &source.pending_deltas().last().unwrap().delta_bytes;
        restored.import_local_tracked("docs", delta).unwrap();
        assert_eq!(ids(&restored), ["a", "b"]);
        source.delete("docs", "a").unwrap();
        let delta = &source.pending_deltas().last().unwrap().delta_bytes;
        restored.import_remote("docs", delta).unwrap();
        assert_eq!(ids(&restored), ["b"]);
        restored.rotate_peer_id(3).unwrap();
        assert_eq!(ids(&restored), ["b"]);
        let mutation = restored.pending_deltas()[0].mutation_id;
        restored.reject_delta(mutation);
        assert!(ids(&restored).is_empty());
        assert_eq!(restored.live_id_bytes, 0);
    }

    #[test]
    fn live_id_memory_is_released_by_collection_clear() {
        let mut engine = CrdtEngine::new(1).unwrap();
        engine.upsert("docs", "a", &fields()).unwrap();
        let accounted = engine.live_id_bytes;
        engine.upsert("other", "b", &fields()).unwrap();
        assert!(engine.estimated_memory_bytes() >= engine.live_id_bytes);
        engine.clear_collection("other").unwrap();
        assert_eq!(engine.live_id_bytes, accounted);
        engine.clear_collection("docs").unwrap();
        assert_eq!(engine.live_id_bytes, 0);
        assert!(engine.live_ids.is_empty());
    }

    #[test]
    fn live_ids_follow_list_edits_and_policy_rejection() {
        let mut engine = CrdtEngine::new(1).unwrap();
        engine.upsert("docs", "*", &fields()).unwrap();
        engine
            .list_insert("docs", "*", "blocks", 0, &sonic_rs::json!({"text": "a"}))
            .unwrap();
        engine
            .list_insert("docs", "*", "blocks", 1, &sonic_rs::json!({"text": "b"}))
            .unwrap();
        engine.list_move("docs", "*", "blocks", 0, 1).unwrap();
        engine.list_delete("docs", "*", "blocks", 0).unwrap();
        assert_eq!(ids(&engine), ["*"]);
        let mutation = engine.pending_deltas().last().unwrap().mutation_id;
        engine.reject_delta_with_policy(
            mutation,
            &nodedb_types::sync::compensation::CompensationHint::IntegrityViolation,
        );
        assert!(ids(&engine).is_empty());
        assert_eq!(engine.live_id_bytes, 0);
    }
}
