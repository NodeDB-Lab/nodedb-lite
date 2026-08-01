// SPDX-License-Identifier: BUSL-1.1

//! Engine construction, per-collection state access, snapshot import/export,
//! and history compaction.

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::btree_map::Entry;
use std::sync::atomic::AtomicU64;

use nodedb_crdt::{CrdtState, ImportAdmission};

use crate::error::LiteError;

use super::types::CrdtEngine;

/// Warn when an import carried operations but contributed none of them.
///
/// Loro trims operations the importing document already knows, so a fully
/// trimmed import returns `Ok` while changing nothing. That is normal for an
/// idempotent resync of a replayed prefix, but it is also exactly what a
/// peer-id collision looks like when it silently discards a healthy client's
/// writes — so it must at least be visible.
fn warn_if_fully_trimmed(collection: &str, kind: &str, admission: &ImportAdmission) {
    if admission.encoded_operations > 0 && admission.new_operations == 0 {
        tracing::warn!(
            collection,
            kind,
            encoded_operations = admission.encoded_operations,
            "CRDT import contributed no operations — every operation was already \
             known. Expected for an idempotent resync; otherwise it indicates a \
             peer-id collision silently discarding writes."
        );
    }
}

impl CrdtEngine {
    /// Create a new empty CRDT engine with the given peer ID.
    pub fn new(peer_id: u64) -> Result<Self, LiteError> {
        Ok(Self {
            peer_id,
            states: BTreeMap::new(),
            next_mutation_id: AtomicU64::new(1),
            pending_deltas: Vec::new(),
            acked_versions: HashMap::new(),
            policies: nodedb_crdt::PolicyRegistry::new(),
            registered_collections: std::collections::HashSet::new(),
            deferred: Vec::new(),
            flushed_versions: HashMap::new(),
            checkpoint_bytes: HashMap::new(),
            delta_bytes: HashMap::new(),
            next_delta_seq: HashMap::new(),
            snapshot_exports: AtomicU64::new(0),
        })
    }

    /// Restore a single collection from a Loro snapshot (cold start).
    pub fn from_snapshot(
        peer_id: u64,
        collection: &str,
        snapshot: &[u8],
    ) -> Result<Self, LiteError> {
        let mut engine = Self::new(peer_id)?;
        engine.import_snapshot(collection, snapshot)?;
        Ok(engine)
    }

    /// Derive a collection's Loro peer ID from this device's base peer ID.
    ///
    /// Loro operation identity is `(peer_id, counter)` and every document
    /// counts its own counter from zero. Handing each collection's document
    /// the same peer ID verbatim therefore mints identical operation IDs for
    /// unrelated writes in different collections; anything that later merges
    /// two of those collections into one document sees the second operation as
    /// a replay of the first and silently drops a row.
    ///
    /// The derivation is a pure function of `(peer_id, collection)` so both
    /// ends of a sync session compute the same ID for the same collection.
    /// Zero is avoided because Loro reads it as "unset".
    pub(in crate::engine::crdt) fn collection_peer_id(peer_id: u64, collection: &str) -> u64 {
        const FNV_OFFSET_BASIS: u64 = 14695981039346656037;
        const FNV_PRIME: u64 = 1099511628211;

        let mut hash = FNV_OFFSET_BASIS;
        for byte in peer_id.to_le_bytes() {
            hash ^= byte as u64;
            hash = hash.wrapping_mul(FNV_PRIME);
        }
        for byte in collection.as_bytes() {
            hash ^= *byte as u64;
            hash = hash.wrapping_mul(FNV_PRIME);
        }
        // Loro reserves the top bit of a peer ID, and 0 reads as "unset".
        let id = hash & ((1u64 << 63) - 1);
        if id == 0 { 1 } else { id }
    }

    /// Get this collection's document, creating an empty one if absent.
    pub(in crate::engine::crdt) fn state_mut(
        &mut self,
        collection: &str,
    ) -> Result<&mut CrdtState, LiteError> {
        let peer_id = Self::collection_peer_id(self.peer_id, collection);
        match self.states.entry(collection.to_string()) {
            Entry::Occupied(e) => Ok(e.into_mut()),
            Entry::Vacant(e) => {
                let state = CrdtState::new(peer_id).map_err(|err| LiteError::Storage {
                    detail: format!("failed to create CrdtState for '{collection}': {err}"),
                })?;
                Ok(e.insert(state))
            }
        }
    }

    /// This device's base peer ID.
    pub fn peer_id(&self) -> u64 {
        self.peer_id
    }

    /// Import remote deltas for a collection (received via sync).
    ///
    /// Returns the [`ImportAdmission`] so callers can tell "this delta advanced
    /// the document" from "every operation in it was already known". The two are
    /// indistinguishable from a bare `Ok(())`: Loro trims operations the
    /// document already has, so a fully-trimmed import succeeds while
    /// contributing nothing — which is also what a peer-id collision looks like
    /// when it discards a healthy client's writes.
    pub fn import_remote(
        &mut self,
        collection: &str,
        data: &[u8],
    ) -> Result<ImportAdmission, LiteError> {
        let admission =
            self.state_mut(collection)?
                .import(data)
                .map_err(|e| LiteError::Storage {
                    detail: format!("remote delta import for '{collection}' failed: {e}"),
                })?;
        warn_if_fully_trimmed(collection, "remote delta", &admission);
        Ok(admission)
    }

    // ─── Snapshot & Persistence ──────────────────────────────────────

    /// Import a full Loro snapshot this device wrote itself — a collection
    /// restored from durable storage at cold start.
    ///
    /// Admitted as local. The size ceilings on [`Self::import_remote`] bound
    /// how much work an untrusted peer may cause; applied to a store's own
    /// snapshot they instead cap how large a document this device may reload
    /// after writing it, and the export side has no such bound. A store that
    /// grew past the ceiling by succeeding at writes would refuse to open, with
    /// no way to recover from inside the library — raising one limit only moves
    /// the wall to the next. Every structural check still runs: authenticated
    /// metadata, per-peer ranges that do not regress, pending dependencies.
    ///
    /// Peer snapshots do not come through here — sync routes them to
    /// [`Self::import_remote`], which stays capped.
    ///
    /// See [`Self::import_remote`] for why the admission is returned rather
    /// than discarded.
    pub fn import_snapshot(
        &mut self,
        collection: &str,
        snapshot: &[u8],
    ) -> Result<ImportAdmission, LiteError> {
        let admission = self
            .state_mut(collection)?
            .import_local(snapshot)
            .map_err(|e| LiteError::Storage {
                detail: format!("snapshot import for '{collection}' failed: {e}"),
            })?;
        warn_if_fully_trimmed(collection, "snapshot", &admission);
        Ok(admission)
    }

    /// Export a full Loro state snapshot for one collection.
    ///
    /// Returns an empty vector when the collection has no document yet.
    pub fn export_snapshot(&self, collection: &str) -> Result<Vec<u8>, LiteError> {
        let Some(state) = self.states.get(collection) else {
            return Ok(Vec::new());
        };
        self.export_one(collection, state)
    }

    /// Export every collection's snapshot as `(collection, snapshot_bytes)`,
    /// in deterministic collection order.
    pub fn export_all_snapshots(&self) -> Result<Vec<(String, Vec<u8>)>, LiteError> {
        let mut out = Vec::with_capacity(self.states.len());
        for (collection, state) in &self.states {
            let bytes = self.export_one(collection, state)?;
            out.push((collection.clone(), bytes));
        }
        Ok(out)
    }

    /// Number of full snapshot exports performed since this engine was created.
    pub fn snapshot_export_count(&self) -> u64 {
        self.snapshot_exports
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    pub(in crate::engine::crdt) fn export_one(
        &self,
        collection: &str,
        state: &CrdtState,
    ) -> Result<Vec<u8>, LiteError> {
        self.snapshot_exports
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        state.export_snapshot().map_err(|e| LiteError::Storage {
            detail: format!("snapshot export for '{collection}' failed: {e}"),
        })
    }

    /// Compact Loro history on every collection to prevent unbounded growth.
    ///
    /// Replaces each internal LoroDoc with a shallow snapshot. Historical
    /// operations are discarded. Current state is fully preserved.
    pub fn compact_history(&mut self) -> Result<(), LiteError> {
        for (collection, state) in &mut self.states {
            state.compact_history().map_err(|e| LiteError::Storage {
                detail: format!("history compaction for '{collection}' failed: {e}"),
            })?;
        }
        // Compaction rewrites the document without advancing its frontier, so
        // neither the persisted base nor the updates on top of it describe the
        // document any more, and the discarded history means an update export
        // from the old frontier may not even be possible. Dropping both marks
        // forces a fresh checkpoint, which also deletes the stale updates —
        // `next_delta_seq` is deliberately kept, since it is the count of the
        // entries that checkpoint has to delete.
        self.flushed_versions.clear();
        self.checkpoint_bytes.clear();
        self.delta_bytes.clear();
        Ok(())
    }

    /// Estimated memory usage in bytes across all collections.
    pub fn estimated_memory_bytes(&self) -> usize {
        let state_bytes: usize = self
            .states
            .values()
            .map(|s| s.estimated_memory_bytes())
            .sum();
        let delta_bytes: usize = self
            .pending_deltas
            .iter()
            .map(|d| d.delta_bytes.len())
            .sum();
        state_bytes + delta_bytes
    }

    /// Access a collection's underlying `CrdtState` for advanced operations.
    pub fn state(&self, collection: &str) -> Option<&CrdtState> {
        self.states.get(collection)
    }

    // ─── Version-History Operations ──────────────────────────────────

    /// Export a collection's oplog delta from a specific version to its
    /// current state.
    ///
    /// Returns the Loro update bytes that transform `from_version` into the
    /// current oplog state, or an empty vector when the collection has no
    /// document. Used by `ExportDelta`.
    pub fn export_delta_from(
        &self,
        collection: &str,
        from_version: &loro::VersionVector,
    ) -> Result<Vec<u8>, LiteError> {
        let Some(state) = self.states.get(collection) else {
            return Ok(Vec::new());
        };
        state
            .export_updates_since(from_version)
            .map_err(|e| LiteError::Storage {
                detail: format!("export_delta_from '{collection}': {e}"),
            })
    }

    /// Compact a collection's history at a specific version, discarding oplog
    /// entries before it.
    ///
    /// The current state and all versions after the target are preserved.
    /// Used by `CompactAtVersion`. A collection with no document is a no-op.
    pub fn compact_at_version(
        &mut self,
        collection: &str,
        version: &loro::VersionVector,
    ) -> Result<(), LiteError> {
        let Some(state) = self.states.get_mut(collection) else {
            return Ok(());
        };
        state
            .compact_at_version(version)
            .map_err(|e| LiteError::Storage {
                detail: format!("compact_at_version '{collection}': {e}"),
            })?;
        // See `compact_history`: the frontier is unchanged but the exported
        // bytes are not, so the collection must be re-persisted — as a fresh
        // checkpoint, since the updates on top of the old base no longer
        // describe this document.
        self.flushed_versions.remove(collection);
        self.checkpoint_bytes.remove(collection);
        self.delta_bytes.remove(collection);
        Ok(())
    }
}
