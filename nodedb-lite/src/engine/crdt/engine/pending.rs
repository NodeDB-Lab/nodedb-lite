// SPDX-License-Identifier: BUSL-1.1

//! Pending-delta queue management, acknowledgement, rejection, and the
//! local vector clock.

use std::collections::HashMap;

use super::types::{CrdtEngine, PendingDelta};

impl CrdtEngine {
    // ─── Sync: Delta Management ──────────────────────────────────────

    /// Get all pending (unsent) deltas.
    pub fn pending_deltas(&self) -> &[PendingDelta] {
        &self.pending_deltas
    }

    /// Number of unsent deltas.
    pub fn pending_count(&self) -> usize {
        self.pending_deltas.len()
    }

    /// Clear all pending deltas (used for partial flush recovery).
    /// The CRDT state is authoritative — pending deltas are regenerated on next mutation.
    pub fn clear_pending_deltas(&mut self) {
        self.pending_deltas.clear();
    }

    /// Drop a single pending delta by `mutation_id` without touching CRDT state.
    ///
    /// Unlike [`reject_delta`](Self::reject_delta), this does **not** delete the
    /// document — the row stays in local CRDT state (so local reads/search work);
    /// it is simply never pushed to Origin. Used to keep a document local-only
    /// when the host's `SyncGate` rejects it for sync.
    pub fn drop_pending(&mut self, mutation_id: u64) {
        self.pending_deltas.retain(|d| d.mutation_id != mutation_id);
    }

    /// Assign a stable stream seq to a pending delta the first time it is sent.
    ///
    /// If the delta already has a non-zero seq (assigned on a previous send)
    /// the call is a no-op — the existing seq is reused on reconnect re-sends
    /// so Origin can deduplicate rather than double-apply.
    pub fn set_pending_delta_seq(&mut self, mutation_id: u64, seq: u64) {
        if let Some(d) = self
            .pending_deltas
            .iter_mut()
            .find(|d| d.mutation_id == mutation_id)
            && d.seq == 0
        {
            d.seq = seq;
        }
    }

    /// Retire the single delta Origin acknowledged (after DeltaAck received).
    ///
    /// Acks are per-mutation and are not ordered: an ack for a later mutation
    /// can arrive before one for an earlier mutation, and a non-applied status
    /// never produces an ack at all. Retiring the whole range at or below
    /// `acked_id` would therefore discard deltas Origin never acknowledged —
    /// one ack silently dropping the entire backlog behind it. Only the
    /// acknowledged mutation is removed; the rest stay queued until their own
    /// acks arrive.
    pub fn acknowledge(&mut self, acked_id: u64) {
        self.pending_deltas.retain(|d| d.mutation_id != acked_id);
    }

    /// Roll back a specific pending delta (after DeltaReject with CompensationHint).
    ///
    /// This is a best-effort operation — Loro CRDTs don't support true undo.
    /// For document mutations, we delete the affected row and let the
    /// application re-create it with corrected values.
    ///
    /// Returns the rejected delta if found.
    pub fn reject_delta(&mut self, mutation_id: u64) -> Option<PendingDelta> {
        if let Some(pos) = self
            .pending_deltas
            .iter()
            .position(|d| d.mutation_id == mutation_id)
        {
            let delta = self.pending_deltas.remove(pos);
            // Best-effort rollback: delete the affected document.
            // The application should handle the CompensationHint and
            // re-create with corrected values.
            let _ = self.state.delete(&delta.collection, &delta.document_id);
            Some(delta)
        } else {
            None
        }
    }
    // ─── Vector Clock ────────────────────────────────────────────────

    /// Export the current vector clock as a serializable map.
    ///
    /// Format: `{ peer_id_hex: counter }` — matches the Loro version vector.
    pub fn export_vector_clock(&self) -> HashMap<String, u64> {
        let vv = self.state.oplog_version_vector();
        let mut clock = HashMap::new();
        // Loro's VersionVector maps PeerID → Counter.
        // We encode PeerID as hex string for wire compatibility.
        for (peer, counter) in vv.iter() {
            clock.insert(format!("{peer:016x}"), *counter as u64);
        }
        clock
    }

    /// Set the acked version for a collection (after sync handshake).
    pub fn set_acked_version(&mut self, collection: &str, version: u64) {
        self.acked_versions.insert(collection.to_string(), version);
    }

    /// Get the acked version for a collection.
    pub fn acked_version(&self, collection: &str) -> u64 {
        self.acked_versions.get(collection).copied().unwrap_or(0)
    }
}
