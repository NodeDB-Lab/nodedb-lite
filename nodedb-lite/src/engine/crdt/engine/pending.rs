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
        self.unpersisted_deltas.clear();
    }

    /// Mark a queue entry as not matching its stored form, stamping it with a
    /// fresh revision.
    ///
    /// Every path that adds an entry or edits one in place goes through here,
    /// so an edit that lands while a flush is committing is distinguishable
    /// from the state that flush actually wrote.
    pub(in crate::engine::crdt) fn mark_delta_unpersisted(&mut self, mutation_id: u64) {
        self.delta_revision += 1;
        self.unpersisted_deltas
            .insert(mutation_id, self.delta_revision);
    }

    /// The pending deltas whose stored form may not match the queue, each with
    /// the revision to report back once it is durable.
    ///
    /// Entries already written under their own key are absent: the queue is
    /// append-only, so an unchanged entry does not need rewriting. Report the
    /// write back with [`Self::mark_pending_deltas_persisted`] once it has
    /// committed, passing the revision handed out here — not the entry's
    /// current one, which may have moved on since.
    pub fn pending_deltas_needing_write(&self) -> impl Iterator<Item = (&PendingDelta, u64)> {
        self.pending_deltas.iter().filter_map(|d| {
            self.unpersisted_deltas
                .get(&d.mutation_id)
                .map(|&revision| (d, revision))
        })
    }

    /// Number of queue entries written and acknowledged durable since this
    /// engine was created.
    pub fn pending_delta_write_count(&self) -> u64 {
        self.delta_writes
    }

    /// Whether any pending delta needs writing.
    pub fn has_unpersisted_deltas(&self) -> bool {
        !self.unpersisted_deltas.is_empty()
    }

    /// Retire the dirty marks for queue entries that are now durable.
    ///
    /// Each `(mutation_id, revision)` pair must be one handed out by
    /// [`Self::pending_deltas_needing_write`] for the batch that has just
    /// committed. An entry whose revision has moved on since was added or
    /// edited while that batch was in flight and so was never in it; its mark
    /// stays, and the next flush writes it.
    ///
    /// Call only after the batch has committed.
    pub fn mark_pending_deltas_persisted(&mut self, written: impl IntoIterator<Item = (u64, u64)>) {
        for (mutation_id, revision) in written {
            if self.unpersisted_deltas.get(&mutation_id) == Some(&revision) {
                self.unpersisted_deltas.remove(&mutation_id);
                self.delta_writes += 1;
            }
        }
    }

    /// Drop a single pending delta by `mutation_id` without touching CRDT state.
    ///
    /// Unlike [`reject_delta`](Self::reject_delta), this does **not** delete the
    /// document — the row stays in local CRDT state (so local reads/search work);
    /// it is simply never pushed to Origin. Used to keep a document local-only
    /// when the host's `SyncGate` rejects it for sync.
    pub fn drop_pending(&mut self, mutation_id: u64) {
        self.pending_deltas.retain(|d| d.mutation_id != mutation_id);
        self.unpersisted_deltas.remove(&mutation_id);
    }

    /// Assign a stable stream seq to a pending delta the first time it is sent.
    ///
    /// If the delta already has a non-zero seq (assigned on a previous send)
    /// the call is a no-op — the existing seq is reused on reconnect re-sends
    /// so Origin can deduplicate rather than double-apply.
    pub fn set_pending_delta_seq(&mut self, mutation_id: u64, seq: u64) {
        let assigned = match self
            .pending_deltas
            .iter_mut()
            .find(|d| d.mutation_id == mutation_id)
        {
            Some(d) if d.seq == 0 => {
                d.seq = seq;
                true
            }
            _ => false,
        };
        if assigned {
            // The stored entry now carries a stale seq.
            self.mark_delta_unpersisted(mutation_id);
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
        self.unpersisted_deltas.remove(&acked_id);
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
            self.unpersisted_deltas.remove(&mutation_id);
            // Best-effort rollback: delete the affected document from its own
            // collection's document. The application should handle the
            // CompensationHint and re-create with corrected values.
            if let Some(state) = self.states.get(&delta.collection) {
                let _ = state.delete(&delta.collection, &delta.document_id);
            }
            Some(delta)
        } else {
            None
        }
    }
    // ─── Vector Clock ────────────────────────────────────────────────

    /// Export the current vector clock as a serializable map.
    ///
    /// Format: `{ peer_id_hex: counter }` — matches the Loro version vector.
    ///
    /// Each collection owns its own document (and its own derived peer ID), so
    /// the returned clock is the merge of every collection's version vector.
    /// Peer IDs are per-collection-derived and therefore disjoint, but the
    /// merge takes the maximum counter so an id shared with a remote peer is
    /// never regressed.
    pub fn export_vector_clock(&self) -> HashMap<String, u64> {
        let mut clock: HashMap<String, u64> = HashMap::new();
        // Loro's VersionVector maps PeerID → Counter.
        // We encode PeerID as hex string for wire compatibility.
        for state in self.states.values() {
            for (peer, counter) in state.oplog_version_vector().iter() {
                let entry = clock.entry(format!("{peer:016x}")).or_insert(0);
                *entry = (*entry).max(*counter as u64);
            }
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
