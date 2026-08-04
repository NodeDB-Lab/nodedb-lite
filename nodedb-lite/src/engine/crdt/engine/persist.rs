// SPDX-License-Identifier: BUSL-1.1

//! Serialization of pending deltas and storage-key construction.

use std::sync::atomic::Ordering;

use super::types::{
    CrdtEngine, DELTA_KEY_PREFIX, PendingDelta, SNAPSHOT_KEY, STATE_DELTA_KEY, VCLOCK_KEY,
};

impl CrdtEngine {
    // ─── Persistence Helpers ─────────────────────────────────────────

    /// Serialize pending deltas to bytes for StorageEngine persistence.
    pub fn serialize_pending_deltas(&self) -> Result<Vec<u8>, crate::error::LiteError> {
        zerompk::to_msgpack_vec(&self.pending_deltas).map_err(|e| {
            crate::error::LiteError::Serialization {
                detail: format!("pending deltas: {e}"),
            }
        })
    }

    /// Restore pending deltas from bytes (cold start).
    pub fn restore_pending_deltas(&mut self, bytes: &[u8]) {
        match zerompk::from_msgpack::<Vec<PendingDelta>>(bytes) {
            Ok(deltas) => {
                // Advance mutation ID counter past any restored deltas.
                let max_id = deltas.iter().map(|d| d.mutation_id).max().unwrap_or(0);
                self.next_mutation_id.store(max_id + 1, Ordering::Relaxed);
                // The bulk blob is the only copy these came from, so none of
                // them is stored under its own key yet.
                self.unpersisted_deltas.clear();
                for delta in &deltas {
                    self.mark_delta_unpersisted(delta.mutation_id);
                }
                self.pending_deltas = deltas;
            }
            Err(e) => {
                tracing::warn!(error = %e, "failed to restore pending deltas, continuing with empty state");
            }
        }
    }

    /// Serialize a single pending delta to bytes (for append-only persistence).
    pub fn serialize_delta(delta: &PendingDelta) -> Result<Vec<u8>, crate::error::LiteError> {
        zerompk::to_msgpack_vec(delta).map_err(|e| crate::error::LiteError::Serialization {
            detail: format!("pending delta: {e}"),
        })
    }

    /// Build the KV key for a single pending delta: `delta:{mutation_id:016x}`.
    /// Zero-padded hex ensures lexicographic ordering matches numeric ordering.
    pub fn delta_storage_key(mutation_id: u64) -> Vec<u8> {
        format!("delta:{mutation_id:016x}").into_bytes()
    }

    /// Restore pending deltas from individual KV entries (append-only format).
    ///
    /// Each entry is stored under `Namespace::Crdt` with key `delta:{mutation_id:016x}`.
    /// Falls back to legacy bulk restore if no individual entries found.
    ///
    /// A queued delta is a local mutation no Origin has acknowledged yet, so an
    /// entry that will not decode is unrecoverable data, not noise to step
    /// over. `allow_discard` reflects the caller's corruption policy: when it
    /// is false the undecodable entry is reported and every entry is left in
    /// storage untouched.
    pub fn restore_pending_deltas_incremental(
        &mut self,
        entries: &[(Vec<u8>, Vec<u8>)],
        allow_discard: bool,
    ) -> Result<(), crate::error::LiteError> {
        let mut deltas = Vec::with_capacity(entries.len());
        for (_key, value) in entries {
            match zerompk::from_msgpack::<PendingDelta>(value) {
                Ok(delta) => deltas.push(delta),
                Err(e) => {
                    if !allow_discard {
                        return Err(crate::error::LiteError::Corrupted {
                            detail: format!(
                                "queued CRDT mutation failed to decode: {e}. It carries a local \
                                 write that has not reached Origin, and has been left in place."
                            ),
                        });
                    }
                    tracing::warn!(error = %e, "skipping corrupted pending delta entry");
                }
            }
        }
        // Sort by mutation_id to ensure ordering.
        deltas.sort_by_key(|d| d.mutation_id);

        if let Some(max_id) = deltas.iter().map(|d| d.mutation_id).max() {
            self.next_mutation_id.store(max_id + 1, Ordering::Relaxed);
        }
        // Every one of these was just read from its own key, so the stored
        // form matches by construction.
        self.unpersisted_deltas.clear();
        self.pending_deltas = deltas;
        Ok(())
    }

    /// Key for storing one collection's Loro snapshot in `StorageEngine`:
    /// `loro_snapshot:<collection>`.
    pub fn snapshot_key_for(collection: &str) -> Vec<u8> {
        let mut key = Vec::with_capacity(SNAPSHOT_KEY.len() + collection.len());
        key.extend_from_slice(SNAPSHOT_KEY);
        key.extend_from_slice(collection.as_bytes());
        key
    }

    /// Prefix shared by every per-collection snapshot key, for prefix scans.
    pub fn snapshot_key_prefix() -> &'static [u8] {
        SNAPSHOT_KEY
    }

    /// Key for one incremental update on top of a collection's snapshot:
    /// `loro_delta:<collection>:<seq:016x>`.
    ///
    /// Zero-padded hex so lexicographic key order is replay order, which is
    /// what a prefix scan returns them in.
    pub fn state_delta_key_for(collection: &str, seq: u64) -> Vec<u8> {
        let mut key = Vec::with_capacity(STATE_DELTA_KEY.len() + collection.len() + 17);
        key.extend_from_slice(STATE_DELTA_KEY);
        key.extend_from_slice(collection.as_bytes());
        key.extend_from_slice(format!(":{seq:016x}").as_bytes());
        key
    }

    /// Prefix shared by every state-update key, for prefix scans.
    pub fn state_delta_key_prefix() -> &'static [u8] {
        STATE_DELTA_KEY
    }

    /// Recover `(collection, seq)` from a key produced by
    /// [`Self::state_delta_key_for`].
    ///
    /// Returns `None` for keys that do not carry the prefix, whose collection
    /// is not UTF-8, or whose sequence does not parse — such an entry cannot be
    /// routed or ordered, so the caller must skip it rather than guess.
    pub fn state_delta_from_key(key: &[u8]) -> Option<(&str, u64)> {
        let suffix = std::str::from_utf8(key.strip_prefix(STATE_DELTA_KEY)?).ok()?;
        let (collection, seq) = suffix.rsplit_once(':')?;
        if collection.is_empty() {
            return None;
        }
        Some((collection, u64::from_str_radix(seq, 16).ok()?))
    }

    /// Recover the collection name from a snapshot key produced by
    /// [`Self::snapshot_key_for`].
    ///
    /// Returns `None` for keys that do not carry the prefix or whose suffix is
    /// not UTF-8 — such an entry cannot be routed to a document, so the caller
    /// must skip it rather than guess a collection.
    pub fn collection_from_snapshot_key(key: &[u8]) -> Option<&str> {
        let suffix = key.strip_prefix(SNAPSHOT_KEY)?;
        std::str::from_utf8(suffix).ok().filter(|s| !s.is_empty())
    }

    /// Key for storing pending deltas in `StorageEngine`.
    pub fn delta_key() -> &'static [u8] {
        DELTA_KEY_PREFIX
    }

    /// Key for storing the vector clock in `StorageEngine`.
    pub fn vclock_key() -> &'static [u8] {
        VCLOCK_KEY
    }
}
