// SPDX-License-Identifier: BUSL-1.1

//! Serialization of pending deltas and storage-key construction.

use std::sync::atomic::Ordering;

use super::types::{CrdtEngine, DELTA_KEY_PREFIX, PendingDelta, SNAPSHOT_KEY, VCLOCK_KEY};

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
    pub fn restore_pending_deltas_incremental(&mut self, entries: &[(Vec<u8>, Vec<u8>)]) {
        let mut deltas = Vec::with_capacity(entries.len());
        for (_key, value) in entries {
            match zerompk::from_msgpack::<PendingDelta>(value) {
                Ok(delta) => deltas.push(delta),
                Err(e) => {
                    tracing::warn!(error = %e, "skipping corrupted pending delta entry");
                }
            }
        }
        // Sort by mutation_id to ensure ordering.
        deltas.sort_by_key(|d| d.mutation_id);

        if let Some(max_id) = deltas.iter().map(|d| d.mutation_id).max() {
            self.next_mutation_id.store(max_id + 1, Ordering::Relaxed);
        }
        self.pending_deltas = deltas;
    }

    /// Key for storing the Loro snapshot in `StorageEngine`.
    pub fn snapshot_key() -> &'static [u8] {
        SNAPSHOT_KEY
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
