// SPDX-License-Identifier: BUSL-1.1

//! Engine construction, snapshot export, history compaction, and
//! direct access to the underlying CRDT state.

use std::collections::HashMap;
use std::sync::atomic::AtomicU64;

use nodedb_crdt::CrdtState;

use crate::error::LiteError;

use super::types::CrdtEngine;

impl CrdtEngine {
    /// Create a new empty CRDT engine with the given peer ID.
    pub fn new(peer_id: u64) -> Result<Self, LiteError> {
        let state = CrdtState::new(peer_id).map_err(|e| LiteError::Storage {
            detail: format!("failed to create CrdtState: {e}"),
        })?;

        Ok(Self {
            state,
            next_mutation_id: AtomicU64::new(1),
            pending_deltas: Vec::new(),
            acked_versions: HashMap::new(),
            policies: nodedb_crdt::PolicyRegistry::new(),
            registered_collections: std::collections::HashSet::new(),
            deferred_version: None,
            deferred_count: 0,
        })
    }

    /// Restore from a Loro snapshot (cold start).
    pub fn from_snapshot(peer_id: u64, snapshot: &[u8]) -> Result<Self, LiteError> {
        let state = CrdtState::new(peer_id).map_err(|e| LiteError::Storage {
            detail: format!("failed to create CrdtState: {e}"),
        })?;
        state.import(snapshot).map_err(|e| LiteError::Storage {
            detail: format!("failed to import snapshot: {e}"),
        })?;

        Ok(Self {
            state,
            next_mutation_id: AtomicU64::new(1),
            pending_deltas: Vec::new(),
            acked_versions: HashMap::new(),
            policies: nodedb_crdt::PolicyRegistry::new(),
            registered_collections: std::collections::HashSet::new(),
            deferred_version: None,
            deferred_count: 0,
        })
    }

    /// The peer ID of this engine.
    pub fn peer_id(&self) -> u64 {
        self.state.peer_id()
    }
    /// Import remote deltas from Origin (received via sync).
    pub fn import_remote(&self, data: &[u8]) -> Result<(), LiteError> {
        self.state.import(data).map_err(|e| LiteError::Storage {
            detail: format!("remote delta import failed: {e}"),
        })
    }
    // ─── Snapshot & Persistence ──────────────────────────────────────

    /// Export a full Loro state snapshot (for persistence to StorageEngine).
    pub fn export_snapshot(&self) -> Result<Vec<u8>, LiteError> {
        self.state
            .export_snapshot()
            .map_err(|e| LiteError::Storage {
                detail: format!("snapshot export failed: {e}"),
            })
    }

    /// Compact Loro history to prevent unbounded growth.
    ///
    /// Replaces the internal LoroDoc with a shallow snapshot. Historical
    /// operations are discarded. Current state is fully preserved.
    pub fn compact_history(&mut self) -> Result<(), LiteError> {
        self.state
            .compact_history()
            .map_err(|e| LiteError::Storage {
                detail: format!("history compaction failed: {e}"),
            })
    }

    /// Estimated memory usage in bytes.
    pub fn estimated_memory_bytes(&self) -> usize {
        let state_bytes = self.state.estimated_memory_bytes();
        let delta_bytes: usize = self
            .pending_deltas
            .iter()
            .map(|d| d.delta_bytes.len())
            .sum();
        state_bytes + delta_bytes
    }
    /// Access the underlying `CrdtState` for advanced operations.
    pub fn state(&self) -> &CrdtState {
        &self.state
    }
    // ─── Version-History Operations ──────────────────────────────────

    /// Export the oplog delta from a specific version to the current state.
    ///
    /// Returns the Loro update bytes that transform `from_version` into
    /// the current oplog state. Used by `ExportDelta`.
    pub fn export_delta_from(
        &self,
        from_version: &loro::VersionVector,
    ) -> Result<Vec<u8>, LiteError> {
        self.state
            .export_updates_since(from_version)
            .map_err(|e| LiteError::Storage {
                detail: format!("export_delta_from: {e}"),
            })
    }

    /// Compact history at a specific version, discarding oplog entries before it.
    ///
    /// The current state and all versions after the target are preserved.
    /// Used by `CompactAtVersion`.
    pub fn compact_at_version(&mut self, version: &loro::VersionVector) -> Result<(), LiteError> {
        self.state
            .compact_at_version(version)
            .map_err(|e| LiteError::Storage {
                detail: format!("compact_at_version: {e}"),
            })
    }
}
