// SPDX-License-Identifier: Apache-2.0

//! CRDT sync gate, pending-delta bookkeeping, remote-delta import/apply, and
//! background sync startup.

use std::sync::Arc;

use nodedb_types::error::{NodeDbError, NodeDbResult};

use crate::nodedb::core::types::{NodeDbLite, SyncGate};
use crate::nodedb::lock_ext::LockExt;
use crate::storage::engine::StorageEngine;

impl<S: StorageEngine> NodeDbLite<S> {
    /// Install a per-document [`SyncGate`]. Documents the gate rejects are kept
    /// local-only (excluded from CRDT delta, FTS, and vector sync channels).
    pub fn set_sync_gate(&self, gate: Arc<dyn SyncGate>) {
        let mut slot = self
            .sync_gate
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *slot = Some(gate);
    }

    /// Whether a document write may be synced. Defaults to `true` when no gate is
    /// installed (sync-everything, the prior behavior).
    pub(crate) fn should_sync_doc(
        &self,
        collection: &str,
        fields: &std::collections::HashMap<String, nodedb_types::Value>,
    ) -> bool {
        match self.sync_gate.read() {
            Ok(slot) => slot
                .as_ref()
                .map(|g| g.should_sync(collection, fields))
                .unwrap_or(true),
            Err(_) => true,
        }
    }

    /// Access pending CRDT deltas (for sync client).
    pub fn pending_crdt_deltas(
        &self,
    ) -> NodeDbResult<Vec<crate::engine::crdt::engine::PendingDelta>> {
        let crdt = self.crdt.lock_or_recover();
        Ok(crdt.pending_deltas().to_vec())
    }

    /// Acknowledge synced deltas (called after Origin ACK).
    pub fn acknowledge_deltas(&self, acked_id: u64) -> NodeDbResult<()> {
        let mut crdt = self.crdt.lock_or_recover();
        crdt.acknowledge(acked_id);
        Ok(())
    }

    /// Assign a stable stream seq to a pending CRDT delta (first-send only).
    ///
    /// No-op if the delta already has a non-zero seq. Called by the sync
    /// transport to ensure the same seq is reused on reconnect re-sends.
    pub fn set_crdt_pending_delta_seq(&self, mutation_id: u64, seq: u64) -> NodeDbResult<()> {
        let mut crdt = self.crdt.lock_or_recover();
        crdt.set_pending_delta_seq(mutation_id, seq);
        Ok(())
    }

    /// Import remote deltas from Origin into `collection`'s Loro document.
    ///
    /// The collection must be supplied by the caller: each collection has its
    /// own document, and the update bytes alone do not identify which one.
    pub fn import_remote_deltas(&self, collection: &str, data: &[u8]) -> NodeDbResult<()> {
        let mut crdt = self.crdt.lock_or_recover();
        crdt.import_remote(collection, data)
            .map(|_admission| ())
            .map_err(NodeDbError::storage)
    }

    /// Apply a server-originated row post-image from Origin.
    ///
    /// Unlike [`Self::import_remote_deltas`], the payload here is a
    /// MessagePack row image rather than Loro update bytes — Origin sends it
    /// for writes that have no client-authored CRDT operation to replicate
    /// (SQL DML, DDL-managed system rows). An empty payload with `delete` set
    /// removes the row.
    ///
    /// The resulting local mutation is dropped from the outbound queue: the
    /// write came FROM Origin, so pushing it back would echo it into a loop.
    pub fn apply_remote_row(
        &self,
        collection: &str,
        document_id: &str,
        payload: &[u8],
        delete: bool,
    ) -> NodeDbResult<()> {
        use crate::nodedb::convert::value_to_loro;
        use nodedb_types::value::Value;

        let mut crdt = self.crdt.lock_or_recover();
        let mutation_id = if delete {
            crdt.delete(collection, document_id)
                .map_err(NodeDbError::storage)?
        } else {
            let value: Value = zerompk::from_msgpack(payload).map_err(|e| {
                NodeDbError::storage(format!("remote row payload decode failed: {e}"))
            })?;
            let Value::Object(fields) = value else {
                return Err(NodeDbError::storage(
                    "remote row payload is not an object".to_string(),
                ));
            };
            let loro_fields: Vec<(&str, loro::LoroValue)> = fields
                .iter()
                .map(|(k, v)| (k.as_str(), value_to_loro(v)))
                .collect();
            crdt.upsert(collection, document_id, &loro_fields)
                .map_err(NodeDbError::storage)?
        };
        crdt.drop_pending(mutation_id);
        Ok(())
    }

    /// Reject a specific delta (rollback optimistic local state).
    pub fn reject_delta(&self, mutation_id: u64) -> NodeDbResult<()> {
        let mut crdt = self.crdt.lock_or_recover();
        crdt.reject_delta(mutation_id);
        Ok(())
    }

    /// Start background sync to Origin.
    ///
    /// Spawns a Tokio task that connects to the Origin WebSocket endpoint,
    /// pushes pending deltas, and receives shape updates. Runs forever
    /// with auto-reconnect.
    ///
    /// Returns immediately — the sync runs in the background.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn start_sync(
        self: &Arc<Self>,
        config: crate::sync::SyncConfig,
    ) -> Arc<crate::sync::SyncClient> {
        let client = Arc::new(crate::sync::SyncClient::new(config));
        let delegate: Arc<dyn crate::sync::SyncDelegate> = Arc::clone(self) as _;
        let client_clone = Arc::clone(&client);
        tokio::spawn(async move {
            crate::sync::run_sync_loop(client_clone, delegate).await;
        });
        client
    }
}
