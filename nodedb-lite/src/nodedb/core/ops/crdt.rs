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
        let guard = self
            .fts_state
            .try_admit_mutation("import_remote_deltas")
            .map_err(NodeDbError::from)?;
        let result = (|| {
            let imported = self
                .crdt
                .lock_or_recover()
                .import_remote(collection, data)
                .map_err(NodeDbError::storage)?;
            // Re-index the rows the delta changed. They came from a peer, so
            // nothing is staged back.
            self.reindex_documents_local(
                collection,
                imported.changed_rows.iter().map(String::as_str),
            )?;
            Ok(())
        })();
        guard.finish(result)
    }

    /// Apply a server-originated row post-image from Origin.
    ///
    /// Unlike [`Self::import_remote_deltas`], the payload here is a
    /// MessagePack row image rather than Loro update bytes. Origin sends it
    /// for writes that have no client-authored CRDT operation to replicate
    /// (SQL DML, DDL-managed system rows). An empty payload with `delete` set
    /// removes the row.
    ///
    /// A KV collection's row is the `{key, value…}` row every KV read
    /// returns. It lands in the KV store as the body a local write of the
    /// same row stores. Any other collection's row lands in its Loro
    /// document.
    ///
    /// Neither write reaches the outbound queue: the write came FROM Origin,
    /// so pushing it back would echo it into a loop.
    pub async fn apply_remote_row(
        &self,
        collection: &str,
        document_id: &str,
        payload: &[u8],
        delete: bool,
    ) -> NodeDbResult<()> {
        let guard = self.fts_state.admit_mutation().await;
        let result = async {
            if self.is_kv_collection(collection).await? {
                return self
                    .kv_apply_remote_row(collection, document_id, payload, delete)
                    .await;
            }
            self.apply_remote_document_row(collection, document_id, payload, delete)
        }
        .await;
        guard.finish(result)
    }

    /// Apply a remote row post-image to `collection`'s Loro document.
    fn apply_remote_document_row(
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
            // Origin writes a row post-image as standard msgpack, not the
            // tagged zerompk `Value` encoding.
            let value = nodedb_types::value_from_msgpack(payload).map_err(|e| {
                NodeDbError::serialization(
                    "msgpack",
                    format!("row push for '{collection}'/'{document_id}' does not decode: {e}"),
                )
            })?;
            let Value::Object(fields) = value else {
                return Err(NodeDbError::serialization(
                    "msgpack",
                    format!(
                        "row push for '{collection}'/'{document_id}' is a {}, not a row map",
                        value.type_name()
                    ),
                ));
            };
            let loro_fields: Vec<(&str, loro::LoroValue)> = fields
                .iter()
                .map(|(k, v)| (k.as_str(), value_to_loro(v)))
                .collect();
            crdt.upsert(collection, document_id, &loro_fields)
                .map_err(NodeDbError::from)?
        };
        crdt.drop_pending(mutation_id);

        drop(crdt);

        // Keep the text index on the row as it now stands. The row came from
        // Origin, so nothing is staged back to it.
        self.reindex_documents_local(collection, [document_id])?;
        Ok(())
    }

    /// Reject a specific delta (rollback optimistic local state).
    pub fn reject_delta(&self, mutation_id: u64) -> NodeDbResult<()> {
        let guard = self
            .fts_state
            .try_admit_mutation("reject_delta")
            .map_err(NodeDbError::from)?;
        let result = (|| {
            let mut crdt = self.crdt.lock_or_recover();
            let rejected = crdt.reject_delta(mutation_id);
            drop(crdt);
            if let Some(delta) = rejected {
                self.reindex_documents_local(&delta.collection, [delta.document_id.as_str()])?;
            }
            Ok(())
        })();
        guard.finish(result)
    }

    /// Start background sync to Origin.
    ///
    /// Spawns a task that connects to the Origin WebSocket endpoint, pushes
    /// pending deltas, and receives shape updates, reconnecting on its own.
    ///
    /// Returns immediately — the sync runs in the background. Returns `None`
    /// when sync is already running on this database: a second loop would
    /// push the same deltas twice, and the first would no longer be the one
    /// [`stop_sync`](Self::stop_sync) stops.
    ///
    /// The task is registered with the database, so `stop_sync` and
    /// [`shutdown`](Self::shutdown) both stop it.
    ///
    /// Requires the `sync` feature.
    #[cfg(all(not(target_arch = "wasm32"), feature = "sync"))]
    pub fn start_sync(
        self: &Arc<Self>,
        config: crate::sync::SyncConfig,
    ) -> Option<Arc<crate::sync::SyncClient>> {
        if self.tasks.has(crate::tasks::TaskKind::Sync) {
            return None;
        }

        let client = Arc::new(crate::sync::SyncClient::new(config));
        let delegate: Arc<dyn crate::sync::SyncDelegate> = Arc::clone(self) as _;
        let client_clone = Arc::clone(&client);
        let (stop_tx, mut stop) = crate::tasks::TaskRegistry::signal();
        let handle = crate::runtime::spawn(async move {
            // `run_sync_loop` reconnects forever and has no exit of its own,
            // so the stop signal is what ends it. Racing the two cancels the
            // loop at its current await point, which is why the transport
            // aborts its own children on drop rather than relying on the
            // normal return path.
            tokio::select! {
                _ = crate::sync::run_sync_loop(client_clone, delegate) => {}
                _ = stop.stopped() => {}
            }
        });
        self.tasks
            .track(crate::tasks::TaskKind::Sync, stop_tx, handle);
        Some(client)
    }

    /// Stop background sync, leaving the database open and usable.
    ///
    /// Returns `true` when sync was running. Sync can be restarted afterwards
    /// with a fresh [`start_sync`](Self::start_sync) — the reconnect-with-a-
    /// new-token path.
    ///
    /// Requires the `sync` feature.
    #[cfg(all(not(target_arch = "wasm32"), feature = "sync"))]
    pub async fn stop_sync(&self) -> bool {
        self.tasks.stop(crate::tasks::TaskKind::Sync).await
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use nodedb_types::value::Value;

    use crate::storage::pagedb_storage::PagedbStorageMem;

    use super::*;

    async fn open_db() -> Arc<NodeDbLite<PagedbStorageMem>> {
        let storage = PagedbStorageMem::open_in_memory()
            .await
            .expect("open in-memory storage");
        NodeDbLite::open(storage).await.expect("open NodeDbLite")
    }

    #[tokio::test]
    async fn a_pushed_document_row_lands_in_its_loro_document() {
        let db = open_db().await;
        let mut fields = HashMap::new();
        fields.insert("name".to_string(), Value::String("ada".into()));
        let row = nodedb_types::value_to_msgpack(&Value::Object(fields)).expect("encode");

        db.apply_remote_row("people", "p1", &row, false)
            .await
            .expect("apply row");

        let stored = db.crdt.lock_or_recover().read("people", "p1");
        let Some(loro::LoroValue::Map(map)) = stored else {
            panic!("row p1 is not a map: {stored:?}");
        };
        assert_eq!(
            map.get("name"),
            Some(&loro::LoroValue::String("ada".to_string().into()))
        );
        assert!(db.pending_crdt_deltas().expect("pending").is_empty());
    }

    #[tokio::test]
    async fn a_pushed_document_payload_that_is_not_a_row_map_is_refused() {
        let db = open_db().await;
        let scalar = nodedb_types::value_to_msgpack(&Value::Integer(1)).expect("encode");

        let err = db
            .apply_remote_row("people", "p1", &scalar, false)
            .await
            .expect_err("a scalar is not a row");
        assert!(err.message().contains("not a row map"), "{err}");
    }
}
