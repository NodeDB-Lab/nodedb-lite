// SPDX-License-Identifier: Apache-2.0

//! `NodeDbLite::flush` — persist all in-memory state to storage.

use crate::storage::engine::{StorageEngine, WriteOp};
use nodedb_types::Namespace;
use nodedb_types::error::{NodeDbError, NodeDbResult};

use crate::engine::crdt::CrdtEngine;
use crate::nodedb::lock_ext::LockExt;

use super::types::{
    META_CRDT_DELTAS, META_CRDT_SNAPSHOT, META_CSR_COLLECTIONS, META_HNSW_COLLECTIONS,
    META_LAST_FLUSHED_MID, NodeDbLite,
};

impl<S: StorageEngine> NodeDbLite<S> {
    /// Persist all in-memory state to storage (call before shutdown).
    pub async fn flush(&self) -> NodeDbResult<()> {
        let mut ops = Vec::new();

        // ── Persist CRDT snapshot (CRC32C wrapped) ──
        {
            let crdt = self.crdt.lock_or_recover();
            let snapshot = crdt.export_snapshot().map_err(NodeDbError::storage)?;
            ops.push(WriteOp::Put {
                ns: Namespace::LoroState,
                key: META_CRDT_SNAPSHOT.to_vec(),
                value: crate::storage::checksum::wrap(&snapshot),
            });

            // Write pending deltas individually (append-only persistence).
            // Each delta is stored under `crdt:delta:{mutation_id:016x}`.
            // Also write the legacy bulk blob for backward compatibility.
            let pending = crdt.pending_deltas();
            let max_mid = pending.iter().map(|d| d.mutation_id).max().unwrap_or(0);

            for delta in pending {
                let key = CrdtEngine::delta_storage_key(delta.mutation_id);
                let value = CrdtEngine::serialize_delta(delta).map_err(NodeDbError::storage)?;
                ops.push(WriteOp::Put {
                    ns: Namespace::Crdt,
                    key,
                    value,
                });
            }

            // Legacy bulk blob (for clients that haven't upgraded to incremental restore).
            let deltas_bulk = crdt
                .serialize_pending_deltas()
                .map_err(NodeDbError::storage)?;
            ops.push(WriteOp::Put {
                ns: Namespace::Crdt,
                key: META_CRDT_DELTAS.to_vec(),
                value: deltas_bulk,
            });

            // Write the last-flushed mutation_id for partial flush safety.
            ops.push(WriteOp::Put {
                ns: Namespace::Meta,
                key: META_LAST_FLUSHED_MID.to_vec(),
                value: max_mid.to_le_bytes().to_vec(),
            });
        }

        // ── Persist per-collection CSR indices (CRC32C wrapped) ──
        {
            let csr_map = self.csr.lock_or_recover();
            let names: Vec<String> = csr_map.keys().cloned().collect();
            let names_bytes = zerompk::to_msgpack_vec(&names)
                .map_err(|e| NodeDbError::serialization("msgpack", e))?;
            ops.push(WriteOp::Put {
                ns: Namespace::Meta,
                key: META_CSR_COLLECTIONS.to_vec(),
                value: names_bytes,
            });

            for (name, index) in csr_map.iter() {
                let key = format!("csr:{name}");
                match index.checkpoint_to_bytes() {
                    Ok(checkpoint) => {
                        ops.push(WriteOp::Put {
                            ns: Namespace::Graph,
                            key: key.into_bytes(),
                            value: crate::storage::checksum::wrap(&checkpoint),
                        });
                    }
                    Err(e) => {
                        tracing::error!(
                            collection = %name,
                            error = %e,
                            "CSR checkpoint failed for collection; graph state not persisted"
                        );
                    }
                }
            }
        }

        // ── Persist HNSW indices ──
        {
            let indices = self.vector_state.hnsw_indices.lock_or_recover();
            let names: Vec<String> = indices.keys().cloned().collect();
            let names_bytes = zerompk::to_msgpack_vec(&names)
                .map_err(|e| NodeDbError::serialization("msgpack", e))?;
            ops.push(WriteOp::Put {
                ns: Namespace::Meta,
                key: META_HNSW_COLLECTIONS.to_vec(),
                value: names_bytes,
            });

            for (name, index) in indices.iter() {
                let key = format!("hnsw:{name}");
                let checkpoint = index.checkpoint_to_bytes();
                ops.push(WriteOp::Put {
                    ns: Namespace::Vector,
                    key: key.into_bytes(),
                    value: crate::storage::checksum::wrap(&checkpoint),
                });
            }
        }

        self.storage
            .batch_write(&ops)
            .await
            .map_err(NodeDbError::storage)?;

        // ── Persist spatial indices (separate batch — includes docmap) ────────
        let (spatial_checkpoints, spatial_doc_to_entry, spatial_next_id) =
            self.spatial.lock_or_recover().checkpoint_data();
        crate::engine::spatial::checkpoint::flush_spatial(
            self.storage.as_ref(),
            &spatial_checkpoints,
            &spatial_doc_to_entry,
            spatial_next_id,
        )
        .await?;

        // ── Persist FTS indices (separate batch — potentially large) ──
        // Serialize synchronously while holding the lock, then write after.
        let fts_ops = {
            let fts = self.fts_state.manager.lock_or_recover();
            let (indices, id_to_surrogate, next_surrogate) = fts.checkpoint_data();
            crate::engine::fts::checkpoint::serialize_fts(indices, id_to_surrogate, next_surrogate)?
        };
        self.storage
            .batch_write(&fts_ops)
            .await
            .map_err(NodeDbError::storage)?;

        Ok(())
    }
}
