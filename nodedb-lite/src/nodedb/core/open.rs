// SPDX-License-Identifier: Apache-2.0

//! `NodeDbLite` constructors and cold-start restore helpers.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use nodedb_types::Namespace;
use nodedb_types::error::{NodeDbError, NodeDbResult};

use crate::engine::columnar::ColumnarEngine;
use crate::engine::crdt::CrdtEngine;
use crate::engine::fts::FtsState;
use crate::engine::graph::index::CsrIndex;
use crate::engine::htap::HtapBridge;
use crate::engine::strict::StrictEngine;
use crate::engine::vector::VectorState;
use crate::engine::vector::graph::HnswIndex;
use crate::nodedb::lock_ext::LockExt;
use crate::storage::engine::StorageEngine;

use super::types::{
    KvWriteBuffer, META_CRDT_DELTAS, META_CRDT_SNAPSHOT, META_CSR_COLLECTIONS, META_CSR_LEGACY,
    META_HNSW_COLLECTIONS, META_LAST_FLUSHED_MID, NodeDbLite,
};

impl<S: StorageEngine> NodeDbLite<S> {
    /// Open or create a Lite database backed by the given storage engine.
    ///
    /// Memory budget and per-engine percentages are resolved from environment
    /// variables via [`LiteConfig::from_env()`], falling back to defaults when
    /// variables are absent or malformed.
    pub async fn open(storage: S, peer_id: u64) -> NodeDbResult<Self> {
        Self::open_with_config(storage, peer_id, crate::config::LiteConfig::from_env()).await
    }

    /// Open with an explicit [`LiteConfig`].
    ///
    /// This is the primary constructor for callers that need fine-grained
    /// control over memory budgets (e.g. FFI, WASM, tests).
    pub async fn open_with_config(
        storage: S,
        peer_id: u64,
        config: crate::config::LiteConfig,
    ) -> NodeDbResult<Self> {
        let governor = crate::memory::MemoryGovernor::from_config(&config);
        let sync_enabled = config.sync_enabled;
        Self::open_inner(storage, peer_id, governor, sync_enabled).await
    }

    /// Open with a custom memory budget (convenience wrapper using default percentages).
    ///
    /// Prefer [`open_with_config`] for new callers.
    pub async fn open_with_budget(
        storage: S,
        peer_id: u64,
        memory_budget: usize,
    ) -> NodeDbResult<Self> {
        let governor = crate::memory::MemoryGovernor::new(memory_budget);
        Self::open_inner(storage, peer_id, governor, true).await
    }

    async fn open_inner(
        storage: S,
        peer_id: u64,
        governor: crate::memory::MemoryGovernor,
        sync_enabled: bool,
    ) -> NodeDbResult<Self> {
        let storage = Arc::new(storage);

        // ── Restore CRDT state (with CRC32C validation) ──
        let mut crdt = match storage
            .get(Namespace::LoroState, META_CRDT_SNAPSHOT)
            .await?
        {
            Some(envelope) => {
                match crate::storage::checksum::unwrap(&envelope) {
                    Some(snapshot) => CrdtEngine::from_snapshot(peer_id, &snapshot)
                        .map_err(|e| NodeDbError::storage(format!("CRDT restore failed: {e}")))?,
                    None => {
                        tracing::error!(
                            "CRDT snapshot CRC32C mismatch — discarding corrupted snapshot. \
                             Will start with empty state. A full re-sync from Origin is needed."
                        );
                        // Delete the corrupted snapshot so we don't re-read it.
                        let _ = storage
                            .delete(Namespace::LoroState, META_CRDT_SNAPSHOT)
                            .await;
                        CrdtEngine::new(peer_id)
                            .map_err(|e| NodeDbError::storage(format!("CRDT init failed: {e}")))?
                    }
                }
            }
            None => CrdtEngine::new(peer_id)
                .map_err(|e| NodeDbError::storage(format!("CRDT init failed: {e}")))?,
        };

        // Restore pending deltas — prefer incremental entries over legacy bulk blob.
        let incremental_entries = storage.scan_prefix(Namespace::Crdt, b"delta:").await?;

        if !incremental_entries.is_empty() {
            // Use incremental entries (append-only format).
            crdt.restore_pending_deltas_incremental(&incremental_entries);
        } else if let Some(delta_bytes) = storage.get(Namespace::Crdt, META_CRDT_DELTAS).await? {
            // Fall back to legacy bulk blob.
            crdt.restore_pending_deltas(&delta_bytes);
        }

        // Partial flush safety: check if the last-flushed mutation_id matches.
        if crdt.pending_count() > 0
            && let Some(last_flushed_bytes) =
                storage.get(Namespace::Meta, META_LAST_FLUSHED_MID).await?
            && last_flushed_bytes.len() == 8
        {
            let last_flushed = u64::from_le_bytes(last_flushed_bytes.try_into().unwrap_or([0; 8]));
            let max_pending = crdt
                .pending_deltas()
                .iter()
                .map(|d| d.mutation_id)
                .max()
                .unwrap_or(0);

            if max_pending > 0 && last_flushed > 0 && max_pending != last_flushed {
                tracing::warn!(
                    last_flushed,
                    max_pending,
                    "partial flush detected — pending deltas may be inconsistent. \
                     Clearing pending queue; CRDT state is authoritative."
                );
                crdt.clear_pending_deltas();
            }
        }

        // ── Delete legacy single-CSR checkpoint if present ──
        if storage
            .get(Namespace::Graph, META_CSR_LEGACY)
            .await?
            .is_some()
        {
            let _ = storage.delete(Namespace::Graph, META_CSR_LEGACY).await;
        }

        // ── Restore FTS indices ──
        let fts_manager = Self::restore_fts_indices(&storage).await?;

        // ── Restore per-collection CSR indices ──
        let csr = Self::restore_csr_indices(&storage).await?;

        // ── Restore HNSW indices ──
        let hnsw_map = Self::restore_hnsw_indices(&storage).await?;

        // ── Restore spatial indices ──
        let spatial = Arc::new(Mutex::new(Self::restore_spatial_indices(&storage).await));

        // ── Restore strict document engine ──
        let strict = StrictEngine::restore(Arc::clone(&storage))
            .await
            .map_err(NodeDbError::storage)?;

        // ── Restore columnar engine ──
        #[cfg(not(target_arch = "wasm32"))]
        let mut columnar = ColumnarEngine::restore(Arc::clone(&storage))
            .await
            .map_err(NodeDbError::storage)?;
        #[cfg(target_arch = "wasm32")]
        let columnar = ColumnarEngine::restore(Arc::clone(&storage))
            .await
            .map_err(NodeDbError::storage)?;

        // Wire per-engine sync outbound queues when sync is enabled (native only).
        #[cfg(not(target_arch = "wasm32"))]
        let columnar_outbound: Option<Arc<crate::sync::ColumnarOutbound>> = if sync_enabled {
            let q = Arc::new(crate::sync::ColumnarOutbound::new());
            columnar.set_outbound(Arc::clone(&q));
            Some(q)
        } else {
            None
        };

        #[cfg(not(target_arch = "wasm32"))]
        let vector_outbound: Option<Arc<crate::sync::VectorOutbound>> = if sync_enabled {
            Some(Arc::new(crate::sync::VectorOutbound::new()))
        } else {
            None
        };

        #[cfg(not(target_arch = "wasm32"))]
        let fts_outbound_init: Option<Arc<crate::sync::FtsOutbound>> = if sync_enabled {
            Some(Arc::new(crate::sync::FtsOutbound::new()))
        } else {
            None
        };

        #[cfg(not(target_arch = "wasm32"))]
        let spatial_outbound_init: Option<Arc<crate::sync::SpatialOutbound>> = if sync_enabled {
            Some(Arc::new(crate::sync::SpatialOutbound::new()))
        } else {
            None
        };

        #[cfg(not(target_arch = "wasm32"))]
        let timeseries_outbound_init: Option<Arc<crate::sync::TimeseriesOutbound>> = if sync_enabled
        {
            let q = Arc::new(crate::sync::TimeseriesOutbound::new());
            columnar.set_timeseries_outbound(Arc::clone(&q));
            Some(q)
        } else {
            None
        };

        let crdt = Arc::new(Mutex::new(crdt));
        let strict = Arc::new(strict);
        let columnar = Arc::new(columnar);
        let htap = Arc::new(HtapBridge::new());
        let timeseries = Arc::new(Mutex::new(
            crate::engine::timeseries::engine::TimeseriesEngine::new(),
        ));
        let vector_state = Arc::new(VectorState::from_restored(
            Arc::clone(&storage),
            128,
            hnsw_map,
        ));
        let fts_state = Arc::new(FtsState::from_restored(fts_manager));
        let array_engine = crate::engine::array::ArrayEngineState::open(&storage)
            .await
            .map_err(NodeDbError::storage)?;
        let array_state = Arc::new(tokio::sync::Mutex::new(array_engine));

        let csr_arc = Arc::new(Mutex::new(csr));
        let query_engine = crate::query::LiteQueryEngine::new(
            Arc::clone(&crdt),
            Arc::clone(&strict),
            Arc::clone(&columnar),
            Arc::clone(&htap),
            Arc::clone(&storage),
            Arc::clone(&timeseries),
            Arc::clone(&vector_state),
            Arc::clone(&array_state),
            Arc::clone(&fts_state),
            Arc::clone(&spatial),
            Arc::clone(&csr_arc),
        );

        // ── Array CRDT sync state (non-wasm only) ─────────────────────────────
        #[cfg(not(target_arch = "wasm32"))]
        let array_replica = Arc::new(
            crate::sync::array::ReplicaState::load_or_init(&*storage)
                .await
                .map_err(NodeDbError::storage)?,
        );
        #[cfg(not(target_arch = "wasm32"))]
        let array_schemas = Arc::new(
            crate::sync::array::SchemaRegistry::load(
                Arc::clone(&storage),
                Arc::clone(&array_replica),
            )
            .await
            .map_err(NodeDbError::storage)?,
        );
        #[cfg(not(target_arch = "wasm32"))]
        let array_op_log = Arc::new(crate::sync::array::RedbOpLog::new(Arc::clone(&storage)));
        #[cfg(not(target_arch = "wasm32"))]
        let array_pending = Arc::new(crate::sync::array::PendingQueue::new(Arc::clone(&storage)));
        #[cfg(not(target_arch = "wasm32"))]
        let array_outbound = Arc::new(crate::sync::array::ArrayOutbound::new(
            Arc::clone(&array_op_log),
            Arc::clone(&array_pending),
            Arc::clone(&array_schemas),
            Arc::clone(&array_replica),
        ));

        // ── Array CRDT inbound receive path (non-wasm only) ───────────────────
        #[cfg(not(target_arch = "wasm32"))]
        let array_catchup = Arc::new(
            crate::sync::array::CatchupTracker::load(Arc::clone(&storage))
                .await
                .map_err(NodeDbError::storage)?,
        );
        #[cfg(not(target_arch = "wasm32"))]
        let array_apply_engine = Arc::new(
            crate::sync::array::LiteApplyEngine::new(
                Arc::clone(&storage),
                Arc::clone(&array_state),
                Arc::clone(&array_schemas),
                Arc::clone(array_outbound.op_log()),
            )
            .await,
        );
        #[cfg(not(target_arch = "wasm32"))]
        let array_inbound = Arc::new(crate::sync::array::ArrayInbound::new(
            array_apply_engine,
            Arc::clone(&array_schemas),
            Arc::clone(&array_replica),
            Arc::clone(array_outbound.pending()),
            Arc::clone(array_outbound.op_log()),
            Arc::clone(&array_catchup),
        ));

        let db = Self {
            storage,
            vector_state,
            csr: csr_arc,
            crdt,
            governor,
            query_engine,
            fts_state,
            spatial,
            secondary_indices: Mutex::new(HashMap::new()),
            strict,
            columnar,
            htap,
            timeseries,
            array_state,
            #[cfg(not(target_arch = "wasm32"))]
            array_replica,
            #[cfg(not(target_arch = "wasm32"))]
            array_schemas,
            #[cfg(not(target_arch = "wasm32"))]
            array_outbound,
            #[cfg(not(target_arch = "wasm32"))]
            array_inbound,
            #[cfg(not(target_arch = "wasm32"))]
            array_catchup,
            #[cfg(not(target_arch = "wasm32"))]
            columnar_outbound,
            #[cfg(not(target_arch = "wasm32"))]
            vector_outbound,
            #[cfg(not(target_arch = "wasm32"))]
            fts_outbound: fts_outbound_init,
            #[cfg(not(target_arch = "wasm32"))]
            spatial_outbound: spatial_outbound_init,
            #[cfg(not(target_arch = "wasm32"))]
            timeseries_outbound: timeseries_outbound_init,
            sync_enabled,
            kv_write_buf: Mutex::new(KvWriteBuffer {
                ops: Vec::with_capacity(1024),
                overlay: HashMap::new(),
            }),
        };

        // Rebuild text indices from CRDT state only when no checkpoint exists.
        // When a checkpoint is present, `restore_fts_indices` has already loaded
        // the full index without re-tokenizing source documents.
        {
            let fts = db.fts_state.manager.lock_or_recover();
            if fts.is_empty() {
                drop(fts);
                db.rebuild_text_indices();
            }
        }

        // Rebuild spatial indices if restore produced empty trees.
        // The R-tree checkpoint only stores bounding boxes, not doc IDs.
        // A full rebuild from CRDT documents ensures doc_to_entry is correct.
        {
            let spatial = db.spatial.lock_or_recover();
            if spatial.is_empty() {
                drop(spatial);
                db.rebuild_spatial_indices();
            }
        }

        Ok(db)
    }

    /// Restore per-collection CSR graph indices from storage.
    async fn restore_csr_indices(storage: &Arc<S>) -> NodeDbResult<HashMap<String, CsrIndex>> {
        let mut csr_map: HashMap<String, CsrIndex> = HashMap::new();
        let Some(collections_bytes) = storage.get(Namespace::Meta, META_CSR_COLLECTIONS).await?
        else {
            return Ok(csr_map);
        };
        let Ok(names) = zerompk::from_msgpack::<Vec<String>>(&collections_bytes) else {
            return Ok(csr_map);
        };
        for name in &names {
            let key = format!("csr:{name}");
            if let Some(envelope) = storage.get(Namespace::Graph, key.as_bytes()).await? {
                match crate::storage::checksum::unwrap(&envelope) {
                    Some(bytes) => match CsrIndex::from_checkpoint(&bytes) {
                        Ok(Some(idx)) => {
                            csr_map.insert(name.clone(), idx);
                        }
                        Ok(None) | Err(_) => {
                            tracing::warn!(
                                collection = %name,
                                "CSR checkpoint deserialization failed, will rebuild from CRDT"
                            );
                        }
                    },
                    None => {
                        tracing::error!(
                            collection = %name,
                            "CSR checkpoint CRC32C mismatch — discarding. \
                             Will rebuild from CRDT edge documents on next insert."
                        );
                        let _ = storage.delete(Namespace::Graph, key.as_bytes()).await;
                    }
                }
            }
        }
        Ok(csr_map)
    }

    /// Restore HNSW indices from storage.
    async fn restore_hnsw_indices(storage: &Arc<S>) -> NodeDbResult<HashMap<String, HnswIndex>> {
        let mut hnsw_indices = HashMap::new();
        let Some(collections_bytes) = storage.get(Namespace::Meta, META_HNSW_COLLECTIONS).await?
        else {
            return Ok(hnsw_indices);
        };
        let Ok(names) = zerompk::from_msgpack::<Vec<String>>(&collections_bytes) else {
            return Ok(hnsw_indices);
        };
        for name in &names {
            let key = format!("hnsw:{name}");
            if let Some(envelope) = storage.get(Namespace::Vector, key.as_bytes()).await? {
                match crate::storage::checksum::unwrap(&envelope) {
                    Some(checkpoint) => match HnswIndex::from_checkpoint(&checkpoint) {
                        Ok(Some(index)) => {
                            hnsw_indices.insert(name.clone(), index);
                        }
                        Ok(None) | Err(_) => {
                            tracing::warn!(
                                collection = %name,
                                "HNSW checkpoint deserialization failed, will rebuild from CRDT"
                            );
                        }
                    },
                    None => {
                        tracing::error!(
                            collection = %name,
                            "HNSW checkpoint CRC32C mismatch — discarding. \
                             Will rebuild from CRDT document vectors on next vector insert."
                        );
                        let _ = storage.delete(Namespace::Vector, key.as_bytes()).await;
                    }
                }
            }
        }
        Ok(hnsw_indices)
    }

    /// Restore spatial indices from storage.
    async fn restore_spatial_indices(
        storage: &Arc<S>,
    ) -> crate::engine::spatial::SpatialIndexManager {
        match crate::engine::spatial::checkpoint::restore_spatial(storage.as_ref()).await {
            Ok((checkpoints, doc_to_entry, next_id)) if !checkpoints.is_empty() => {
                let mut mgr = crate::engine::spatial::SpatialIndexManager::new();
                mgr.load_checkpoint(&checkpoints, doc_to_entry, next_id);
                mgr
            }
            Ok(_) => crate::engine::spatial::SpatialIndexManager::new(),
            Err(e) => {
                tracing::error!(
                    error = %e,
                    "spatial checkpoint restore failed — starting with empty index; \
                     will rebuild from CRDT state on cold open"
                );
                crate::engine::spatial::SpatialIndexManager::new()
            }
        }
    }

    /// Restore FTS indices from a persistent checkpoint.
    ///
    /// Returns an empty `FtsCollectionManager` when no checkpoint exists (first
    /// open or after a collection drop).  The caller decides whether to fall
    /// back to `rebuild_text_indices` — see `open_inner`.
    async fn restore_fts_indices(
        storage: &Arc<S>,
    ) -> NodeDbResult<crate::engine::fts::FtsCollectionManager> {
        let mut mgr = crate::engine::fts::FtsCollectionManager::new();
        match crate::engine::fts::checkpoint::restore_fts(storage.as_ref()).await {
            Ok((indices, id_to_surrogate, surrogate_to_id, next_surrogate))
                if !indices.is_empty() =>
            {
                mgr.load_checkpoint(indices, id_to_surrogate, surrogate_to_id, next_surrogate);
            }
            Ok(_) => {
                // No checkpoint found — caller will rebuild from CRDT state.
            }
            Err(e) => {
                tracing::error!(
                    error = %e,
                    "FTS checkpoint restore failed — starting with empty index; \
                     will rebuild from CRDT state on cold open"
                );
            }
        }
        Ok(mgr)
    }
}
