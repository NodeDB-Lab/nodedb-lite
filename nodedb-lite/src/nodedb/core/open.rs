// SPDX-License-Identifier: Apache-2.0

//! `NodeDbLite` constructors and cold-start restore helpers.

use std::collections::HashMap;
use std::num::NonZeroUsize;
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
        let kv_cache_capacity = NonZeroUsize::new(config.kv_cache_capacity)
            .ok_or_else(|| NodeDbError::config("kv_cache_capacity must be greater than 0"))?;
        Self::open_inner(storage, peer_id, governor, sync_enabled, kv_cache_capacity).await
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
        let kv_cache_capacity =
            NonZeroUsize::new(crate::config::LiteConfig::default().kv_cache_capacity)
                .expect("default kv_cache_capacity is non-zero");
        Self::open_inner(storage, peer_id, governor, true, kv_cache_capacity).await
    }

    #[allow(clippy::await_holding_lock)]
    async fn open_inner(
        storage: S,
        peer_id: u64,
        governor: crate::memory::MemoryGovernor,
        sync_enabled: bool,
        kv_cache_capacity: NonZeroUsize,
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

        // Rebuild the CRDT's registered-collection set from persisted bitemporal
        // flags so that SELECT queries on bitemporal collections work immediately
        // after open, even for collections with no inserted documents yet.
        // Also backfill the LatestVersion index for collections written before
        // the index was introduced — safe on fresh DBs and idempotent otherwise.
        const BITEMPORAL_PREFIX: &[u8] = b"document_bitemporal:";
        let bitemporal_entries = storage
            .scan_prefix(Namespace::Meta, BITEMPORAL_PREFIX)
            .await
            .unwrap_or_default();
        for (key, value) in &bitemporal_entries {
            // Only process collections where the flag byte is 0x01 (enabled).
            if value.first().copied() != Some(1) {
                continue;
            }
            if let Ok(key_str) = std::str::from_utf8(key)
                && let Some(name) = key_str.strip_prefix("document_bitemporal:")
            {
                crdt.register_collection(name);

                if let Err(e) = crate::engine::document::history::ops::backfill_latest_version(
                    storage.as_ref(),
                    name,
                )
                .await
                {
                    tracing::warn!(
                        collection = name,
                        error = %e,
                        "LatestVersion backfill failed — bitemporal reads will \
                         fall back to prefix scan for this collection"
                    );
                }
            }
        }

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

        // ── Restore HNSW indices and id_map ──
        let (hnsw_map, hnsw_id_map) = Self::restore_hnsw_indices(&storage).await?;

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
            hnsw_id_map,
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
        let array_op_log = Arc::new(crate::sync::array::KvOpLogStore::new(Arc::clone(&storage)));
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
            kv_cache: Mutex::new(lru::LruCache::new(kv_cache_capacity)),
            kv_write_buf: Mutex::new(KvWriteBuffer {
                ops: Vec::with_capacity(1024),
                overlay: HashMap::new(),
            }),
            kv_overlay_len: std::sync::atomic::AtomicUsize::new(0),
            sync_gate: std::sync::RwLock::new(None),
        };

        // Rebuild text indices from CRDT state only when no checkpoint exists.
        // When a checkpoint is present, `restore_fts_indices` has already loaded
        // the full index without re-tokenizing source documents.
        {
            let fts = db.fts_state.manager.lock_or_recover();
            if fts.is_empty() {
                drop(fts);
                db.rebuild_text_indices().await;
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

        // Rebuild CSR graph indices when no checkpoint was written before the
        // previous process exited. Pass 1 reads CRDT edge documents; Pass 2
        // scans the durable Namespace::Graph KV edge store; Pass 3 reads
        // Namespace::GraphHistory for bitemporal collections.
        {
            let csr = db.csr.lock_or_recover();
            if csr.is_empty() {
                drop(csr);
                db.rebuild_graph_indices().await;
            }
        }

        Ok(db)
    }

    /// Restore per-collection CSR graph indices from storage.
    ///
    /// On native targets with `PagedbStorage`, CSR blobs are read from pagedb
    /// segments (segment-first, then fall back to the legacy B+ tree KV blob
    /// for databases written by older builds).  On WASM, only the B+ tree path
    /// is used.
    async fn restore_csr_indices(storage: &Arc<S>) -> NodeDbResult<HashMap<String, CsrIndex>> {
        let mut csr_map: HashMap<String, CsrIndex> = HashMap::new();
        let Some(collections_bytes) = storage.get(Namespace::Meta, META_CSR_COLLECTIONS).await?
        else {
            return Ok(csr_map);
        };
        let Ok(names) = zerompk::from_msgpack::<Vec<String>>(&collections_bytes) else {
            return Ok(csr_map);
        };

        // On native targets, prefer the pagedb segment path when available.
        #[cfg(not(target_arch = "wasm32"))]
        let graph_seg_ext = storage.as_graph_segment_ext();

        for name in &names {
            // ── Segment path (native PagedbStorage) ──
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(ext) = graph_seg_ext {
                match ext.open_graph_segment(name).await {
                    Ok(Some(bytes)) => {
                        match CsrIndex::from_checkpoint(&bytes) {
                            Ok(Some(idx)) => {
                                csr_map.insert(name.clone(), idx);
                            }
                            Ok(None) | Err(_) => {
                                tracing::warn!(
                                    collection = %name,
                                    "CSR segment deserialization failed, will rebuild from CRDT"
                                );
                            }
                        }
                        continue;
                    }
                    Ok(None) => {
                        // No segment yet — fall through to legacy B+ tree path below.
                    }
                    Err(e) => {
                        tracing::warn!(
                            collection = %name,
                            error = %e,
                            "CSR segment open failed, falling back to legacy B+ tree path"
                        );
                    }
                }
            }

            // ── Legacy B+ tree path (WASM or pre-migration data) ──
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

    /// Restore HNSW indices and the vector id_map from storage.
    ///
    /// Returns `(indices, id_map)`. The id_map maps `"{index_key}:{internal_id}"`
    /// to `(doc_id, internal_id)` and is loaded from the blob written by `flush`.
    /// When no id_map blob exists (first open or pre-fix databases), the returned
    /// map is empty and vector search will fall back to HNSW integer IDs until the
    /// next flush.
    async fn restore_hnsw_indices(
        storage: &Arc<S>,
    ) -> NodeDbResult<(HashMap<String, HnswIndex>, HashMap<String, (String, u32)>)> {
        let mut hnsw_indices = HashMap::new();
        let Some(collections_bytes) = storage.get(Namespace::Meta, META_HNSW_COLLECTIONS).await?
        else {
            return Ok((hnsw_indices, HashMap::new()));
        };
        let Ok(names) = zerompk::from_msgpack::<Vec<String>>(&collections_bytes) else {
            return Ok((hnsw_indices, HashMap::new()));
        };

        // On native targets, check if vector segment operations are available.
        // When yes, the graph blob has empty vector placeholders; we load the
        // backing from the pagedb segment and attach it to the restored index.
        #[cfg(not(target_arch = "wasm32"))]
        let seg_ext = storage.as_vector_segment_ext();

        for name in &names {
            let key = format!("hnsw:{name}");
            if let Some(envelope) = storage.get(Namespace::Vector, key.as_bytes()).await? {
                match crate::storage::checksum::unwrap(&envelope) {
                    Some(checkpoint) => match HnswIndex::from_checkpoint(&checkpoint) {
                        Ok(Some(mut index)) => {
                            // Attach vector segment backing when available (native pagedb path).
                            #[cfg(not(target_arch = "wasm32"))]
                            if let Some(ext) = seg_ext {
                                match ext.open_vector_segment(name).await {
                                    Ok(Some(backing)) => {
                                        use std::sync::Arc;
                                        index.with_backing(Arc::new(backing));
                                        tracing::debug!(
                                            collection = %name,
                                            "HNSW restored with pagedb vector segment backing"
                                        );
                                    }
                                    Ok(None) => {
                                        tracing::debug!(
                                            collection = %name,
                                            "no vector segment found; \
                                             HNSW restored with inline vectors (legacy path)"
                                        );
                                    }
                                    Err(e) => {
                                        tracing::warn!(
                                            collection = %name,
                                            error = %e,
                                            "vector segment open failed; \
                                             HNSW restored with inline vectors"
                                        );
                                    }
                                }
                            }
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

        // ── Restore vector_id_map ──
        // The blob is written by `flush` and contains the full flat map.
        // Without this, vector_search returns HNSW integer strings after restart.
        let id_map = match storage
            .get(Namespace::Vector, b"hnsw_id_map")
            .await
            .unwrap_or(None)
        {
            Some(envelope) => match crate::storage::checksum::unwrap(&envelope) {
                Some(bytes) => match zerompk::from_msgpack::<Vec<(String, String, u32)>>(&bytes) {
                    Ok(entries) => entries
                        .into_iter()
                        .map(|(k, doc_id, iid)| (k, (doc_id, iid)))
                        .collect(),
                    Err(e) => {
                        tracing::error!(
                            error = %e,
                            "vector_id_map deserialization failed — \
                             vector search will fall back to HNSW integer IDs until next flush"
                        );
                        HashMap::new()
                    }
                },
                None => {
                    tracing::error!(
                        "vector_id_map CRC32C mismatch — discarding. \
                         Vector search will fall back to HNSW integer IDs until next flush."
                    );
                    let _ = storage.delete(Namespace::Vector, b"hnsw_id_map").await;
                    HashMap::new()
                }
            },
            None => HashMap::new(),
        };

        Ok((hnsw_indices, id_map))
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
