// SPDX-License-Identifier: Apache-2.0

//! `NodeDbLite` constructors: `open`, `open_with_config`, `open_with_budget`,
//! and the shared `open_inner` orchestration.

use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};

use nodedb_types::error::{NodeDbError, NodeDbResult};

use crate::engine::columnar::ColumnarEngine;
use crate::engine::fts::FtsState;
use crate::engine::htap::HtapBridge;
use crate::engine::sparse_vector::SparseVectorState;
use crate::engine::strict::StrictEngine;
use crate::engine::vector::VectorState;
use crate::nodedb::lock_ext::LockExt;
use crate::storage::engine::StorageEngine;

use crate::nodedb::core::types::{KvWriteBuffer, NodeDbLite};

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
        let outbound_queue_cap = config.outbound_queue_cap;
        let kv_cache_capacity = NonZeroUsize::new(config.kv_cache_capacity)
            .ok_or_else(|| NodeDbError::config("kv_cache_capacity must be greater than 0"))?;
        Self::open_inner(
            storage,
            peer_id,
            governor,
            sync_enabled,
            outbound_queue_cap,
            kv_cache_capacity,
        )
        .await
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
        let defaults = crate::config::LiteConfig::default();
        let kv_cache_capacity = NonZeroUsize::new(defaults.kv_cache_capacity)
            .expect("default kv_cache_capacity is non-zero");
        Self::open_inner(
            storage,
            peer_id,
            governor,
            true,
            defaults.outbound_queue_cap,
            kv_cache_capacity,
        )
        .await
    }

    #[allow(clippy::await_holding_lock)]
    async fn open_inner(
        storage: S,
        peer_id: u64,
        governor: crate::memory::MemoryGovernor,
        sync_enabled: bool,
        outbound_queue_cap: usize,
        kv_cache_capacity: NonZeroUsize,
    ) -> NodeDbResult<Self> {
        // Only the outbound sync queues (compiled out on wasm32) consume the cap.
        #[cfg(target_arch = "wasm32")]
        let _ = outbound_queue_cap;

        let storage = Arc::new(storage);

        // ── Restore Lite identity + CRDT state (snapshots, bitemporal
        // backfill, pending deltas, partial-flush safety, legacy CSR cleanup) ──
        let (crdt, lite_identity) = Self::restore_identity_and_crdt(&storage, peer_id).await?;

        // ── Restore FTS indices ──
        let fts_manager = Self::restore_fts_indices(&storage).await?;

        // ── Restore sparse-vector inverted indices ──
        let (sparse_manager, sparse_checkpoint_present) =
            Self::restore_sparse_indices(&storage).await;

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
        let outbound_queues =
            Self::build_outbound_queues(&storage, sync_enabled, outbound_queue_cap, &mut columnar)
                .await?;
        #[cfg(not(target_arch = "wasm32"))]
        let columnar_outbound = outbound_queues.columnar_outbound;
        #[cfg(not(target_arch = "wasm32"))]
        let vector_outbound = outbound_queues.vector_outbound;
        #[cfg(not(target_arch = "wasm32"))]
        let fts_outbound_init = outbound_queues.fts_outbound;
        #[cfg(not(target_arch = "wasm32"))]
        let spatial_outbound_init = outbound_queues.spatial_outbound;
        #[cfg(not(target_arch = "wasm32"))]
        let timeseries_outbound_init = outbound_queues.timeseries_outbound;

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
        let sparse_state = Arc::new(SparseVectorState::from_restored(sparse_manager));
        let array_engine = crate::engine::array::ArrayEngineState::open(&storage)
            .await
            .map_err(NodeDbError::storage)?;
        let array_state = Arc::new(tokio::sync::Mutex::new(array_engine));

        let csr_arc = Arc::new(Mutex::new(csr));
        #[allow(unused_mut)]
        let mut query_engine = crate::query::LiteQueryEngine::new(
            Arc::clone(&crdt),
            Arc::clone(&strict),
            Arc::clone(&columnar),
            Arc::clone(&htap),
            Arc::clone(&storage),
            Arc::clone(&timeseries),
            Arc::clone(&vector_state),
            Arc::clone(&array_state),
            Arc::clone(&fts_state),
            Arc::clone(&sparse_state),
            Arc::clone(&spatial),
            Arc::clone(&csr_arc),
        );

        // Wire FTS and spatial outbound queues into the query engine so that
        // SQL-path writes (SpatialOp::Insert, FtsIndexOp) also enqueue for sync.
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(ref q) = fts_outbound_init {
            query_engine.set_fts_outbound(Arc::clone(q));
        }
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(ref q) = spatial_outbound_init {
            query_engine.set_spatial_outbound(Arc::clone(q));
        }

        // ── Array CRDT sync state (send path, receive path, stream sequence
        // frontier) — non-wasm only ──
        #[cfg(not(target_arch = "wasm32"))]
        let array_sync = Self::build_array_sync_state(&storage, &array_state).await?;
        #[cfg(not(target_arch = "wasm32"))]
        let array_replica = array_sync.array_replica;
        #[cfg(not(target_arch = "wasm32"))]
        let array_schemas = array_sync.array_schemas;
        #[cfg(not(target_arch = "wasm32"))]
        let array_outbound = array_sync.array_outbound;
        #[cfg(not(target_arch = "wasm32"))]
        let array_inbound = array_sync.array_inbound;
        #[cfg(not(target_arch = "wasm32"))]
        let array_catchup = array_sync.array_catchup;
        #[cfg(not(target_arch = "wasm32"))]
        let stream_seq = array_sync.stream_seq;

        let db = Self {
            storage,
            vector_state,
            csr: csr_arc,
            crdt,
            governor,
            query_engine,
            fts_state,
            sparse_state,
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
            stream_seq,
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
            sync_lite_id: lite_identity.lite_id,
            sync_epoch: lite_identity.epoch,
            sync_enabled,
            kv_cache: Mutex::new(lru::LruCache::new(kv_cache_capacity)),
            kv_write_buf: Mutex::new(KvWriteBuffer {
                ops: Vec::with_capacity(1024),
                overlay: HashMap::new(),
            }),
            sync_gate: std::sync::RwLock::new(None),
        };

        // Rebuild text indices from CRDT state only when no checkpoint exists.
        // When a checkpoint is present, `restore_fts_indices` has already loaded
        // the full index without re-tokenizing source documents.
        {
            // `sparse_checkpoint_present` covers databases written before the
            // sparse index existed: they have a valid FTS checkpoint but no
            // sparse one, so emptiness alone cannot distinguish "no sparse
            // columns" from "never checkpointed". The first flush writes the
            // sparse catalog key even when empty, so this rebuild runs once.
            let fts_empty = db.fts_state.manager.lock_or_recover().is_empty();
            if fts_empty || !sparse_checkpoint_present {
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
}
