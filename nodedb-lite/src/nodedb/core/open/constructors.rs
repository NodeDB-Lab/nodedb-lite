// SPDX-License-Identifier: Apache-2.0

//! `NodeDbLite` constructors: `open`, `open_with_config`, `open_with_budget`,
//! and the shared `open_inner` orchestration.
//!
//! Every constructor funnels through `open_inner` with a complete
//! [`LiteConfig`](crate::config::LiteConfig), and `open_inner` is the single
//! place that consumes it. That is deliberate: a constructor that destructured
//! its own subset of the config could silently drop a field, which is how
//! `auto_flush_ms` and `auto_compact_ms` came to be documented as behavior
//! nothing implemented.
//!
//! The constructors return `Arc<Self>` because two of those fields describe
//! background tasks. The tasks hold a `Weak` back-reference and are spawned
//! here from the caller's configuration, so the durability and space bounds the
//! config states hold on the library surface and not only through the FFI and
//! WASM bindings.

use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};

use nodedb_types::error::{NodeDbError, NodeDbResult};

use crate::config::LiteConfig;
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
    /// Configuration is resolved from environment variables via
    /// [`LiteConfig::from_env()`], falling back to defaults when variables are
    /// absent or malformed — which includes the default one-second auto-flush
    /// interval, so writes are durable within a second of landing.
    ///
    /// The instance's Loro peer id is not a parameter: it is minted on first
    /// open and persisted with the data it authors. A caller-supplied id is
    /// the same constant in every install of an application, which hands two
    /// live replicas one producer identity and has the CRDT merge discard one
    /// of them; and an id that lives only in the caller's argument cannot be
    /// rotated durably when Origin refuses it.
    pub async fn open(storage: S) -> NodeDbResult<Arc<Self>> {
        Self::open_with_config(storage, LiteConfig::from_env()).await
    }

    /// Open with an explicit [`LiteConfig`].
    ///
    /// This is the primary constructor for callers that need control over
    /// memory budgets or the background maintenance intervals. The config is
    /// validated before any storage work happens, so an incoherent budget is
    /// rejected rather than silently over-allocating.
    pub async fn open_with_config(storage: S, config: LiteConfig) -> NodeDbResult<Arc<Self>> {
        Self::open_inner(storage, config).await
    }

    /// Open with a custom memory budget, taking every other setting from
    /// [`LiteConfig::default()`] — including the default auto-flush interval.
    ///
    /// Prefer [`open_with_config`](Self::open_with_config) for new callers.
    pub async fn open_with_budget(storage: S, memory_budget: usize) -> NodeDbResult<Arc<Self>> {
        let config = LiteConfig {
            memory_budget,
            ..LiteConfig::default()
        };
        Self::open_with_config(storage, config).await
    }

    #[allow(clippy::await_holding_lock)]
    async fn open_inner(storage: S, config: LiteConfig) -> NodeDbResult<Arc<Self>> {
        config.validate()?;

        let governor = crate::memory::MemoryGovernor::from_config(&config);
        let sync_enabled = config.sync_enabled;
        let kv_cache_capacity = NonZeroUsize::new(config.kv_cache_capacity)
            .ok_or_else(|| NodeDbError::config("kv_cache_capacity must be greater than 0"))?;

        // Only the outbound sync queues (compiled out on wasm32) consume the cap.
        #[cfg(not(target_arch = "wasm32"))]
        let outbound_queue_cap = config.outbound_queue_cap;

        let storage = Arc::new(storage);

        // ── Restore Lite identity + CRDT state (snapshots, bitemporal
        // backfill, pending deltas, partial-flush safety, legacy CSR cleanup) ──
        let (crdt, lite_identity) =
            Self::restore_identity_and_crdt(&storage, config.corruption_policy).await?;

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
            identity: Mutex::new(lite_identity),
            identity_change: tokio::sync::Mutex::new(()),
            flush_lock: tokio::sync::Mutex::new(()),
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
                db.rebuild_text_indices().await?;
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

        // ── Spawn the background maintenance tasks the config asked for ──
        // Both hold a `Weak` handle, so they stop when the last `Arc` returned
        // from here is dropped. A zero interval spawns nothing.
        let db = Arc::new(db);
        db.start_auto_flush(config.auto_flush_ms);
        db.start_auto_compact(config.auto_compact_ms);

        Ok(db)
    }
}
