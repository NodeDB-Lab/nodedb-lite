//! `NodeDbLite` struct definition, open/flush, and utility methods.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use nodedb_types::Namespace;
use nodedb_types::error::{NodeDbError, NodeDbResult};

use super::lock_ext::LockExt;
use crate::engine::columnar::ColumnarEngine;
use crate::engine::crdt::CrdtEngine;
use crate::engine::graph::index::CsrIndex;
use crate::engine::htap::HtapBridge;
use crate::engine::strict::StrictEngine;
use crate::engine::vector::graph::{HnswIndex, HnswParams};
use crate::memory::{EngineId, MemoryGovernor};
use crate::storage::engine::{StorageEngine, StorageEngineSync, WriteOp};

/// Storage key constants.
pub(crate) const META_HNSW_COLLECTIONS: &[u8] = b"meta:hnsw_collections";
/// Legacy single-CSR checkpoint key (pre-0.1.0). Ignored on open; deleted if present.
pub(crate) const META_CSR_LEGACY: &[u8] = b"meta:csr_checkpoint";
/// List of collection names that have a CSR checkpoint (MessagePack Vec<String>).
pub(crate) const META_CSR_COLLECTIONS: &[u8] = b"meta:csr_collections";
pub(crate) const META_CRDT_SNAPSHOT: &[u8] = b"crdt:snapshot";
pub(crate) const META_CRDT_DELTAS: &[u8] = b"crdt:pending_deltas";
/// Last flushed mutation_id — used for partial flush safety.
/// On cold start, if pending deltas have mutation_ids that don't align
/// with this watermark, we know the previous flush was interrupted.
pub(crate) const META_LAST_FLUSHED_MID: &[u8] = b"meta:last_flushed_mid";

/// NodeDB-Lite — the embedded edge database.
///
/// Fully capable of vector search, graph traversal, and document CRUD
/// entirely offline. Optional sync to Origin via WebSocket.
pub struct NodeDbLite<S: StorageEngine + StorageEngineSync> {
    pub(crate) storage: Arc<S>,
    /// Per-collection HNSW indices.
    pub(crate) hnsw_indices: Mutex<HashMap<String, HnswIndex>>,
    /// Per-collection CSR graph indices, keyed by collection name.
    pub(crate) csr: Mutex<HashMap<String, CsrIndex>>,
    /// CRDT engine for delta generation and sync.
    /// Arc-wrapped for sharing with the query engine's TableProvider.
    pub(crate) crdt: Arc<Mutex<CrdtEngine>>,
    /// Memory budget governor.
    pub(crate) governor: MemoryGovernor,
    /// HNSW search ef parameter (configurable).
    pub(crate) search_ef: usize,
    /// Vector ID to collection+doc_id mapping (for CRDT integration).
    pub(crate) vector_id_map: Mutex<HashMap<String, (String, u32)>>,
    /// SQL query engine (DataFusion over Loro documents and strict collections).
    pub(crate) query_engine: crate::query::LiteQueryEngine<S>,
    /// Per-collection in-memory full-text search engine.
    /// Updated incrementally on `document_put` and `document_delete`.
    pub(crate) fts: Mutex<crate::engine::fts::FtsCollectionManager>,
    /// Spatial R-tree indexes for geometry fields.
    pub(crate) spatial: Mutex<crate::engine::spatial::SpatialIndexManager>,
    /// Per-column secondary B-tree indexes for strict collections.
    /// Key: `{collection}:{column}` → SecondaryIndex.
    pub(crate) secondary_indices:
        Mutex<HashMap<String, crate::engine::strict::secondary_index::SecondaryIndex>>,
    /// Strict document engine (Binary Tuple collections).
    /// Arc-wrapped for sharing with the query engine's StrictTableProvider.
    pub(crate) strict: Arc<StrictEngine<S>>,
    /// Columnar engine (compressed segment collections).
    /// Arc-wrapped for sharing with the query engine's ColumnarTableProvider.
    pub(crate) columnar: Arc<ColumnarEngine<S>>,
    /// HTAP bridge: CDC from strict → columnar materialized views.
    /// Arc-wrapped for sharing with the query engine's DDL handlers.
    pub(crate) htap: Arc<HtapBridge>,
    /// Lite timeseries engine.
    /// Arc-wrapped for sharing with the query engine's DDL handlers.
    pub(crate) timeseries: Arc<Mutex<crate::engine::timeseries::engine::TimeseriesEngine>>,
    /// Array engine in-memory state (storage-agnostic; calls via NodeDbLite methods).
    ///
    /// `Arc`-wrapped so it can be shared with [`crate::sync::array::LiteApplyEngine`]
    /// for the inbound receive path without borrowing `NodeDbLite`.
    pub(crate) array_state: Arc<std::sync::Mutex<crate::engine::array::engine::ArrayEngineState>>,
    /// Stable per-replica identity + HLC generator for array CRDT sync.
    /// Used by the transport-layer wiring; held here so that
    /// the `NodeDbLite` constructor owns the lifetime.
    #[cfg(not(target_arch = "wasm32"))]
    #[allow(dead_code)]
    pub(crate) array_replica: Arc<crate::sync::array::ReplicaState>,
    /// Per-array [`SchemaDoc`] registry (persisted Loro snapshots).
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) array_schemas: Arc<crate::sync::array::SchemaRegistry<S>>,
    /// Array CRDT send path: op-log + pending queue emitters.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) array_outbound: Arc<crate::sync::array::ArrayOutbound<S>>,
    /// Array CRDT receive path: applies inbound wire messages from Origin.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) array_inbound: Arc<crate::sync::array::ArrayInbound<S>>,
    /// Per-array last-seen HLC tracker for catch-up requests.
    #[cfg(not(target_arch = "wasm32"))]
    #[allow(dead_code)]
    pub(crate) array_catchup: Arc<crate::sync::array::CatchupTracker<S>>,
    /// Outbound queue for columnar insert sync.
    ///
    /// Shared with `ColumnarEngine` so inserts are automatically enqueued.
    /// `None` when sync is disabled.
    pub(crate) columnar_outbound: Option<Arc<crate::sync::ColumnarOutbound>>,
    /// Outbound queue for vector insert/delete sync.
    ///
    /// Populated by `vector_insert_impl` / `vector_delete_impl` when sync is
    /// enabled. `None` when sync is disabled.
    pub(crate) vector_outbound: Option<Arc<crate::sync::VectorOutbound>>,
    /// Outbound queue for FTS index/delete sync.
    ///
    /// Populated by `index_document_text` / `remove_document_text` when sync
    /// is enabled. `None` when sync is disabled.
    pub(crate) fts_outbound: Option<Arc<crate::sync::FtsOutbound>>,
    /// Outbound queue for spatial geometry insert/delete sync.
    ///
    /// Populated by `spatial_insert` / `spatial_delete` when sync is enabled.
    /// `None` when sync is disabled.
    pub(crate) spatial_outbound: Option<Arc<crate::sync::SpatialOutbound>>,
    /// Outbound queue for timeseries-profile columnar insert sync.
    ///
    /// Shared with `ColumnarEngine` so timeseries inserts are automatically
    /// enqueued.  `None` when sync is disabled.
    pub(crate) timeseries_outbound: Option<Arc<crate::sync::TimeseriesOutbound>>,
    /// When `false`, KV operations go directly to redb, bypassing Loro.
    /// Other engines (vector, graph, document) are unaffected.
    pub(crate) sync_enabled: bool,
    /// Buffered KV writes awaiting batch commit to redb.
    /// Flushed on `kv_flush()`, threshold (1000 ops), or `flush()`.
    /// The HashMap overlay lets reads see uncommitted writes.
    pub(crate) kv_write_buf: Mutex<KvWriteBuffer>,
}

/// Buffered KV writes for batch commit.
///
/// # Safety: single-writer design
///
/// The overlay allowing uncommitted reads is intentional and safe because
/// `NodeDbLite` is designed for single-writer access. All public KV methods
/// acquire the outer `Mutex<KvWriteBuffer>`, which serializes every write and
/// read-through-overlay access to this buffer. There is no way for two callers
/// to observe a torn write or a half-applied overlay entry.
pub(crate) struct KvWriteBuffer {
    /// Pending write operations for batch commit.
    pub ops: Vec<crate::storage::engine::WriteOp>,
    /// Read overlay: maps redb composite key → value (None = deleted).
    /// Lets `kv_get` see uncommitted writes without hitting redb.
    pub overlay: HashMap<Vec<u8>, Option<Vec<u8>>>,
}

impl<S: StorageEngine + StorageEngineSync> NodeDbLite<S> {
    /// Open or create a Lite database backed by the given storage engine.
    ///
    /// Memory budget and per-engine percentages are resolved from environment
    /// variables via [`LiteConfig::from_env()`], falling back to defaults when
    /// variables are absent or malformed.
    pub async fn open(storage: S, peer_id: u64) -> NodeDbResult<Self>
    where
        S: crate::storage::engine::StorageEngineSync,
    {
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
    ) -> NodeDbResult<Self>
    where
        S: crate::storage::engine::StorageEngineSync,
    {
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
    ) -> NodeDbResult<Self>
    where
        S: crate::storage::engine::StorageEngineSync,
    {
        let governor = crate::memory::MemoryGovernor::new(memory_budget);
        Self::open_inner(storage, peer_id, governor, true).await
    }

    async fn open_inner(
        storage: S,
        peer_id: u64,
        governor: crate::memory::MemoryGovernor,
        sync_enabled: bool,
    ) -> NodeDbResult<Self>
    where
        S: crate::storage::engine::StorageEngineSync,
    {
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
        let hnsw_indices = Self::restore_hnsw_indices(&storage).await?;

        // ── Restore spatial indices ──
        let spatial = Self::restore_spatial_indices(&storage).await;

        // ── Restore strict document engine ──
        let strict = StrictEngine::restore(Arc::clone(&storage))
            .await
            .map_err(NodeDbError::storage)?;

        // ── Restore columnar engine ──
        let mut columnar = ColumnarEngine::restore(Arc::clone(&storage))
            .await
            .map_err(NodeDbError::storage)?;

        // Wire columnar sync outbound queue when sync is enabled.
        let columnar_outbound: Option<Arc<crate::sync::ColumnarOutbound>> = if sync_enabled {
            let q = Arc::new(crate::sync::ColumnarOutbound::new());
            columnar.set_outbound(Arc::clone(&q));
            Some(q)
        } else {
            None
        };

        // Wire vector sync outbound queue when sync is enabled.
        let vector_outbound: Option<Arc<crate::sync::VectorOutbound>> = if sync_enabled {
            Some(Arc::new(crate::sync::VectorOutbound::new()))
        } else {
            None
        };

        // Wire FTS sync outbound queue when sync is enabled.
        let fts_outbound_init: Option<Arc<crate::sync::FtsOutbound>> = if sync_enabled {
            Some(Arc::new(crate::sync::FtsOutbound::new()))
        } else {
            None
        };

        // Wire spatial sync outbound queue when sync is enabled.
        let spatial_outbound_init: Option<Arc<crate::sync::SpatialOutbound>> = if sync_enabled {
            Some(Arc::new(crate::sync::SpatialOutbound::new()))
        } else {
            None
        };

        // Wire timeseries sync outbound queue when sync is enabled.
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
        let query_engine = crate::query::LiteQueryEngine::new(
            Arc::clone(&crdt),
            Arc::clone(&strict),
            Arc::clone(&columnar),
            Arc::clone(&htap),
            Arc::clone(&storage),
            Arc::clone(&timeseries),
        );

        let array_engine =
            crate::engine::array::ArrayEngineState::open(&storage).map_err(NodeDbError::storage)?;
        let array_state = Arc::new(Mutex::new(array_engine));

        // ── Array CRDT sync state (non-wasm only) ─────────────────────────────
        #[cfg(not(target_arch = "wasm32"))]
        let array_replica = Arc::new(
            crate::sync::array::ReplicaState::load_or_init(&*storage)
                .map_err(NodeDbError::storage)?,
        );
        #[cfg(not(target_arch = "wasm32"))]
        let array_schemas = Arc::new(
            crate::sync::array::SchemaRegistry::load(
                Arc::clone(&storage),
                Arc::clone(&array_replica),
            )
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
                .map_err(NodeDbError::storage)?,
        );
        #[cfg(not(target_arch = "wasm32"))]
        let array_apply_engine = Arc::new(crate::sync::array::LiteApplyEngine::new(
            Arc::clone(&storage),
            Arc::clone(&array_state),
            Arc::clone(&array_schemas),
            Arc::clone(array_outbound.op_log()),
        ));
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
            hnsw_indices: Mutex::new(hnsw_indices),
            csr: Mutex::new(csr),
            crdt,
            governor,
            search_ef: 128,
            vector_id_map: Mutex::new(HashMap::new()),
            query_engine,
            fts: Mutex::new(fts_manager),
            spatial: Mutex::new(spatial),
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
            columnar_outbound,
            vector_outbound,
            fts_outbound: fts_outbound_init,
            spatial_outbound: spatial_outbound_init,
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
            let fts = db.fts.lock_or_recover();
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
            let indices = self.hnsw_indices.lock_or_recover();
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
            let fts = self.fts.lock_or_recover();
            let (indices, id_to_surrogate, next_surrogate) = fts.checkpoint_data();
            crate::engine::fts::checkpoint::serialize_fts(indices, id_to_surrogate, next_surrogate)?
        };
        self.storage
            .batch_write(&fts_ops)
            .await
            .map_err(NodeDbError::storage)?;

        Ok(())
    }

    /// Get or create an HNSW index for a collection.
    /// Rebuild all text indices from CRDT state.
    ///
    /// Called once on cold start after CRDT snapshot restore.
    /// Scans all collections and indexes all string fields.
    fn rebuild_text_indices(&self) {
        let crdt = self.crdt.lock_or_recover();
        let collections = crdt.collection_names();
        let mut fts = self.fts.lock_or_recover();

        for collection in &collections {
            if collection.starts_with("__") {
                continue;
            }
            let ids = crdt.list_ids(collection);
            if ids.is_empty() {
                continue;
            }

            for id in &ids {
                if let Some(loro_val) = crdt.read(collection, id) {
                    let doc = crate::nodedb::convert::loro_value_to_document(id, &loro_val);
                    let text: String = doc
                        .fields
                        .values()
                        .filter_map(|v| match v {
                            nodedb_types::Value::String(s) => Some(s.as_str()),
                            _ => None,
                        })
                        .collect::<Vec<_>>()
                        .join(" ");
                    fts.index_document(collection, id, &text);
                }
            }
        }
    }

    /// Rebuild spatial indices from CRDT state (cold start fallback).
    ///
    /// Scans all collections for geometry-valued fields and indexes them.
    /// Called when checkpoint restore produces empty spatial indices.
    fn rebuild_spatial_indices(&self) {
        let crdt = self.crdt.lock_or_recover();
        let collections = crdt.collection_names();
        let mut spatial = self.spatial.lock_or_recover();

        for collection in &collections {
            if collection.starts_with("__") {
                continue;
            }
            let ids = crdt.list_ids(collection);
            for id in &ids {
                if let Some(loro_val) = crdt.read(collection, id) {
                    let doc = crate::nodedb::convert::loro_value_to_document(id, &loro_val);
                    for (field, value) in &doc.fields {
                        // Geometry fields are stored as GeoJSON strings.
                        if let nodedb_types::Value::String(s) = value
                            && let Ok(geom) =
                                sonic_rs::from_str::<nodedb_types::geometry::Geometry>(s)
                        {
                            spatial.index_document(collection, field, id, &geom);
                        }
                    }
                }
            }
        }
    }

    /// Update the inverted text index after a document write.
    ///
    /// Called by `document_put` to keep the text index in sync.
    /// Concatenates all string fields for full-text indexing.
    pub(crate) fn index_document_text(
        &self,
        collection: &str,
        doc_id: &str,
        fields: &std::collections::HashMap<String, nodedb_types::Value>,
    ) {
        let text: String = fields
            .values()
            .filter_map(|v| match v {
                nodedb_types::Value::String(s) => Some(s.as_str()),
                _ => None,
            })
            .collect::<Vec<_>>()
            .join(" ");

        self.fts
            .lock_or_recover()
            .index_document(collection, doc_id, &text);

        // Propagate to Origin via sync outbound queue.
        if let Some(q) = &self.fts_outbound {
            q.enqueue_index(collection, doc_id, text);
        }
    }

    /// Remove a document from the text index.
    pub(crate) fn remove_document_text(&self, collection: &str, doc_id: &str) {
        self.fts
            .lock_or_recover()
            .remove_document(collection, doc_id);

        // Propagate deletion to Origin via sync outbound queue.
        if let Some(q) = &self.fts_outbound {
            q.enqueue_delete(collection, doc_id);
        }
    }

    // ── Spatial public API ────────────────────────────────────────────────────

    /// Index a geometry in a collection's spatial index.
    ///
    /// `field` identifies which geometry field is being indexed (allows a
    /// collection to carry multiple spatial fields).  If the document was
    /// previously indexed under the same `(collection, doc_id)`, the old entry
    /// is replaced (upsert semantics).
    pub fn spatial_insert(
        &self,
        collection: &str,
        field: &str,
        doc_id: &str,
        geometry: &nodedb_types::geometry::Geometry,
    ) {
        let mut spatial = self.spatial.lock_or_recover();
        spatial.index_document(collection, field, doc_id, geometry);
        drop(spatial);
        if let Some(q) = &self.spatial_outbound {
            q.enqueue_insert(collection, field, doc_id, geometry);
        }
    }

    /// Remove a document's geometry from the spatial index.
    pub fn spatial_delete(&self, collection: &str, field: &str, doc_id: &str) {
        let mut spatial = self.spatial.lock_or_recover();
        spatial.remove_document(collection, field, doc_id);
        drop(spatial);
        if let Some(q) = &self.spatial_outbound {
            q.enqueue_delete(collection, field, doc_id);
        }
    }

    /// Bounding-box range search: returns all doc entry IDs whose bbox
    /// intersects the query rectangle.
    ///
    /// Returns `(entry_id, bbox)` pairs so callers can resolve back to
    /// doc_ids through their own mapping if needed.  For the gate tests,
    /// we expose a convenience wrapper that returns entry IDs directly.
    pub fn spatial_search_bbox(
        &self,
        collection: &str,
        field: &str,
        query: &nodedb_types::BoundingBox,
    ) -> Vec<nodedb_spatial::rtree::RTreeEntry> {
        let spatial = self.spatial.lock_or_recover();
        spatial
            .search(collection, field, query)
            .into_iter()
            .cloned()
            .collect()
    }

    /// Nearest-neighbor search: returns the `k` closest spatial entries to
    /// the given `(lng, lat)` point.
    pub fn spatial_nearest(
        &self,
        collection: &str,
        field: &str,
        lng: f64,
        lat: f64,
        k: usize,
    ) -> Vec<nodedb_spatial::rtree::NnResult> {
        let spatial = self.spatial.lock_or_recover();
        spatial.nearest(collection, field, lng, lat, k)
    }

    pub(crate) fn ensure_hnsw<'a>(
        indices: &'a mut HashMap<String, HnswIndex>,
        collection: &str,
        dim: usize,
    ) -> &'a mut HnswIndex {
        indices
            .entry(collection.to_string())
            .or_insert_with(|| HnswIndex::new(dim, HnswParams::default()))
    }

    /// Update memory governor with current engine usage.
    pub fn update_memory_stats(&self) {
        if let Ok(indices) = self.hnsw_indices.lock() {
            let hnsw_bytes: usize = indices
                .values()
                .map(|idx| idx.len() * (idx.dim() * 4 + 128))
                .sum();
            self.governor.report_usage(EngineId::Hnsw, hnsw_bytes);
        }
        if let Ok(csr_map) = self.csr.lock() {
            let total: usize = csr_map
                .values()
                .map(|idx| idx.estimated_memory_bytes())
                .sum();
            self.governor.report_usage(EngineId::Csr, total);
        }
        if let Ok(crdt) = self.crdt.lock() {
            self.governor
                .report_usage(EngineId::Loro, crdt.estimated_memory_bytes());
        }
    }

    /// List currently loaded HNSW collections.
    pub fn loaded_collections(&self) -> NodeDbResult<Vec<String>> {
        let indices = self.hnsw_indices.lock_or_recover();
        Ok(indices.keys().cloned().collect())
    }

    /// Access the memory governor.
    pub fn governor(&self) -> &MemoryGovernor {
        &self.governor
    }

    /// Access the strict document engine (for direct Binary Tuple CRUD).
    ///
    /// `StrictEngine` is natively `Send + Sync` and methods take `&self`,
    /// so no outer `Mutex` is needed. Public-API note: this signature
    /// changed from `&Arc<Mutex<StrictEngine<S>>>` — external callers must
    /// drop their `.lock()` calls and call methods directly.
    pub fn strict_engine(&self) -> &Arc<StrictEngine<S>> {
        &self.strict
    }

    /// Access the columnar analytics engine (for direct segment operations).
    pub fn columnar_engine(&self) -> &Arc<crate::engine::columnar::ColumnarEngine<S>> {
        &self.columnar
    }

    /// Access the HTAP bridge (for materialized view inspection).
    pub fn htap_bridge(&self) -> &Arc<crate::engine::htap::HtapBridge> {
        &self.htap
    }

    /// Access the timeseries engine (continuous aggregates, ingest, flush).
    pub fn timeseries_engine(
        &self,
    ) -> &Arc<Mutex<crate::engine::timeseries::engine::TimeseriesEngine>> {
        &self.timeseries
    }

    // -- Indexed CRUD for strict/columnar collections --

    /// Insert a row into a strict collection and update secondary indexes.
    ///
    /// Combines `StrictEngine.insert()` with `index_row()` for geometry,
    /// vector, and text columns.
    pub async fn strict_insert(
        &self,
        collection: &str,
        values: &[nodedb_types::value::Value],
    ) -> NodeDbResult<()> {
        let schema = self.strict.schema(collection).ok_or_else(|| {
            NodeDbError::storage(format!("strict collection '{collection}' not found"))
        })?;

        // Insert into storage. `StrictEngine` is interior-mutable; await directly.
        self.strict
            .insert(collection, values)
            .await
            .map_err(NodeDbError::storage)?;

        // Build a row_id string from the PK value for index keying.
        let row_id = pk_to_string(&schema.columns, values);

        // Update secondary indexes.
        crate::engine::index_integration::index_row(
            collection,
            &row_id,
            &schema.columns,
            values,
            &self.hnsw_indices,
            &self.spatial,
            &self.fts,
        );

        // Update secondary B-tree indexes on non-PK columns.
        {
            use crate::engine::strict::secondary_index::SecondaryIndex;
            let mut sec = self.secondary_indices.lock_or_recover();
            for (i, col) in schema.columns.iter().enumerate() {
                if col.primary_key || i >= values.len() {
                    continue;
                }
                let key = format!("{collection}:{}", col.name);
                sec.entry(key)
                    .or_insert_with(|| SecondaryIndex::new(&col.name))
                    .insert(&values[i], &row_id);
            }
        }

        // Replicate to materialized columnar views (HTAP CDC).
        self.htap
            .replicate_insert(collection, values, &self.columnar);

        Ok(())
    }

    /// Delete a row from a strict collection and clean up text indexes.
    pub async fn strict_delete(
        &self,
        collection: &str,
        pk: &nodedb_types::value::Value,
    ) -> NodeDbResult<bool> {
        let schema = self.strict.schema(collection).ok_or_else(|| {
            NodeDbError::storage(format!("strict collection '{collection}' not found"))
        })?;

        let row_id = format!("{pk:?}");

        // Read old values for secondary index removal before deleting.
        // Note: secondary index removal on delete is best-effort — if we can't
        // read the old row (e.g., already deleted), we skip deindexing.
        // Stale secondary entries are cleaned up on compaction.
        // We avoid holding the strict mutex across async boundaries here.

        // Remove text index entries before deleting the row.
        crate::engine::index_integration::deindex_row_text(
            collection,
            &row_id,
            &schema.columns,
            &self.fts,
        );

        // Replicate delete to materialized columnar views (HTAP CDC).
        self.htap.replicate_delete(collection, pk, &self.columnar);

        self.strict
            .delete(collection, pk)
            .await
            .map_err(NodeDbError::storage)
    }

    /// Insert a row into a columnar collection and update secondary indexes.
    pub fn columnar_insert(
        &self,
        collection: &str,
        values: &[nodedb_types::value::Value],
    ) -> NodeDbResult<()> {
        let schema = self.columnar.schema(collection).ok_or_else(|| {
            NodeDbError::storage(format!("columnar collection '{collection}' not found"))
        })?;

        self.columnar
            .insert(collection, values)
            .map_err(NodeDbError::storage)?;

        let row_id = pk_to_string(&schema.columns, values);

        crate::engine::index_integration::index_row(
            collection,
            &row_id,
            &schema.columns,
            values,
            &self.hnsw_indices,
            &self.spatial,
            &self.fts,
        );

        // Spatial profile: compute geohash for Point geometries and store
        // in the text index for prefix-based proximity queries.
        if let Some(profile) = self.columnar.profile(collection)
            && let Some((_idx, geom)) = crate::engine::columnar::spatial_profile::extract_geometry(
                &schema, &profile, values,
            )
            && let Some(hash) = crate::engine::columnar::spatial_profile::compute_geohash(&geom)
        {
            self.fts
                .lock_or_recover()
                .index_field(collection, "_geohash", &row_id, &hash);
        }
        Ok(())
    }

    /// Apply a CRDT field-level update to a strict collection row.
    ///
    /// Used during sync: a remote delta specifies field changes for a row.
    /// This reads the current tuple, patches the fields, and writes back.
    pub async fn strict_crdt_patch(
        &self,
        collection: &str,
        pk: &nodedb_types::value::Value,
        field_updates: &std::collections::HashMap<String, nodedb_types::value::Value>,
    ) -> NodeDbResult<()> {
        let schema = self.strict.schema(collection).ok_or_else(|| {
            NodeDbError::storage(format!("strict collection '{collection}' not found"))
        })?;

        // Read existing tuple.
        let existing = self
            .strict
            .get(collection, pk)
            .await
            .map_err(NodeDbError::storage)?
            .ok_or_else(|| NodeDbError::storage("row not found for CRDT patch"))?;

        // Re-encode as tuple bytes for the adapter.
        let encoder = nodedb_strict::TupleEncoder::new(&schema);
        let tuple_bytes = encoder
            .encode(&existing)
            .map_err(|e| NodeDbError::storage(e.to_string()))?;

        // Apply the CRDT patch.
        let patched = crate::engine::strict::crdt_adapter::apply_crdt_set(
            &tuple_bytes,
            &schema,
            field_updates,
        )
        .map_err(NodeDbError::storage)?;

        // Decode patched tuple back to values and update.
        let decoder = nodedb_strict::TupleDecoder::new(&schema);
        let new_values = decoder
            .extract_all(&patched)
            .map_err(|e| NodeDbError::storage(e.to_string()))?;

        // Write back via the standard update path.
        self.strict
            .update_by_values(collection, pk, &new_values)
            .await
            .map_err(NodeDbError::storage)?;

        Ok(())
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

    /// Import remote deltas from Origin.
    pub fn import_remote_deltas(&self, data: &[u8]) -> NodeDbResult<()> {
        let crdt = self.crdt.lock_or_recover();
        crdt.import_remote(data).map_err(NodeDbError::storage)
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
        let client = Arc::new(crate::sync::SyncClient::new(config, self.peer_id()));
        let delegate: Arc<dyn crate::sync::SyncDelegate> = Arc::clone(self) as _;
        let client_clone = Arc::clone(&client);
        tokio::spawn(async move {
            crate::sync::run_sync_loop(client_clone, delegate).await;
        });
        client
    }

    /// Get the peer ID (from the CRDT engine).
    pub fn peer_id(&self) -> u64 {
        self.crdt.lock().map(|c| c.peer_id()).unwrap_or(0)
    }
}

/// Build a string row ID from PK column values (for index keying).
fn pk_to_string(
    columns: &[nodedb_types::columnar::ColumnDef],
    values: &[nodedb_types::value::Value],
) -> String {
    use nodedb_types::value::Value;
    let mut parts = Vec::new();
    for (i, col) in columns.iter().enumerate() {
        if col.primary_key
            && let Some(val) = values.get(i)
        {
            match val {
                Value::Integer(n) => parts.push(n.to_string()),
                Value::String(s) => parts.push(s.clone()),
                Value::Uuid(s) => parts.push(s.clone()),
                other => parts.push(format!("{other:?}")),
            }
        }
    }
    parts.join(":")
}
