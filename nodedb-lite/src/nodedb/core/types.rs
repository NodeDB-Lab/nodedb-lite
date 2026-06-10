// SPDX-License-Identifier: Apache-2.0

//! `NodeDbLite` struct definition and storage key constants.

use std::collections::HashMap;
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, Mutex};

use crate::engine::columnar::ColumnarEngine;
use crate::engine::crdt::CrdtEngine;
use crate::engine::fts::FtsState;
use crate::engine::graph::index::CsrIndex;
use crate::engine::htap::HtapBridge;
use crate::engine::strict::StrictEngine;
use crate::engine::vector::VectorState;
use crate::memory::MemoryGovernor;
use crate::storage::engine::StorageEngine;

/// Storage key constants.
pub(crate) const META_HNSW_COLLECTIONS: &[u8] = b"meta:hnsw_collections";
/// Legacy single-CSR checkpoint key (pre-0.1.0). Ignored on open; deleted if present.
pub(crate) const META_CSR_LEGACY: &[u8] = b"meta:csr_checkpoint";
/// List of collection names that have a CSR checkpoint (MessagePack Vec<String>).
pub(crate) const META_CSR_COLLECTIONS: &[u8] = b"meta:csr_collections";
pub(crate) const META_CRDT_SNAPSHOT: &[u8] = b"crdt:snapshot";
pub(crate) const META_CRDT_DELTAS: &[u8] = b"crdt:pending_deltas";
/// Last flushed mutation_id — used for partial flush safety.
pub(crate) const META_LAST_FLUSHED_MID: &[u8] = b"meta:last_flushed_mid";

/// NodeDB-Lite — the embedded edge database.
///
/// Fully capable of vector search, graph traversal, and document CRUD
/// entirely offline. Optional sync to Origin via WebSocket.
pub struct NodeDbLite<S: StorageEngine> {
    pub(crate) storage: Arc<S>,
    /// Shared HNSW runtime state (indices, ID map, search_ef).
    pub(crate) vector_state: Arc<VectorState<S>>,
    /// Per-collection CSR graph indices, keyed by collection name.
    pub(crate) csr: Arc<Mutex<HashMap<String, CsrIndex>>>,
    /// CRDT engine for delta generation and sync.
    /// Arc-wrapped for sharing with the query engine's TableProvider.
    pub(crate) crdt: Arc<Mutex<CrdtEngine>>,
    /// Memory budget governor.
    pub(crate) governor: MemoryGovernor,
    /// SQL query engine (DataFusion over Loro documents and strict collections).
    pub(crate) query_engine: crate::query::LiteQueryEngine<S>,
    /// Shared FTS runtime state.
    pub(crate) fts_state: Arc<FtsState>,
    /// Spatial R-tree indexes for geometry fields.
    pub(crate) spatial: Arc<Mutex<crate::engine::spatial::SpatialIndexManager>>,
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
    pub(crate) htap: Arc<HtapBridge>,
    /// Lite timeseries engine.
    pub(crate) timeseries: Arc<Mutex<crate::engine::timeseries::engine::TimeseriesEngine>>,
    /// Array engine in-memory state (storage-agnostic; calls via NodeDbLite methods).
    ///
    /// `Arc`-wrapped so it can be shared with [`crate::sync::array::LiteApplyEngine`]
    /// for the inbound receive path without borrowing `NodeDbLite`.
    pub(crate) array_state: Arc<tokio::sync::Mutex<crate::engine::array::engine::ArrayEngineState>>,
    /// Stable per-replica identity + HLC generator for array CRDT sync.
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
    /// Outbound queue for columnar insert sync. `None` when sync is disabled.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) columnar_outbound: Option<Arc<crate::sync::ColumnarOutbound>>,
    /// Outbound queue for vector insert/delete sync. `None` when sync is disabled.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) vector_outbound: Option<Arc<crate::sync::VectorOutbound>>,
    /// Outbound queue for FTS index/delete sync. `None` when sync is disabled.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fts_outbound: Option<Arc<crate::sync::FtsOutbound>>,
    /// Outbound queue for spatial geometry insert/delete sync. `None` when sync is disabled.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) spatial_outbound: Option<Arc<crate::sync::SpatialOutbound>>,
    /// Outbound queue for timeseries-profile columnar insert sync. `None` when sync is disabled.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) timeseries_outbound: Option<Arc<crate::sync::TimeseriesOutbound>>,
    /// When `false`, KV operations go directly to storage, bypassing Loro.
    pub(crate) sync_enabled: bool,
    /// Buffered KV writes awaiting batch commit to storage.
    /// Flushed on `kv_flush()`, threshold (1000 ops), or `flush()`.
    /// The HashMap overlay lets reads see uncommitted writes.
    pub(crate) kv_write_buf: Mutex<KvWriteBuffer>,
    /// In-memory LRU cache for KV get hot path.
    ///
    /// Stores raw encoded bytes (8-byte LE deadline + user value) keyed by the
    /// composite KV key (`{collection}\0{user_key}`). TTL expiry is re-checked
    /// on every cache hit so no entry is served past its deadline.
    ///
    /// Capacity is controlled by [`crate::config::LiteConfig::kv_cache_capacity`].
    pub(crate) kv_cache: Mutex<lru::LruCache<Vec<u8>, Vec<u8>>>,
    /// Mirrors `kv_write_buf.overlay.len()` for lock-free fast-path reads.
    /// Updated under the same lock as the overlay; readers may load with
    /// `Acquire` to skip the mutex when the overlay is empty. Single-writer
    /// design means `Release` stores happen-before the lock release; readers
    /// that see 0 observe a consistent snapshot.
    pub(crate) kv_overlay_len: AtomicUsize,
    /// Optional per-document sync gate. When set, each document write consults
    /// it; documents the gate rejects are kept local-only — excluded from the
    /// CRDT delta push, the FTS index sync, and the vector insert sync. Used by
    /// hosts (e.g. ma8e) to keep confidential entries from leaving the machine.
    /// Set-once-at-startup; read on every write, so `RwLock` keeps reads cheap.
    pub(crate) sync_gate: std::sync::RwLock<Option<std::sync::Arc<dyn SyncGate>>>,
}

/// Per-document policy deciding whether a write may leave this node via sync.
///
/// Returning `false` keeps the document local-only: it is still written to local
/// CRDT state, the local FTS index, and the local vector index (so local reads
/// and search work), but it is excluded from every outbound sync channel.
pub trait SyncGate: Send + Sync {
    /// Decide whether a document write should be synced. Called with the
    /// collection name and the document's fields (so the policy can inspect,
    /// e.g., a `share` field).
    fn should_sync(&self, collection: &str, fields: &HashMap<String, nodedb_types::Value>) -> bool;
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
    /// Read overlay: maps composite KV key → value (None = deleted).
    /// Lets `kv_get` see uncommitted writes without hitting storage.
    pub overlay: HashMap<Vec<u8>, Option<Vec<u8>>>,
}
