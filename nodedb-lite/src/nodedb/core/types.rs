// SPDX-License-Identifier: Apache-2.0

//! `NodeDbLite` struct definition and storage key constants.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use nodedb_mem::{MemoryGovernor, ReservationToken};

use crate::engine::columnar::ColumnarEngine;
use crate::engine::crdt::CrdtEngine;
use crate::engine::fts::FtsState;
use crate::engine::graph::index::CsrIndex;
use crate::engine::htap::HtapBridge;
use crate::engine::sparse_vector::SparseVectorState;
use crate::engine::strict::StrictEngine;
use crate::engine::vector::VectorState;
use crate::storage::engine::StorageEngine;

/// Storage key constants.
pub(crate) const META_HNSW_COLLECTIONS: &[u8] = b"meta:hnsw_collections";
/// Legacy single-CSR checkpoint key (pre-0.1.0). Ignored on open; deleted if present.
pub(crate) const META_CSR_LEGACY: &[u8] = b"meta:csr_checkpoint";
/// List of collection names that have a CSR checkpoint (MessagePack Vec<String>).
pub(crate) const META_CSR_COLLECTIONS: &[u8] = b"meta:csr_collections";
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
    pub(crate) governor: Arc<MemoryGovernor>,
    /// Held reservation for HNSW vector-index memory, last reported by
    /// `update_memory_stats`. Replaced (drop old, charge new) on every
    /// report so accounting tracks the current footprint, not the sum of
    /// every report.
    pub(crate) vector_mem_token: Mutex<Option<ReservationToken>>,
    /// Held reservation for CSR graph-index memory. Same replace-on-report
    /// discipline as `vector_mem_token`.
    pub(crate) graph_mem_token: Mutex<Option<ReservationToken>>,
    /// Held reservation for CRDT/Loro memory. Same replace-on-report
    /// discipline as `vector_mem_token`.
    pub(crate) crdt_mem_token: Mutex<Option<ReservationToken>>,
    /// SQL query engine (DataFusion over Loro documents and strict collections).
    pub(crate) query_engine: crate::query::LiteQueryEngine<S>,
    /// Shared FTS runtime state.
    pub(crate) fts_state: Arc<FtsState>,
    /// Shared sparse-vector inverted index state.
    pub(crate) sparse_state: Arc<SparseVectorState>,
    /// Spatial R-tree indexes for geometry fields.
    pub(crate) spatial: Arc<Mutex<crate::engine::spatial::SpatialIndexManager>>,
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
    /// Per-array [`SchemaDoc`] registry (persisted Loro snapshots).
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) array_schemas: Arc<crate::sync::array::SchemaRegistry<S>>,
    /// Array CRDT send path: op-log + pending queue emitters.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) array_outbound: Arc<crate::sync::array::ArrayOutbound<S>>,
    /// Array CRDT receive path: applies inbound wire messages from Origin.
    ///
    /// Owns the shared replica identity and the per-array catch-up tracker.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) array_inbound: Arc<crate::sync::array::ArrayInbound<S>>,
    /// Per-stream monotonic sequence frontier for outbound frame stamping.
    ///
    /// Loaded once from `Namespace::Meta` at `open()` and never reset on
    /// reconnect. The 7b outbound push path calls `stream_seq.next_seq(stream_id)`
    /// to obtain a durable, monotonically-increasing seq for each frame. The 7b
    /// inbound ack path calls `stream_seq.record_ack(stream_id, seq)` to advance
    /// the acknowledged frontier.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) stream_seq: Arc<crate::sync::StreamSeqTracker<S>>,
    /// Durable outbound queue for columnar insert sync. `None` when sync is disabled.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) columnar_outbound: Option<Arc<crate::sync::ColumnarOutbound<S>>>,
    /// Durable outbound queue for vector insert/delete sync. `None` when sync is disabled.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) vector_outbound: Option<Arc<crate::sync::VectorOutbound<S>>>,
    /// Durable outbound queue for FTS index/delete sync. `None` when sync is disabled.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fts_outbound: Option<Arc<crate::sync::FtsOutbound<S>>>,
    /// Durable outbound queue for spatial geometry insert/delete sync. `None` when sync is disabled.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) spatial_outbound: Option<Arc<crate::sync::SpatialOutbound<S>>>,
    /// Durable outbound queue for timeseries-profile columnar insert sync. `None` when sync is disabled.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) timeseries_outbound: Option<Arc<crate::sync::TimeseriesOutbound<S>>>,
    /// Durable outbound queue for KV write sync. `None` when sync is disabled.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) kv_outbound: Option<Arc<crate::sync::KvOutbound<S>>>,
    /// This instance's durable identity: `lite_id`, `epoch`, and the Loro peer
    /// id every local operation is authored under.
    ///
    /// Loaded at `open()` via `LiteIdentity::load_or_create` and mutable
    /// thereafter: Origin can refuse the peer id as another replica's, or
    /// report the whole producer identity as forked, and both are recovered by
    /// replacing the identity in place rather than by reopening the database.
    /// Every change is persisted before it is adopted, so a rotation cannot be
    /// forgotten across a restart and resurrect an id Origin already refused.
    pub(crate) identity: Mutex<crate::identity::LiteIdentity>,
    /// Serializes identity replacements against each other.
    ///
    /// Replacing an identity spans an await (it persists before it is adopted),
    /// which `identity`'s guard cannot. Two refusals in flight would otherwise
    /// interleave — each minting from the same starting value, the second
    /// overwriting the first — leaving the documents re-authored under an id
    /// the persisted record no longer names.
    pub(crate) identity_change: tokio::sync::Mutex<()>,
    /// Serializes `flush` calls against each other.
    ///
    /// A flush plans its CRDT writes under the `crdt` guard, releases it while
    /// the batch commits, then re-takes it to record what is now durable — a
    /// span no `std::sync` guard can cover. Two flushes in flight would each
    /// plan from the same bookkeeping and hand out the same update sequence,
    /// so one batch's checkpoint could retire entries the other had just
    /// written under those numbers, leaving updates on disk that no later
    /// checkpoint knows to delete. Serializing them keeps the sequence a
    /// single writer's to allocate.
    pub(crate) flush_lock: tokio::sync::Mutex<()>,
    /// The KV write buffer and read cache, shared with the query engine so a
    /// SQL-path `TRUNCATE` forgets what they hold for the cleared collection.
    pub(crate) kv_local: Arc<super::kv_local::KvLocalState>,
    /// Optional per-document sync gate. When set, each document write consults
    /// it; documents the gate rejects are kept local-only — excluded from the
    /// CRDT delta push, the FTS index sync, and the vector insert sync. Used by
    /// hosts (e.g. ma8e) to keep confidential entries from leaving the machine.
    /// Set-once-at-startup; read on every write, so `RwLock` keeps reads cheap.
    pub(crate) sync_gate: std::sync::RwLock<Option<std::sync::Arc<dyn SyncGate>>>,
    /// Background tasks started by this database: auto-flush, auto-compact,
    /// and the sync loop.
    ///
    /// Every long-lived task registers here so [`NodeDbLite::shutdown`] can
    /// stop it before the host drops its async runtime. A task left detached
    /// keeps polling through that teardown.
    pub(crate) tasks: crate::tasks::TaskRegistry,
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

#[cfg(test)]
mod tests {
    use nodedb_client::NodeDb;
    use nodedb_types::document::Document;
    use nodedb_types::id::NodeId;
    use nodedb_types::value::Value;

    use crate::PagedbStorageMem;

    use super::NodeDbLite;

    async fn make_db() -> std::sync::Arc<NodeDbLite<PagedbStorageMem>> {
        let storage = PagedbStorageMem::open_in_memory().await.unwrap();
        NodeDbLite::open(storage).await.unwrap()
    }

    #[tokio::test]
    async fn open_empty_db() {
        let db = make_db().await;
        assert_eq!(db.governor().total_allocated(), 0);
    }

    #[tokio::test]
    async fn vector_insert_and_search() {
        let db = make_db().await;

        db.vector_insert("embeddings", "v1", &[1.0, 0.0, 0.0], None)
            .await
            .unwrap();
        db.vector_insert("embeddings", "v2", &[0.0, 1.0, 0.0], None)
            .await
            .unwrap();
        db.vector_insert("embeddings", "v3", &[0.0, 0.0, 1.0], None)
            .await
            .unwrap();

        let results = db
            .vector_search("embeddings", &[1.0, 0.0, 0.0], 2, None, None)
            .await
            .unwrap();

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].id, "v1"); // Closest.
    }

    #[tokio::test]
    async fn vector_delete() {
        let db = make_db().await;
        db.vector_insert("coll", "v1", &[1.0, 0.0], None)
            .await
            .unwrap();
        db.vector_delete("coll", "v1").await.unwrap();

        let results = db
            .vector_search("coll", &[1.0, 0.0], 5, None, None)
            .await
            .unwrap();
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn graph_insert_and_traverse() {
        let db = make_db().await;

        db.graph_insert_edge(
            "social",
            &NodeId::from_validated("alice".to_string()),
            &NodeId::from_validated("bob".to_string()),
            "KNOWS",
            None,
        )
        .await
        .unwrap();
        db.graph_insert_edge(
            "social",
            &NodeId::from_validated("bob".to_string()),
            &NodeId::from_validated("carol".to_string()),
            "KNOWS",
            None,
        )
        .await
        .unwrap();

        let subgraph = db
            .graph_traverse(
                "social",
                &NodeId::from_validated("alice".to_string()),
                2,
                nodedb_types::graph::Direction::Out,
                None,
            )
            .await
            .unwrap();

        assert!(subgraph.node_count() >= 2);
        assert!(subgraph.edge_count() >= 1);
    }

    #[tokio::test]
    async fn graph_delete_edge() {
        let db = make_db().await;
        let edge_id = db
            .graph_insert_edge(
                "test",
                &NodeId::from_validated("a".to_string()),
                &NodeId::from_validated("b".to_string()),
                "L",
                None,
            )
            .await
            .unwrap();

        db.graph_delete_edge("test", &edge_id).await.unwrap();

        let subgraph = db
            .graph_traverse(
                "test",
                &NodeId::from_validated("a".to_string()),
                1,
                nodedb_types::graph::Direction::Out,
                None,
            )
            .await
            .unwrap();
        assert_eq!(subgraph.edge_count(), 0);
    }

    #[tokio::test]
    async fn document_crud() {
        let db = make_db().await;

        let doc = db.document_get("notes", "n1").await.unwrap();
        assert!(doc.is_none());

        let mut doc = Document::new("n1");
        doc.set("title", Value::String("Hello".into()));
        doc.set("score", Value::Float(9.5));
        db.document_put("notes", doc).await.unwrap();

        let doc = db.document_get("notes", "n1").await.unwrap().unwrap();
        assert_eq!(doc.id, "n1");
        assert_eq!(doc.get_str("title"), Some("Hello"));

        db.document_delete("notes", "n1").await.unwrap();
        let doc = db.document_get("notes", "n1").await.unwrap();
        assert!(doc.is_none());
    }

    #[tokio::test]
    async fn sql_basic_query() {
        let db = make_db().await;
        let result = db.execute_sql("SELECT 1 AS value", &[]).await.unwrap();
        assert_eq!(result.row_count(), 1);
        assert_eq!(result.columns, vec!["value"]);
    }

    #[tokio::test]
    async fn sql_query_documents() {
        let db = make_db().await;
        let mut doc1 = Document::new("u1");
        doc1.set("name", Value::String("Alice".into()));
        doc1.set("age", Value::Integer(30));
        db.document_put("users", doc1).await.unwrap();

        let mut doc2 = Document::new("u2");
        doc2.set("name", Value::String("Bob".into()));
        doc2.set("age", Value::Integer(25));
        db.document_put("users", doc2).await.unwrap();

        let result = db
            .execute_sql("SELECT id, document FROM users", &[])
            .await
            .unwrap();
        assert_eq!(result.row_count(), 2);
    }

    #[tokio::test]
    async fn flush_and_reopen() {
        {
            let s = PagedbStorageMem::open_in_memory().await.unwrap();
            let db = NodeDbLite::open(s).await.unwrap();

            let mut doc = Document::new("d1");
            doc.set("key", Value::String("val".into()));
            db.document_put("docs", doc).await.unwrap();
            db.graph_insert_edge(
                "test",
                &NodeId::from_validated("x".to_string()),
                &NodeId::from_validated("y".to_string()),
                "REL",
                None,
            )
            .await
            .unwrap();

            db.flush().await.unwrap();

            let doc = db.document_get("docs", "d1").await.unwrap();
            assert!(doc.is_some());
        }
    }

    #[tokio::test]
    async fn crdt_deltas_generated() {
        let db = make_db().await;

        let mut doc = Document::new("d1");
        doc.set("x", Value::Integer(42));
        db.document_put("docs", doc).await.unwrap();

        let deltas = db.pending_crdt_deltas().unwrap();
        assert!(!deltas.is_empty());
    }

    #[tokio::test]
    async fn acknowledge_deltas() {
        let db = make_db().await;

        db.document_put("a", Document::new("1")).await.unwrap();
        db.document_put("a", Document::new("2")).await.unwrap();

        let deltas = db.pending_crdt_deltas().unwrap();
        assert_eq!(deltas.len(), 2);

        // Acks are per-mutation: each acknowledged delta retires on its own
        // ack, and an un-acknowledged delta is never retired as a side effect
        // of a later one.
        for id in deltas.iter().map(|d| d.mutation_id) {
            db.acknowledge_deltas(id).unwrap();
        }

        let deltas = db.pending_crdt_deltas().unwrap();
        assert!(deltas.is_empty());
    }

    #[tokio::test]
    async fn memory_governor_tracks_usage() {
        let db = make_db().await;

        for i in 0..100 {
            db.vector_insert("vecs", &format!("v{i}"), &[i as f32, 0.0, 0.0], None)
                .await
                .unwrap();
        }

        assert!(db.governor().total_allocated() > 0);
    }

    #[tokio::test]
    async fn search_nonexistent_collection() {
        let db = make_db().await;
        let results = db
            .vector_search("no_such_collection", &[1.0], 5, None, None)
            .await
            .unwrap();
        assert!(results.is_empty());
    }

    /// Verify that a vector insert is rejected with Backpressure when the
    /// memory governor reports Emergency pressure.
    ///
    /// Strategy: open a db with a tiny budget so that a first insert (which
    /// calls `update_memory_stats` at the end) pushes reported usage over the
    /// 95% threshold, then assert the second insert returns a Backpressure
    /// error.
    #[tokio::test]
    async fn vector_insert_rejected_at_emergency_pressure() {
        use crate::config::LiteConfig;
        use nodedb_mem::PressureLevel;

        // Budget is tiny (1 byte) so any HNSW usage immediately reports Emergency.
        let config = LiteConfig {
            memory_budget: 1,
            ..LiteConfig::default()
        };
        let storage = PagedbStorageMem::open_in_memory().await.unwrap();
        let db = NodeDbLite::open_with_config(storage, config).await.unwrap();

        // First insert: succeeds and updates memory stats so the governor
        // reports Emergency after this call returns.
        db.vector_insert("embeddings", "v1", &[1.0, 0.0, 0.0], None)
            .await
            .unwrap();

        // Confirm the governor is now Emergency before the second insert.
        assert_eq!(
            db.governor().worst_engine_pressure(),
            PressureLevel::Emergency,
            "governor should be Emergency after first insert with 1-byte budget"
        );

        // Second insert must be rejected with a Backpressure error.
        let result = db
            .vector_insert("embeddings", "v2", &[0.0, 1.0, 0.0], None)
            .await;

        assert!(
            result.is_err(),
            "second vector insert should fail under Emergency pressure"
        );
        let err_str = result.unwrap_err().to_string();
        assert!(
            err_str.contains("backpressure") || err_str.contains("Backpressure"),
            "error should mention backpressure, got: {err_str}"
        );
    }
}
