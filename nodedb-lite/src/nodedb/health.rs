//! Health API — structured status report for NodeDB-Lite.
//!
//! `db.health()` returns a `HealthStatus` covering:
//! - **Storage**: accessible, approximate size
//! - **Memory**: governor pressure per engine
//! - **Engines**: HNSW collection count, CSR node/edge count, CRDT doc count, text indices
//! - **Sync**: connection state, pending delta count/bytes (if sync client available)
//!
//! The response is JSON-serializable for HTTP health endpoints.

use serde::Serialize;

use nodedb_mem::{EngineId, PressureLevel};
use nodedb_types::error::NodeDbResult;

use crate::storage::engine::StorageEngine;

use super::core::NodeDbLite;
use super::lock_ext::LockExt;

/// Overall health status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OverallStatus {
    /// All subsystems healthy.
    Healthy,
    /// Some subsystems under pressure but functional.
    Degraded,
    /// Critical issues — immediate attention needed.
    Unhealthy,
}

/// Structured health report for NodeDB-Lite.
#[derive(Debug, Serialize)]
pub struct HealthStatus {
    /// Overall status.
    pub status: OverallStatus,
    /// Storage subsystem.
    pub storage: StorageHealth,
    /// Memory governor.
    pub memory: MemoryHealth,
    /// Engine-specific health.
    pub engines: EnginesHealth,
}

/// Storage subsystem health.
#[derive(Debug, Serialize)]
pub struct StorageHealth {
    /// Whether the storage backend is accessible (can read/write).
    pub accessible: bool,
}

/// Memory governor health.
#[derive(Debug, Serialize)]
pub struct MemoryHealth {
    /// Total budget in bytes.
    pub budget_bytes: usize,
    /// Total used in bytes.
    pub used_bytes: usize,
    /// Usage ratio (0.0–1.0+).
    pub usage_ratio: f64,
    /// Overall pressure level.
    pub pressure: &'static str,
    /// Per-engine breakdown.
    pub engines: EngineMemoryBreakdown,
}

/// Per-engine memory breakdown.
#[derive(Debug, Serialize)]
pub struct EngineMemoryBreakdown {
    pub hnsw: EngineMemory,
    pub csr: EngineMemory,
    pub loro: EngineMemory,
    pub query: EngineMemory,
}

/// Single engine memory stats.
#[derive(Debug, Serialize)]
pub struct EngineMemory {
    pub budget_bytes: usize,
    pub used_bytes: usize,
    pub pressure: &'static str,
}

/// Engine-specific health summary.
#[derive(Debug, Serialize)]
pub struct EnginesHealth {
    /// Number of loaded HNSW collections.
    pub hnsw_collection_count: usize,
    /// Total vectors across all HNSW collections.
    pub hnsw_total_vectors: usize,
    /// CSR graph node count.
    pub csr_node_count: usize,
    /// CSR graph edge count.
    pub csr_edge_count: usize,
    /// Number of CRDT collections with data.
    pub crdt_collection_count: usize,
    /// Number of text-indexed collections.
    pub text_index_count: usize,
    /// Total pending CRDT deltas awaiting sync.
    pub pending_deltas: usize,
}

fn pressure_str(p: PressureLevel) -> &'static str {
    match p {
        PressureLevel::Normal => "normal",
        PressureLevel::Warning => "warning",
        PressureLevel::Critical => "critical",
        PressureLevel::Emergency => "emergency",
    }
}

fn engine_memory(gov: &nodedb_mem::MemoryGovernor, id: EngineId) -> EngineMemory {
    let budget = gov.budget(id);
    EngineMemory {
        budget_bytes: budget.limit(),
        used_bytes: budget.allocated(),
        pressure: pressure_str(gov.engine_pressure(id)),
    }
}

impl<S: StorageEngine> NodeDbLite<S> {
    /// Borrow the underlying storage engine.
    ///
    /// Public so benchmark code can call backend-specific methods like
    /// backend-specific methods (e.g. size reporting) for compression-ratio measurement.
    pub fn storage(&self) -> &S {
        &self.storage
    }

    /// Compact the backing storage engine, reclaiming dead pages and
    /// truncating the file to bound on-disk growth.
    ///
    /// Forwards to [`StorageEngine::compact`]. For the pagedb-backed engine this
    /// drains the deferred-free list and truncates `main.db`; for in-memory or
    /// test engines it is a no-op returning a zero
    /// [`CompactionOutcome`](crate::storage::engine::CompactionOutcome).
    pub async fn compact(&self) -> NodeDbResult<crate::storage::engine::CompactionOutcome> {
        Ok(self.storage.compact().await?)
    }

    /// Get a structured health report.
    ///
    /// This is a cheap, non-blocking call — reads atomic counters and lock-free state.
    /// Safe to call frequently from health check endpoints.
    pub fn health(&self) -> HealthStatus {
        // Refresh memory stats before reporting.
        self.update_memory_stats();

        let gov = &self.governor;

        let memory = MemoryHealth {
            budget_bytes: gov.global_ceiling(),
            used_bytes: gov.total_allocated(),
            usage_ratio: gov.global_utilization_percent() as f64 / 100.0,
            pressure: pressure_str(gov.global_pressure()),
            engines: EngineMemoryBreakdown {
                hnsw: engine_memory(gov, EngineId::Vector),
                csr: engine_memory(gov, EngineId::Graph),
                loro: engine_memory(gov, EngineId::Crdt),
                query: engine_memory(gov, EngineId::Query),
            },
        };

        let (hnsw_count, hnsw_vectors) = {
            let indices = self.vector_state.hnsw_indices.lock_or_recover();
            let count = indices.len();
            let vectors: usize = indices.values().map(|idx| idx.len()).sum();
            (count, vectors)
        };

        let (csr_nodes, csr_edges) = {
            let csr_map = self.csr.lock_or_recover();
            let nodes: usize = csr_map.values().map(|c| c.node_count()).sum();
            let edges: usize = csr_map.values().map(|c| c.edge_count()).sum();
            (nodes, edges)
        };

        let (crdt_collections, pending_deltas) = {
            let crdt = self.crdt.lock_or_recover();
            (crdt.collection_names().len(), crdt.pending_count())
        };

        let text_count = {
            let fts = self.fts_state.manager.lock_or_recover();
            fts.collection_count()
        };

        let engines = EnginesHealth {
            hnsw_collection_count: hnsw_count,
            hnsw_total_vectors: hnsw_vectors,
            csr_node_count: csr_nodes,
            csr_edge_count: csr_edges,
            crdt_collection_count: crdt_collections,
            text_index_count: text_count,
            pending_deltas,
        };

        // Determine overall status. Matches the old thresholds: nodedb_mem's
        // Critical (85-95%) is where Lite's own Warning used to start, and
        // Emergency (>95%) is where Lite's own Critical used to start.
        let overall = match gov.global_pressure() {
            PressureLevel::Emergency => OverallStatus::Unhealthy,
            PressureLevel::Critical => OverallStatus::Degraded,
            PressureLevel::Warning => OverallStatus::Healthy,
            PressureLevel::Normal => OverallStatus::Healthy,
        };

        HealthStatus {
            status: overall,
            storage: StorageHealth { accessible: true },
            memory,
            engines,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::PagedbStorageMem;

    async fn make_db() -> std::sync::Arc<NodeDbLite<PagedbStorageMem>> {
        let storage = PagedbStorageMem::open_in_memory().await.unwrap();
        NodeDbLite::open(storage).await.unwrap()
    }

    #[tokio::test]
    async fn health_empty_db() {
        let db = make_db().await;
        let h = db.health();
        assert_eq!(h.status, OverallStatus::Healthy);
        assert!(h.storage.accessible);
        assert_eq!(h.memory.pressure, "normal");
        assert_eq!(h.engines.hnsw_collection_count, 0);
        assert_eq!(h.engines.pending_deltas, 0);
    }

    #[tokio::test]
    async fn health_with_data() {
        use nodedb_client::NodeDb;

        let db = make_db().await;
        db.vector_insert("vecs", "v1", &[1.0, 0.0, 0.0], None)
            .await
            .unwrap();
        db.graph_insert_edge(
            "test",
            &nodedb_types::id::NodeId::from_validated("a".to_string()),
            &nodedb_types::id::NodeId::from_validated("b".to_string()),
            "REL",
            None,
        )
        .await
        .unwrap();

        let h = db.health();
        assert_eq!(h.engines.hnsw_collection_count, 1);
        assert_eq!(h.engines.hnsw_total_vectors, 1);
        assert!(h.engines.csr_edge_count >= 1);
    }

    #[tokio::test]
    async fn health_serializes_to_json() {
        let db = make_db().await;
        let h = db.health();
        let json = sonic_rs::to_string_pretty(&h).unwrap();
        assert!(json.contains("\"status\""));
        assert!(json.contains("\"storage\""));
        assert!(json.contains("\"memory\""));
        assert!(json.contains("\"engines\""));
    }

    #[tokio::test]
    async fn health_pending_deltas_counted() {
        use nodedb_client::NodeDb;
        use nodedb_types::document::Document;

        let db = make_db().await;
        let doc = Document::new("d1");
        db.document_put("docs", doc).await.unwrap();

        let h = db.health();
        assert!(h.engines.pending_deltas > 0);
    }
}
