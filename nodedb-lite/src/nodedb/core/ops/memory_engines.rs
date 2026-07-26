// SPDX-License-Identifier: Apache-2.0

//! Memory governor reporting and per-engine handle accessors.

use std::sync::{Arc, Mutex};

use nodedb_types::error::NodeDbResult;

use crate::engine::strict::StrictEngine;
use crate::memory::{EngineId, MemoryGovernor};
use crate::nodedb::core::types::NodeDbLite;
use crate::nodedb::lock_ext::LockExt;
use crate::storage::engine::StorageEngine;

impl<S: StorageEngine> NodeDbLite<S> {
    /// Update memory governor with current engine usage.
    pub fn update_memory_stats(&self) {
        if let Ok(indices) = self.vector_state.hnsw_indices.lock() {
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
        let indices = self.vector_state.hnsw_indices.lock_or_recover();
        Ok(indices.keys().cloned().collect())
    }

    /// Access the memory governor.
    pub fn governor(&self) -> &MemoryGovernor {
        &self.governor
    }

    /// Access the strict document engine (for direct Binary Tuple CRUD).
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
}
