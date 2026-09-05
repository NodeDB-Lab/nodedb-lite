// SPDX-License-Identifier: Apache-2.0

//! Memory governor reporting and per-engine handle accessors.

use std::sync::{Arc, Mutex};

use nodedb_mem::{EngineId, MemoryGovernor, ReservationToken, ScopedMemory};
use nodedb_types::error::NodeDbResult;
use nodedb_types::{DatabaseId, TenantId};

use crate::engine::strict::StrictEngine;
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
            self.charge_engine_usage(&self.vector_mem_token, EngineId::Vector, hnsw_bytes);
        }
        if let Ok(csr_map) = self.csr.lock() {
            let total: usize = csr_map
                .values()
                .map(|idx| idx.estimated_memory_bytes())
                .sum();
            self.charge_engine_usage(&self.graph_mem_token, EngineId::Graph, total);
        }
        if let Ok(crdt) = self.crdt.lock() {
            self.charge_engine_usage(
                &self.crdt_mem_token,
                EngineId::Crdt,
                crdt.estimated_memory_bytes(),
            );
        }
    }

    /// Replace `slot`'s held reservation with one charging `bytes` for `engine`.
    ///
    /// `report_usage` reports an absolute current footprint, not a delta, so
    /// the previous token drops before the new one is charged — otherwise the
    /// governor would see the sum of every report instead of the current one.
    /// Single-database, single-tenant: uses [`DatabaseId::DEFAULT`] and
    /// `TenantId::new(0)`, matching every other Lite call site.
    fn charge_engine_usage(
        &self,
        slot: &Mutex<Option<ReservationToken>>,
        engine: EngineId,
        bytes: usize,
    ) {
        let scoped = ScopedMemory::new(
            Arc::clone(&self.governor),
            DatabaseId::DEFAULT,
            TenantId::new(0),
            engine,
        );
        let mut token = slot.lock_or_recover();
        token.take();
        *token = Some(scoped.charge(bytes));
    }

    /// List currently loaded HNSW collections.
    pub fn loaded_collections(&self) -> NodeDbResult<Vec<String>> {
        let indices = self.vector_state.hnsw_indices.lock_or_recover();
        Ok(indices.keys().cloned().collect())
    }

    /// Access the memory governor.
    pub fn governor(&self) -> &Arc<MemoryGovernor> {
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
