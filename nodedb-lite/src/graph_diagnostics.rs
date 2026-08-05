// SPDX-License-Identifier: Apache-2.0

//! Generic stage measurements for bounded graph import.

use std::time::Duration;

use crate::storage::engine::StorageWriteProfile;

#[derive(Debug, Default, Clone)]
pub struct GraphImportMeasurements {
    pub vertices: u64,
    pub edges: u64,
    pub load: Duration,
    pub prepare: Duration,
    pub vertex_parse: Duration,
    pub edge_parse: Duration,
    pub csr_staging: Duration,
    pub value_regeneration: Duration,
    pub storage: StorageWriteProfile,
    pub sorter: SortDiagnostics,
}

impl GraphImportMeasurements {
    pub(crate) fn add_vertex_parse(&mut self, elapsed: Duration) {
        self.vertex_parse += elapsed;
    }

    pub(crate) fn add_edge_parse(&mut self, elapsed: Duration) {
        self.edge_parse += elapsed;
    }

    pub(crate) fn add_csr_staging(&mut self, elapsed: Duration) {
        self.csr_staging += elapsed;
    }

    pub(crate) fn add_value_regeneration(&mut self, elapsed: Duration) {
        self.value_regeneration += elapsed;
    }

    pub(crate) fn add_storage_batch_write(&mut self, profile: StorageWriteProfile) {
        self.storage.begin += profile.begin;
        self.storage.prepare += profile.prepare;
        self.storage.apply += profile.apply;
        self.storage.commit += profile.commit;
        self.storage.total += profile.total;
        self.storage.operations += profile.operations;
    }

    pub(crate) fn add_sort(&mut self, sort: SortDiagnostics) {
        self.sorter.spill_sort += sort.spill_sort;
        self.sorter.spill_write += sort.spill_write;
        self.sorter.spill_runs += sort.spill_runs;
        self.sorter.spill_records += sort.spill_records;
        self.sorter.spill_bytes += sort.spill_bytes;
        self.sorter.merge_batches += sort.merge_batches;
        self.sorter.merge_unique_records += sort.merge_unique_records;
    }

    pub(crate) fn finish(&mut self, total: Duration, load: Duration, prepare: Duration) {
        self.load = load.min(total);
        self.prepare = prepare;
    }

    pub(crate) fn set_counts(&mut self, vertices: usize, edges: usize) {
        self.vertices = vertices as u64;
        self.edges = edges as u64;
    }
}

#[derive(Debug, Default, Clone)]
pub struct SortDiagnostics {
    pub spill_sort: Duration,
    pub spill_write: Duration,
    pub spill_runs: u64,
    pub spill_records: u64,
    pub spill_bytes: u64,
    pub merge_batches: Duration,
    pub merge_unique_records: u64,
}
