// SPDX-License-Identifier: Apache-2.0

//! Opt-in load-stage diagnostics for the embedded Graphalytics runner.

use std::time::Duration;

use sonic_rs::json;

use crate::error::LiteError;
use crate::storage::engine::StorageWriteProfile;

const FORMAT_VERSION: u32 = 1;

/// Investigation-only load diagnostics for the Graphalytics runner.
///
/// This is intentionally not a stable storage API. The runner enriches it with
/// post-algorithm database metadata before it writes the optional sidecar.
#[doc(hidden)]
#[derive(Debug, Default)]
pub struct GraphalyticsLoadDiagnostics {
    raw_wall_seconds: f64,
    classified_load_seconds: f64,
    classified_stages_seconds: f64,
    classification_residual_seconds: f64,
    prepare_seconds: f64,
    vertices: u64,
    edges: u64,
    vertex_parse_seconds: f64,
    edge_parse_seconds: f64,
    csr_staging_seconds: f64,
    sorter_spill_sort_seconds: f64,
    sorter_spill_write_seconds: f64,
    sorter_spill_runs: u64,
    sorter_spill_records: u64,
    sorter_spill_bytes: u64,
    merge_batch_seconds: f64,
    merge_unique_records: u64,
    value_regeneration_seconds: f64,
    storage_batch_write_seconds: f64,
    storage_begin_seconds: f64,
    storage_prepare_seconds: f64,
    storage_apply_seconds: f64,
    storage_commit_seconds: f64,
    storage_batch_commits: u64,
    storage_batch_operations: u64,
    database_bytes: Option<u64>,
}

impl GraphalyticsLoadDiagnostics {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn add_vertex_parse(&mut self, elapsed: Duration) {
        self.vertex_parse_seconds += elapsed.as_secs_f64();
    }

    pub(crate) fn add_edge_parse(&mut self, elapsed: Duration) {
        self.edge_parse_seconds += elapsed.as_secs_f64();
    }

    pub(crate) fn add_csr_staging(&mut self, elapsed: Duration) {
        self.csr_staging_seconds += elapsed.as_secs_f64();
    }

    pub(crate) fn add_value_regeneration(&mut self, elapsed: Duration) {
        self.value_regeneration_seconds += elapsed.as_secs_f64();
    }

    pub(crate) fn add_storage_batch_write(&mut self, profile: StorageWriteProfile) {
        self.storage_batch_write_seconds += profile.total.as_secs_f64();
        self.storage_begin_seconds += profile.begin.as_secs_f64();
        self.storage_prepare_seconds += profile.prepare.as_secs_f64();
        self.storage_apply_seconds += profile.apply.as_secs_f64();
        self.storage_commit_seconds += profile.commit.as_secs_f64();
        self.storage_batch_commits += 1;
        self.storage_batch_operations += profile.operations;
    }

    pub(crate) fn add_sort(&mut self, sort: SortDiagnostics) {
        self.sorter_spill_sort_seconds += sort.spill_sort.as_secs_f64();
        self.sorter_spill_write_seconds += sort.spill_write.as_secs_f64();
        self.sorter_spill_runs += sort.spill_runs;
        self.sorter_spill_records += sort.spill_records;
        self.sorter_spill_bytes += sort.spill_bytes;
        self.merge_batch_seconds += sort.merge_batches.as_secs_f64();
        self.merge_unique_records += sort.merge_unique_records;
    }

    pub(crate) fn finish(&mut self, raw_wall: Duration, load: Duration, prepare: Duration) {
        self.raw_wall_seconds = raw_wall.as_secs_f64();
        self.classified_load_seconds = load.as_secs_f64();
        self.prepare_seconds = prepare.as_secs_f64();
        self.classified_stages_seconds = self.vertex_parse_seconds
            + self.edge_parse_seconds
            + self.sorter_spill_sort_seconds
            + self.sorter_spill_write_seconds
            + self.merge_batch_seconds
            + self.value_regeneration_seconds
            + self.storage_batch_write_seconds;
        self.classification_residual_seconds =
            self.classified_load_seconds - self.classified_stages_seconds;
    }

    pub(crate) fn set_counts(&mut self, vertices: usize, edges: usize) {
        self.vertices = vertices as u64;
        self.edges = edges as u64;
    }

    /// Set the recursive on-disk PageDB directory size after the final flush.
    #[doc(hidden)]
    pub fn set_database_bytes(&mut self, bytes: Option<u64>) {
        self.database_bytes = bytes;
    }

    /// Serialize the versioned investigation sidecar after benchmark output.
    #[doc(hidden)]
    pub fn to_json(&self, dataset: &str) -> Result<Vec<u8>, LiteError> {
        sonic_rs::to_vec(&json!({
            "format_version": FORMAT_VERSION,
            "system": "nodedb-lite",
            "dataset": dataset,
            "source_revision": null,
            "durability": {
                "mode": "pagedb-atomic",
                "final_flush_completed": true,
            },
            "load": {
                "raw_wall_seconds": self.raw_wall_seconds,
                "classified_load_seconds": self.classified_load_seconds,
                "classified_stages_seconds": self.classified_stages_seconds,
                "residual_seconds": self.classification_residual_seconds,
            },
            "stages": {
                "vertex_parse_seconds": self.vertex_parse_seconds,
                "edge_parse_seconds": self.edge_parse_seconds,
                "csr_staging_prepare_seconds": self.csr_staging_seconds,
                "spill_sort_seconds": self.sorter_spill_sort_seconds,
                "spill_write_seconds": self.sorter_spill_write_seconds,
                "merge_batches_seconds": self.merge_batch_seconds,
                "value_regeneration_seconds": self.value_regeneration_seconds,
                "storage_batch_write_seconds": self.storage_batch_write_seconds,
                "storage_begin_seconds": self.storage_begin_seconds,
                "storage_prepare_seconds": self.storage_prepare_seconds,
                "storage_apply_seconds": self.storage_apply_seconds,
                "storage_commit_seconds": self.storage_commit_seconds,
                "prepare_total_seconds": self.prepare_seconds,
            },
            "counts": {
                "vertices": self.vertices,
                "edges": self.edges,
                "spill_runs": self.sorter_spill_runs,
                "spill_records": self.sorter_spill_records,
                "merge_records": self.sorter_spill_records,
                "merge_unique_records": self.merge_unique_records,
                "storage_batch_commits": self.storage_batch_commits,
                "storage_batch_operations": self.storage_batch_operations,
            },
            "storage": {
                "spill_bytes": self.sorter_spill_bytes,
                "database_bytes": self.database_bytes,
            },
            "unsupported": {
                "peak_rss_bytes": null,
                "peak_open_file_descriptors": null,
                "storage_page_write_seconds": null,
                "storage_sync_count": null,
            },
        }))
        .map_err(|error| LiteError::Serialization {
            detail: format!("serialize Graphalytics diagnostics: {error}"),
        })
    }
}

#[derive(Default)]
pub(crate) struct SortDiagnostics {
    pub(crate) spill_sort: Duration,
    pub(crate) spill_write: Duration,
    pub(crate) spill_runs: u64,
    pub(crate) spill_records: u64,
    pub(crate) spill_bytes: u64,
    pub(crate) merge_batches: Duration,
    pub(crate) merge_unique_records: u64,
}

#[cfg(test)]
mod tests {
    use sonic_rs::JsonValueTrait;

    use super::*;

    #[test]
    fn sidecar_has_the_shared_nested_schema_and_counts() {
        let mut diagnostics = GraphalyticsLoadDiagnostics::new();
        diagnostics.set_counts(3, 5);
        diagnostics.add_sort(SortDiagnostics {
            spill_runs: 2,
            spill_records: 5,
            merge_unique_records: 4,
            ..Default::default()
        });
        diagnostics.add_storage_batch_write(StorageWriteProfile {
            total: Duration::from_secs(5),
            begin: Duration::from_secs(1),
            prepare: Duration::from_secs(1),
            apply: Duration::from_secs(2),
            commit: Duration::from_secs(1),
            operations: 4,
        });
        diagnostics.finish(
            Duration::from_secs(8),
            Duration::from_secs(6),
            Duration::from_secs(2),
        );
        let value: sonic_rs::Value =
            sonic_rs::from_slice(&diagnostics.to_json("fixture").unwrap()).unwrap();
        for key in [
            "format_version",
            "system",
            "dataset",
            "source_revision",
            "durability",
            "load",
            "stages",
            "counts",
            "storage",
            "unsupported",
        ] {
            assert!(value.get(key).is_some(), "missing {key}");
        }
        assert_eq!(value["counts"]["merge_records"].as_u64(), Some(5));
        assert_eq!(value["counts"]["merge_unique_records"].as_u64(), Some(4));
        assert_eq!(value["counts"]["storage_batch_commits"].as_u64(), Some(1));
        assert_eq!(
            value["counts"]["storage_batch_operations"].as_u64(),
            Some(4)
        );
        assert_eq!(
            value["stages"]["storage_batch_write_seconds"].as_f64(),
            Some(5.0)
        );
        assert_eq!(value["stages"]["storage_apply_seconds"].as_f64(), Some(2.0));
        for stage in [
            "storage_batch_write_seconds",
            "storage_begin_seconds",
            "storage_prepare_seconds",
            "storage_apply_seconds",
            "storage_commit_seconds",
        ] {
            assert!(value["stages"][stage].is_number(), "missing {stage}");
        }
        assert!(value["unsupported"]["storage_sync_count"].is_null());
    }
}
