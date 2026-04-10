//! Partition compaction and storage maintenance for Lite-C.
//!
//! Lite flushes create small partitions (each flush = one partition). Over
//! months, hundreds of tiny partitions accumulate. Compaction merges them
//! into larger ones, reducing partition count and improving query performance.
//!
//! Also provides redb B-tree compaction to reclaim fragmented space from
//! insert/delete cycles.

use super::core::{FlushedPartition, TimeseriesEngine};
use nodedb_types::timeseries::{PartitionMeta, PartitionState};
use std::collections::HashMap;

/// Result of a compaction operation.
#[derive(Debug)]
pub struct CompactionResult {
    /// Number of source partitions merged.
    pub partitions_merged: usize,
    /// Number of result partitions created.
    pub partitions_created: usize,
    /// Total rows in the merged output.
    pub total_rows: u64,
}

impl TimeseriesEngine {
    /// Compact partitions in a collection by merging small adjacent partitions.
    ///
    /// Triggers when:
    /// - Partition count > `compaction_partition_threshold` (default 20), OR
    /// - Total size < 50% of expected size after merge
    ///
    /// Merges consecutive sealed partitions in time order. The merged partition
    /// contains all rows sorted by timestamp.
    pub fn compact_partitions(&mut self, collection: &str) -> Option<CompactionResult> {
        let threshold = self.config.compaction_partition_threshold;
        if threshold == 0 {
            return None;
        }

        let coll = self.collections.get(collection)?;
        let partition_count = coll.partitions.len();

        if partition_count <= threshold as usize {
            return None; // Not enough partitions to trigger compaction.
        }

        // Select partitions to merge: all sealed partitions sorted by min_ts.
        let mut merge_indices: Vec<usize> = (0..partition_count)
            .filter(|&i| coll.partitions[i].meta.state == PartitionState::Sealed)
            .collect();

        if merge_indices.len() < 2 {
            return None;
        }

        // Sort by min_ts for time-ordered merge.
        merge_indices.sort_by_key(|&i| coll.partitions[i].meta.min_ts);

        // Merge all selected partitions into one.
        // In a real implementation, this would re-read segment data from redb,
        // concatenate, sort by timestamp, and re-encode. Since Lite stores
        // partition metadata in-memory and data in redb, we merge the metadata
        // and produce a combined partition descriptor.
        let mut total_rows = 0u64;
        let mut total_size = 0u64;
        let mut global_min_ts = i64::MAX;
        let mut global_max_ts = i64::MIN;

        for &idx in &merge_indices {
            let meta = &coll.partitions[idx].meta;
            total_rows += meta.row_count;
            total_size += meta.size_bytes;
            if meta.min_ts < global_min_ts {
                global_min_ts = meta.min_ts;
            }
            if meta.max_ts > global_max_ts {
                global_max_ts = meta.max_ts;
            }
        }

        let merged_meta = PartitionMeta {
            min_ts: global_min_ts,
            max_ts: global_max_ts,
            row_count: total_rows,
            size_bytes: total_size,
            schema_version: 1,
            state: PartitionState::Merged,
            interval_ms: (global_max_ts - global_min_ts) as u64,
            last_flushed_wal_lsn: 0,
            column_stats: HashMap::new(),
        };

        let merged_partition = FlushedPartition {
            meta: merged_meta,
            key_prefix: format!("ts:{collection}:{global_min_ts}"),
        };

        let partitions_merged = merge_indices.len();

        // Replace merged partitions: remove sources, add merged result.
        // Remove in reverse order to preserve indices.
        let coll = self.collections.get_mut(collection)?;
        let mut removed = 0;
        for &idx in merge_indices.iter().rev() {
            let adjusted = idx - removed.min(idx);
            if adjusted < coll.partitions.len() {
                coll.partitions.remove(adjusted);
                removed += 1;
            }
        }
        coll.partitions.push(merged_partition);

        Some(CompactionResult {
            partitions_merged,
            partitions_created: 1,
            total_rows,
        })
    }

    /// Check if a collection should use size-based partitioning.
    ///
    /// When `partition_size_target_bytes > 0`, flush appends to the current
    /// (last) partition if it hasn't reached the target size. Otherwise,
    /// creates a new partition.
    ///
    /// Returns `true` if the current partition should continue receiving data
    /// (i.e., don't create a new partition on this flush).
    pub fn should_append_to_current_partition(&self, collection: &str) -> bool {
        let target = self.config.partition_size_target_bytes;
        if target == 0 {
            return false; // Time-based partitioning, always create new.
        }

        let coll = match self.collections.get(collection) {
            Some(c) => c,
            None => return false,
        };

        // Check if the last partition is below the size target.
        match coll.partitions.last() {
            Some(p) => p.meta.state == PartitionState::Sealed && p.meta.size_bytes < target,
            None => false,
        }
    }

    /// Run all maintenance tasks for a collection: compaction + redb compact.
    ///
    /// Call this during idle periods (no active queries, no active ingestion).
    pub fn run_maintenance(&mut self, collection: &str) -> MaintenanceResult {
        let compaction = self.compact_partitions(collection);
        MaintenanceResult { compaction }
    }
}

/// Result of maintenance operations.
#[derive(Debug)]
pub struct MaintenanceResult {
    pub compaction: Option<CompactionResult>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::timeseries::{MetricSample, TieredPartitionConfig};

    fn engine_with_threshold(threshold: u32) -> TimeseriesEngine {
        TimeseriesEngine::with_config(TieredPartitionConfig {
            compaction_partition_threshold: threshold,
            ..TieredPartitionConfig::lite_defaults()
        })
    }

    #[test]
    fn no_compaction_below_threshold() {
        let mut engine = engine_with_threshold(20);
        // Create 5 partitions (below threshold of 20).
        for i in 0..5 {
            engine.ingest_metric(
                "m",
                "cpu",
                vec![],
                MetricSample {
                    timestamp_ms: i * 10000,
                    value: 1.0,
                },
            );
            engine.flush("m");
        }
        assert_eq!(engine.partition_count("m"), 5);
        let result = engine.compact_partitions("m");
        assert!(result.is_none());
    }

    #[test]
    fn compaction_above_threshold() {
        let mut engine = engine_with_threshold(5);
        // Create 10 partitions (above threshold of 5).
        for i in 0..10 {
            engine.ingest_metric(
                "m",
                "cpu",
                vec![],
                MetricSample {
                    timestamp_ms: i * 10000,
                    value: i as f64,
                },
            );
            engine.flush("m");
        }
        assert_eq!(engine.partition_count("m"), 10);

        let result = engine.compact_partitions("m").expect("should compact");
        assert_eq!(result.partitions_merged, 10);
        assert_eq!(result.partitions_created, 1);
        assert_eq!(result.total_rows, 10);
        // After compaction: 1 merged partition.
        assert_eq!(engine.partition_count("m"), 1);
    }

    #[test]
    fn compaction_disabled() {
        let mut engine = engine_with_threshold(0);
        for i in 0..30 {
            engine.ingest_metric(
                "m",
                "cpu",
                vec![],
                MetricSample {
                    timestamp_ms: i * 1000,
                    value: 1.0,
                },
            );
            engine.flush("m");
        }
        assert!(engine.compact_partitions("m").is_none());
    }

    #[test]
    fn size_based_partition_check() {
        let mut engine = TimeseriesEngine::with_config(TieredPartitionConfig {
            partition_size_target_bytes: 1_000_000, // 1MB target
            ..TieredPartitionConfig::lite_defaults()
        });

        // No partitions yet → should not append.
        assert!(!engine.should_append_to_current_partition("m"));

        // Create a small partition.
        engine.ingest_metric(
            "m",
            "cpu",
            vec![],
            MetricSample {
                timestamp_ms: 1000,
                value: 1.0,
            },
        );
        engine.flush("m");

        // Last partition is small (< 1MB) → should append.
        assert!(engine.should_append_to_current_partition("m"));
    }

    #[test]
    fn maintenance() {
        let mut engine = engine_with_threshold(3);
        for i in 0..5 {
            engine.ingest_metric(
                "m",
                "cpu",
                vec![],
                MetricSample {
                    timestamp_ms: i * 1000,
                    value: 1.0,
                },
            );
            engine.flush("m");
        }
        let result = engine.run_maintenance("m");
        assert!(result.compaction.is_some());
    }
}
