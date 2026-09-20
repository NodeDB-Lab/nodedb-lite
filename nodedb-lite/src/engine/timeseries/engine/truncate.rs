//! Whole-collection `TRUNCATE` on the Lite timeseries engine.

use super::core::TimeseriesEngine;

impl TimeseriesEngine {
    /// Remove every sample of `collection`: the memtable, every flushed
    /// partition, the pending downsample windows, and the WAL entries that
    /// have not reached the flush watermark. The series catalog is shared
    /// across collections and stays as it is. Returns the number of rows
    /// that existed across the memtable and partitions; a collection the
    /// engine never saw reports zero.
    pub fn truncate_collection(&mut self, collection: &str) -> usize {
        let rows = match self.collections.get_mut(collection) {
            Some(coll) => {
                let partition_rows: u64 = coll.partitions.iter().map(|p| p.meta.row_count).sum();
                let rows = coll.row_count() + partition_rows as usize;
                coll.timestamps.clear();
                coll.values.clear();
                coll.series_ids.clear();
                coll.partitions.clear();
                coll.memory_bytes = 0;
                coll.dirty = false;
                rows
            }
            None => 0,
        };
        self.downsample_accumulators
            .retain(|(coll, _), _| coll != collection);
        self.wal_entries.retain(|e| e.collection != collection);
        rows
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::timeseries::{MetricSample, TimeRange};

    use super::*;

    fn ingest(engine: &mut TimeseriesEngine, collection: &str, n: i64) {
        for i in 0..n {
            engine.ingest_metric(
                collection,
                "cpu",
                vec![("host".into(), "a".into())],
                MetricSample {
                    timestamp_ms: 1_000 + i,
                    value: i as f64,
                },
            );
        }
    }

    #[test]
    fn truncate_empties_memtable_and_partitions_of_one_collection() {
        let mut engine = TimeseriesEngine::new();
        ingest(&mut engine, "m", 3);
        engine.flush("m").expect("flush");
        ingest(&mut engine, "m", 2);
        ingest(&mut engine, "other", 1);

        assert_eq!(engine.truncate_collection("m"), 5);
        assert_eq!(engine.row_count("m"), 0);
        assert_eq!(engine.partition_count("m"), 0);
        assert!(engine.scan("m", &TimeRange::new(0, i64::MAX)).is_empty());
        assert_eq!(engine.row_count("other"), 1);

        ingest(&mut engine, "m", 1);
        assert_eq!(engine.row_count("m"), 1);
    }

    #[test]
    fn truncate_unknown_collection_reports_zero() {
        let mut engine = TimeseriesEngine::new();
        assert_eq!(engine.truncate_collection("nope"), 0);
    }
}
