//! Timeseries query: scan and aggregation.

use nodedb_types::timeseries::{SeriesId, TimeRange};

use super::core::TimeseriesEngine;

impl TimeseriesEngine {
    /// Scan metric samples in a time range.
    pub fn scan(&self, collection: &str, range: &TimeRange) -> Vec<(i64, f64, SeriesId)> {
        let Some(coll) = self.collections.get(collection) else {
            return Vec::new();
        };

        let mut results = Vec::new();
        for i in 0..coll.timestamps.len() {
            let ts = coll.timestamps[i];
            if range.contains(ts) {
                results.push((ts, coll.values[i], coll.series_ids[i]));
            }
        }

        results.sort_by_key(|(ts, _, _)| *ts);
        results
    }

    /// Aggregate over a time range with time_bucket grouping.
    pub fn aggregate_by_bucket(
        &self,
        collection: &str,
        range: &TimeRange,
        bucket_ms: i64,
    ) -> Vec<(i64, u64, f64, f64, f64)> {
        let rows = self.scan(collection, range);
        if rows.is_empty() || bucket_ms <= 0 {
            return Vec::new();
        }

        let mut buckets: std::collections::BTreeMap<i64, (u64, f64, f64, f64)> =
            std::collections::BTreeMap::new();

        for (ts, val, _) in &rows {
            let bucket = (*ts / bucket_ms) * bucket_ms;
            let entry = buckets
                .entry(bucket)
                .or_insert((0, 0.0, f64::INFINITY, f64::NEG_INFINITY));
            entry.0 += 1;
            entry.1 += val;
            if *val < entry.2 {
                entry.2 = *val;
            }
            if *val > entry.3 {
                entry.3 = *val;
            }
        }

        buckets
            .into_iter()
            .map(|(bucket, (count, sum, min, max))| (bucket, count, sum, min, max))
            .collect()
    }
}
