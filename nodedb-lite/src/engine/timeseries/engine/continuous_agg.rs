//! Continuous aggregation for Lite timeseries: automatic rollups on flush.
//!
//! Uses shared `ContinuousAggregateDef` from `nodedb_types` so definitions
//! are wire-compatible with Origin. Supports GROUP BY, multiple aggregate
//! expressions, watermark tracking, and retention.

use std::collections::HashMap;

use nodedb_types::timeseries::{AggFunction, ContinuousAggregateDef, RefreshPolicy};

/// Materialized bucket: pre-computed aggregates for one (bucket_ts, group_key) pair.
#[derive(Debug, Clone)]
pub struct MaterializedBucket {
    pub bucket_ts: i64,
    /// GROUP BY key: "\0"-joined tag values. Empty string if no GROUP BY.
    pub group_key: String,
    /// Per-aggregate accumulator, ordered same as `ContinuousAggregateDef.aggregates`.
    pub accumulators: Vec<Accumulator>,
}

/// Per-function accumulator state.
#[derive(Debug, Clone)]
pub struct Accumulator {
    pub count: u64,
    pub sum: f64,
    pub min: f64,
    pub max: f64,
    pub first: Option<f64>,
    pub last: Option<f64>,
}

impl Accumulator {
    fn new() -> Self {
        Self {
            count: 0,
            sum: 0.0,
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
            first: None,
            last: None,
        }
    }

    fn ingest(&mut self, value: f64) {
        self.count += 1;
        self.sum += value;
        if value < self.min {
            self.min = value;
        }
        if value > self.max {
            self.max = value;
        }
        if self.first.is_none() {
            self.first = Some(value);
        }
        self.last = Some(value);
    }

    /// Extract the final value for a given aggregate function.
    pub fn result(&self, func: &AggFunction) -> f64 {
        match func {
            AggFunction::Count => self.count as f64,
            AggFunction::Sum => self.sum,
            AggFunction::Min => self.min,
            AggFunction::Max => self.max,
            AggFunction::Avg => {
                if self.count == 0 {
                    0.0
                } else {
                    self.sum / self.count as f64
                }
            }
            AggFunction::First => self.first.unwrap_or(f64::NAN),
            AggFunction::Last => self.last.unwrap_or(f64::NAN),
            _ => f64::NAN, // Sketch-based functions not yet supported in Lite.
        }
    }
}

/// Key for materialized bucket lookup: (bucket_ts, group_key).
type BucketKey = (i64, String);

/// Lite continuous aggregate manager.
///
/// Processes flush data and maintains materialized bucketed aggregates
/// in memory. Supports GROUP BY and multiple aggregate expressions.
pub struct LiteContinuousAggManager {
    definitions: HashMap<String, ContinuousAggregateDef>,
    /// `agg_name → (bucket_ts, group_key) → MaterializedBucket`.
    materialized: HashMap<String, HashMap<BucketKey, MaterializedBucket>>,
    /// `source_collection → [agg_names]`.
    dependencies: HashMap<String, Vec<String>>,
    /// Per-aggregate watermark: highest bucket_ts fully processed.
    watermarks: HashMap<String, i64>,
}

impl LiteContinuousAggManager {
    pub fn new() -> Self {
        Self {
            definitions: HashMap::new(),
            materialized: HashMap::new(),
            dependencies: HashMap::new(),
            watermarks: HashMap::new(),
        }
    }

    /// Register a continuous aggregate definition.
    pub fn register(&mut self, def: ContinuousAggregateDef) {
        let source = def.source.clone();
        let name = def.name.clone();
        self.materialized.entry(name.clone()).or_default();
        self.dependencies
            .entry(source)
            .or_default()
            .push(name.clone());
        self.definitions.insert(name, def);
    }

    /// Remove a continuous aggregate.
    pub fn unregister(&mut self, name: &str) {
        if let Some(def) = self.definitions.remove(name) {
            self.materialized.remove(name);
            self.watermarks.remove(name);
            if let Some(deps) = self.dependencies.get_mut(&def.source) {
                deps.retain(|n| n != name);
            }
        }
    }

    /// Process a flush event with columnar data.
    ///
    /// `timestamps` and `columns` are parallel arrays. `columns` maps
    /// column name → values (f64 for numeric, or tag strings encoded as
    /// indices into the column's own namespace).
    ///
    /// For the simple (timestamp, value) case, pass a single column named
    /// after the source metric.
    ///
    /// Returns names of aggregates that were refreshed.
    pub fn on_flush(
        &mut self,
        source_collection: &str,
        timestamps: &[i64],
        columns: &HashMap<String, Vec<f64>>,
    ) -> Vec<String> {
        let agg_names: Vec<String> = self
            .dependencies
            .get(source_collection)
            .cloned()
            .unwrap_or_default();

        let mut refreshed = Vec::new();

        for agg_name in &agg_names {
            let Some(def) = self.definitions.get(agg_name) else {
                continue;
            };
            if def.bucket_interval_ms <= 0 {
                continue;
            }
            if def.refresh_policy != RefreshPolicy::OnFlush {
                continue;
            }

            let interval = def.bucket_interval_ms;
            let num_aggs = def.aggregates.len();
            let mat = self.materialized.entry(agg_name.clone()).or_default();

            let mut max_bucket_ts = self.watermarks.get(agg_name).copied().unwrap_or(0);

            for (row_idx, &ts) in timestamps.iter().enumerate() {
                let bucket_ts = ts - ts.rem_euclid(interval);
                if bucket_ts > max_bucket_ts {
                    max_bucket_ts = bucket_ts;
                }

                // Build group key (empty string if no GROUP BY).
                let group_key = if def.group_by.is_empty() {
                    String::new()
                } else {
                    // For now, GROUP BY columns are looked up from `columns`.
                    // The value is encoded as f64 (symbol ID cast). For display,
                    // the caller resolves back to string.
                    let parts: Vec<String> = def
                        .group_by
                        .iter()
                        .map(|col| {
                            columns
                                .get(col)
                                .and_then(|vals| vals.get(row_idx))
                                .map(|v| format!("{v}"))
                                .unwrap_or_default()
                        })
                        .collect();
                    parts.join("\0")
                };

                let key = (bucket_ts, group_key.clone());
                let bucket = mat.entry(key).or_insert_with(|| MaterializedBucket {
                    bucket_ts,
                    group_key,
                    accumulators: (0..num_aggs).map(|_| Accumulator::new()).collect(),
                });

                // Accumulate each aggregate expression.
                for (agg_idx, expr) in def.aggregates.iter().enumerate() {
                    if agg_idx >= bucket.accumulators.len() {
                        break;
                    }
                    let value = if expr.source_column == "*" {
                        1.0 // COUNT(*)
                    } else {
                        columns
                            .get(&expr.source_column)
                            .and_then(|vals| vals.get(row_idx))
                            .copied()
                            .unwrap_or(f64::NAN)
                    };
                    if !value.is_nan() {
                        bucket.accumulators[agg_idx].ingest(value);
                    }
                }
            }

            self.watermarks.insert(agg_name.clone(), max_bucket_ts);
            refreshed.push(agg_name.clone());
        }

        refreshed
    }

    /// Convenience: flush with a single value column (backward compat).
    pub fn on_flush_simple(
        &mut self,
        source_collection: &str,
        timestamps: &[i64],
        values: &[f64],
    ) -> Vec<String> {
        let mut columns = HashMap::new();
        columns.insert("value".to_string(), values.to_vec());
        self.on_flush(source_collection, timestamps, &columns)
    }

    /// Query materialized buckets for an aggregate within a time range.
    pub fn query(&self, agg_name: &str, start_ms: i64, end_ms: i64) -> Vec<&MaterializedBucket> {
        let Some(mat) = self.materialized.get(agg_name) else {
            return Vec::new();
        };
        let mut results: Vec<&MaterializedBucket> = mat
            .values()
            .filter(|b| b.bucket_ts >= start_ms && b.bucket_ts <= end_ms)
            .collect();
        results.sort_by_key(|b| (b.bucket_ts, b.group_key.as_str()));
        results
    }

    /// Get all materialized buckets for an aggregate, sorted by time.
    pub fn query_all(&self, agg_name: &str) -> Vec<&MaterializedBucket> {
        let Some(mat) = self.materialized.get(agg_name) else {
            return Vec::new();
        };
        let mut results: Vec<&MaterializedBucket> = mat.values().collect();
        results.sort_by_key(|b| (b.bucket_ts, b.group_key.as_str()));
        results
    }

    /// Apply retention: remove buckets older than each aggregate's retention period.
    pub fn apply_retention(&mut self, now_ms: i64) -> usize {
        let mut total = 0;
        let defs: Vec<(String, u64)> = self
            .definitions
            .values()
            .map(|d| (d.name.clone(), d.retention_period_ms))
            .collect();
        for (name, retention) in defs {
            if retention == 0 {
                continue;
            }
            let cutoff = now_ms - retention as i64;
            if let Some(mat) = self.materialized.get_mut(&name) {
                let before = mat.len();
                mat.retain(|_, b| b.bucket_ts > cutoff);
                total += before - mat.len();
            }
        }
        total
    }

    /// Watermark for an aggregate (highest bucket_ts fully processed).
    pub fn watermark(&self, agg_name: &str) -> i64 {
        self.watermarks.get(agg_name).copied().unwrap_or(0)
    }

    /// Number of registered aggregates.
    pub fn count(&self) -> usize {
        self.definitions.len()
    }

    /// List all registered aggregate names.
    pub fn list(&self) -> Vec<&str> {
        self.definitions.keys().map(String::as_str).collect()
    }

    /// Get a definition by name.
    pub fn get(&self, name: &str) -> Option<&ContinuousAggregateDef> {
        self.definitions.get(name)
    }
}

impl Default for LiteContinuousAggManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::timeseries::{AggregateExpr, RefreshPolicy};

    fn make_def(name: &str, source: &str, interval_ms: i64) -> ContinuousAggregateDef {
        ContinuousAggregateDef {
            name: name.into(),
            source: source.into(),
            bucket_interval: format!("{}ms", interval_ms),
            bucket_interval_ms: interval_ms,
            group_by: vec![],
            aggregates: vec![
                AggregateExpr {
                    function: AggFunction::Sum,
                    source_column: "value".into(),
                    output_column: "value_sum".into(),
                },
                AggregateExpr {
                    function: AggFunction::Count,
                    source_column: "*".into(),
                    output_column: "row_count".into(),
                },
            ],
            refresh_policy: RefreshPolicy::OnFlush,
            retention_period_ms: 0,
            stale: false,
        }
    }

    #[test]
    fn register_and_flush() {
        let mut mgr = LiteContinuousAggManager::new();
        mgr.register(make_def("cpu_1m", "cpu", 60_000));

        let base = 1_700_000_040_000_i64;
        let ts: Vec<i64> = (0..120).map(|i| base + i * 1000).collect();
        let vals: Vec<f64> = (0..120).map(|i| 50.0 + i as f64).collect();

        let refreshed = mgr.on_flush_simple("cpu", &ts, &vals);
        assert_eq!(refreshed, vec!["cpu_1m"]);

        let results = mgr.query_all("cpu_1m");
        assert_eq!(results.len(), 2); // 120s / 60s = 2 buckets
        let total_count: u64 = results
            .iter()
            .map(|b| b.accumulators[1].count) // COUNT(*) is index 1
            .sum();
        assert_eq!(total_count, 120);
    }

    #[test]
    fn incremental_accumulation() {
        let mut mgr = LiteContinuousAggManager::new();
        mgr.register(make_def("cpu_1m", "cpu", 60_000));

        let base = 1_700_000_000_000 - (1_700_000_000_000 % 60_000);
        let ts: Vec<i64> = (0..60).map(|i| base + i * 1000).collect();
        let vals: Vec<f64> = (0..60).map(|i| i as f64).collect();
        mgr.on_flush_simple("cpu", &ts, &vals);
        mgr.on_flush_simple("cpu", &ts, &vals);

        let results = mgr.query_all("cpu_1m");
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].accumulators[1].count, 120); // COUNT accumulated
    }

    #[test]
    fn group_by_support() {
        let mut mgr = LiteContinuousAggManager::new();
        let mut def = make_def("cpu_by_host_1m", "cpu", 60_000);
        def.group_by = vec!["host".into()];
        mgr.register(def);

        let base = 1_700_000_000_000 - (1_700_000_000_000 % 60_000);
        let ts: Vec<i64> = (0..60).map(|i| base + i * 1000).collect();
        let vals: Vec<f64> = (0..60).map(|i| i as f64).collect();
        // host column: alternating 1.0 and 2.0 (representing symbol IDs)
        let hosts: Vec<f64> = (0..60)
            .map(|i| if i % 2 == 0 { 1.0 } else { 2.0 })
            .collect();

        let mut columns = HashMap::new();
        columns.insert("value".to_string(), vals);
        columns.insert("host".to_string(), hosts);

        mgr.on_flush("cpu", &ts, &columns);

        let results = mgr.query_all("cpu_by_host_1m");
        assert_eq!(results.len(), 2); // 2 group keys in 1 bucket
        let total: u64 = results.iter().map(|b| b.accumulators[1].count).sum();
        assert_eq!(total, 60);
    }

    #[test]
    fn watermark_tracking() {
        let mut mgr = LiteContinuousAggManager::new();
        mgr.register(make_def("m_1m", "m", 60_000));

        assert_eq!(mgr.watermark("m_1m"), 0);

        let ts = vec![1_700_000_000_000, 1_700_000_060_000];
        let vals = vec![1.0, 2.0];
        mgr.on_flush_simple("m", &ts, &vals);

        // ts 1_700_000_060_000 → bucket 1_700_000_040_000 (rem_euclid = 20_000)
        assert_eq!(mgr.watermark("m_1m"), 1_700_000_040_000);
    }

    #[test]
    fn retention() {
        let mut mgr = LiteContinuousAggManager::new();
        let mut def = make_def("cpu_1m", "cpu", 60_000);
        def.retention_period_ms = 120_000;
        mgr.register(def);

        let ts: Vec<i64> = (0..300).map(|i| 1_700_000_000_000 + i * 1000).collect();
        let vals: Vec<f64> = vec![1.0; 300];
        mgr.on_flush_simple("cpu", &ts, &vals);

        let before = mgr.query_all("cpu_1m").len();
        let now = 1_700_000_000_000 + 5 * 60_000;
        let removed = mgr.apply_retention(now);
        assert!(removed > 0);
        assert!(mgr.query_all("cpu_1m").len() < before);
    }

    #[test]
    fn unregister_removes() {
        let mut mgr = LiteContinuousAggManager::new();
        mgr.register(make_def("cpu_1m", "cpu", 60_000));
        assert_eq!(mgr.count(), 1);
        mgr.unregister("cpu_1m");
        assert_eq!(mgr.count(), 0);
    }

    #[test]
    fn no_source_match_no_refresh() {
        let mut mgr = LiteContinuousAggManager::new();
        mgr.register(make_def("cpu_1m", "cpu", 60_000));

        let refreshed = mgr.on_flush_simple("memory", &[1_700_000_000_000], &[50.0]);
        assert!(refreshed.is_empty());
    }

    #[test]
    fn accumulator_result_functions() {
        let mut acc = Accumulator::new();
        acc.ingest(10.0);
        acc.ingest(20.0);
        acc.ingest(30.0);
        acc.ingest(40.0);

        assert_eq!(acc.result(&AggFunction::Count), 4.0);
        assert_eq!(acc.result(&AggFunction::Sum), 100.0);
        assert_eq!(acc.result(&AggFunction::Min), 10.0);
        assert_eq!(acc.result(&AggFunction::Max), 40.0);
        assert!((acc.result(&AggFunction::Avg) - 25.0).abs() < f64::EPSILON);
        assert_eq!(acc.result(&AggFunction::First), 10.0);
        assert_eq!(acc.result(&AggFunction::Last), 40.0);
    }
}
