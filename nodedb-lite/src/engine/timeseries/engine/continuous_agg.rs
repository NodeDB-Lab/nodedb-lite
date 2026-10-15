//! Continuous aggregation for Lite timeseries: automatic rollups on flush.
//!
//! Uses shared `ContinuousAggregateDef` from `nodedb_types` so definitions
//! are wire-compatible with Origin. Supports GROUP BY, multiple aggregate
//! expressions, watermark tracking, and retention.

use std::collections::HashMap;

use nodedb_query::EvalError;
use nodedb_types::approx::{HyperLogLog, SpaceSaving, TDigest};
use nodedb_types::timeseries::{AggFunction, ContinuousAggregateDef, RefreshPolicy};
use nodedb_types::value::Value;

use crate::error::LiteError;

/// Materialized bucket: pre-computed aggregates for one (bucket_ts, group_key) pair.
#[derive(Debug)]
pub struct MaterializedBucket {
    pub bucket_ts: i64,
    /// GROUP BY key: "\0"-joined tag values. Empty string if no GROUP BY.
    pub group_key: String,
    /// Per-aggregate accumulator, ordered same as `ContinuousAggregateDef.aggregates`.
    pub accumulators: Vec<Accumulator>,
}

/// Accumulator state of one aggregate expression.
///
/// The sketch the expression's function reads is present: an HLL for
/// `CountDistinct`, a t-digest for `Percentile`, a SpaceSaving table for
/// `TopK`. Sketches key a value by the FNV-1a hash of its `f64` bits, the
/// identity Origin gives a float cell.
#[derive(Debug)]
pub struct Accumulator {
    pub count: u64,
    pub sum: f64,
    pub min: f64,
    pub max: f64,
    pub first: Option<f64>,
    pub last: Option<f64>,
    pub hll: Option<HyperLogLog>,
    pub tdigest: Option<TDigest>,
    pub topk: Option<SpaceSaving>,
}

impl Accumulator {
    /// An empty accumulator for `function`, with the sketch it reads.
    fn new(function: &AggFunction) -> Self {
        let mut acc = Self {
            count: 0,
            sum: 0.0,
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
            first: None,
            last: None,
            hll: None,
            tdigest: None,
            topk: None,
        };
        match function {
            AggFunction::CountDistinct => acc.hll = Some(HyperLogLog::new()),
            AggFunction::Percentile(_) => acc.tdigest = Some(TDigest::new()),
            AggFunction::TopK(k) => acc.topk = Some(SpaceSaving::new(*k)),
            AggFunction::Sum
            | AggFunction::Count
            | AggFunction::Min
            | AggFunction::Max
            | AggFunction::Avg
            | AggFunction::First
            | AggFunction::Last => {}
            // `AggFunction` is `#[non_exhaustive]`, so a match outside its
            // crate needs this arm. `register` refuses such a function.
            _ => {}
        }
        acc
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
        let hash = fnv1a(&value.to_bits().to_le_bytes());
        if let Some(hll) = &mut self.hll {
            hll.add(hash);
        }
        if let Some(td) = &mut self.tdigest {
            td.add(value);
        }
        if let Some(ss) = &mut self.topk {
            ss.add(hash);
        }
    }

    /// The final value of `func` over this accumulator.
    ///
    /// The values match Origin's: `Count` is an `Integer`, `TopK` the
    /// `Integer` number of tracked heavy hitters, every other function a
    /// `Float`. A function over no values is NULL, except `Count`, which is
    /// zero.
    pub fn result(&self, func: &AggFunction) -> Result<Value, LiteError> {
        let empty = self.count == 0;
        let float = |v: f64| {
            if empty { Value::Null } else { Value::Float(v) }
        };
        Ok(match func {
            AggFunction::Count => i64::try_from(self.count)
                .map(Value::Integer)
                .map_err(|_| EvalError::NumericOverflow { function: "count" })?,
            AggFunction::Sum => float(self.sum),
            AggFunction::Min => float(self.min),
            AggFunction::Max => float(self.max),
            AggFunction::Avg => float(self.sum / self.count as f64),
            AggFunction::First => self.first.map_or(Value::Null, Value::Float),
            AggFunction::Last => self.last.map_or(Value::Null, Value::Float),
            AggFunction::CountDistinct => match &self.hll {
                Some(hll) => float(hll.estimate()),
                None => Value::Null,
            },
            AggFunction::Percentile(q) => match &self.tdigest {
                Some(td) => float(td.quantile(*q)),
                None => Value::Null,
            },
            AggFunction::TopK(_) => match &self.topk {
                Some(ss) => i64::try_from(ss.top_k().len())
                    .map(Value::Integer)
                    .map_err(|_| EvalError::NumericOverflow { function: "topk" })?,
                None => Value::Null,
            },
            // `AggFunction` is `#[non_exhaustive]`. `register` refuses a
            // function this accumulator keeps no state for.
            other => return Err(unsupported_function(other)),
        })
    }
}

/// Refuse an aggregate function this module keeps no state for.
fn check_supported(function: &AggFunction) -> Result<(), LiteError> {
    match function {
        AggFunction::Sum
        | AggFunction::Count
        | AggFunction::Min
        | AggFunction::Max
        | AggFunction::Avg
        | AggFunction::First
        | AggFunction::Last
        | AggFunction::CountDistinct
        | AggFunction::Percentile(_)
        | AggFunction::TopK(_) => Ok(()),
        // `AggFunction` is `#[non_exhaustive]`, so a match outside its crate
        // needs this arm.
        other => Err(unsupported_function(other)),
    }
}

fn unsupported_function(function: &AggFunction) -> LiteError {
    LiteError::Unsupported {
        detail: format!(
            "continuous aggregate function {}() is not supported in Lite",
            function.as_str()
        ),
    }
}

/// 64-bit FNV-1a hash, the sketch identity Origin gives a cell.
fn fnv1a(bytes: &[u8]) -> u64 {
    const OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;
    bytes.iter().fold(OFFSET_BASIS, |hash, &b| {
        (hash ^ u64::from(b)).wrapping_mul(PRIME)
    })
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

    /// Register a continuous aggregate definition. Refuses a definition with
    /// an aggregate function Lite keeps no state for, and registers nothing.
    pub fn register(&mut self, def: ContinuousAggregateDef) -> Result<(), LiteError> {
        for expr in &def.aggregates {
            check_supported(&expr.function)?;
        }
        let source = def.source.clone();
        let name = def.name.clone();
        self.materialized.entry(name.clone()).or_default();
        self.dependencies
            .entry(source)
            .or_default()
            .push(name.clone());
        self.definitions.insert(name, def);
        Ok(())
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
                    accumulators: def
                        .aggregates
                        .iter()
                        .map(|expr| Accumulator::new(&expr.function))
                        .collect(),
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
            database_id: nodedb_types::id::DatabaseId::DEFAULT.as_u64(),
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
            incarnation: nodedb_types::Hlc::ZERO,
        }
    }

    #[test]
    fn register_and_flush() {
        let mut mgr = LiteContinuousAggManager::new();
        mgr.register(make_def("cpu_1m", "cpu", 60_000)).unwrap();

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
        mgr.register(make_def("cpu_1m", "cpu", 60_000)).unwrap();

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
        mgr.register(def).unwrap();

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
        mgr.register(make_def("m_1m", "m", 60_000)).unwrap();

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
        mgr.register(def).unwrap();

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
        mgr.register(make_def("cpu_1m", "cpu", 60_000)).unwrap();
        assert_eq!(mgr.count(), 1);
        mgr.unregister("cpu_1m");
        assert_eq!(mgr.count(), 0);
    }

    #[test]
    fn no_source_match_no_refresh() {
        let mut mgr = LiteContinuousAggManager::new();
        mgr.register(make_def("cpu_1m", "cpu", 60_000)).unwrap();

        let refreshed = mgr.on_flush_simple("memory", &[1_700_000_000_000], &[50.0]);
        assert!(refreshed.is_empty());
    }

    /// An accumulator for `function` fed `values`, finalized.
    fn finalize(function: AggFunction, values: &[f64]) -> Value {
        let mut acc = Accumulator::new(&function);
        for &v in values {
            acc.ingest(v);
        }
        acc.result(&function).unwrap()
    }

    #[test]
    fn accumulator_result_functions() {
        let values = [10.0, 20.0, 30.0, 40.0];
        assert_eq!(finalize(AggFunction::Count, &values), Value::Integer(4));
        assert_eq!(finalize(AggFunction::Sum, &values), Value::Float(100.0));
        assert_eq!(finalize(AggFunction::Min, &values), Value::Float(10.0));
        assert_eq!(finalize(AggFunction::Max, &values), Value::Float(40.0));
        assert_eq!(finalize(AggFunction::Avg, &values), Value::Float(25.0));
        assert_eq!(finalize(AggFunction::First, &values), Value::Float(10.0));
        assert_eq!(finalize(AggFunction::Last, &values), Value::Float(40.0));
    }

    #[test]
    fn sketch_functions_compute_values() {
        let values: Vec<f64> = (0..100).map(|i| f64::from(i % 10)).collect();

        let Value::Float(distinct) = finalize(AggFunction::CountDistinct, &values) else {
            panic!("count_distinct must be a float");
        };
        assert!((distinct - 10.0).abs() < 1.0, "estimate {distinct}");

        let Value::Float(median) = finalize(AggFunction::Percentile(0.5), &values) else {
            panic!("percentile must be a float");
        };
        assert!((3.0..=6.0).contains(&median), "median {median}");

        assert_eq!(finalize(AggFunction::TopK(3), &values), Value::Integer(3));
    }

    #[test]
    fn functions_over_no_values_are_null_never_nan() {
        for function in [
            AggFunction::Sum,
            AggFunction::Min,
            AggFunction::Max,
            AggFunction::Avg,
            AggFunction::First,
            AggFunction::Last,
            AggFunction::CountDistinct,
            AggFunction::Percentile(0.9),
        ] {
            assert_eq!(finalize(function.clone(), &[]), Value::Null, "{function:?}");
        }
        assert_eq!(finalize(AggFunction::Count, &[]), Value::Integer(0));
        assert_eq!(finalize(AggFunction::TopK(2), &[]), Value::Integer(0));
    }

    #[test]
    fn flushed_sketch_aggregate_finalizes_without_nan() {
        let mut mgr = LiteContinuousAggManager::new();
        let mut def = make_def("cpu_sketch", "cpu", 60_000);
        def.aggregates = vec![
            AggregateExpr {
                function: AggFunction::CountDistinct,
                source_column: "value".into(),
                output_column: "distinct".into(),
            },
            AggregateExpr {
                function: AggFunction::Percentile(0.99),
                source_column: "value".into(),
                output_column: "p99".into(),
            },
            AggregateExpr {
                function: AggFunction::TopK(5),
                source_column: "value".into(),
                output_column: "top".into(),
            },
        ];
        mgr.register(def).unwrap();

        let base = 1_700_000_000_000 - (1_700_000_000_000 % 60_000);
        let ts: Vec<i64> = (0..50).map(|i| base + i * 1000).collect();
        let vals: Vec<f64> = (0..50).map(|i| f64::from(i % 7)).collect();
        mgr.on_flush_simple("cpu", &ts, &vals);

        let buckets = mgr.query_all("cpu_sketch");
        assert_eq!(buckets.len(), 1);
        let def = mgr.get("cpu_sketch").unwrap();
        for (acc, expr) in buckets[0].accumulators.iter().zip(&def.aggregates) {
            let value = acc.result(&expr.function).unwrap();
            match value {
                Value::Float(f) => assert!(f.is_finite(), "{expr:?} gave {f}"),
                Value::Integer(_) => {}
                other => panic!("{expr:?} gave {other:?}"),
            }
        }
    }
}
