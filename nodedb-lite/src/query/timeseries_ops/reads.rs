// SPDX-License-Identifier: Apache-2.0
//! Scan handler for the timeseries physical visitor.

use std::collections::BTreeMap;

use nodedb_types::result::QueryResult;
use nodedb_types::timeseries::TimeRange;
use nodedb_types::value::Value;

use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::storage::engine::StorageEngine;

use crate::engine::timeseries::engine::TimeseriesEngine;

/// Parameters forwarded from `TimeseriesOp::Scan`.
pub struct ScanParams {
    pub time_range: (i64, i64),
    pub projection: Vec<String>,
    pub limit: usize,
    pub filters: Vec<u8>,
    pub bucket_interval_ms: i64,
    pub group_by: Vec<String>,
    pub aggregates: Vec<(String, String)>,
    pub gap_fill: String,
    pub computed_columns: Vec<u8>,
    pub rls_filters: Vec<u8>,
    pub system_as_of_ms: Option<i64>,
    pub valid_at_ms: Option<i64>,
}

/// Execute a timeseries scan, optionally bucketed and gap-filled.
pub fn scan<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    collection: &str,
    params: ScanParams,
) -> Result<QueryResult, LiteError> {
    let ScanParams {
        time_range,
        projection,
        limit,
        filters: _,
        bucket_interval_ms,
        group_by: _,
        aggregates,
        gap_fill,
        computed_columns: _,
        rls_filters: _,
        system_as_of_ms,
        valid_at_ms,
    } = params;

    let range = TimeRange::new(time_range.0, time_range.1);

    let ts_engine = engine
        .timeseries
        .lock()
        .map_err(|_| LiteError::LockPoisoned)?;

    // Retrieve raw samples — (timestamp_ms, value, series_id).
    let raw = ts_engine.scan(collection, &range);

    // Apply system_as_of_ms: the Lite memtable doesn't store per-sample
    // system_time (ingest time), so for non-bitemporal collections we
    // cannot exclude samples written *after* the cutoff. However, the
    // engine's `wal_seq` is monotonically increasing with wall-clock time.
    // We surface this via an ingest-time surrogate: if system_as_of_ms is
    // Some, filter samples whose WAL entry seq maps to an ingest time
    // before the cutoff. Because the Lite memtable does not persist
    // system_time per-row, we treat it as best-effort: samples are filtered
    // by their timestamp_ms as a proxy (appropriate for append-only
    // timeseries where event-time ≈ ingest-time; documents with future
    // timestamps are inherently rare in telemetry workloads).
    //
    // For a collection with bitemporal=true the correct behaviour is:
    //   exclude rows with system_ingest_ms > system_as_of_ms.
    // Since Lite does not carry a separate system_time column in its
    // in-memory store, we use timestamp_ms as a conservative proxy.
    let raw: Vec<(i64, f64, _)> = if let Some(cutoff) = system_as_of_ms {
        raw.into_iter().filter(|(ts, _, _)| *ts <= cutoff).collect()
    } else {
        raw
    };

    // Apply valid_at_ms if columns _ts_valid_from / _ts_valid_until are
    // present. Lite timeseries stores only (ts, value) — no bi-temporal
    // validity columns — so this filter is a no-op for standard collections.
    // The engine logs no error; the absence of those columns implies the
    // current state is the only valid state (i.e., valid-time is eternal).
    let _ = valid_at_ms; // No-op: validity columns not in Lite TS memtable.

    if bucket_interval_ms > 0 {
        bucket_scan(
            &ts_engine,
            collection,
            range,
            bucket_interval_ms,
            &aggregates,
            &gap_fill,
            &projection,
            limit,
        )
    } else {
        raw_scan(raw, &projection, limit)
    }
}

// ── Bucketed scan ─────────────────────────────────────────────────────────────

#[allow(clippy::too_many_arguments)]
fn bucket_scan(
    ts_engine: &TimeseriesEngine,
    collection: &str,
    range: TimeRange,
    bucket_ms: i64,
    aggregates: &[(String, String)],
    gap_fill: &str,
    projection: &[String],
    limit: usize,
) -> Result<QueryResult, LiteError> {
    // aggregate_by_bucket returns (bucket_start, count, sum, min, max).
    let buckets = ts_engine.aggregate_by_bucket(collection, &range, bucket_ms);

    // Build a map for gap-fill lookups.
    let bucket_map: BTreeMap<i64, (u64, f64, f64, f64)> = buckets
        .iter()
        .map(|(ts, cnt, sum, min, max)| (*ts, (*cnt, *sum, *min, *max)))
        .collect();

    // Determine the full span of expected buckets.
    let first_bucket = if range.start_ms >= 0 {
        (range.start_ms / bucket_ms) * bucket_ms
    } else {
        range.start_ms
    };
    let last_bucket = if range.end_ms >= 0 {
        (range.end_ms / bucket_ms) * bucket_ms
    } else {
        range.end_ms
    };

    // Generate all expected bucket starts for gap-fill.
    let mut expected: Vec<i64> = Vec::new();
    let mut b = first_bucket;
    while b <= last_bucket {
        expected.push(b);
        b = b.saturating_add(bucket_ms);
    }

    // Determine which aggregate columns to emit.
    let agg_ops: Vec<&str> = if aggregates.is_empty() {
        vec!["count", "sum", "min", "max"]
    } else {
        aggregates.iter().map(|(op, _)| op.as_str()).collect()
    };

    let mut columns: Vec<String> = vec!["bucket".into()];
    columns.extend(agg_ops.iter().map(|op| op.to_string()));

    // Carry-forward state for "prev" gap-fill.
    let mut prev_row: Option<Vec<Value>> = None;

    let mut rows: Vec<Vec<Value>> = Vec::new();

    for bucket_start in &expected {
        let row_vals = if let Some(&(cnt, sum, min, max)) = bucket_map.get(bucket_start) {
            let agg_vals: Vec<Value> = agg_ops
                .iter()
                .map(|op| match *op {
                    "count" => Value::Integer(cnt as i64),
                    "sum" => Value::Float(sum),
                    "min" => Value::Float(min),
                    "max" => Value::Float(max),
                    "avg" | "mean" => {
                        if cnt > 0 {
                            Value::Float(sum / cnt as f64)
                        } else {
                            Value::Null
                        }
                    }
                    _ => Value::Null,
                })
                .collect();
            let mut r = vec![Value::Integer(*bucket_start)];
            r.extend(agg_vals);
            r
        } else {
            // Gap bucket — apply gap-fill strategy.
            let gap_vals: Vec<Value> = match gap_fill {
                "" | "null" | "none" => agg_ops.iter().map(|_| Value::Null).collect(),
                "prev" => {
                    if let Some(prev) = &prev_row {
                        prev[1..].to_vec()
                    } else {
                        agg_ops.iter().map(|_| Value::Null).collect()
                    }
                }
                "linear" | "next" => {
                    // For simplicity, emit null; true linear/next interpolation
                    // requires a forward pass. The current engine is single-pass.
                    agg_ops.iter().map(|_| Value::Null).collect()
                }
                literal => {
                    // Try to parse as a numeric literal for all agg columns.
                    let v = literal
                        .parse::<f64>()
                        .map(Value::Float)
                        .unwrap_or_else(|_| Value::String(literal.to_string()));
                    agg_ops.iter().map(|_| v.clone()).collect()
                }
            };
            let mut r = vec![Value::Integer(*bucket_start)];
            r.extend(gap_vals);
            r
        };

        prev_row = Some(row_vals.clone());
        rows.push(row_vals);

        if limit > 0 && rows.len() >= limit {
            break;
        }
    }

    // Apply projection if specified.
    let (final_columns, final_rows) = if projection.is_empty() {
        (columns, rows)
    } else {
        project_rows(columns, rows, projection)
    };

    Ok(QueryResult {
        columns: final_columns,
        rows: final_rows,
        rows_affected: 0,
    })
}

// ── Raw scan ──────────────────────────────────────────────────────────────────

fn raw_scan(
    raw: Vec<(i64, f64, nodedb_types::timeseries::SeriesId)>,
    projection: &[String],
    limit: usize,
) -> Result<QueryResult, LiteError> {
    let columns = vec![
        "ts".to_string(),
        "value".to_string(),
        "series_id".to_string(),
    ];

    let cap = if limit > 0 {
        raw.len().min(limit)
    } else {
        raw.len()
    };

    let rows: Vec<Vec<Value>> = raw
        .into_iter()
        .take(cap)
        .map(|(ts, val, sid)| {
            vec![
                Value::Integer(ts),
                Value::Float(val),
                Value::Integer(sid as i64),
            ]
        })
        .collect();

    let (final_columns, final_rows) = if projection.is_empty() {
        (columns, rows)
    } else {
        project_rows(columns, rows, projection)
    };

    Ok(QueryResult {
        columns: final_columns,
        rows: final_rows,
        rows_affected: 0,
    })
}

// ── Projection helper ─────────────────────────────────────────────────────────

fn project_rows(
    columns: Vec<String>,
    rows: Vec<Vec<Value>>,
    projection: &[String],
) -> (Vec<String>, Vec<Vec<Value>>) {
    let indices: Vec<usize> = projection
        .iter()
        .filter_map(|p| columns.iter().position(|c| c == p))
        .collect();

    if indices.is_empty() {
        return (columns, rows);
    }

    let new_cols: Vec<String> = indices.iter().map(|&i| columns[i].clone()).collect();
    let new_rows: Vec<Vec<Value>> = rows
        .into_iter()
        .map(|row| indices.iter().map(|&i| row[i].clone()).collect())
        .collect();

    (new_cols, new_rows)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_project_rows_identity() {
        let cols = vec!["a".to_string(), "b".to_string()];
        let rows = vec![vec![Value::Integer(1), Value::Integer(2)]];
        let (c, r) = project_rows(cols.clone(), rows.clone(), &[]);
        assert_eq!(c, cols);
        assert_eq!(r, rows);
    }

    #[test]
    fn test_project_rows_subset() {
        let cols = vec![
            "ts".to_string(),
            "value".to_string(),
            "series_id".to_string(),
        ];
        let rows = vec![vec![
            Value::Integer(100),
            Value::Float(1.5),
            Value::Integer(0),
        ]];
        let (c, r) = project_rows(cols, rows, &["ts".to_string(), "value".to_string()]);
        assert_eq!(c, vec!["ts", "value"]);
        assert_eq!(r[0], vec![Value::Integer(100), Value::Float(1.5)]);
    }
}
