// SPDX-License-Identifier: Apache-2.0
//! Continuous aggregate meta-ops.

use nodedb_types::result::QueryResult;
use nodedb_types::timeseries::continuous_agg::ContinuousAggregateDef;
use nodedb_types::value::Value;

use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::storage::engine::StorageEngine;

fn lock_ts<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
) -> Result<
    std::sync::MutexGuard<'_, crate::engine::timeseries::engine::core::TimeseriesEngine>,
    LiteError,
> {
    engine
        .timeseries
        .lock()
        .map_err(|_| LiteError::LockPoisoned)
}

/// `RegisterContinuousAggregate` — register a new aggregate definition.
pub async fn handle_register_continuous_aggregate<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    def: ContinuousAggregateDef,
) -> Result<QueryResult, LiteError> {
    let mut ts = lock_ts(engine)?;
    ts.continuous_agg_mgr.register(def.clone());
    Ok(QueryResult {
        columns: vec!["name".into()],
        rows: vec![vec![Value::String(def.name)]],
        rows_affected: 1,
    })
}

/// `UnregisterContinuousAggregate` — remove an aggregate by name.
pub async fn handle_unregister_continuous_aggregate<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    name: &str,
) -> Result<QueryResult, LiteError> {
    let mut ts = lock_ts(engine)?;
    ts.continuous_agg_mgr.unregister(name);
    Ok(QueryResult {
        columns: vec!["name".into()],
        rows: vec![vec![Value::String(name.to_owned())]],
        rows_affected: 1,
    })
}

/// `ListContinuousAggregates` — list all registered aggregate names.
pub async fn handle_list_continuous_aggregates<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
) -> Result<QueryResult, LiteError> {
    let ts = lock_ts(engine)?;
    let names: Vec<Vec<Value>> = ts
        .continuous_agg_mgr
        .list()
        .into_iter()
        .map(|n| vec![Value::String(n.to_owned())])
        .collect();
    Ok(QueryResult {
        columns: vec!["name".into()],
        rows: names,
        rows_affected: 0,
    })
}

/// `ApplyContinuousAggRetention` — drop materialized buckets older than each
/// aggregate's configured retention period.
pub async fn handle_apply_continuous_agg_retention<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
) -> Result<QueryResult, LiteError> {
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64;
    let mut ts = lock_ts(engine)?;
    let dropped = ts.continuous_agg_mgr.apply_retention(now_ms);
    Ok(QueryResult {
        columns: vec!["dropped_buckets".into()],
        rows: vec![vec![Value::Integer(dropped as i64)]],
        rows_affected: dropped as u64,
    })
}

/// `QueryAggregateWatermark` — return the highest bucket_ts for an aggregate.
pub async fn handle_query_aggregate_watermark<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    aggregate_name: &str,
) -> Result<QueryResult, LiteError> {
    let ts = lock_ts(engine)?;
    let wm = ts.continuous_agg_mgr.watermark(aggregate_name);
    Ok(QueryResult {
        columns: vec!["watermark_ms".into()],
        rows: vec![vec![Value::Integer(wm)]],
        rows_affected: 0,
    })
}

/// `QueryLastValues` — read the most-recent materialized bucket per group key
/// from each registered continuous aggregate backed by `collection`.
///
/// Iterates all aggregates whose source matches `collection`, picks the
/// highest-`bucket_ts` bucket for each group key, and returns one row per
/// `(aggregate_name, group_key, bucket_ts, <agg_col>, ...)`. Returns
/// `BadRequest` when no aggregates are registered for this collection so
/// callers fail loudly instead of silently getting zero rows.
pub async fn handle_query_aggregate_last_values<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    collection: &str,
) -> Result<QueryResult, LiteError> {
    let ts = lock_ts(engine)?;

    // Collect aggregates that source from this collection.
    let agg_names: Vec<String> = ts
        .continuous_agg_mgr
        .list()
        .into_iter()
        .filter(|name| {
            ts.continuous_agg_mgr
                .get(name)
                .is_some_and(|d| d.source == collection)
        })
        .map(|s| s.to_owned())
        .collect();

    if agg_names.is_empty() {
        return Err(LiteError::BadRequest {
            detail: format!(
                "no continuous aggregates registered for collection '{collection}'; \
                 register one via RegisterContinuousAggregate before querying last values"
            ),
        });
    }

    // Columns: agg_name, group_key, bucket_ts, plus per-agg output columns.
    // We emit one row per (agg_name, group_key) using the most-recent bucket.
    let mut rows: Vec<Vec<Value>> = Vec::new();

    for agg_name in &agg_names {
        let all_buckets = ts.continuous_agg_mgr.query_all(agg_name);
        // query_all returns buckets sorted by (bucket_ts, group_key) ascending.
        // To get the last value per group_key, scan in reverse and take the
        // first occurrence of each group_key.
        let mut seen_groups = std::collections::HashSet::new();
        for bucket in all_buckets.into_iter().rev() {
            if seen_groups.insert(bucket.group_key.clone()) {
                let def = match ts.continuous_agg_mgr.get(agg_name) {
                    Some(d) => d,
                    None => continue,
                };
                let mut row = vec![
                    Value::String(agg_name.clone()),
                    Value::String(bucket.group_key.clone()),
                    Value::Integer(bucket.bucket_ts),
                ];
                for (acc, expr) in bucket.accumulators.iter().zip(def.aggregates.iter()) {
                    row.push(Value::Float(acc.result(&expr.function)));
                }
                rows.push(row);
            }
        }
    }

    Ok(QueryResult {
        columns: vec![
            "agg_name".into(),
            "group_key".into(),
            "bucket_ts".into(),
            "value".into(),
        ],
        rows,
        rows_affected: 0,
    })
}

/// `QueryLastValue` — read the single most-recent materialized bucket for
/// `series_id` (treated as an aggregate name index or direct aggregate name
/// lookup) from the continuous aggregate backed by `collection`.
///
/// In Lite, `series_id` indexes into the list of aggregates registered for
/// `collection` (0-based). The most-recent bucket across all group keys is
/// returned. Returns `BadRequest` when no matching aggregate exists.
pub async fn handle_query_aggregate_last_value<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    collection: &str,
    series_id: u64,
) -> Result<QueryResult, LiteError> {
    let ts = lock_ts(engine)?;

    let agg_names: Vec<String> = ts
        .continuous_agg_mgr
        .list()
        .into_iter()
        .filter(|name| {
            ts.continuous_agg_mgr
                .get(name)
                .is_some_and(|d| d.source == collection)
        })
        .map(|s| s.to_owned())
        .collect();

    let agg_name = agg_names
        .get(series_id as usize)
        .ok_or_else(|| LiteError::BadRequest {
            detail: format!(
                "series_id {series_id} out of range: collection '{collection}' has {} \
             registered continuous aggregates",
                agg_names.len()
            ),
        })?;

    let all_buckets = ts.continuous_agg_mgr.query_all(agg_name);
    let bucket = all_buckets.last().ok_or_else(|| LiteError::BadRequest {
        detail: format!(
            "continuous aggregate '{agg_name}' has no materialized buckets yet; \
             ingest data into '{collection}' to populate it"
        ),
    })?;

    let def = ts
        .continuous_agg_mgr
        .get(agg_name)
        .ok_or_else(|| LiteError::BadRequest {
            detail: format!("continuous aggregate '{agg_name}' definition not found"),
        })?;

    let mut row = vec![
        Value::String(agg_name.clone()),
        Value::String(bucket.group_key.clone()),
        Value::Integer(bucket.bucket_ts),
    ];
    for (acc, expr) in bucket.accumulators.iter().zip(def.aggregates.iter()) {
        row.push(Value::Float(acc.result(&expr.function)));
    }

    Ok(QueryResult {
        columns: vec![
            "agg_name".into(),
            "group_key".into(),
            "bucket_ts".into(),
            "value".into(),
        ],
        rows: vec![row],
        rows_affected: 0,
    })
}
