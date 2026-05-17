// SPDX-License-Identifier: Apache-2.0
//! SQL-visitor lowering for timeseries SqlPlan variants:
//! TimeseriesScan, TimeseriesIngest.

use nodedb_physical::PhysicalTaskVisitor;
use nodedb_physical::physical_plan::TimeseriesOp;
use nodedb_sql::temporal::TemporalScope;
use nodedb_sql::types::filter::Filter;
use nodedb_sql::types::query::{AggregateExpr, Projection};
use nodedb_sql::types_expr::SqlExpr;
use nodedb_sql::types_expr::SqlValue;

use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::query::filter_convert::sql_filters_to_metadata;
use crate::query::physical_visitor::LiteDataPlaneVisitor;
use crate::storage::engine::{StorageEngine, StorageEngineSync};

use super::adapter::LiteFut;

fn encode_filters(filters: &[Filter]) -> Result<Vec<u8>, LiteError> {
    if filters.is_empty() {
        return Ok(Vec::new());
    }
    match sql_filters_to_metadata(filters, &[])? {
        None => Ok(Vec::new()),
        Some(mf) => zerompk::to_msgpack_vec(&mf).map_err(|e| LiteError::Serialization {
            detail: format!("encode timeseries filters: {e}"),
        }),
    }
}

/// Extract column-name projections from a `Projection` slice.
fn extract_projection_cols(projection: &[Projection]) -> Vec<String> {
    projection
        .iter()
        .filter_map(|p| match p {
            Projection::Column(name) => Some(name.clone()),
            Projection::Computed { alias, .. } => Some(alias.clone()),
            _ => None,
        })
        .collect()
}

/// Convert SQL `AggregateExpr` list to `(op, field)` pairs expected by `TimeseriesOp::Scan`.
fn convert_aggregates(aggregates: &[AggregateExpr]) -> Vec<(String, String)> {
    aggregates
        .iter()
        .map(|agg| {
            let field = agg
                .args
                .first()
                .and_then(|a| match a {
                    SqlExpr::Column { name, .. } => Some(name.clone()),
                    SqlExpr::Wildcard => Some("*".to_string()),
                    _ => None,
                })
                .unwrap_or_else(|| "*".to_string());
            (agg.function.clone(), field)
        })
        .collect()
}

// ── TimeseriesScan ────────────────────────────────────────────────────────────

/// Lower `SqlPlan::TimeseriesScan` to `TimeseriesOp::Scan`.
#[allow(clippy::too_many_arguments)]
pub(super) fn lower_timeseries_scan<'a, S: StorageEngine + StorageEngineSync + 'a>(
    engine: &'a LiteQueryEngine<S>,
    collection: &str,
    time_range: (i64, i64),
    bucket_interval_ms: i64,
    group_by: &[String],
    aggregates: &[AggregateExpr],
    filters: &[Filter],
    projection: &[Projection],
    gap_fill: &str,
    limit: usize,
    _tiered: bool,
    temporal: &TemporalScope,
) -> Result<LiteFut<'a>, LiteError> {
    let filter_bytes = encode_filters(filters)?;
    let proj_cols = extract_projection_cols(projection);
    let agg_pairs = convert_aggregates(aggregates);

    let (system_as_of_ms, valid_at_ms) = extract_temporal(temporal);

    let op = TimeseriesOp::Scan {
        collection: collection.to_string(),
        time_range,
        projection: proj_cols,
        limit,
        filters: filter_bytes,
        bucket_interval_ms,
        group_by: group_by.to_vec(),
        aggregates: agg_pairs,
        gap_fill: gap_fill.to_string(),
        computed_columns: Vec::new(),
        rls_filters: Vec::new(),
        system_as_of_ms,
        valid_at_ms,
    };

    let mut phys = LiteDataPlaneVisitor { engine };
    let fut = phys.timeseries(&op)?;
    Ok(Box::pin(fut))
}

/// Extract bitemporal cutoffs from a `TemporalScope`.
///
/// `system_as_of_ms` maps directly from `TemporalScope::system_as_of_ms`;
/// `valid_at_ms` maps from `ValidTime::At`.
fn extract_temporal(scope: &TemporalScope) -> (Option<i64>, Option<i64>) {
    use nodedb_sql::temporal::ValidTime;
    let sys = scope.system_as_of_ms;
    let valid = match &scope.valid_time {
        ValidTime::At(ms) => Some(*ms),
        _ => None,
    };
    (sys, valid)
}

// ── TimeseriesIngest ──────────────────────────────────────────────────────────

/// Lower `SqlPlan::TimeseriesIngest` to `TimeseriesOp::Ingest`.
///
/// Rows are serialized to MessagePack in the `samples` format expected by the
/// Lite timeseries engine. Each row is a flat `HashMap<String, Value>` encoded
/// with zerompk; the payload field holds the concatenated msgpack bytes of a
/// `Vec<HashMap<String, Value>>`.
pub(super) fn lower_timeseries_ingest<'a, S: StorageEngine + StorageEngineSync + 'a>(
    engine: &'a LiteQueryEngine<S>,
    collection: &str,
    rows: &[Vec<(String, SqlValue)>],
) -> Result<LiteFut<'a>, LiteError> {
    use nodedb_types::value::Value;
    use std::collections::HashMap;

    let row_maps: Vec<HashMap<String, Value>> = rows
        .iter()
        .map(|row| {
            row.iter()
                .map(|(col, sv)| {
                    let v = crate::query::filter_convert::sql_value_to_value(sv)?;
                    Ok((col.clone(), v))
                })
                .collect::<Result<HashMap<String, Value>, LiteError>>()
        })
        .collect::<Result<Vec<_>, LiteError>>()?;

    let payload = zerompk::to_msgpack_vec(&row_maps).map_err(|e| LiteError::Serialization {
        detail: format!("encode timeseries ingest payload: {e}"),
    })?;

    let op = TimeseriesOp::Ingest {
        collection: collection.to_string(),
        payload,
        format: "samples".to_string(),
        wal_lsn: None,
        surrogates: Vec::new(),
    };

    let mut phys = LiteDataPlaneVisitor { engine };
    let fut = phys.timeseries(&op)?;
    Ok(Box::pin(fut))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::query::engine::LiteQueryEngine;
    use crate::storage::redb_storage::RedbStorage;

    fn make_engine() -> LiteQueryEngine<RedbStorage> {
        use std::sync::Mutex;
        let storage = Arc::new(RedbStorage::open_in_memory().expect("in-memory redb"));
        let crdt = Arc::new(Mutex::new(
            crate::engine::crdt::CrdtEngine::new(1).expect("crdt"),
        ));
        let strict = Arc::new(crate::engine::strict::StrictEngine::new(Arc::clone(
            &storage,
        )));
        let columnar = Arc::new(crate::engine::columnar::ColumnarEngine::new(Arc::clone(
            &storage,
        )));
        let htap = Arc::new(crate::engine::htap::HtapBridge::new());
        let timeseries = Arc::new(Mutex::new(
            crate::engine::timeseries::engine::TimeseriesEngine::new(),
        ));
        let vector_state = Arc::new(crate::engine::vector::VectorState::new(
            Arc::clone(&storage),
            100,
        ));
        let array_state = Arc::new(Mutex::new(
            crate::engine::array::engine::ArrayEngineState::open(&storage).expect("array"),
        ));
        let fts_state = Arc::new(crate::engine::fts::FtsState::new());
        let spatial = Arc::new(Mutex::new(
            crate::engine::spatial::SpatialIndexManager::new(),
        ));
        LiteQueryEngine::new(
            crdt,
            strict,
            columnar,
            htap,
            storage,
            timeseries,
            vector_state,
            array_state,
            fts_state,
            spatial,
            Arc::new(Mutex::new(std::collections::HashMap::new())),
        )
    }

    #[tokio::test]
    async fn test_timeseries_scan_lower() {
        use nodedb_sql::temporal::TemporalScope;
        let engine = make_engine();
        let result = super::lower_timeseries_scan(
            &engine,
            "metrics",
            (0, i64::MAX),
            0,
            &[],
            &[],
            &[],
            &[],
            "",
            100,
            false,
            &TemporalScope::default(),
        );
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_timeseries_ingest_lower() {
        use nodedb_sql::types_expr::SqlValue;
        let engine = make_engine();
        let rows = vec![vec![
            ("ts".to_string(), SqlValue::Int(1_700_000_000_000)),
            ("value".to_string(), SqlValue::Float(42.0)),
        ]];
        let result = super::lower_timeseries_ingest(&engine, "metrics", &rows);
        assert!(result.is_ok());
    }
}
