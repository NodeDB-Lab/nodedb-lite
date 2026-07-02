// SPDX-License-Identifier: Apache-2.0
//! SQL-visitor lowering for query-shaped SqlPlan variants:
//! Aggregate, Join, DocumentIndexLookup, RangeScan, Cte.

use std::collections::HashMap;

use nodedb_physical::PhysicalTaskVisitor;
use nodedb_physical::physical_plan::document::DocumentOp;
use nodedb_physical::physical_plan::query::{AggregateSpec, JoinProjection};
use nodedb_sql::temporal::TemporalScope;
use nodedb_sql::types::SqlPlan;
use nodedb_sql::types::filter::Filter;
use nodedb_sql::types::query::EngineType;
use nodedb_sql::types::query::{AggregateExpr, JoinType, Projection, SortKey, WindowSpec};
use nodedb_sql::types_expr::{SqlExpr, SqlValue};
use nodedb_types::result::QueryResult;
use nodedb_types::value::Value;

use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::query::filter_convert::{sql_filters_to_metadata, sql_value_to_value};
use crate::query::physical_visitor::LiteDataPlaneVisitor;
use crate::query::query_ops::aggregate::execute_aggregate;
use crate::query::query_ops::joins::inline_hash::execute_inline_hash_join;
use crate::storage::engine::StorageEngine;

use super::adapter::LiteFut;
use super::having_eval::{apply_having_result, make_agg_alias_map};
use super::scan_post::apply_scan_post_processing;

/// Convert a `nodedb_sql` `AggregateExpr` to a physical `AggregateSpec`.
pub(super) fn sql_agg_to_spec(agg: &AggregateExpr) -> AggregateSpec {
    let field = agg
        .args
        .first()
        .and_then(|a| match a {
            SqlExpr::Column { name, .. } => Some(name.clone()),
            SqlExpr::Wildcard => Some("*".to_string()),
            _ => None,
        })
        .unwrap_or_else(|| "*".to_string());

    AggregateSpec {
        function: agg.function.clone(),
        alias: agg.alias.clone(),
        user_alias: None,
        field,
        expr: None,
    }
}

fn convert_aggregates(aggs: &[AggregateExpr]) -> Vec<AggregateSpec> {
    aggs.iter().map(sql_agg_to_spec).collect()
}

/// Extract a column name string from a `SortKey.expr` for use in aggregate sort.
fn sort_key_to_pair(k: &SortKey) -> (String, bool) {
    let name = match &k.expr {
        SqlExpr::Column { name, .. } => name.clone(),
        other => format!("{other:?}"),
    };
    (name, k.ascending)
}

/// Encode `Vec<Filter>` → msgpack bytes via `ScanFilter`.
fn encode_filters(filters: &[Filter]) -> Result<Vec<u8>, LiteError> {
    if filters.is_empty() {
        return Ok(Vec::new());
    }
    // Complex QExpr predicates are evaluated post-scan; only primitive conditions
    // are pushed to the physical visitor via serialized MetadataFilter.
    match sql_filters_to_metadata(filters, &[])?.meta {
        None => Ok(Vec::new()),
        Some(mf) => zerompk::to_msgpack_vec(&mf).map_err(|e| LiteError::Serialization {
            detail: format!("encode filters: {e}"),
        }),
    }
}

/// Convert `QueryResult` rows to `Vec<HashMap<String, Value>>`.
fn result_to_maps(result: QueryResult) -> Vec<HashMap<String, Value>> {
    let cols = result.columns;
    result
        .rows
        .into_iter()
        .map(|row| cols.iter().cloned().zip(row).collect())
        .collect()
}

/// Encode a `QueryResult` as msgpack bytes for inline hash join.
fn encode_result_msgpack(result: &QueryResult) -> Result<Vec<u8>, LiteError> {
    let maps: Vec<HashMap<String, Value>> = result
        .rows
        .iter()
        .map(|row| {
            result
                .columns
                .iter()
                .cloned()
                .zip(row.iter().cloned())
                .collect()
        })
        .collect();
    zerompk::to_msgpack_vec(&maps).map_err(|e| LiteError::Serialization {
        detail: format!("encode join side msgpack: {e}"),
    })
}

/// Convert `SqlValue` to its string representation for index lookups.
fn sql_value_to_index_str(v: &SqlValue) -> String {
    match v {
        SqlValue::String(s) => s.clone(),
        SqlValue::Int(i) => i.to_string(),
        SqlValue::Float(f) => f.to_string(),
        SqlValue::Bool(b) => b.to_string(),
        _ => String::new(),
    }
}

// ── Aggregate ────────────────────────────────────────────────────────────────

#[allow(clippy::too_many_arguments)]
pub(super) fn lower_aggregate<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    input: &SqlPlan,
    group_by: &[SqlExpr],
    aggregates: &[AggregateExpr],
    having: &[Filter],
    _limit: usize,
    grouping_sets: Option<&[Vec<usize>]>,
    sort_keys: &[SortKey],
) -> Result<LiteFut<'a>, LiteError> {
    let input = input.clone();
    let group_cols: Vec<String> = group_by
        .iter()
        .map(|e| match e {
            SqlExpr::Column { name, .. } => name.clone(),
            other => format!("{other:?}"),
        })
        .collect();
    let agg_specs = convert_aggregates(aggregates);
    // Build aggregate-function → alias lookup for HAVING post-filter.
    let agg_alias_map = make_agg_alias_map(aggregates);
    // HAVING predicates always reference aggregate results (e.g. SUM(salary) > 100)
    // which are not present as named columns until after aggregation.
    // apply_having_result handles all predicate shapes via having_eval, including
    // function-call resolution through agg_alias_map. We always do the post-filter
    // and pass empty bytes to execute_aggregate (no pushdown for HAVING).
    let having_post = having.to_vec();
    let sort_pairs: Vec<(String, bool)> = sort_keys.iter().map(sort_key_to_pair).collect();
    let gs: Vec<Vec<u32>> = grouping_sets
        .unwrap_or(&[])
        .iter()
        .map(|s| s.iter().map(|&i| i as u32).collect())
        .collect();

    Ok(Box::pin(async move {
        let source_result = engine.execute_plan(&input).await?;
        let rows = result_to_maps(source_result);
        let result = execute_aggregate(rows, &group_cols, &agg_specs, &[], &[], &sort_pairs, &gs)?;
        Ok(apply_having_result(result, &having_post, &agg_alias_map))
    }))
}

// ── Join ─────────────────────────────────────────────────────────────────────

#[allow(clippy::too_many_arguments)]
pub(super) fn lower_join<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    left: &SqlPlan,
    right: &SqlPlan,
    on: &[(String, String)],
    join_type: JoinType,
    _condition: Option<&SqlExpr>,
    limit: Option<usize>,
    projection: &[Projection],
    filters: &[Filter],
) -> Result<LiteFut<'a>, LiteError> {
    let left = left.clone();
    let right = right.clone();
    let on = on.to_vec();
    let limit = limit.unwrap_or(usize::MAX);
    // JoinType debug output: Inner, Left, Right, Full — lower to string for hash join.
    let join_type_str = format!("{join_type:?}").to_lowercase();
    let proj: Vec<JoinProjection> = projection
        .iter()
        .filter_map(|p| match p {
            Projection::Column(name) => Some(JoinProjection {
                source: name.clone(),
                output: name.clone(),
            }),
            Projection::Computed { alias, .. } => Some(JoinProjection {
                source: alias.clone(),
                output: alias.clone(),
            }),
            _ => None,
        })
        .collect();
    let post_filters_bytes = encode_filters(filters)?;

    Ok(Box::pin(async move {
        let left_result = engine.execute_plan(&left).await?;
        let right_result = engine.execute_plan(&right).await?;

        let left_bytes = encode_result_msgpack(&left_result)?;
        let right_bytes = encode_result_msgpack(&right_result)?;

        execute_inline_hash_join(
            &left_bytes,
            &right_bytes,
            None,
            &on,
            &join_type_str,
            limit,
            &proj,
            &post_filters_bytes,
        )
    }))
}

// ── DocumentIndexLookup ──────────────────────────────────────────────────────

#[allow(clippy::too_many_arguments)]
pub(super) fn lower_document_index_lookup<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    collection: &str,
    _alias: Option<&str>,
    _engine_type: EngineType,
    field: &str,
    value: &SqlValue,
    filters: &[Filter],
    projection: &[Projection],
    sort_keys: &[SortKey],
    limit: Option<usize>,
    offset: usize,
    distinct: bool,
    window_functions: &[WindowSpec],
    case_insensitive: bool,
    _temporal: &TemporalScope,
) -> Result<LiteFut<'a>, LiteError> {
    let col = collection.to_string();
    let path = field.to_string();
    let mut val_str = sql_value_to_index_str(value);
    if case_insensitive {
        val_str = val_str.to_lowercase();
    }

    // Encode remaining filters.
    let filter_bytes = encode_filters(filters)?;

    // Extract column-name projections (Star = all columns → empty Vec).
    let proj_cols: Vec<String> = projection
        .iter()
        .filter_map(|p| match p {
            Projection::Column(name) => Some(name.clone()),
            Projection::Computed { alias, .. } => Some(alias.clone()),
            _ => None,
        })
        .collect();

    let raw_limit = limit.unwrap_or(0);
    let filters = filters.to_vec();
    let sort_keys = sort_keys.to_vec();
    let window_functions = window_functions.to_vec();

    let op = DocumentOp::IndexedFetch {
        collection: col,
        path,
        value: val_str,
        filters: filter_bytes,
        projection: proj_cols,
        limit: raw_limit,
        offset,
    };

    let mut phys = LiteDataPlaneVisitor { engine };
    let fut = phys.document(&op)?;

    Ok(Box::pin(async move {
        let raw = fut.await?;
        apply_scan_post_processing(
            raw,
            &filters,
            &sort_keys,
            &window_functions,
            limit,
            offset,
            distinct,
        )
    }))
}

// ── RangeScan ────────────────────────────────────────────────────────────────

pub(super) fn lower_range_scan<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    collection: &str,
    field: &str,
    lower: Option<&SqlValue>,
    upper: Option<&SqlValue>,
    limit: usize,
) -> Result<LiteFut<'a>, LiteError> {
    let col = collection.to_string();
    let fld = field.to_string();

    let encode_bound = |v: &SqlValue| -> Result<Vec<u8>, LiteError> {
        let ndb_val = sql_value_to_value(v)?;
        zerompk::to_msgpack_vec(&ndb_val).map_err(|e| LiteError::Serialization {
            detail: format!("encode range bound: {e}"),
        })
    };

    let lo_bytes: Option<Vec<u8>> = lower.map(encode_bound).transpose()?;
    let hi_bytes: Option<Vec<u8>> = upper.map(encode_bound).transpose()?;

    let op = DocumentOp::RangeScan {
        collection: col,
        field: fld,
        lower: lo_bytes,
        upper: hi_bytes,
        limit,
    };

    let mut phys = LiteDataPlaneVisitor { engine };
    let fut = phys.document(&op)?;

    Ok(Box::pin(fut))
}

// ── Cte ──────────────────────────────────────────────────────────────────────

/// Non-recursive CTE lowering for Lite single-node.
///
/// Each CTE definition is executed in order (surfacing any errors it would
/// produce), then the outer query — which the planner has already resolved
/// against the CTE bodies — is executed. On Lite, the SQL planner inlines
/// non-recursive CTEs into the outer plan, so the outer query is always
/// self-contained; the definition executions here serve as an eager
/// validation pass.
pub(super) fn lower_cte<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    definitions: &[(String, SqlPlan)],
    outer: &SqlPlan,
) -> Result<LiteFut<'a>, LiteError> {
    let definitions = definitions.to_vec();
    let outer = outer.clone();

    Ok(Box::pin(async move {
        for (_name, def_plan) in &definitions {
            let _ = engine.execute_plan(def_plan).await?;
        }
        engine.execute_plan(&outer).await
    }))
}
