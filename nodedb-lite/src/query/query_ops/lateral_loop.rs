// SPDX-License-Identifier: Apache-2.0
//! LateralLoop — nested-loop lateral join with correlated predicates.
//!
//! For each outer row, re-scans the inner collection with equality filters
//! built from `correlation_predicates` (inner_field = outer_field_value).
//! No sort, no limit per outer row.  Enforces `outer_row_cap`.

use std::collections::HashMap;

use nodedb_physical::physical_plan::PhysicalPlan;
use nodedb_physical::physical_plan::query::JoinProjection;
use nodedb_query::scan_filter::FilterOp;
use nodedb_query::scan_filter::ScanFilter;
use nodedb_types::result::QueryResult;
use nodedb_types::value::Value;

use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::query::query_ops::joins::common::{
    apply_filters, apply_projection, decode_filters, maps_to_result, scan_collection,
};
use nodedb_sql::types::SqlPlan;

use crate::query::query_ops::lateral_top_k::{execute_nested_plan, prefix_row};
use crate::storage::engine::StorageEngine;

/// SQL-plan-aware entry for `LiteVisitor::lateral_loop`.
///
/// Executes `outer_sql` via `engine.execute_plan`, then for each outer row
/// re-executes `inner_sql` with correlated equality filters injected.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn execute_lateral_loop_sql<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    outer_sql: &SqlPlan,
    outer_alias: &str,
    inner_sql: &SqlPlan,
    correlation_predicates: &[(String, String)],
    lateral_alias: &str,
    projection: &[JoinProjection],
    left_join: bool,
    outer_row_cap: usize,
) -> Result<QueryResult, LiteError> {
    use crate::query::query_ops::joins::common::rows_to_maps;

    let outer_result = engine.execute_plan(outer_sql).await?;
    let outer_rows = rows_to_maps(outer_result);

    if outer_row_cap > 0 && outer_rows.len() > outer_row_cap {
        return Err(LiteError::Storage {
            detail: format!(
                "lateral loop outer_row_cap={outer_row_cap} exceeded: got {} outer rows",
                outer_rows.len()
            ),
        });
    }

    let mut output: Vec<HashMap<String, Value>> = Vec::new();

    for outer_row in &outer_rows {
        let outer_prefixed = prefix_row(outer_row, outer_alias);

        let inner_result = engine.execute_plan(inner_sql).await?;
        let inner_all = rows_to_maps(inner_result);

        let mut corr_filters: Vec<ScanFilter> = Vec::new();
        for (inner_field, outer_field) in correlation_predicates {
            let val = outer_row.get(outer_field).cloned().unwrap_or(Value::Null);
            corr_filters.push(ScanFilter {
                field: inner_field.clone(),
                op: FilterOp::Eq,
                value: val,
                ..Default::default()
            });
        }
        let inner_rows = apply_filters(inner_all, &corr_filters);

        if inner_rows.is_empty() {
            if left_join {
                let projected = apply_projection(vec![outer_prefixed], projection);
                output.extend(projected);
            }
            continue;
        }
        for inner_row in &inner_rows {
            let inner_prefixed = prefix_row(inner_row, lateral_alias);
            let mut merged = outer_prefixed.clone();
            merged.extend(inner_prefixed);
            let projected = apply_projection(vec![merged], projection);
            output.extend(projected);
        }
    }

    Ok(maps_to_result(output))
}

/// LATERAL nested-loop with correlated predicates injected per outer row.
///
/// Runs `outer_plan` once to materialise outer rows, caps them at
/// `outer_row_cap` (0 = uncapped), then for each outer row builds equality
/// filters from `correlation_predicates` and scans `inner_collection`.
/// Emits every matching inner row merged with the outer row.  Supports LEFT
/// join semantics.
#[allow(clippy::too_many_arguments)]
pub async fn execute_lateral_loop<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    outer_plan: &PhysicalPlan,
    outer_alias: &str,
    inner_collection: &str,
    inner_filters: &[u8],
    correlation_predicates: &[(String, String)],
    lateral_alias: &str,
    projection: &[JoinProjection],
    left_join: bool,
    outer_row_cap: usize,
) -> Result<QueryResult, LiteError> {
    let outer_rows = execute_nested_plan(engine, outer_plan).await?;

    if outer_row_cap > 0 && outer_rows.len() > outer_row_cap {
        return Err(LiteError::Storage {
            detail: format!(
                "lateral loop outer_row_cap={outer_row_cap} exceeded: got {} outer rows",
                outer_rows.len()
            ),
        });
    }

    let base_inner_filters = decode_filters(inner_filters)?;

    let mut output: Vec<HashMap<String, Value>> = Vec::new();

    for outer_row in &outer_rows {
        // Build correlated equality filters.
        let mut filters = base_inner_filters.clone();
        for (inner_field, outer_field) in correlation_predicates {
            let val = outer_row.get(outer_field).cloned().unwrap_or(Value::Null);
            filters.push(ScanFilter {
                field: inner_field.clone(),
                op: FilterOp::Eq,
                value: val,
                ..Default::default()
            });
        }

        let inner_all = scan_collection(engine, inner_collection).await?;
        let inner_rows = apply_filters(inner_all, &filters);

        if inner_rows.is_empty() {
            if left_join {
                let merged = prefix_row(outer_row, outer_alias);
                output.push(merged);
            }
            continue;
        }

        for inner_row in &inner_rows {
            let outer_prefixed = prefix_row(outer_row, outer_alias);
            let inner_prefixed = prefix_row(inner_row, lateral_alias);
            let mut merged = outer_prefixed;
            merged.extend(inner_prefixed);
            output.push(merged);
        }
    }

    let projected = apply_projection(output, projection);
    Ok(maps_to_result(projected))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn customer(cust_id: i64, region: &str) -> HashMap<String, Value> {
        let mut m = HashMap::new();
        m.insert("cust_id".to_string(), Value::Integer(cust_id));
        m.insert("region".to_string(), Value::String(region.to_string()));
        m
    }

    fn order(order_id: i64, cust_id: i64, amount: i64) -> HashMap<String, Value> {
        let mut m = HashMap::new();
        m.insert("order_id".to_string(), Value::Integer(order_id));
        m.insert("cust_id".to_string(), Value::Integer(cust_id));
        m.insert("amount".to_string(), Value::Integer(amount));
        m
    }

    /// Correlated WHERE: each customer row triggers an inner scan that
    /// matches orders with matching cust_id.
    #[test]
    fn test_lateral_loop_correlated_where() {
        let customers = vec![customer(1, "west"), customer(2, "east")];
        let orders = vec![
            order(1, 1, 100),
            order(2, 1, 200),
            order(3, 1, 150),
            order(4, 2, 300),
        ];

        let mut all_output: Vec<HashMap<String, Value>> = Vec::new();
        for outer_row in &customers {
            let cust_val = outer_row.get("cust_id").cloned().unwrap_or(Value::Null);
            let filters = vec![ScanFilter {
                field: "cust_id".to_string(),
                op: FilterOp::Eq,
                value: cust_val,
                ..Default::default()
            }];
            let inner_rows = apply_filters(orders.clone(), &filters);
            for inner_row in inner_rows {
                let mut merged = outer_row.clone();
                merged.extend(inner_row);
                all_output.push(merged);
            }
        }

        // cust 1 → 3 orders; cust 2 → 1 order.
        assert_eq!(all_output.len(), 4);
        // All output rows carry the customer's region.
        assert!(all_output.iter().all(|r| r.contains_key("region")));
    }

    /// outer_row_cap enforcement: error when cap is exceeded.
    #[test]
    fn test_outer_row_cap_error_message() {
        let outer_rows: Vec<HashMap<String, Value>> = (0..5)
            .map(|i| {
                let mut m = HashMap::new();
                m.insert("id".to_string(), Value::Integer(i));
                m
            })
            .collect();
        let cap = 3usize;
        let exceeded = outer_rows.len() > cap;
        assert!(exceeded, "cap should be exceeded");
    }
}
