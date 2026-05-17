// SPDX-License-Identifier: Apache-2.0
//! LateralTopK — correlated top-K scan: for each outer row, scan an inner
//! collection with equality filters derived from the outer row, sort, and
//! keep the top `inner_limit` results.

use std::collections::HashMap;

use nodedb_physical::physical_plan::PhysicalPlan;
use nodedb_physical::physical_plan::query::JoinProjection;
use nodedb_query::scan_filter::FilterOp;
use nodedb_query::scan_filter::ScanFilter;
use nodedb_types::result::QueryResult;
use nodedb_types::value::Value;

use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::query::physical_visitor::LiteDataPlaneVisitor;
use crate::query::query_ops::joins::common::{
    apply_filters, apply_projection, decode_filters, maps_to_result, merge_rows, rows_to_maps,
    scan_collection,
};
use nodedb_sql::types::SqlPlan;

use crate::storage::engine::{StorageEngine, StorageEngineSync};

/// SQL-plan-aware entry for `LiteVisitor::lateral_top_k`.
///
/// Runs `outer_sql` via `engine.execute_plan`, then delegates to
/// `execute_lateral_top_k` with the materialised rows embedded in an
/// in-memory sentinel plan.  This avoids requiring a second visitor pass
/// to produce a `PhysicalPlan` from the SQL outer plan.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn execute_lateral_top_k_sql<S: StorageEngine + StorageEngineSync>(
    engine: &LiteQueryEngine<S>,
    outer_sql: &SqlPlan,
    outer_alias: &str,
    inner_collection: &str,
    inner_filters: &[u8],
    inner_order_by: &[(String, bool)],
    inner_limit: usize,
    correlation_keys: &[(String, String)],
    lateral_alias: &str,
    projection: &[JoinProjection],
    left_join: bool,
) -> Result<QueryResult, LiteError> {
    let outer_result = engine.execute_plan(outer_sql).await?;
    let outer_rows = rows_to_maps(outer_result);
    let base_inner_filters = decode_filters(inner_filters)?;
    let effective_limit = if inner_limit == 0 {
        usize::MAX
    } else {
        inner_limit
    };

    let mut output: Vec<HashMap<String, Value>> = Vec::new();

    for outer_row in &outer_rows {
        let prefixed = prefix_row(outer_row, outer_alias);
        let mut corr_filters = base_inner_filters.clone();
        for (outer_col, inner_col) in correlation_keys {
            let val = outer_row.get(outer_col).cloned().unwrap_or(Value::Null);
            corr_filters.push(ScanFilter {
                field: inner_col.clone(),
                op: FilterOp::Eq,
                value: val,
                ..Default::default()
            });
        }
        let inner_rows = scan_collection(engine, inner_collection).await?;
        let mut matched = apply_filters(inner_rows, &corr_filters);
        sort_rows(&mut matched, inner_order_by);
        matched.truncate(effective_limit);
        if matched.is_empty() && left_join {
            let projected = apply_projection(vec![prefixed], projection);
            output.extend(projected);
        } else {
            for inner_row in matched {
                let ir_prefixed = prefix_row(&inner_row, lateral_alias);
                let merged = merge_rows(&prefixed, &ir_prefixed, None);
                let projected = apply_projection(vec![merged], projection);
                output.extend(projected);
            }
        }
    }

    Ok(maps_to_result(output))
}

/// LATERAL equi-correlated top-K scan.
///
/// For each row produced by `outer_plan`, injects `correlation_keys` values as
/// equality filters on `inner_collection`, applies any non-correlated
/// `inner_filters`, sorts by `inner_order_by`, takes the top `inner_limit`
/// rows, and merges them with the outer row.  Supports LEFT join semantics
/// (preserve outer rows with no inner match).
#[allow(clippy::too_many_arguments)]
pub async fn execute_lateral_top_k<S: StorageEngine + StorageEngineSync>(
    engine: &LiteQueryEngine<S>,
    outer_plan: &PhysicalPlan,
    outer_alias: &str,
    inner_collection: &str,
    inner_filters: &[u8],
    inner_order_by: &[(String, bool)],
    inner_limit: usize,
    correlation_keys: &[(String, String)],
    lateral_alias: &str,
    projection: &[JoinProjection],
    left_join: bool,
) -> Result<QueryResult, LiteError> {
    let outer_rows = execute_nested_plan(engine, outer_plan).await?;
    let base_inner_filters = decode_filters(inner_filters)?;
    let effective_limit = if inner_limit == 0 {
        usize::MAX
    } else {
        inner_limit
    };

    let mut output: Vec<HashMap<String, Value>> = Vec::new();

    for outer_row in &outer_rows {
        // Build correlated equality filters from outer row values.
        let mut filters = base_inner_filters.clone();
        for (outer_col, inner_col) in correlation_keys {
            let val = outer_row.get(outer_col).cloned().unwrap_or(Value::Null);
            filters.push(ScanFilter {
                field: inner_col.clone(),
                op: FilterOp::Eq,
                value: val,
                ..Default::default()
            });
        }

        // Scan inner collection and apply all filters.
        let inner_all = scan_collection(engine, inner_collection).await?;
        let mut inner_rows = apply_filters(inner_all, &filters);

        // Sort inner rows.
        if !inner_order_by.is_empty() {
            sort_rows(&mut inner_rows, inner_order_by);
        }

        // Take top inner_limit.
        inner_rows.truncate(effective_limit);

        if inner_rows.is_empty() {
            if left_join {
                // Emit outer row with null inner columns.
                let merged = prefix_row(outer_row, outer_alias);
                output.push(merged);
            }
            continue;
        }

        for inner_row in &inner_rows {
            let outer_prefixed = prefix_row(outer_row, outer_alias);
            let inner_prefixed = prefix_row(inner_row, lateral_alias);
            let merged = merge_rows(&outer_prefixed, &inner_prefixed, None);
            output.push(merged);
        }
    }

    let projected = apply_projection(output, projection);
    Ok(maps_to_result(projected))
}

// ── Nested-plan re-entry ─────────────────────────────────────────────────────

/// Execute an arbitrary `PhysicalPlan` via the Lite visitor, returning rows as maps.
///
/// This is the canonical re-entry point for sub-plans carried inside QueryOp
/// variants (LateralTopK, LateralLoop).  It creates a fresh `LiteDataPlaneVisitor`,
/// dispatches the plan through `nodedb_physical::dispatch`, and materialises
/// the result into column-keyed maps.
pub(crate) async fn execute_nested_plan<S: StorageEngine + StorageEngineSync>(
    engine: &LiteQueryEngine<S>,
    plan: &PhysicalPlan,
) -> Result<Vec<HashMap<String, Value>>, LiteError> {
    let mut visitor = LiteDataPlaneVisitor { engine };
    let fut = nodedb_physical::dispatch(&mut visitor, plan)?;
    let result = fut.await?;
    Ok(rows_to_maps(result))
}

// ── Sorting ──────────────────────────────────────────────────────────────────

fn sort_rows(rows: &mut [HashMap<String, Value>], order_by: &[(String, bool)]) {
    rows.sort_by(|a, b| {
        for (field, ascending) in order_by {
            let va = a.get(field).unwrap_or(&Value::Null);
            let vb = b.get(field).unwrap_or(&Value::Null);
            let ord = compare_values(va, vb);
            let ord = if *ascending { ord } else { ord.reverse() };
            if ord != std::cmp::Ordering::Equal {
                return ord;
            }
        }
        std::cmp::Ordering::Equal
    });
}

fn compare_values(a: &Value, b: &Value) -> std::cmp::Ordering {
    match (a, b) {
        (Value::Integer(x), Value::Integer(y)) => x.cmp(y),
        (Value::Float(x), Value::Float(y)) => x.partial_cmp(y).unwrap_or(std::cmp::Ordering::Equal),
        (Value::String(x), Value::String(y)) => x.cmp(y),
        (Value::Bool(x), Value::Bool(y)) => x.cmp(y),
        (Value::Null, Value::Null) => std::cmp::Ordering::Equal,
        (Value::Null, _) => std::cmp::Ordering::Less,
        (_, Value::Null) => std::cmp::Ordering::Greater,
        _ => std::cmp::Ordering::Equal,
    }
}

// ── Alias prefix ─────────────────────────────────────────────────────────────

pub(crate) fn prefix_row(row: &HashMap<String, Value>, alias: &str) -> HashMap<String, Value> {
    if alias.is_empty() {
        return row.clone();
    }
    row.iter()
        .map(|(k, v)| (format!("{alias}.{k}"), v.clone()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn emp(id: i64, dept: i64, salary: i64) -> HashMap<String, Value> {
        let mut m = HashMap::new();
        m.insert("emp_id".to_string(), Value::Integer(id));
        m.insert("dept_id".to_string(), Value::Integer(dept));
        m.insert("salary".to_string(), Value::Integer(salary));
        m
    }

    fn dept(dept_id: i64, name: &str) -> HashMap<String, Value> {
        let mut m = HashMap::new();
        m.insert("dept_id".to_string(), Value::Integer(dept_id));
        m.insert("name".to_string(), Value::String(name.to_string()));
        m
    }

    /// For each department, get top-3 employees by salary descending.
    #[test]
    fn test_lateral_top_k_top3_by_salary() {
        let departments = vec![dept(1, "Engineering"), dept(2, "Marketing")];
        let employees = vec![
            emp(1, 1, 90000),
            emp(2, 1, 80000),
            emp(3, 1, 70000),
            emp(4, 1, 60000),
            emp(5, 1, 50000),
            emp(6, 2, 55000),
            emp(7, 2, 45000),
        ];

        let inner_limit = 3usize;
        let mut all_output: Vec<HashMap<String, Value>> = Vec::new();

        for outer_row in &departments {
            let dept_val = outer_row.get("dept_id").cloned().unwrap_or(Value::Null);
            let filters = vec![ScanFilter {
                field: "dept_id".to_string(),
                op: FilterOp::Eq,
                value: dept_val,
                ..Default::default()
            }];
            let mut inner_rows = apply_filters(employees.clone(), &filters);
            sort_rows(&mut inner_rows, &[("salary".to_string(), false)]);
            inner_rows.truncate(inner_limit);
            for inner_row in inner_rows {
                let mut merged = outer_row.clone();
                merged.extend(inner_row);
                all_output.push(merged);
            }
        }

        // dept 1: top 3 employees (90k/80k/70k); dept 2: only 2 employees → total 5 rows.
        assert_eq!(all_output.len(), 5);
        // First row (dept 1 top) should be the highest earner.
        assert_eq!(all_output[0]["salary"], Value::Integer(90000));
    }

    #[test]
    fn test_sort_rows_ascending_and_descending() {
        let mut rows: Vec<HashMap<String, Value>> = (0..3)
            .map(|i| {
                let mut m = HashMap::new();
                m.insert("n".to_string(), Value::Integer([3i64, 1, 2][i]));
                m
            })
            .collect();
        sort_rows(&mut rows, &[("n".to_string(), true)]);
        assert_eq!(rows[0]["n"], Value::Integer(1));
        sort_rows(&mut rows, &[("n".to_string(), false)]);
        assert_eq!(rows[0]["n"], Value::Integer(3));
    }
}
