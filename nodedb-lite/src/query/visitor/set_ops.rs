// SPDX-License-Identifier: Apache-2.0
//! SQL-visitor lowering for set-operation SqlPlan variants: Union, Intersect, Except.

use std::collections::HashSet;

use nodedb_sql::types::SqlPlan;
use nodedb_types::result::QueryResult;
use nodedb_types::value::Value;

use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::storage::engine::StorageEngine;

use super::adapter::LiteFut;

/// Serialize a row to a canonical string key for deduplication.
fn row_key(row: &[Value]) -> String {
    row.iter()
        .map(|v| format!("{v:?}"))
        .collect::<Vec<_>>()
        .join("\x00")
}

// ── Union ────────────────────────────────────────────────────────────────────

pub(super) fn lower_union<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    inputs: &[SqlPlan],
    distinct: bool,
) -> Result<LiteFut<'a>, LiteError> {
    let inputs = inputs.to_vec();

    Ok(Box::pin(async move {
        let mut columns: Vec<String> = Vec::new();
        let mut all_rows: Vec<Vec<Value>> = Vec::new();
        let mut seen: HashSet<String> = HashSet::new();

        for plan in &inputs {
            let result = engine.execute_plan(plan).await?;
            if columns.is_empty() {
                columns = result.columns.clone();
            }
            for row in result.rows {
                if distinct {
                    let key = row_key(&row);
                    if seen.insert(key) {
                        all_rows.push(row);
                    }
                } else {
                    all_rows.push(row);
                }
            }
        }

        Ok(QueryResult {
            columns,
            rows: all_rows,
            rows_affected: 0,
        })
    }))
}

// ── Intersect ────────────────────────────────────────────────────────────────

pub(super) fn lower_intersect<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    left: &SqlPlan,
    right: &SqlPlan,
    all: bool,
) -> Result<LiteFut<'a>, LiteError> {
    let left = left.clone();
    let right = right.clone();

    Ok(Box::pin(async move {
        let left_result = engine.execute_plan(&left).await?;
        let right_result = engine.execute_plan(&right).await?;

        let columns = left_result.columns.clone();

        if all {
            // INTERSECT ALL: for each left row, count how many times
            // it appears in right; keep min(left_count, right_count) copies.
            let mut right_counts: std::collections::HashMap<String, usize> =
                std::collections::HashMap::new();
            for row in &right_result.rows {
                *right_counts.entry(row_key(row)).or_insert(0) += 1;
            }
            let mut output: Vec<Vec<Value>> = Vec::new();
            for row in left_result.rows {
                let key = row_key(&row);
                if let Some(count) = right_counts.get_mut(&key)
                    && *count > 0
                {
                    *count -= 1;
                    output.push(row);
                }
            }
            Ok(QueryResult {
                columns,
                rows: output,
                rows_affected: 0,
            })
        } else {
            // INTERSECT DISTINCT
            let right_set: HashSet<String> = right_result.rows.iter().map(|r| row_key(r)).collect();
            let mut seen: HashSet<String> = HashSet::new();
            let mut output: Vec<Vec<Value>> = Vec::new();
            for row in left_result.rows {
                let key = row_key(&row);
                if right_set.contains(&key) && seen.insert(key) {
                    output.push(row);
                }
            }
            Ok(QueryResult {
                columns,
                rows: output,
                rows_affected: 0,
            })
        }
    }))
}

// ── Except ───────────────────────────────────────────────────────────────────

pub(super) fn lower_except<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    left: &SqlPlan,
    right: &SqlPlan,
    all: bool,
) -> Result<LiteFut<'a>, LiteError> {
    let left = left.clone();
    let right = right.clone();

    Ok(Box::pin(async move {
        let left_result = engine.execute_plan(&left).await?;
        let right_result = engine.execute_plan(&right).await?;

        let columns = left_result.columns.clone();

        if all {
            // EXCEPT ALL: subtract right counts from left counts.
            let mut right_counts: std::collections::HashMap<String, usize> =
                std::collections::HashMap::new();
            for row in &right_result.rows {
                *right_counts.entry(row_key(row)).or_insert(0) += 1;
            }
            let mut output: Vec<Vec<Value>> = Vec::new();
            for row in left_result.rows {
                let key = row_key(&row);
                let count = right_counts.entry(key).or_insert(0);
                if *count > 0 {
                    *count -= 1;
                } else {
                    output.push(row);
                }
            }
            Ok(QueryResult {
                columns,
                rows: output,
                rows_affected: 0,
            })
        } else {
            // EXCEPT DISTINCT
            let right_set: HashSet<String> = right_result.rows.iter().map(|r| row_key(r)).collect();
            let mut seen: HashSet<String> = HashSet::new();
            let mut output: Vec<Vec<Value>> = Vec::new();
            for row in left_result.rows {
                let key = row_key(&row);
                if !right_set.contains(&key) && seen.insert(key) {
                    output.push(row);
                }
            }
            Ok(QueryResult {
                columns,
                rows: output,
                rows_affected: 0,
            })
        }
    }))
}
