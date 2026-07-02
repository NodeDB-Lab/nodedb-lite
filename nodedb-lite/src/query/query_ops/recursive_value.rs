// SPDX-License-Identifier: Apache-2.0
//! RecursiveValue — iterative expression evaluation for value-generating CTEs.
//!
//! Implements `WITH RECURSIVE cte(cols) AS (VALUES(init) UNION [ALL] SELECT step FROM cte
//! WHERE condition)` where no underlying collection is scanned. Column values are
//! produced purely by expression evaluation on the previous row.

use std::collections::{HashMap, HashSet};

use nodedb_query::expr_parse::parser::parse_generated_expr;
use nodedb_query::value_ops::is_truthy;
use nodedb_types::result::QueryResult;
use nodedb_types::value::Value;

use crate::error::LiteError;
use crate::query::query_ops::joins::common::maps_to_result;

/// Iterative expression evaluation for value-generating recursive CTEs.
///
/// Evaluates `init_exprs` once to produce the anchor row, then iterates:
/// bind the previous row as column variables, evaluate `condition` (stop when
/// false), evaluate `step_exprs` to get the next row.  Stops at `max_depth`
/// iterations (returns a typed error when exceeded) or when `condition` is
/// false.  Deduplicates if `distinct` is true.
pub async fn execute_recursive_value(
    cte_name: &str,
    columns: &[String],
    init_exprs: &[String],
    step_exprs: &[String],
    condition: Option<&str>,
    max_depth: usize,
    distinct: bool,
) -> Result<QueryResult, LiteError> {
    if columns.len() != init_exprs.len() || columns.len() != step_exprs.len() {
        return Err(LiteError::Storage {
            detail: format!(
                "recursive CTE '{cte_name}': columns/init_exprs/step_exprs length mismatch \
                 (columns={}, init={}, step={})",
                columns.len(),
                init_exprs.len(),
                step_exprs.len(),
            ),
        });
    }

    // Parse anchor expressions.
    let init_parsed: Vec<_> = init_exprs
        .iter()
        .map(|e| parse_expr_text(cte_name, e))
        .collect::<Result<_, _>>()?;

    // Parse step expressions.
    let step_parsed: Vec<_> = step_exprs
        .iter()
        .map(|e| parse_expr_text(cte_name, e))
        .collect::<Result<_, _>>()?;

    // Parse condition if present.
    let cond_parsed = condition
        .map(|c| parse_expr_text(cte_name, c))
        .transpose()?;

    let effective_max = if max_depth == 0 { 100 } else { max_depth };

    let mut accumulator: Vec<HashMap<String, Value>> = Vec::new();
    let mut seen: HashSet<String> = HashSet::new();

    // Evaluate anchor row against an empty document.
    let empty_doc = Value::Object(HashMap::new());
    let anchor_row = eval_exprs(&init_parsed, &empty_doc, columns);
    maybe_push(&mut accumulator, &mut seen, anchor_row.clone(), distinct);

    let mut current_row = anchor_row;

    for iter in 0..usize::MAX {
        if iter >= effective_max {
            return Err(LiteError::Storage {
                detail: format!("recursive CTE '{cte_name}' exceeded max_depth={effective_max}"),
            });
        }

        let doc = Value::Object(current_row.clone());

        // Evaluate condition; stop when false (or absent after first anchor).
        if let Some(cond) = &cond_parsed {
            let result = cond.eval(&doc);
            if !is_truthy(&result) {
                break;
            }
        } else {
            // No condition: run exactly one step (anchor only) — but that was
            // already done, so we stop if we've already added at least one row.
            if !accumulator.is_empty() {
                break;
            }
        }

        let next_row = eval_exprs(&step_parsed, &doc, columns);
        // When distinct, a duplicate step row means fixed point — stop.
        if distinct {
            let key = row_dedup_key(&next_row);
            if seen.contains(&key) {
                break;
            }
        }
        maybe_push(&mut accumulator, &mut seen, next_row.clone(), distinct);
        current_row = next_row;
    }

    Ok(maps_to_result(accumulator))
}

// ── Helpers ─────────────────────────────────────────────────────────────────

fn parse_expr_text(
    cte_name: &str,
    text: &str,
) -> Result<nodedb_query::expr::types::SqlExpr, LiteError> {
    parse_generated_expr(text)
        .map(|(expr, _)| expr)
        .map_err(|e| LiteError::Storage {
            detail: format!("recursive CTE '{cte_name}': failed to parse expr '{text}': {e}"),
        })
}

fn eval_exprs(
    exprs: &[nodedb_query::expr::types::SqlExpr],
    doc: &Value,
    columns: &[String],
) -> HashMap<String, Value> {
    columns
        .iter()
        .zip(exprs.iter())
        .map(|(col, expr)| (col.clone(), expr.eval(doc)))
        .collect()
}

fn maybe_push(
    acc: &mut Vec<HashMap<String, Value>>,
    seen: &mut HashSet<String>,
    row: HashMap<String, Value>,
    distinct: bool,
) {
    if distinct {
        let key = row_dedup_key(&row);
        if seen.contains(&key) {
            return;
        }
        seen.insert(key);
    }
    acc.push(row);
}

fn row_dedup_key(row: &HashMap<String, Value>) -> String {
    let mut pairs: Vec<(&String, &Value)> = row.iter().collect();
    pairs.sort_by_key(|(k, _)| k.as_str());
    pairs
        .into_iter()
        .map(|(k, v)| format!("{k}={v:?}"))
        .collect::<Vec<_>>()
        .join(";")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Counter CTE: VALUES(1) UNION ALL SELECT n+1 FROM cte WHERE n < 10.
    #[tokio::test]
    async fn test_recursive_value_counter_1_to_10() {
        let columns = vec!["n".to_string()];
        let init = vec!["1".to_string()];
        let step = vec!["n + 1".to_string()];
        let condition = Some("n < 10");

        let result =
            execute_recursive_value("counter", &columns, &init, &step, condition, 50, false)
                .await
                .unwrap();

        // Rows: n = 1, 2, ..., 10.
        assert_eq!(result.rows.len(), 10, "expected 10 rows (1 through 10)");
        let n_col = result.columns.iter().position(|c| c == "n").unwrap();
        let first = &result.rows[0][n_col];
        let last = &result.rows[9][n_col];
        assert_eq!(*first, Value::Integer(1));
        assert_eq!(*last, Value::Integer(10));
    }

    /// Distinct deduplication: all step values are the same constant, so only
    /// the anchor should appear.
    #[tokio::test]
    async fn test_recursive_value_distinct_deduplication() {
        // init = 5, step = 5, condition = n < 10 — step never changes n.
        // Without distinct: would loop to max_depth producing [5, 5, 5, ...].
        // With distinct: stops after anchor because the next row is a duplicate.
        let columns = vec!["n".to_string()];
        let init = vec!["5".to_string()];
        let step = vec!["5".to_string()]; // constant
        let condition = Some("n < 10");

        let result =
            execute_recursive_value("dup_cte", &columns, &init, &step, condition, 20, true)
                .await
                .unwrap();

        // Only the anchor row should appear (dedup eliminates identical step rows).
        assert_eq!(result.rows.len(), 1);
    }
}
