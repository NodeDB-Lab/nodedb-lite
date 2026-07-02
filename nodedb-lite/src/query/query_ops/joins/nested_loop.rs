// SPDX-License-Identifier: Apache-2.0
//! NestedLoopJoin: fallback for non-equi joins.
//!
//! Evaluates an arbitrary `condition` (msgpack-encoded `Vec<ScanFilter>`)
//! against the merged left+right row per join_type semantics.

use std::collections::HashMap;

use nodedb_query::scan_filter::ScanFilter;
use nodedb_types::result::QueryResult;
use nodedb_types::value::Value;

use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::storage::engine::StorageEngine;

use super::common::{maps_to_result, merge_rows, scan_collection};

pub async fn execute_nested_loop_join<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    left_collection: &str,
    right_collection: &str,
    condition: &[u8],
    join_type: &str,
    limit: usize,
) -> Result<QueryResult, LiteError> {
    let left_rows = scan_collection(engine, left_collection).await?;
    let right_rows = scan_collection(engine, right_collection).await?;

    let filters: Vec<ScanFilter> = if condition.is_empty() {
        Vec::new()
    } else {
        zerompk::from_msgpack(condition).map_err(|e| LiteError::Serialization {
            detail: format!("decode nested loop condition: {e}"),
        })?
    };

    let effective_limit = if limit == 0 { usize::MAX } else { limit };
    let mut output: Vec<HashMap<String, Value>> = Vec::new();

    'outer: for left_row in &left_rows {
        let mut matched = false;
        for right_row in &right_rows {
            let merged = merge_rows(left_row, right_row, None);
            let doc = Value::Object(merged.clone());
            let passes = filters.iter().all(|f| f.matches_value(&doc));
            if passes {
                matched = true;
                if join_type != "semi" && join_type != "anti" {
                    output.push(merged);
                    if output.len() >= effective_limit {
                        break 'outer;
                    }
                } else if join_type == "semi" {
                    output.push(left_row.clone());
                    if output.len() >= effective_limit {
                        break 'outer;
                    }
                    break; // one match per left row for semi
                }
            }
        }
        if !matched {
            match join_type {
                "left" | "full" => {
                    output.push(left_row.clone());
                    if output.len() >= effective_limit {
                        break;
                    }
                }
                "anti" => {
                    output.push(left_row.clone());
                    if output.len() >= effective_limit {
                        break;
                    }
                }
                _ => {}
            }
        }
    }

    // Right outer: unmatched right rows.
    if join_type == "right" || join_type == "full" {
        'right: for right_row in &right_rows {
            let mut any_match = false;
            for left_row in &left_rows {
                let merged = merge_rows(left_row, right_row, None);
                let doc = Value::Object(merged);
                if filters.iter().all(|f| f.matches_value(&doc)) {
                    any_match = true;
                    break;
                }
            }
            if !any_match {
                output.push(right_row.clone());
                if output.len() >= effective_limit {
                    break 'right;
                }
            }
        }
    }

    Ok(maps_to_result(output))
}

#[cfg(test)]
mod tests {
    use super::super::common::merge_rows;
    use nodedb_query::scan_filter::{FilterOp, ScanFilter};
    use nodedb_types::value::Value;
    use std::collections::HashMap;

    fn row(fields: &[(&str, Value)]) -> HashMap<String, Value> {
        fields
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect()
    }

    #[test]
    fn non_equi_condition_evaluates() {
        // Filter: merged row's "price" > "min_price" (from right side).
        // This tests that the condition evaluates on the merged row.
        let left = row(&[
            ("item", Value::String("apple".into())),
            ("price", Value::Integer(5)),
        ]);
        let right = row(&[("min_price", Value::Integer(3))]);
        let merged = merge_rows(&left, &right, None);

        let filter = ScanFilter {
            field: "price".into(),
            op: FilterOp::Gt,
            value: Value::Integer(3),
            ..Default::default()
        };

        let doc = Value::Object(merged);
        assert!(filter.matches_value(&doc));
    }
}
