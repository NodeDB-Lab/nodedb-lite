// SPDX-License-Identifier: Apache-2.0
//! SortMergeJoin: merge-join on sorted inputs.
//!
//! When `pre_sorted` is false, sorts both sides by the join key before merging.
//! When `pre_sorted` is true, streams both sides assuming they are already sorted.

use std::collections::HashMap;

use nodedb_types::result::QueryResult;
use nodedb_types::value::Value;

use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::storage::engine::StorageEngine;

use super::common::{maps_to_result, merge_rows, scan_collection};
use crate::query::query_ops::aggregate::value_cmp;

pub async fn execute_sort_merge_join<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    left_collection: &str,
    right_collection: &str,
    on: &[(String, String)],
    join_type: &str,
    limit: usize,
    pre_sorted: bool,
) -> Result<QueryResult, LiteError> {
    let mut left_rows = scan_collection(engine, left_collection).await?;
    let mut right_rows = scan_collection(engine, right_collection).await?;

    let left_keys: Vec<String> = on.iter().map(|(l, _)| l.clone()).collect();
    let right_keys: Vec<String> = on.iter().map(|(_, r)| r.clone()).collect();

    if !pre_sorted {
        left_rows.sort_by(|a, b| compare_row_keys(a, b, &left_keys));
        right_rows.sort_by(|a, b| compare_row_keys(a, b, &right_keys));
    }

    let effective_limit = if limit == 0 { usize::MAX } else { limit };
    let output = merge_sorted(
        left_rows,
        right_rows,
        &left_keys,
        &right_keys,
        join_type,
        effective_limit,
    );
    Ok(maps_to_result(output))
}

fn row_key(row: &HashMap<String, Value>, keys: &[String]) -> Vec<Value> {
    keys.iter()
        .map(|k| row.get(k).cloned().unwrap_or(Value::Null))
        .collect()
}

fn compare_row_keys(
    a: &HashMap<String, Value>,
    b: &HashMap<String, Value>,
    keys: &[String],
) -> std::cmp::Ordering {
    for k in keys {
        let av = a.get(k).unwrap_or(&Value::Null);
        let bv = b.get(k).unwrap_or(&Value::Null);
        let ord = value_cmp(av, bv);
        if ord != std::cmp::Ordering::Equal {
            return ord;
        }
    }
    std::cmp::Ordering::Equal
}

fn key_cmp(a: &[Value], b: &[Value]) -> std::cmp::Ordering {
    for (av, bv) in a.iter().zip(b.iter()) {
        let ord = value_cmp(av, bv);
        if ord != std::cmp::Ordering::Equal {
            return ord;
        }
    }
    std::cmp::Ordering::Equal
}

fn merge_sorted(
    left: Vec<HashMap<String, Value>>,
    right: Vec<HashMap<String, Value>>,
    left_keys: &[String],
    right_keys: &[String],
    join_type: &str,
    limit: usize,
) -> Vec<HashMap<String, Value>> {
    let mut output = Vec::new();
    let mut li = 0usize;
    let mut ri = 0usize;

    // Track which right indices were matched (for right/full outer).
    let mut right_matched = vec![false; right.len()];

    while li < left.len() && output.len() < limit {
        let lk = row_key(&left[li], left_keys);

        // Advance right past rows with smaller keys.
        while ri < right.len() {
            let rk = row_key(&right[ri], right_keys);
            if key_cmp(&rk, &lk) != std::cmp::Ordering::Less {
                break;
            }
            ri += 1;
        }

        // Collect all right rows matching the current left key.
        let mut match_ri = ri;
        let mut matched = false;
        while match_ri < right.len() {
            let rk = row_key(&right[match_ri], right_keys);
            if key_cmp(&rk, &lk) != std::cmp::Ordering::Equal {
                break;
            }
            matched = true;
            right_matched[match_ri] = true;

            match join_type {
                "semi" => {
                    if output.is_empty()
                        || !output
                            .last()
                            .map(|r: &HashMap<String, Value>| row_key(r, left_keys) == lk)
                            .unwrap_or(false)
                    {
                        output.push(left[li].clone());
                    }
                }
                "anti" => {}
                _ => {
                    let merged = merge_rows(&left[li], &right[match_ri], None);
                    output.push(merged);
                    if output.len() >= limit {
                        return output;
                    }
                }
            }
            match_ri += 1;
        }

        if !matched {
            match join_type {
                "left" | "full" => {
                    output.push(left[li].clone());
                }
                "anti" => {
                    output.push(left[li].clone());
                }
                _ => {}
            }
        }

        li += 1;
    }

    // Unmatched right rows for right/full outer.
    if join_type == "right" || join_type == "full" {
        for (idx, right_row) in right.iter().enumerate() {
            if !right_matched[idx] {
                output.push(right_row.clone());
                if output.len() >= limit {
                    break;
                }
            }
        }
    }

    output
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::value::Value;
    use std::collections::HashMap;

    fn row(fields: &[(&str, Value)]) -> HashMap<String, Value> {
        fields
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect()
    }

    #[test]
    fn sort_merge_pre_sorted_both_sides() {
        // Both sides are pre-sorted by "id".
        let left = vec![
            row(&[("id", Value::Integer(1)), ("lv", Value::Integer(10))]),
            row(&[("id", Value::Integer(2)), ("lv", Value::Integer(20))]),
            row(&[("id", Value::Integer(3)), ("lv", Value::Integer(30))]),
        ];
        let right = vec![
            row(&[("rid", Value::Integer(1)), ("rv", Value::Integer(100))]),
            row(&[("rid", Value::Integer(3)), ("rv", Value::Integer(300))]),
        ];
        let on = vec![("id".to_string(), "rid".to_string())];
        let result = merge_sorted(
            left,
            right,
            &["id".into()],
            &["rid".into()],
            "inner",
            usize::MAX,
        );
        assert_eq!(result.len(), 2);
        let vals: Vec<i64> = result
            .iter()
            .map(|r| {
                if let Value::Integer(n) = r["rv"] {
                    n
                } else {
                    0
                }
            })
            .collect();
        assert!(vals.contains(&100));
        assert!(vals.contains(&300));
        let _ = on;
    }
}
