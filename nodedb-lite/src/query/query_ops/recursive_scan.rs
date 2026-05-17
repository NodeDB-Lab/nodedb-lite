// SPDX-License-Identifier: Apache-2.0
//! RecursiveScan — iterative fixed-point CTE scan over a single collection.
//!
//! Implements `WITH RECURSIVE cte AS (base UNION [ALL] recursive)` semantics
//! where both anchor and recursive branches query the same physical collection.
//! Each recursive iteration hash-joins the collection against the current
//! working set via `join_link`, producing the next working set.

use std::collections::{HashMap, HashSet};

use nodedb_types::result::QueryResult;
use nodedb_types::value::Value;

use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::query::query_ops::joins::common::{
    apply_filters, decode_filters, maps_to_result, scan_collection,
};
use crate::storage::engine::{StorageEngine, StorageEngineSync};

/// Iterative fixed-point CTE scan over a collection.
///
/// Executes the base query once, then repeatedly extends the working set by
/// joining the collection against it via `join_link`, stopping when no new
/// rows emerge, the iteration cap is hit, or the `limit` is reached.
#[allow(clippy::too_many_arguments)]
pub async fn execute_recursive_scan<S: StorageEngine + StorageEngineSync>(
    engine: &LiteQueryEngine<S>,
    collection: &str,
    base_filters: &[u8],
    recursive_filters: &[u8],
    join_link: Option<&(String, String)>,
    max_iterations: usize,
    distinct: bool,
    limit: usize,
) -> Result<QueryResult, LiteError> {
    let base_parsed = decode_filters(base_filters)?;
    let rec_parsed = decode_filters(recursive_filters)?;

    // All rows in the collection (scanned once; re-filtered each iteration).
    let full_collection = scan_collection(engine, collection).await?;

    // Anchor: base case rows.
    let anchor = apply_filters(full_collection.clone(), &base_parsed);

    let effective_limit = if limit == 0 { usize::MAX } else { limit };
    let max_iter = if max_iterations == 0 {
        100
    } else {
        max_iterations
    };

    let mut accumulator: Vec<HashMap<String, Value>> = Vec::new();
    let mut seen: HashSet<String> = HashSet::new();

    let append_rows = |acc: &mut Vec<HashMap<String, Value>>,
                       seen: &mut HashSet<String>,
                       rows: Vec<HashMap<String, Value>>,
                       dedup: bool,
                       cap: usize|
     -> Vec<HashMap<String, Value>> {
        let mut new_working: Vec<HashMap<String, Value>> = Vec::new();
        for row in rows {
            if acc.len() >= cap {
                break;
            }
            if dedup {
                let key = row_key(&row);
                if seen.contains(&key) {
                    continue;
                }
                seen.insert(key);
            }
            new_working.push(row.clone());
            acc.push(row);
        }
        new_working
    };

    let mut working_set = append_rows(
        &mut accumulator,
        &mut seen,
        anchor,
        distinct,
        effective_limit,
    );

    for _ in 0..max_iter {
        if working_set.is_empty() || accumulator.len() >= effective_limit {
            break;
        }

        // Filter the full collection with recursive_filters.
        let candidates = apply_filters(full_collection.clone(), &rec_parsed);

        // Join candidates against working_set via join_link.
        let next_rows = match join_link {
            Some((coll_field, working_field)) => {
                // Build lookup from working set: working_field value → present.
                let working_vals: HashSet<String> = working_set
                    .iter()
                    .map(|r| value_key(r.get(working_field).unwrap_or(&Value::Null)))
                    .collect();

                candidates
                    .into_iter()
                    .filter(|row| {
                        let v = value_key(row.get(coll_field).unwrap_or(&Value::Null));
                        working_vals.contains(&v)
                    })
                    .collect()
            }
            None => candidates,
        };

        working_set = append_rows(
            &mut accumulator,
            &mut seen,
            next_rows,
            distinct,
            effective_limit,
        );
    }

    Ok(maps_to_result(accumulator))
}

/// Deterministic string identity for deduplication.
fn row_key(row: &HashMap<String, Value>) -> String {
    let mut pairs: Vec<(&String, &Value)> = row.iter().collect();
    pairs.sort_by_key(|(k, _)| k.as_str());
    pairs
        .into_iter()
        .map(|(k, v)| format!("{k}={}", value_key(v)))
        .collect::<Vec<_>>()
        .join(";")
}

fn value_key(v: &Value) -> String {
    match v {
        Value::Null => "null".to_string(),
        Value::Bool(b) => format!("b:{b}"),
        Value::Integer(n) => format!("i:{n}"),
        Value::Float(f) => format!("f:{f}"),
        Value::String(s) => format!("s:{s}"),
        Value::Uuid(s) | Value::Ulid(s) => format!("id:{s}"),
        Value::Bytes(b) => format!("by:{}", b.len()),
        Value::Array(a) | Value::Set(a) => format!("a:{}", a.len()),
        Value::Object(m) => format!("o:{}", m.len()),
        _ => "other".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(id: i64, parent_id: i64) -> HashMap<String, Value> {
        let mut m = HashMap::new();
        m.insert("id".to_string(), Value::Integer(id));
        m.insert("parent_id".to_string(), Value::Integer(parent_id));
        m
    }

    /// Tree: root(id=1, parent_id=0), two children (2,1) and (3,1),
    /// one grandchild (4,2).  Anchor = parent_id==0.  join_link = (parent_id, id).
    /// Expected: all 4 nodes in accumulator after traversal.
    #[test]
    fn test_recursive_scan_tree_logic() {
        let all_nodes = [node(1, 0), node(2, 1), node(3, 1), node(4, 2)];

        // Simulate the iterative logic inline (no async needed).
        let anchor: Vec<_> = all_nodes
            .iter()
            .filter(|r| r.get("parent_id") == Some(&Value::Integer(0)))
            .cloned()
            .collect();
        assert_eq!(anchor.len(), 1); // root only

        let join_link = ("parent_id".to_string(), "id".to_string());
        let mut accumulator: Vec<HashMap<String, Value>> = anchor.clone();
        let mut working_set: Vec<HashMap<String, Value>> = anchor;

        for _ in 0..10 {
            if working_set.is_empty() {
                break;
            }
            let working_vals: std::collections::HashSet<String> = working_set
                .iter()
                .map(|r| value_key(r.get(&join_link.1).unwrap_or(&Value::Null)))
                .collect();
            let next: Vec<_> = all_nodes
                .iter()
                .filter(|r| {
                    let v = value_key(r.get(&join_link.0).unwrap_or(&Value::Null));
                    working_vals.contains(&v)
                })
                .cloned()
                .collect();
            // Exclude already-accumulated rows (distinct).
            let already: std::collections::HashSet<String> =
                accumulator.iter().map(row_key).collect();
            let new_rows: Vec<_> = next
                .into_iter()
                .filter(|r| !already.contains(&row_key(r)))
                .collect();
            working_set = new_rows.clone();
            accumulator.extend(new_rows);
        }

        assert_eq!(accumulator.len(), 4, "all 4 tree nodes reachable from root");
    }
}
