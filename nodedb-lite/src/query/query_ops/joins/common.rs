// SPDX-License-Identifier: Apache-2.0
//! Shared helpers reused across join variants.

use std::collections::HashMap;

use nodedb_physical::physical_plan::query::JoinProjection;
use nodedb_query::scan_filter::ScanFilter;
use nodedb_types::result::QueryResult;
use nodedb_types::value::Value;

use crate::error::LiteError;
use crate::query::document_ops::reads::scan;
use crate::query::engine::LiteQueryEngine;
use crate::storage::engine::StorageEngine;

// ─── Collection scan ─────────────────────────────────────────────────────────

/// Scan all rows from a collection and return them as `HashMap<String, Value>`.
///
/// Uses document_ops::reads::scan (schemaless/strict), then converts each row
/// to a column-keyed map using the result's column names.
pub async fn scan_collection<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    collection: &str,
) -> Result<Vec<HashMap<String, Value>>, LiteError> {
    let result = scan(engine, collection, usize::MAX, 0).await?;
    Ok(rows_to_maps(result))
}

/// Convert a QueryResult into a list of column-keyed maps.
pub fn rows_to_maps(result: QueryResult) -> Vec<HashMap<String, Value>> {
    let columns = result.columns;
    result
        .rows
        .into_iter()
        .map(|row| {
            columns
                .iter()
                .zip(row)
                .map(|(col, val)| (col.clone(), val))
                .collect()
        })
        .collect()
}

// ─── Filter application ───────────────────────────────────────────────────────

pub fn decode_filters(bytes: &[u8]) -> Result<Vec<ScanFilter>, LiteError> {
    if bytes.is_empty() {
        return Ok(Vec::new());
    }
    zerompk::from_msgpack(bytes).map_err(|e| LiteError::Serialization {
        detail: format!("decode join filters: {e}"),
    })
}

pub fn apply_filters(
    rows: Vec<HashMap<String, Value>>,
    filters: &[ScanFilter],
) -> Vec<HashMap<String, Value>> {
    if filters.is_empty() {
        return rows;
    }
    rows.into_iter()
        .filter(|row| {
            let doc = Value::Object(row.clone());
            filters.iter().all(|f| f.matches_value(&doc))
        })
        .collect()
}

// ─── Join key extraction ──────────────────────────────────────────────────────

pub fn join_key(row: &HashMap<String, Value>, cols: &[String]) -> Vec<Value> {
    cols.iter()
        .map(|c| row.get(c).cloned().unwrap_or(Value::Null))
        .collect()
}

// ─── Merge rows ───────────────────────────────────────────────────────────────

/// Merge left and right rows, right-side keys prefixed by alias if non-empty.
pub fn merge_rows(
    left: &HashMap<String, Value>,
    right: &HashMap<String, Value>,
    right_alias: Option<&str>,
) -> HashMap<String, Value> {
    let mut out = left.clone();
    for (k, v) in right {
        let key = match right_alias {
            Some(alias) if !alias.is_empty() => format!("{alias}.{k}"),
            _ => k.clone(),
        };
        out.insert(key, v.clone());
    }
    out
}

// ─── Projection ───────────────────────────────────────────────────────────────

pub fn apply_projection(
    rows: Vec<HashMap<String, Value>>,
    projection: &[JoinProjection],
) -> Vec<HashMap<String, Value>> {
    if projection.is_empty() {
        return rows;
    }
    rows.into_iter()
        .map(|row| {
            projection
                .iter()
                .map(|p| {
                    let val = row.get(&p.source).cloned().unwrap_or(Value::Null);
                    (p.output.clone(), val)
                })
                .collect()
        })
        .collect()
}

// ─── QueryResult conversion ───────────────────────────────────────────────────

/// Convert a list of maps to a QueryResult with stable column order.
pub fn maps_to_result(rows: Vec<HashMap<String, Value>>) -> QueryResult {
    if rows.is_empty() {
        return QueryResult::empty();
    }
    // Collect all column names from first row (stable insertion order not guaranteed,
    // use a sorted list for determinism).
    let mut columns: Vec<String> = rows[0].keys().cloned().collect();
    columns.sort();
    let result_rows = rows
        .into_iter()
        .map(|row| {
            columns
                .iter()
                .map(|col| row.get(col).cloned().unwrap_or(Value::Null))
                .collect()
        })
        .collect();
    QueryResult {
        columns,
        rows: result_rows,
        rows_affected: 0,
    }
}

// ─── Hash-join core ───────────────────────────────────────────────────────────

/// Stable string discriminant for a `Vec<Value>` join key.
fn join_key_str(key: &[Value]) -> String {
    key.iter()
        .map(|v| match v {
            Value::Null => "null".to_string(),
            Value::Bool(b) => format!("b:{b}"),
            Value::Integer(n) => format!("i:{n}"),
            Value::Float(f) => format!("f:{f}"),
            Value::String(s) => format!("s:{s}"),
            Value::Uuid(s) | Value::Ulid(s) | Value::Regex(s) => format!("str:{s}"),
            Value::Bytes(b) => format!("by:{}", b.len()),
            Value::Array(a) | Value::Set(a) => format!("a:{}", a.len()),
            Value::Object(m) => format!("o:{}", m.len()),
            _ => "other".to_string(),
        })
        .collect::<Vec<_>>()
        .join("|")
}

/// Core hash-join: build from `build_side`, probe `probe_side`.
///
/// Returns merged rows in probe order. `join_type` governs null-extension:
/// - "inner": only matched rows
/// - "left": all probe rows, right-null on miss
/// - "right": all build rows, left-null on miss (inverted roles returned)
/// - "full": union of left + right
/// - "semi": probe rows that matched (no right cols)
/// - "anti": probe rows that did NOT match (no right cols)
pub fn hash_join(
    build_side: Vec<HashMap<String, Value>>,
    probe_side: Vec<HashMap<String, Value>>,
    build_keys: &[String],
    probe_keys: &[String],
    join_type: &str,
    right_alias: Option<&str>,
    limit: usize,
) -> Vec<HashMap<String, Value>> {
    // Build phase: HashMap<key_str, (original_build_indices, rows)>.
    // We track the original index into `build_side` to enable right/full outer.
    let mut build_map: HashMap<String, Vec<usize>> = HashMap::new();
    for (idx, row) in build_side.iter().enumerate() {
        let key = join_key(row, build_keys);
        let key_str = join_key_str(&key);
        build_map.entry(key_str).or_default().push(idx);
    }

    let mut output: Vec<HashMap<String, Value>> = Vec::new();
    let mut matched_build: std::collections::HashSet<usize> = std::collections::HashSet::new();

    // Probe phase.
    'probe: for probe_row in &probe_side {
        let key = join_key(probe_row, probe_keys);
        let key_str = join_key_str(&key);
        match build_map.get(&key_str) {
            Some(indices) => {
                for &bi in indices {
                    let build_row = &build_side[bi];
                    matched_build.insert(bi);

                    if join_type == "semi" {
                        output.push(probe_row.clone());
                        if output.len() >= limit {
                            break 'probe;
                        }
                        break; // only one output per probe row for semi
                    }
                    if join_type == "anti" {
                        break; // matched; do not emit for anti
                    }
                    let merged = merge_rows(probe_row, build_row, right_alias);
                    output.push(merged);
                    if output.len() >= limit {
                        break 'probe;
                    }
                }
            }
            None => match join_type {
                "left" | "full" => {
                    output.push(probe_row.clone());
                    if output.len() >= limit {
                        break;
                    }
                }
                "anti" => {
                    output.push(probe_row.clone());
                    if output.len() >= limit {
                        break;
                    }
                }
                _ => {}
            },
        }
    }

    // Right / full outer: emit unmatched build rows.
    if join_type == "right" || join_type == "full" {
        for (idx, build_row) in build_side.iter().enumerate() {
            if !matched_build.contains(&idx) {
                output.push(build_row.clone());
                if output.len() >= limit {
                    break;
                }
            }
        }
    }

    output
}
