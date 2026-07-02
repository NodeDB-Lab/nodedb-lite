// SPDX-License-Identifier: Apache-2.0
//! Aggregate and PartialAggregate QueryOp implementations for Lite.
//!
//! Supports GROUP BY, aggregate functions (COUNT/SUM/AVG/MIN/MAX/COUNT_DISTINCT),
//! HAVING, ORDER BY, and GROUPING SETS (ROLLUP/CUBE) expansion.

use std::collections::HashMap;

use nodedb_physical::physical_plan::query::AggregateSpec;
use nodedb_query::scan_filter::ScanFilter;
use nodedb_query::simd_agg::ts_runtime;
use nodedb_query::simd_agg_i64::i64_runtime;
use nodedb_types::result::QueryResult;
use nodedb_types::value::Value;

use crate::error::LiteError;

// ─── Type aliases ─────────────────────────────────────────────────────────────

type GroupMap = HashMap<String, (Vec<Value>, Vec<HashMap<String, Value>>)>;

// ─── Public entry points ─────────────────────────────────────────────────────

/// Execute a full Aggregate: apply filters, group, aggregate, HAVING, sort.
pub fn execute_aggregate(
    rows: Vec<HashMap<String, Value>>,
    group_by: &[String],
    aggregates: &[AggregateSpec],
    filters: &[u8],
    having: &[u8],
    sort_keys: &[(String, bool)],
    grouping_sets: &[Vec<u32>],
) -> Result<QueryResult, LiteError> {
    let scan_filters = decode_filters(filters)?;
    let having_filters = decode_filters(having)?;

    let filtered: Vec<HashMap<String, Value>> = rows
        .into_iter()
        .filter(|row| {
            let doc = Value::Object(row.clone());
            scan_filters.iter().all(|f| f.matches_value(&doc))
        })
        .collect();

    if grouping_sets.is_empty() {
        // Plain GROUP BY — single grouping set containing all group_by columns.
        let grouped = group_rows(&filtered, group_by);
        let mut result_rows = compute_aggregate_groups(grouped, group_by, aggregates)?;
        apply_having(&mut result_rows, &having_filters, group_by, aggregates);
        apply_sort(&mut result_rows, sort_keys, group_by, aggregates);
        let columns = make_columns(group_by, aggregates);
        Ok(QueryResult {
            columns,
            rows: result_rows,
            rows_affected: 0,
        })
    } else {
        // GROUPING SETS: union results for each subset.
        let mut all_rows: Vec<Vec<Value>> = Vec::new();
        for set_indices in grouping_sets {
            let subset: Vec<String> = set_indices
                .iter()
                .filter_map(|&i| group_by.get(i as usize).cloned())
                .collect();
            let grouped = group_rows(&filtered, &subset);
            let mut set_rows = compute_aggregate_groups(grouped, &subset, aggregates)?;
            // Null-fill absent group columns.
            let full_col_count = group_by.len();
            for row in &mut set_rows {
                while row.len() < full_col_count + aggregates.len() {
                    row.insert(set_indices.len(), Value::Null);
                }
            }
            apply_having(&mut set_rows, &having_filters, &subset, aggregates);
            all_rows.extend(set_rows);
        }
        apply_sort(&mut all_rows, sort_keys, group_by, aggregates);
        let columns = make_columns(group_by, aggregates);
        Ok(QueryResult {
            columns,
            rows: all_rows,
            rows_affected: 0,
        })
    }
}

/// Execute a PartialAggregate.
///
/// On single-node Lite, data is already local so this produces the same
/// final result as Aggregate (no HAVING/sort/grouping_sets) and encodes it
/// in the partial-state format Origin expects: each output row is
/// `[group_key_col_0, ..., group_key_col_N, count_i64, sum_f64, min, max, ...]`
/// in the same column order as `make_columns`, with a leading `__partial`
/// boolean column set to `true` so the Control-Plane merger can distinguish
/// partial from final results.
pub fn execute_partial_aggregate(
    rows: Vec<HashMap<String, Value>>,
    group_by: &[String],
    aggregates: &[AggregateSpec],
    filters: &[u8],
) -> Result<QueryResult, LiteError> {
    let scan_filters = decode_filters(filters)?;
    let filtered: Vec<HashMap<String, Value>> = rows
        .into_iter()
        .filter(|row| {
            let doc = Value::Object(row.clone());
            scan_filters.iter().all(|f| f.matches_value(&doc))
        })
        .collect();

    let grouped = group_rows(&filtered, group_by);
    let result_rows = compute_aggregate_groups(grouped, group_by, aggregates)?;

    let mut columns = vec!["__partial".to_string()];
    columns.extend(make_columns(group_by, aggregates));

    let output = result_rows
        .into_iter()
        .map(|mut row| {
            row.insert(0, Value::Bool(true));
            row
        })
        .collect();

    Ok(QueryResult {
        columns,
        rows: output,
        rows_affected: 0,
    })
}

// ─── Internal helpers ─────────────────────────────────────────────────────────

fn decode_filters(bytes: &[u8]) -> Result<Vec<ScanFilter>, LiteError> {
    if bytes.is_empty() {
        return Ok(Vec::new());
    }
    zerompk::from_msgpack(bytes).map_err(|e| LiteError::Serialization {
        detail: format!("decode filters: {e}"),
    })
}

/// Group rows by the specified key columns.
///
/// `Value` does not implement `Hash`/`Eq`, so we use a string serialisation of
/// the key for the `HashMap` discriminant and carry the original `Vec<Value>`
/// alongside for output.
pub(crate) fn group_rows(rows: &[HashMap<String, Value>], group_by: &[String]) -> GroupMap {
    let mut groups: GroupMap = HashMap::new();
    for row in rows {
        let key: Vec<Value> = group_by
            .iter()
            .map(|col| row.get(col).cloned().unwrap_or(Value::Null))
            .collect();
        let key_str = value_key_str(&key);
        groups
            .entry(key_str)
            .or_insert_with(|| (key, Vec::new()))
            .1
            .push(row.clone());
    }
    groups
}

/// Stable string discriminant for a `Vec<Value>` group key.
fn value_key_str(key: &[Value]) -> String {
    key.iter()
        .map(value_discriminant)
        .collect::<Vec<_>>()
        .join("|")
}

pub(crate) fn value_discriminant(v: &Value) -> String {
    match v {
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
    }
}

fn compute_aggregate_groups(
    groups: GroupMap,
    _group_by: &[String],
    aggregates: &[AggregateSpec],
) -> Result<Vec<Vec<Value>>, LiteError> {
    let mut result = Vec::with_capacity(groups.len());
    for (_key_str, (key_vals, group_rows)) in groups {
        let mut row: Vec<Value> = key_vals;
        for spec in aggregates {
            let agg_val = compute_one_aggregate(&group_rows, spec)?;
            row.push(agg_val);
        }
        result.push(row);
    }
    Ok(result)
}

fn compute_one_aggregate(
    group: &[HashMap<String, Value>],
    spec: &AggregateSpec,
) -> Result<Value, LiteError> {
    let func = spec.function.to_uppercase();
    match func.as_str() {
        "COUNT" if spec.field == "*" => Ok(Value::Integer(group.len() as i64)),
        "COUNT" => {
            let count = group
                .iter()
                .filter(|row| {
                    row.get(&spec.field)
                        .map(|v| !matches!(v, Value::Null))
                        .unwrap_or(false)
                })
                .count();
            Ok(Value::Integer(count as i64))
        }
        "COUNT_DISTINCT" => {
            let mut seen: Vec<Value> = Vec::new();
            for row in group {
                if let Some(v) = row.get(&spec.field)
                    && !matches!(v, Value::Null)
                    && !seen.contains(v)
                {
                    seen.push(v.clone());
                }
            }
            Ok(Value::Integer(seen.len() as i64))
        }
        "SUM" => {
            let (floats, ints) = collect_numeric(group, &spec.field);
            if !floats.is_empty() {
                Ok(Value::Float((ts_runtime().sum_f64)(&floats)))
            } else if !ints.is_empty() {
                let sum128 = (i64_runtime().sum_i64)(&ints);
                let clamped = sum128.clamp(i64::MIN as i128, i64::MAX as i128) as i64;
                Ok(Value::Integer(clamped))
            } else {
                Ok(Value::Null)
            }
        }
        "AVG" => {
            let (floats, ints) = collect_numeric(group, &spec.field);
            if !floats.is_empty() {
                let sum = (ts_runtime().sum_f64)(&floats);
                Ok(Value::Float(sum / floats.len() as f64))
            } else if !ints.is_empty() {
                let sum: i64 = ints.iter().sum();
                Ok(Value::Float(sum as f64 / ints.len() as f64))
            } else {
                Ok(Value::Null)
            }
        }
        "MIN" => {
            let (floats, ints) = collect_numeric(group, &spec.field);
            if !floats.is_empty() {
                Ok(Value::Float((ts_runtime().min_f64)(&floats)))
            } else if !ints.is_empty() {
                Ok(Value::Integer((i64_runtime().min_i64)(&ints)))
            } else {
                // String MIN
                let s = group
                    .iter()
                    .filter_map(|row| row.get(&spec.field))
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .min();
                Ok(s.map(Value::String).unwrap_or(Value::Null))
            }
        }
        "MAX" => {
            let (floats, ints) = collect_numeric(group, &spec.field);
            if !floats.is_empty() {
                Ok(Value::Float((ts_runtime().max_f64)(&floats)))
            } else if !ints.is_empty() {
                Ok(Value::Integer((i64_runtime().max_i64)(&ints)))
            } else {
                let s = group
                    .iter()
                    .filter_map(|row| row.get(&spec.field))
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .max();
                Ok(s.map(Value::String).unwrap_or(Value::Null))
            }
        }
        other => Err(LiteError::Query(format!(
            "unsupported aggregate function: {other}"
        ))),
    }
}

fn collect_numeric(group: &[HashMap<String, Value>], field: &str) -> (Vec<f64>, Vec<i64>) {
    let mut floats = Vec::new();
    let mut ints = Vec::new();
    for row in group {
        match row.get(field) {
            Some(Value::Float(f)) => floats.push(*f),
            Some(Value::Integer(i)) => ints.push(*i),
            _ => {}
        }
    }
    (floats, ints)
}

fn apply_having(
    rows: &mut Vec<Vec<Value>>,
    having: &[ScanFilter],
    group_by: &[String],
    aggregates: &[AggregateSpec],
) {
    if having.is_empty() {
        return;
    }
    let columns = make_columns(group_by, aggregates);
    rows.retain(|row| {
        let mut map = HashMap::new();
        for (col, val) in columns.iter().zip(row.iter()) {
            map.insert(col.clone(), val.clone());
        }
        let doc = Value::Object(map);
        having.iter().all(|f| f.matches_value(&doc))
    });
}

fn apply_sort(
    rows: &mut [Vec<Value>],
    sort_keys: &[(String, bool)],
    group_by: &[String],
    aggregates: &[AggregateSpec],
) {
    if sort_keys.is_empty() {
        return;
    }
    let columns = make_columns(group_by, aggregates);
    rows.sort_by(|a, b| {
        for (col, asc) in sort_keys {
            let idx = columns.iter().position(|c| c == col);
            if let Some(i) = idx {
                let av = a.get(i).unwrap_or(&Value::Null);
                let bv = b.get(i).unwrap_or(&Value::Null);
                let ord = value_cmp(av, bv);
                if ord != std::cmp::Ordering::Equal {
                    return if *asc { ord } else { ord.reverse() };
                }
            }
        }
        std::cmp::Ordering::Equal
    });
}

pub(crate) fn value_cmp(a: &Value, b: &Value) -> std::cmp::Ordering {
    match (a, b) {
        (Value::Integer(x), Value::Integer(y)) => x.cmp(y),
        (Value::Float(x), Value::Float(y)) => x.partial_cmp(y).unwrap_or(std::cmp::Ordering::Equal),
        (Value::Integer(x), Value::Float(y)) => (*x as f64)
            .partial_cmp(y)
            .unwrap_or(std::cmp::Ordering::Equal),
        (Value::Float(x), Value::Integer(y)) => x
            .partial_cmp(&(*y as f64))
            .unwrap_or(std::cmp::Ordering::Equal),
        (Value::String(x), Value::String(y)) => x.cmp(y),
        (Value::Bool(x), Value::Bool(y)) => x.cmp(y),
        (Value::Null, Value::Null) => std::cmp::Ordering::Equal,
        (Value::Null, _) => std::cmp::Ordering::Less,
        (_, Value::Null) => std::cmp::Ordering::Greater,
        _ => std::cmp::Ordering::Equal,
    }
}

pub(crate) fn make_columns(group_by: &[String], aggregates: &[AggregateSpec]) -> Vec<String> {
    let mut cols: Vec<String> = group_by.to_vec();
    for spec in aggregates {
        if let Some(alias) = &spec.user_alias {
            cols.push(alias.clone());
        } else {
            cols.push(spec.alias.clone());
        }
    }
    cols
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_physical::physical_plan::query::AggregateSpec;

    fn make_spec(function: &str, field: &str, alias: &str) -> AggregateSpec {
        AggregateSpec {
            function: function.to_string(),
            alias: alias.to_string(),
            user_alias: None,
            field: field.to_string(),
            expr: None,
        }
    }

    fn make_rows(data: &[(&str, i64, f64)]) -> Vec<HashMap<String, Value>> {
        data.iter()
            .map(|(cat, n, price)| {
                let mut m = HashMap::new();
                m.insert("category".into(), Value::String(cat.to_string()));
                m.insert("count".into(), Value::Integer(*n));
                m.insert("price".into(), Value::Float(*price));
                m
            })
            .collect()
    }

    #[test]
    fn group_by_with_having() {
        let rows = make_rows(&[("a", 1, 10.0), ("a", 2, 20.0), ("b", 3, 5.0)]);
        let aggregates = vec![
            make_spec("COUNT", "*", "cnt"),
            make_spec("SUM", "price", "total"),
        ];
        // HAVING total > 10
        let having_filter = ScanFilter {
            field: "total".into(),
            op: nodedb_query::scan_filter::FilterOp::Gt,
            value: Value::Float(10.0),
            ..Default::default()
        };
        let having_bytes = zerompk::to_msgpack_vec(&vec![having_filter]).unwrap();

        let result = execute_aggregate(
            rows,
            &["category".to_string()],
            &aggregates,
            &[],
            &having_bytes,
            &[],
            &[],
        )
        .unwrap();
        // Only group "a" has total=30 > 10; group "b" has total=5.0
        assert_eq!(result.rows.len(), 1);
        let cat = &result.rows[0][0];
        assert_eq!(cat, &Value::String("a".into()));
    }

    #[test]
    fn partial_aggregate_prefix_column() {
        let rows = make_rows(&[("x", 1, 1.0)]);
        let aggregates = vec![make_spec("COUNT", "*", "cnt")];
        let result =
            execute_partial_aggregate(rows, &["category".to_string()], &aggregates, &[]).unwrap();
        assert_eq!(result.columns[0], "__partial");
        assert_eq!(result.rows[0][0], Value::Bool(true));
    }
}
