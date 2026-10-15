// SPDX-License-Identifier: Apache-2.0
//! Aggregate and PartialAggregate QueryOp implementations for Lite.
//!
//! Supports GROUP BY, aggregate functions (COUNT, SUM, AVG, MIN, MAX,
//! TS_STDDEV, and the DISTINCT forms of COUNT, SUM and AVG), HAVING,
//! ORDER BY, and GROUPING SETS (ROLLUP/CUBE) expansion. Any other function
//! is refused.

use std::collections::HashMap;

use nodedb_physical::physical_plan::query::AggregateSpec;
use nodedb_physical::physical_plan::{PARAMETERIZED_AGGREGATE_FUNCTIONS, SortKeySpec};
use nodedb_query::EvalError;
use nodedb_query::expr::GroupKeySpec;
use nodedb_query::scan_filter::ScanFilter;
use nodedb_types::result::QueryResult;
use nodedb_types::value::Value;

use crate::error::LiteError;

use super::aggregate_numeric::Numeric;

// ─── Type aliases ─────────────────────────────────────────────────────────────

type GroupMap = HashMap<String, (Vec<Value>, Vec<HashMap<String, Value>>)>;

// ─── Public entry points ─────────────────────────────────────────────────────

/// Execute a full Aggregate: apply filters, group, aggregate, HAVING, sort.
pub fn execute_aggregate(
    rows: Vec<HashMap<String, Value>>,
    group_by: &[GroupKeySpec],
    aggregates: &[AggregateSpec],
    filters: &[u8],
    having: &[u8],
    sort_keys: &[SortKeySpec],
    grouping_sets: &[Vec<u32>],
) -> Result<QueryResult, LiteError> {
    let scan_filters = decode_filters(filters)?;
    let having_filters = decode_filters(having)?;

    let filtered = retain_matching(rows, &scan_filters)?;

    if grouping_sets.is_empty() {
        // Plain GROUP BY — single grouping set containing all group_by columns.
        let grouped = group_rows(&filtered, group_by)?;
        let mut result_rows = compute_aggregate_groups(grouped, group_by, aggregates)?;
        apply_having(&mut result_rows, &having_filters, group_by, aggregates)?;
        apply_sort(&mut result_rows, sort_keys, group_by, aggregates);
        let columns = make_columns(group_by, aggregates);
        Ok(QueryResult {
            columns,
            rows: result_rows,
            rows_affected: 0,
            command: None,
        })
    } else {
        // GROUPING SETS: union results for each subset.
        let mut all_rows: Vec<Vec<Value>> = Vec::new();
        for set_indices in grouping_sets {
            let subset: Vec<GroupKeySpec> = set_indices
                .iter()
                .filter_map(|&i| group_by.get(i as usize).cloned())
                .collect();
            let grouped = group_rows(&filtered, &subset)?;
            let mut set_rows = compute_aggregate_groups(grouped, &subset, aggregates)?;
            // Null-fill absent group columns.
            let full_col_count = group_by.len();
            for row in &mut set_rows {
                while row.len() < full_col_count + aggregates.len() {
                    row.insert(set_indices.len(), Value::Null);
                }
            }
            apply_having(&mut set_rows, &having_filters, &subset, aggregates)?;
            all_rows.extend(set_rows);
        }
        apply_sort(&mut all_rows, sort_keys, group_by, aggregates);
        let columns = make_columns(group_by, aggregates);
        Ok(QueryResult {
            columns,
            rows: all_rows,
            rows_affected: 0,
            command: None,
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
    // `QueryOp::PartialAggregate` groups by bare column names only, so the
    // column → spec lift is lossless here.
    let group_by: Vec<GroupKeySpec> = group_by.iter().map(GroupKeySpec::column).collect();
    let group_by = group_by.as_slice();
    let scan_filters = decode_filters(filters)?;
    let filtered = retain_matching(rows, &scan_filters)?;

    let grouped = group_rows(&filtered, group_by)?;
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
        command: None,
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
pub(crate) fn group_rows(
    rows: &[HashMap<String, Value>],
    group_by: &[GroupKeySpec],
) -> Result<GroupMap, LiteError> {
    let mut groups: GroupMap = HashMap::new();
    for row in rows {
        let key: Vec<Value> = group_by
            .iter()
            .map(|spec| group_key_value(row, spec))
            .collect::<Result<Vec<_>, LiteError>>()?;
        let key_str = value_key_str(&key);
        groups
            .entry(key_str)
            .or_insert_with(|| (key, Vec::new()))
            .1
            .push(row.clone());
    }
    Ok(groups)
}

/// Keep only the rows matching every filter. Evaluation is fallible (a
/// divide-by-zero predicate fails the statement rather than dropping the row),
/// so this replaces the `filter(..).collect()` form it grew out of.
fn retain_matching(
    rows: Vec<HashMap<String, Value>>,
    filters: &[ScanFilter],
) -> Result<Vec<HashMap<String, Value>>, LiteError> {
    if filters.is_empty() {
        return Ok(rows);
    }
    let mut kept = Vec::with_capacity(rows.len());
    for row in rows {
        let doc = Value::Object(row.clone());
        if ScanFilter::all_match_value(filters, &doc)? {
            kept.push(row);
        }
    }
    Ok(kept)
}

/// Resolve one group key for a row.
///
/// A spec carrying an expression (e.g. `GROUP BY date_trunc('day', ts)`) is
/// evaluated against the row; otherwise the named field is extracted directly.
/// A spec with neither groups everything under `Null`.
fn group_key_value(row: &HashMap<String, Value>, spec: &GroupKeySpec) -> Result<Value, LiteError> {
    match (&spec.expr, &spec.field) {
        (Some(expr), _) => Ok(expr.eval(&Value::Object(row.clone()))?),
        (None, Some(field)) => Ok(row.get(field).cloned().unwrap_or(Value::Null)),
        (None, None) => Ok(Value::Null),
    }
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
    _group_by: &[GroupKeySpec],
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
    // Lite has no accumulator for a parameterized aggregate (`approx_topk`,
    // `approx_percentile`, `percentile_cont`), so its parameter has nothing
    // to feed.
    if spec.param.is_some()
        || PARAMETERIZED_AGGREGATE_FUNCTIONS.contains(&spec.function.to_lowercase().as_str())
    {
        return Err(unsupported_function(&spec.function));
    }
    if func == "COUNT" && spec.field == "*" && spec.expr.is_none() {
        return Ok(Value::Integer(group.len() as i64));
    }
    // One input per row: the evaluated argument expression, or the field.
    // A NULL input is skipped by every function below.
    let inputs: Vec<Value> = group
        .iter()
        .map(|row| match &spec.expr {
            Some(expr) => Ok(expr.eval(&Value::Object(row.clone()))?),
            None => Ok(row.get(&spec.field).cloned().unwrap_or(Value::Null)),
        })
        .collect::<Result<_, LiteError>>()?;
    let non_null = || inputs.iter().filter(|v| !matches!(v, Value::Null));
    let distinct = || {
        let mut seen: Vec<&Value> = Vec::new();
        for v in non_null() {
            if !seen.contains(&v) {
                seen.push(v);
            }
        }
        seen
    };
    match func.as_str() {
        "COUNT" => Ok(Value::Integer(non_null().count() as i64)),
        "COUNT_DISTINCT" => Ok(Value::Integer(distinct().len() as i64)),
        "SUM" => Numeric::collect(non_null())
            .sum()
            .map_err(|e| total_error(spec, e)),
        "SUM_DISTINCT" => Numeric::collect(distinct().into_iter())
            .sum()
            .map_err(|e| total_error(spec, e)),
        "AVG" => Numeric::collect(non_null())
            .avg()
            .map_err(|e| total_error(spec, e)),
        "AVG_DISTINCT" => Numeric::collect(distinct().into_iter())
            .avg()
            .map_err(|e| total_error(spec, e)),
        "TS_STDDEV" => {
            // Population standard deviation; NULL below two inputs.
            let mut accum = nodedb_query::ts_functions::TsStddevAccum::new();
            for v in Numeric::collect(non_null()).as_f64() {
                accum.update(v);
            }
            Ok(accum
                .evaluate_population()
                .map_or(Value::Null, Value::Float))
        }
        "MIN" => {
            let numeric = Numeric::collect(non_null());
            if !numeric.is_empty() {
                Ok(numeric.min())
            } else {
                // String MIN
                let s = non_null()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .min();
                Ok(s.map(Value::String).unwrap_or(Value::Null))
            }
        }
        "MAX" => {
            let numeric = Numeric::collect(non_null());
            if !numeric.is_empty() {
                Ok(numeric.max())
            } else {
                let s = non_null()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .max();
                Ok(s.map(Value::String).unwrap_or(Value::Null))
            }
        }
        _ => Err(unsupported_function(&spec.function)),
    }
}

/// The error of a SUM or AVG total. An exact total past the `Decimal` range
/// is `NumericValueOutOfRange`, SQLSTATE `22003` as on Origin, and names the
/// aggregate and its column.
fn total_error(spec: &AggregateSpec, e: EvalError) -> LiteError {
    match e {
        EvalError::NumericOverflow { .. } => LiteError::NumericValueOutOfRange {
            detail: format!(
                "{}({}): integer total out of range",
                spec.function.to_lowercase(),
                spec.field
            ),
        },
        other @ (EvalError::DivisionByZero
        | EvalError::UnknownFunction { .. }
        | EvalError::VectorDimensionMismatch { .. }
        | EvalError::ArgumentType { .. }
        | EvalError::InvalidJsonPath { .. }) => LiteError::from(other),
    }
}

fn unsupported_function(function: &str) -> LiteError {
    LiteError::Unsupported {
        detail: format!("aggregate function {function}() is not supported in Lite"),
    }
}

fn apply_having(
    rows: &mut Vec<Vec<Value>>,
    having: &[ScanFilter],
    group_by: &[GroupKeySpec],
    aggregates: &[AggregateSpec],
) -> Result<(), LiteError> {
    if having.is_empty() {
        return Ok(());
    }
    let columns = make_columns(group_by, aggregates);
    // HAVING evaluation is fallible, so the keep-decision is computed before
    // the retain rather than inside a closure that cannot propagate.
    let mut keep = Vec::with_capacity(rows.len());
    for row in rows.iter() {
        let mut map = HashMap::new();
        for (col, val) in columns.iter().zip(row.iter()) {
            map.insert(col.clone(), val.clone());
        }
        let doc = Value::Object(map);
        keep.push(ScanFilter::all_match_value(having, &doc)?);
    }
    let mut iter = keep.into_iter();
    rows.retain(|_| iter.next().unwrap_or(true));
    Ok(())
}

fn apply_sort(
    rows: &mut [Vec<Value>],
    sort_keys: &[SortKeySpec],
    group_by: &[GroupKeySpec],
    aggregates: &[AggregateSpec],
) {
    if sort_keys.is_empty() {
        return;
    }
    let columns = make_columns(group_by, aggregates);
    rows.sort_by(|a, b| {
        for key in sort_keys {
            // Aggregate output rows are addressed by output column name, so a
            // computed key has nothing to match here — skip it, matching the
            // Origin executor's `sort_aggregated_rows`.
            let Some(col) = key.as_column() else {
                continue;
            };
            let idx = columns.iter().position(|c| c == col);
            if let Some(i) = idx {
                let av = a.get(i).unwrap_or(&Value::Null);
                let bv = b.get(i).unwrap_or(&Value::Null);
                // NULL placement is absolute; the direction never flips it.
                let ord = match key.order_nulls(av == &Value::Null, bv == &Value::Null) {
                    Some(ord) => ord,
                    None => key.direct(value_cmp(av, bv)),
                };
                if ord != std::cmp::Ordering::Equal {
                    return ord;
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

pub(crate) fn make_columns(group_by: &[GroupKeySpec], aggregates: &[AggregateSpec]) -> Vec<String> {
    let mut cols: Vec<String> = group_by.iter().map(|s| s.output_name.clone()).collect();
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
    use nodedb_query::expr::{BinaryOp, SqlExpr};

    fn make_spec(function: &str, field: &str, alias: &str) -> AggregateSpec {
        AggregateSpec {
            function: function.to_string(),
            alias: alias.to_string(),
            user_alias: None,
            field: field.to_string(),
            expr: None,
            param: None,
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
            clauses: Vec::new(),
            expr: None,
        };
        let having_bytes = zerompk::to_msgpack_vec(&vec![having_filter]).unwrap();

        let result = execute_aggregate(
            rows,
            &[GroupKeySpec::column("category")],
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
    fn integer_sum_past_i64_does_not_clamp() {
        let rows = make_rows(&[("a", i64::MAX, 1.0), ("a", 1, 1.0)]);
        let aggregates = vec![make_spec("SUM", "count", "total")];
        let result = execute_aggregate(rows, &[], &aggregates, &[], &[], &[], &[]).unwrap();
        assert_eq!(
            result.rows[0][0],
            Value::Decimal(rust_decimal::Decimal::from_i128_with_scale(
                i128::from(i64::MAX) + 1,
                0
            ))
        );
    }

    #[test]
    fn integer_sum_overflow_names_aggregate_and_column() {
        let row = |amount| {
            let mut m = HashMap::new();
            m.insert("amount".to_string(), Value::Decimal(amount));
            m
        };
        let max = rust_decimal::Decimal::MAX;
        let aggregates = vec![make_spec("SUM", "amount", "total")];
        let err = execute_aggregate(
            vec![row(max), row(max)],
            &[],
            &aggregates,
            &[],
            &[],
            &[],
            &[],
        )
        .unwrap_err();
        match err {
            LiteError::NumericValueOutOfRange { detail } => {
                assert!(detail.contains("sum(amount)"), "{detail}");
            }
            other => panic!("expected NumericValueOutOfRange, got {other:?}"),
        }
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

    #[test]
    fn group_by_computed_expression_key() {
        // GROUP BY on a computed key (count + count) rather than a bare column:
        // rows with counts 1 and 3 collapse into distinct groups 2 and 6, while
        // the two rows with count 1 share a group.
        let rows = make_rows(&[("a", 1, 10.0), ("b", 1, 20.0), ("c", 3, 5.0)]);
        let aggregates = vec![make_spec("COUNT", "*", "cnt")];

        let doubled = GroupKeySpec {
            output_name: "doubled".to_string(),
            field: None,
            expr: Some(SqlExpr::BinaryOp {
                left: Box::new(SqlExpr::Column("count".to_string())),
                op: BinaryOp::Add,
                right: Box::new(SqlExpr::Column("count".to_string())),
            }),
        };

        let result = execute_aggregate(rows, &[doubled], &aggregates, &[], &[], &[], &[]).unwrap();

        assert_eq!(result.columns[0], "doubled");
        assert_eq!(result.rows.len(), 2);

        let mut got: Vec<(i64, i64)> = result
            .rows
            .iter()
            .filter_map(|r| match (&r[0], &r[1]) {
                (Value::Integer(k), Value::Integer(n)) => Some((*k, *n)),
                _ => None,
            })
            .collect();
        got.sort_unstable();
        assert_eq!(got, vec![(2, 2), (6, 1)]);
    }
}
