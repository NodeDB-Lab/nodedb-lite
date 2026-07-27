// SPDX-License-Identifier: Apache-2.0

//! Post-processing for scan results: WHERE, DISTINCT, ORDER BY, window functions, OFFSET, LIMIT.

use std::collections::HashMap;
use std::collections::HashSet;

use nodedb_query::expr::types::SqlExpr as QExpr;
use nodedb_query::metadata_filter::matches_metadata_filter;
use nodedb_query::value_ops::compare_values;
use nodedb_query::window::WindowFuncSpec;
use nodedb_sql::types::filter::Filter;
use nodedb_sql::types::query::{SortKey, WindowSpec};
use nodedb_types::result::QueryResult;
use nodedb_types::value::Value;

use crate::error::LiteError;
use crate::query::expr_convert::convert_sql_expr;
use crate::query::filter_convert::{LiteFilter, sql_filters_to_metadata};

/// Apply WHERE / DISTINCT / ORDER BY / window functions / OFFSET / LIMIT to a raw scan result.
///
/// Steps follow SQL semantics for a flat scan (no grouping or aggregation):
/// 1. WHERE filtering
/// 2. DISTINCT deduplication
/// 3. ORDER BY sorting
/// 4. Window function evaluation
/// 5. OFFSET skip
/// 6. LIMIT take
pub(crate) fn apply_scan_post_processing(
    mut result: QueryResult,
    filters: &[Filter],
    sort_keys: &[SortKey],
    window_specs: &[WindowSpec],
    limit: Option<usize>,
    offset: usize,
    distinct: bool,
) -> Result<QueryResult, LiteError> {
    // 1. WHERE — apply both primitive MetadataFilter and complex QExpr predicates.
    filter_rows(&mut result, filters)?;

    // 2. DISTINCT — over the full row; a scan has no separate target list here.
    if distinct {
        distinct_rows(&mut result, &[]);
    }

    // 3. ORDER BY
    sort_rows(&mut result, sort_keys)?;

    // 4. Window functions
    if !window_specs.is_empty() {
        let converted = convert_window_specs(window_specs)?;
        let column_index: HashMap<String, usize> = result
            .columns
            .iter()
            .enumerate()
            .map(|(i, c)| (c.clone(), i))
            .collect();
        let new_cols = nodedb_query::window::evaluate_window_functions_value(
            &mut result.rows,
            &column_index,
            &converted,
        )
        .map_err(|e| LiteError::BadRequest {
            detail: format!("window function evaluation failed: {e}"),
        })?;
        result.columns.extend(new_cols);
    }

    // 5. OFFSET
    if offset > 0 {
        result.rows = result.rows.into_iter().skip(offset).collect();
    }

    // 6. LIMIT
    if let Some(n) = limit {
        result.rows.truncate(n);
    }

    Ok(result)
}

/// Retain only the rows satisfying `filters`, applying both the primitive
/// `MetadataFilter` form and the complex `QExpr` predicates. A no-op when
/// `filters` is empty or lowers to nothing.
pub(crate) fn filter_rows(result: &mut QueryResult, filters: &[Filter]) -> Result<(), LiteError> {
    if filters.is_empty() {
        return Ok(());
    }
    let lf: LiteFilter = sql_filters_to_metadata(filters, &[])?;
    if lf.is_empty() {
        return Ok(());
    }
    // Evaluation is fallible (a divide-by-zero predicate must fail the
    // statement, not drop the row), so the keep-decision is computed up front
    // rather than inside a `retain` closure that cannot propagate.
    let columns = result.columns.clone();
    let mut keep = Vec::with_capacity(result.rows.len());
    for row in &result.rows {
        let json_doc = row_to_json(&columns, row);
        let meta_pass = lf
            .meta
            .as_ref()
            .map(|f| matches_metadata_filter(&json_doc, f))
            .unwrap_or(true);
        if !meta_pass {
            keep.push(false);
            continue;
        }
        if lf.exprs.is_empty() {
            keep.push(true);
        } else {
            let typed_doc = row_to_typed_value(&columns, row);
            keep.push(lf.eval_exprs(&typed_doc)?);
        }
    }
    let mut iter = keep.into_iter();
    result.rows.retain(|_| iter.next().unwrap_or(true));
    Ok(())
}

/// Deduplicate rows on the *would-be projected* shape, so SQL `DISTINCT`
/// semantics hold: two rows agreeing on every projected column are equal even
/// when their non-projected columns differ. An empty `projection` dedupes on
/// the whole row.
pub(crate) fn distinct_rows(result: &mut QueryResult, projection: &[String]) {
    let columns = result.columns.clone();
    let mut seen: HashSet<String> = HashSet::new();
    result.rows.retain(|row| {
        let doc = if projection.is_empty() {
            row_to_json(&columns, row)
        } else {
            project_row(&columns, row, projection)
        };
        seen.insert(serde_json::to_string(&doc).unwrap_or_default())
    });
}

/// Sort rows by `sort_keys` in place. A no-op when `sort_keys` is empty.
pub(crate) fn sort_rows(result: &mut QueryResult, sort_keys: &[SortKey]) -> Result<(), LiteError> {
    if sort_keys.is_empty() {
        return Ok(());
    }
    let resolved = resolve_sort_keys(sort_keys, &result.columns)?;
    // Decorate-sort-undecorate: an expression sort key is fallible, and a
    // comparator cannot propagate, so every key is materialized once before
    // the sort — which also avoids re-evaluating it on each comparison.
    let mut decorated: Vec<(Vec<Value>, Vec<Value>)> = Vec::with_capacity(result.rows.len());
    for row in std::mem::take(&mut result.rows) {
        let keys = resolved
            .iter()
            .map(|sk| extract_key_value(&row, &result.columns, &sk.key))
            .collect::<Result<Vec<_>, LiteError>>()?;
        decorated.push((keys, row));
    }
    decorated.sort_by(|a, b| compare_keys(&a.0, &b.0, &resolved));
    result.rows = decorated.into_iter().map(|(_, row)| row).collect();
    Ok(())
}

/// Reshape every row down to `projection`, in the order named. Columns absent
/// from a row become `Value::Null` (the row shape stays rectangular). An empty
/// `projection` means `SELECT *` and leaves the result untouched.
pub(crate) fn project_rows(result: &mut QueryResult, projection: &[String]) {
    if projection.is_empty() {
        return;
    }
    let indices: Vec<Option<usize>> = projection
        .iter()
        .map(|name| result.columns.iter().position(|c| c == name))
        .collect();
    for row in &mut result.rows {
        *row = indices
            .iter()
            .map(|idx| idx.and_then(|i| row.get(i).cloned()).unwrap_or(Value::Null))
            .collect();
    }
    result.columns = projection.to_vec();
}

/// A row reduced to the named columns, as JSON — the DISTINCT key for a
/// projected target list.
fn project_row(columns: &[String], row: &[Value], projection: &[String]) -> serde_json::Value {
    let mut map = serde_json::Map::new();
    for name in projection {
        let value = columns
            .iter()
            .position(|c| c == name)
            .and_then(|i| row.get(i))
            .map_or(serde_json::Value::Null, value_to_json);
        map.insert(name.clone(), value);
    }
    serde_json::Value::Object(map)
}

fn convert_window_specs(specs: &[WindowSpec]) -> Result<Vec<WindowFuncSpec>, LiteError> {
    specs.iter().map(convert_one_window_spec).collect()
}

fn convert_one_window_spec(spec: &WindowSpec) -> Result<WindowFuncSpec, LiteError> {
    let args: Result<Vec<_>, _> = spec.args.iter().map(convert_sql_expr).collect();
    let partition_by: Result<Vec<_>, LiteError> = spec
        .partition_by
        .iter()
        .map(|e| {
            convert_sql_expr(e).map_err(|err| LiteError::BadRequest {
                detail: format!("PARTITION BY expression cannot be lowered: {err}"),
            })
        })
        .collect();
    let order_by: Result<Vec<_>, LiteError> = spec
        .order_by
        .iter()
        .map(|k| {
            let expr = convert_sql_expr(&k.expr).map_err(|err| LiteError::BadRequest {
                detail: format!("ORDER BY expression cannot be lowered: {err}"),
            })?;
            Ok((expr, k.ascending))
        })
        .collect();

    Ok(WindowFuncSpec {
        alias: spec.alias.clone(),
        func_name: spec.function.to_lowercase(),
        args: args?,
        partition_by: partition_by?,
        order_by: order_by?,
        frame: spec.frame.clone(),
    })
}

/// Per-sort-key descriptor resolved to either a column index or a query-side expression.
enum ResolvedKey {
    ColIndex(usize),
    Expr(QExpr),
}

struct SortKeyResolved {
    key: ResolvedKey,
    ascending: bool,
    nulls_first: bool,
}

fn resolve_sort_keys(
    sort_keys: &[SortKey],
    columns: &[String],
) -> Result<Vec<SortKeyResolved>, LiteError> {
    sort_keys
        .iter()
        .map(|sk| {
            let key = match &sk.expr {
                nodedb_sql::types_expr::SqlExpr::Column { name, .. } => {
                    // Output columns are bare names, so a qualified reference
                    // (`t.col`) only matches once the qualifier is stripped.
                    let bare = name.rsplit('.').next().unwrap_or(name);
                    let idx = columns
                        .iter()
                        .position(|c| c == name)
                        .or_else(|| columns.iter().position(|c| c == bare))
                        .ok_or_else(|| LiteError::BadRequest {
                            detail: format!("ORDER BY column '{name}' not found in scan output"),
                        })?;
                    ResolvedKey::ColIndex(idx)
                }
                other => ResolvedKey::Expr(convert_sql_expr(other)?),
            };
            Ok(SortKeyResolved {
                key,
                ascending: sk.ascending,
                nulls_first: sk.nulls_first,
            })
        })
        .collect()
}

/// Compare two rows' pre-materialized sort-key values, honouring each key's
/// direction and NULL placement.
fn compare_keys(a: &[Value], b: &[Value], keys: &[SortKeyResolved]) -> std::cmp::Ordering {
    for (i, sk) in keys.iter().enumerate() {
        let va = a.get(i).unwrap_or(&Value::Null);
        let vb = b.get(i).unwrap_or(&Value::Null);
        let ord = cmp_with_nulls(va, vb, sk.nulls_first);
        let ord = if sk.ascending { ord } else { ord.reverse() };
        if ord != std::cmp::Ordering::Equal {
            return ord;
        }
    }
    std::cmp::Ordering::Equal
}

fn extract_key_value(
    row: &[Value],
    columns: &[String],
    key: &ResolvedKey,
) -> Result<Value, LiteError> {
    match key {
        ResolvedKey::ColIndex(idx) => Ok(row.get(*idx).cloned().unwrap_or(Value::Null)),
        ResolvedKey::Expr(expr) => {
            let doc = row_to_typed_value(columns, row);
            Ok(expr.eval(&doc)?)
        }
    }
}

fn cmp_with_nulls(a: &Value, b: &Value, nulls_first: bool) -> std::cmp::Ordering {
    match (a, b) {
        (Value::Null, Value::Null) => std::cmp::Ordering::Equal,
        (Value::Null, _) => {
            if nulls_first {
                std::cmp::Ordering::Less
            } else {
                std::cmp::Ordering::Greater
            }
        }
        (_, Value::Null) => {
            if nulls_first {
                std::cmp::Ordering::Greater
            } else {
                std::cmp::Ordering::Less
            }
        }
        (va, vb) => compare_values(va, vb),
    }
}

fn row_to_json(columns: &[String], row: &[Value]) -> serde_json::Value {
    let mut map = serde_json::Map::new();
    for (col, val) in columns.iter().zip(row.iter()) {
        // For schemaless document rows the physical scan serialises the whole
        // document payload into a single "document" JSON-string column.  Inline
        // its fields into the filter context so that WHERE predicates on
        // user-defined fields (e.g. `tier = 'gold'`) can match them directly.
        if col == "document"
            && let Value::String(json_str) = val
            && let Ok(serde_json::Value::Object(inner)) =
                serde_json::from_str::<serde_json::Value>(json_str)
        {
            for (k, v) in inner {
                map.entry(k).or_insert(v);
            }
            continue;
        }
        map.insert(col.clone(), value_to_json(val));
    }
    serde_json::Value::Object(map)
}

fn value_to_json(v: &Value) -> serde_json::Value {
    match v {
        Value::Null => serde_json::Value::Null,
        Value::Bool(b) => serde_json::Value::Bool(*b),
        Value::Integer(i) => serde_json::Value::Number((*i).into()),
        Value::Float(f) => serde_json::json!(f),
        Value::String(s) => serde_json::Value::String(s.clone()),
        Value::Bytes(b) => {
            serde_json::Value::String(b.iter().map(|x| format!("{x:02x}")).collect())
        }
        Value::Array(arr) => serde_json::Value::Array(arr.iter().map(value_to_json).collect()),
        Value::Object(map) => {
            let mut out = serde_json::Map::new();
            for (k, val) in map {
                out.insert(k.clone(), value_to_json(val));
            }
            serde_json::Value::Object(out)
        }
        Value::NaiveDateTime(dt) => serde_json::Value::String(dt.to_string()),
        Value::DateTime(dt) => serde_json::Value::String(dt.to_string()),
        Value::Vector(f) => {
            serde_json::Value::Array(f.iter().map(|x| serde_json::json!(x)).collect())
        }
        _ => serde_json::Value::Null,
    }
}

fn row_to_typed_value(columns: &[String], row: &[Value]) -> Value {
    let mut map = std::collections::HashMap::new();
    for (col, val) in columns.iter().zip(row.iter()) {
        // For schemaless document rows the physical scan serialises the whole
        // document payload into a single "document" JSON-string column.  Inline
        // its fields so that QExpr predicates on user-defined fields work.
        if col == "document"
            && let Value::String(json_str) = val
            && let Ok(serde_json::Value::Object(inner)) =
                serde_json::from_str::<serde_json::Value>(json_str)
        {
            for (k, v) in inner {
                map.entry(k).or_insert_with(|| json_value_to_value(&v));
            }
            continue;
        }
        map.insert(col.clone(), val.clone());
    }
    Value::Object(map)
}

fn json_value_to_value(v: &serde_json::Value) -> Value {
    match v {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(b) => Value::Bool(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Value::Integer(i)
            } else {
                Value::Float(n.as_f64().unwrap_or(0.0))
            }
        }
        serde_json::Value::String(s) => Value::String(s.clone()),
        serde_json::Value::Array(arr) => {
            Value::Array(arr.iter().map(json_value_to_value).collect())
        }
        serde_json::Value::Object(obj) => {
            let mut m = std::collections::HashMap::new();
            for (k, val) in obj {
                m.insert(k.clone(), json_value_to_value(val));
            }
            Value::Object(m)
        }
    }
}
