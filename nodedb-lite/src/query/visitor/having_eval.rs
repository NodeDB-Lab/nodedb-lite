// SPDX-License-Identifier: Apache-2.0
//! Post-aggregate HAVING predicate evaluator.
//!
//! `sql_filters_to_metadata` cannot encode HAVING predicates that reference
//! aggregate function calls (e.g. `SUM(salary) > 100`). This module evaluates
//! such predicates directly on `QueryResult` rows after aggregation.

use std::collections::HashMap;

use nodedb_sql::types::filter::{CompareOp, Filter, FilterExpr};
use nodedb_sql::types::query::AggregateExpr;
use nodedb_sql::types_expr::{BinaryOp, SqlExpr, SqlValue};
use nodedb_types::result::QueryResult;
use nodedb_types::value::Value;

/// Build a lookup: `"func_name:field"` → result alias for aggregate HAVING.
///
/// For example, `SUM(salary) AS total` produces the key `"sum:salary"` → `"total"`.
pub(super) fn make_agg_alias_map(aggregates: &[AggregateExpr]) -> HashMap<String, String> {
    aggregates
        .iter()
        .map(|a| {
            let func = a.function.to_lowercase();
            let field = a
                .args
                .first()
                .map(|e| match e {
                    SqlExpr::Column { name, .. } => name.clone(),
                    SqlExpr::Wildcard => "*".to_string(),
                    other => format!("{other:?}"),
                })
                .unwrap_or_default();
            let key = format!("{func}:{field}");
            (key, a.alias.clone())
        })
        .collect()
}

/// Apply HAVING `Filter` predicates directly on a `QueryResult`.
///
/// Used when the HAVING predicate contains aggregate function expressions that
/// `sql_filters_to_metadata` cannot encode. Uses `agg_alias_map` to resolve
/// aggregate function calls to their result column aliases.
pub(super) fn apply_having_result(
    mut result: QueryResult,
    having: &[Filter],
    agg_alias_map: &HashMap<String, String>,
) -> QueryResult {
    if having.is_empty() {
        return result;
    }
    let cols = result.columns.clone();
    result.rows.retain(|row| {
        let map: HashMap<&str, &Value> = cols
            .iter()
            .zip(row.iter())
            .map(|(c, v)| (c.as_str(), v))
            .collect();
        having
            .iter()
            .all(|f| eval_having_filter(f, &map, agg_alias_map))
    });
    result
}

fn eval_having_filter(
    f: &Filter,
    row: &HashMap<&str, &Value>,
    agg_map: &HashMap<String, String>,
) -> bool {
    eval_having_expr(&f.expr, row, agg_map)
}

fn eval_having_expr(
    expr: &FilterExpr,
    row: &HashMap<&str, &Value>,
    agg_map: &HashMap<String, String>,
) -> bool {
    match expr {
        FilterExpr::Comparison { field, op, value } => {
            if let Some(rv) = row.get(field.as_str()).copied() {
                let right = sql_value_to_ndb(value);
                compare_values(rv, *op, &right)
            } else {
                true
            }
        }
        FilterExpr::And(filters) => filters.iter().all(|f| eval_having_filter(f, row, agg_map)),
        FilterExpr::Or(filters) => filters.iter().any(|f| eval_having_filter(f, row, agg_map)),
        FilterExpr::Not(f) => !eval_having_filter(f, row, agg_map),
        FilterExpr::InList { field, values } => {
            if let Some(rv) = row.get(field.as_str()).copied() {
                values.iter().any(|v| {
                    let nv = sql_value_to_ndb(v);
                    compare_values(rv, CompareOp::Eq, &nv)
                })
            } else {
                true
            }
        }
        FilterExpr::Between { field, low, high } => {
            if let Some(rv) = row.get(field.as_str()).copied() {
                let lo = sql_value_to_ndb(low);
                let hi = sql_value_to_ndb(high);
                compare_values(rv, CompareOp::Ge, &lo) && compare_values(rv, CompareOp::Le, &hi)
            } else {
                true
            }
        }
        FilterExpr::IsNull { field } => row
            .get(field.as_str())
            .map(|v| **v == Value::Null)
            .unwrap_or(true),
        FilterExpr::IsNotNull { field } => row
            .get(field.as_str())
            .map(|v| **v != Value::Null)
            .unwrap_or(false),
        FilterExpr::Expr(sql_expr) => eval_sql_expr_bool(sql_expr, row, agg_map),
    }
}

/// Evaluate a `SqlExpr` to an optional `Value` against a result row.
///
/// Aggregate function calls (e.g. `SUM(salary)`) are resolved to their alias
/// columns via `agg_map`.
fn eval_sql_expr_value(
    expr: &SqlExpr,
    row: &HashMap<&str, &Value>,
    agg_map: &HashMap<String, String>,
) -> Option<Value> {
    match expr {
        SqlExpr::Literal(v) => Some(sql_value_to_ndb(v)),
        SqlExpr::Column { name, .. } => row.get(name.as_str()).map(|v| (*v).clone()),
        SqlExpr::Function { name, args, .. } => {
            let func_lower = name.to_lowercase();
            let field = args
                .first()
                .map(|a| match a {
                    SqlExpr::Column { name, .. } => name.clone(),
                    SqlExpr::Wildcard => "*".to_string(),
                    other => format!("{other:?}"),
                })
                .unwrap_or_default();
            let key = format!("{func_lower}:{field}");
            let alias = agg_map.get(&key)?;
            row.get(alias.as_str()).map(|v| (*v).clone())
        }
        SqlExpr::BinaryOp { left, op, right } => {
            let lv = eval_sql_expr_value(left, row, agg_map)?;
            let rv = eval_sql_expr_value(right, row, agg_map)?;
            match op {
                BinaryOp::Add => numeric_op(
                    &lv,
                    &rv,
                    |a, b| Value::Float(a + b),
                    |a, b| Value::Integer(a + b),
                ),
                BinaryOp::Sub => numeric_op(
                    &lv,
                    &rv,
                    |a, b| Value::Float(a - b),
                    |a, b| Value::Integer(a - b),
                ),
                BinaryOp::Mul => numeric_op(
                    &lv,
                    &rv,
                    |a, b| Value::Float(a * b),
                    |a, b| Value::Integer(a * b),
                ),
                BinaryOp::Div => numeric_op(
                    &lv,
                    &rv,
                    |a, b| {
                        if b == 0.0 {
                            Value::Null
                        } else {
                            Value::Float(a / b)
                        }
                    },
                    |a, b| {
                        if b == 0 {
                            Value::Null
                        } else {
                            Value::Integer(a / b)
                        }
                    },
                ),
                _ => None,
            }
        }
        _ => None,
    }
}

fn numeric_op(
    l: &Value,
    r: &Value,
    f_float: impl Fn(f64, f64) -> Value,
    f_int: impl Fn(i64, i64) -> Value,
) -> Option<Value> {
    match (l, r) {
        (Value::Integer(a), Value::Integer(b)) => Some(f_int(*a, *b)),
        (Value::Float(a), Value::Float(b)) => Some(f_float(*a, *b)),
        (Value::Integer(a), Value::Float(b)) => Some(f_float(*a as f64, *b)),
        (Value::Float(a), Value::Integer(b)) => Some(f_float(*a, *b as f64)),
        _ => None,
    }
}

/// Evaluate a `SqlExpr` as a boolean predicate.
fn eval_sql_expr_bool(
    expr: &SqlExpr,
    row: &HashMap<&str, &Value>,
    agg_map: &HashMap<String, String>,
) -> bool {
    match expr {
        SqlExpr::BinaryOp { left, op, right } => match op {
            BinaryOp::And => {
                eval_sql_expr_bool(left, row, agg_map) && eval_sql_expr_bool(right, row, agg_map)
            }
            BinaryOp::Or => {
                eval_sql_expr_bool(left, row, agg_map) || eval_sql_expr_bool(right, row, agg_map)
            }
            BinaryOp::Eq
            | BinaryOp::Ne
            | BinaryOp::Gt
            | BinaryOp::Ge
            | BinaryOp::Lt
            | BinaryOp::Le => {
                let lv = match eval_sql_expr_value(left, row, agg_map) {
                    Some(v) => v,
                    None => return true,
                };
                let rv = match eval_sql_expr_value(right, row, agg_map) {
                    Some(v) => v,
                    None => return true,
                };
                let cmp_op = match op {
                    BinaryOp::Eq => CompareOp::Eq,
                    BinaryOp::Ne => CompareOp::Ne,
                    BinaryOp::Gt => CompareOp::Gt,
                    BinaryOp::Ge => CompareOp::Ge,
                    BinaryOp::Lt => CompareOp::Lt,
                    BinaryOp::Le => CompareOp::Le,
                    _ => return true,
                };
                compare_values(&lv, cmp_op, &rv)
            }
            _ => true,
        },
        SqlExpr::IsNull {
            expr: inner,
            negated,
        } => {
            let v = eval_sql_expr_value(inner, row, agg_map);
            let is_null = v.map(|v| v == Value::Null).unwrap_or(true);
            if *negated { !is_null } else { is_null }
        }
        _ => match eval_sql_expr_value(expr, row, agg_map) {
            Some(Value::Bool(b)) => b,
            Some(Value::Null) => false,
            Some(_) => true,
            None => true,
        },
    }
}

fn compare_values(left: &Value, op: CompareOp, right: &Value) -> bool {
    match (left, right) {
        (Value::Integer(l), Value::Integer(r)) => cmp_ord(*l, *r, op),
        (Value::Float(l), Value::Float(r)) => cmp_partial(*l, *r, op),
        (Value::Integer(l), Value::Float(r)) => cmp_partial(*l as f64, *r, op),
        (Value::Float(l), Value::Integer(r)) => cmp_partial(*l, *r as f64, op),
        (Value::String(l), Value::String(r)) => cmp_ord(l, r, op),
        _ => false,
    }
}

fn cmp_ord<T: Ord>(l: T, r: T, op: CompareOp) -> bool {
    match op {
        CompareOp::Eq => l == r,
        CompareOp::Ne => l != r,
        CompareOp::Gt => l > r,
        CompareOp::Ge => l >= r,
        CompareOp::Lt => l < r,
        CompareOp::Le => l <= r,
    }
}

fn cmp_partial<T: PartialOrd>(l: T, r: T, op: CompareOp) -> bool {
    match op {
        CompareOp::Eq => l == r,
        CompareOp::Ne => l != r,
        CompareOp::Gt => l > r,
        CompareOp::Ge => l >= r,
        CompareOp::Lt => l < r,
        CompareOp::Le => l <= r,
    }
}

fn sql_value_to_ndb(v: &SqlValue) -> Value {
    match v {
        SqlValue::String(s) => Value::String(s.clone()),
        SqlValue::Int(i) => Value::Integer(*i),
        SqlValue::Float(f) => Value::Float(*f),
        SqlValue::Bool(b) => Value::Bool(*b),
        SqlValue::Null => Value::Null,
        _ => Value::Null,
    }
}
