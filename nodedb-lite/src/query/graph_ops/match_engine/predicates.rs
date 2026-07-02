// SPDX-License-Identifier: Apache-2.0

//! WHERE-predicate evaluation for MATCH executor: plain node equality, numeric
//! and lexicographic comparison, `NOT EXISTS` sub-pattern, plus CRDT
//! sub-field hydration (`WHERE a.field <op> 'literal'`).

use nodedb_graph::CsrIndex;
use nodedb_types::SurrogateBitmap;
use nodedb_types::value::Value;

use crate::error::LiteError;
use crate::query::document_ops::reads::loro_value_to_ndb_value;

use super::ast::{ComparisonOp, WherePredicate};
use super::executor::{BindingRow, HydrationCtx, execute_clause};

/// Hydrate a single field from the CRDT document bound to `var_name` in `row`.
///
/// Returns `Ok(None)` when the document doesn't exist (row is excluded by the
/// caller), `Ok(Some(value))` on success, and `Err` on lock or missing
/// collection annotation.
fn hydrate_field(
    row: &BindingRow,
    var_name: &str,
    field: &str,
    hydration: Option<&HydrationCtx<'_>>,
) -> Result<Option<Value>, LiteError> {
    let ctx = hydration.ok_or_else(|| LiteError::Storage {
        detail: format!(
            "WHERE clause references node field '{var_name}.{field}' but binding has no \
             collection annotation; declare MATCH (a:Label) where Label is a registered \
             document collection"
        ),
    })?;

    let node_id = match row.get(var_name) {
        Some(id) => id,
        None => return Ok(None),
    };

    let crdt = ctx.crdt.lock().map_err(|_| LiteError::LockPoisoned)?;
    let loro_val = match crdt.read(ctx.collection, node_id) {
        Some(v) => v,
        None => return Ok(None),
    };
    drop(crdt);

    let ndb_val = loro_value_to_ndb_value(&loro_val);
    match ndb_val {
        Value::Object(mut map) => Ok(map.remove(field)),
        _ => Ok(None),
    }
}

/// Compare a hydrated `Value` against a string literal using a `ComparisonOp`.
fn compare_value(val: &Value, op: &ComparisonOp, rhs: &str) -> bool {
    match val {
        Value::Integer(n) => {
            if let Ok(b) = rhs.parse::<i64>() {
                return match op {
                    ComparisonOp::Eq => *n == b,
                    ComparisonOp::Neq => *n != b,
                    ComparisonOp::Lt => *n < b,
                    ComparisonOp::Lte => *n <= b,
                    ComparisonOp::Gt => *n > b,
                    ComparisonOp::Gte => *n >= b,
                };
            }
            if let Ok(b) = rhs.parse::<f64>() {
                let a = *n as f64;
                return match op {
                    ComparisonOp::Eq => (a - b).abs() < f64::EPSILON,
                    ComparisonOp::Neq => (a - b).abs() >= f64::EPSILON,
                    ComparisonOp::Lt => a < b,
                    ComparisonOp::Lte => a <= b,
                    ComparisonOp::Gt => a > b,
                    ComparisonOp::Gte => a >= b,
                };
            }
            false
        }
        Value::Float(a) => {
            if let Ok(b) = rhs.parse::<f64>() {
                return match op {
                    ComparisonOp::Eq => (a - b).abs() < f64::EPSILON,
                    ComparisonOp::Neq => (a - b).abs() >= f64::EPSILON,
                    ComparisonOp::Lt => a < &b,
                    ComparisonOp::Lte => a <= &b,
                    ComparisonOp::Gt => a > &b,
                    ComparisonOp::Gte => a >= &b,
                };
            }
            false
        }
        Value::String(s) => match op {
            ComparisonOp::Eq => s == rhs,
            ComparisonOp::Neq => s != rhs,
            ComparisonOp::Lt => s.as_str() < rhs,
            ComparisonOp::Lte => s.as_str() <= rhs,
            ComparisonOp::Gt => s.as_str() > rhs,
            ComparisonOp::Gte => s.as_str() >= rhs,
        },
        Value::Bool(b) => {
            let rhs_bool = rhs == "true";
            match op {
                ComparisonOp::Eq => *b == rhs_bool,
                ComparisonOp::Neq => *b != rhs_bool,
                _ => false,
            }
        }
        _ => false,
    }
}

/// Compare a bound node-id string against a literal using a `ComparisonOp`,
/// preferring numeric comparison when both sides parse as `f64`.
fn compare_bound_string(bound: &str, op: &ComparisonOp, rhs: &str) -> bool {
    if let (Ok(a), Ok(b)) = (bound.parse::<f64>(), rhs.parse::<f64>()) {
        return match op {
            ComparisonOp::Eq => (a - b).abs() < f64::EPSILON,
            ComparisonOp::Neq => (a - b).abs() >= f64::EPSILON,
            ComparisonOp::Lt => a < b,
            ComparisonOp::Lte => a <= b,
            ComparisonOp::Gt => a > b,
            ComparisonOp::Gte => a >= b,
        };
    }
    match op {
        ComparisonOp::Eq => bound == rhs,
        ComparisonOp::Neq => bound != rhs,
        ComparisonOp::Lt => bound < rhs,
        ComparisonOp::Lte => bound <= rhs,
        ComparisonOp::Gt => bound > rhs,
        ComparisonOp::Gte => bound >= rhs,
    }
}

pub(super) fn apply_predicate(
    rows: Vec<BindingRow>,
    predicate: &WherePredicate,
    csr: &CsrIndex,
    frontier_bitmap: Option<&SurrogateBitmap>,
    hydration: Option<&HydrationCtx<'_>>,
) -> Result<Vec<BindingRow>, LiteError> {
    match predicate {
        WherePredicate::Equals {
            binding,
            field,
            value,
        } => {
            if field.is_empty() || field == binding {
                Ok(rows
                    .into_iter()
                    .filter(|row| row.get(binding).is_some_and(|v| v == value))
                    .collect())
            } else {
                let mut out = Vec::with_capacity(rows.len());
                for row in rows {
                    if let Some(val) = hydrate_field(&row, binding, field, hydration)?
                        && compare_value(&val, &ComparisonOp::Eq, value)
                    {
                        out.push(row);
                    }
                }
                Ok(out)
            }
        }
        WherePredicate::Comparison {
            binding,
            field,
            op,
            value,
        } => {
            if field.is_empty() || field == binding {
                Ok(rows
                    .into_iter()
                    .filter(|row| {
                        row.get(binding)
                            .is_some_and(|bound| compare_bound_string(bound, op, value))
                    })
                    .collect())
            } else {
                let mut out = Vec::with_capacity(rows.len());
                for row in rows {
                    if let Some(val) = hydrate_field(&row, binding, field, hydration)?
                        && compare_value(&val, op, value)
                    {
                        out.push(row);
                    }
                }
                Ok(out)
            }
        }
        WherePredicate::NotExists { sub_pattern } => Ok(rows
            .into_iter()
            .filter(|row| {
                execute_clause(sub_pattern, csr, std::slice::from_ref(row), frontier_bitmap)
                    .is_empty()
            })
            .collect()),
    }
}
