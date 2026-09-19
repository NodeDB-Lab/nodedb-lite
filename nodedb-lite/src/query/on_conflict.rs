// SPDX-License-Identifier: Apache-2.0
//! `ON CONFLICT DO UPDATE SET` assignment application, shared by every
//! write path that merges an incoming row onto a stored one: KV
//! (`kv_ops::writes::basic::kv_insert_on_conflict_update`) and vector-primary
//! direct writes (`physical_visitor::vector_direct`).

use std::collections::HashMap;

use nodedb_physical::physical_plan::UpdateValue;
use nodedb_query::expr::SqlExpr as QExpr;
use nodedb_types::value::Value;

use crate::error::LiteError;

/// Apply `patch` to `row`. A `Literal` decodes to its value; an `Expr`
/// evaluates against the stored row with `excluded` bound to the incoming
/// row, which is what `EXCLUDED.col` in an `ON CONFLICT` clause refers to.
pub(crate) fn apply_patch(
    row: &mut HashMap<String, Value>,
    patch: &[(String, UpdateValue)],
    excluded: &HashMap<String, Value>,
) -> Result<(), LiteError> {
    if patch.is_empty() {
        return Ok(());
    }
    let stored = Value::Object(row.clone());
    let excluded = Value::Object(excluded.clone());
    for (field, update) in patch {
        let value = match update {
            UpdateValue::Literal(bytes) => {
                zerompk::from_msgpack::<Value>(bytes).map_err(|e| LiteError::Serialization {
                    detail: format!("decode assignment '{field}': {e}"),
                })?
            }
            UpdateValue::Expr(expr) => eval_assignment(expr, &stored, &excluded)?,
        };
        row.insert(field.clone(), value);
    }
    Ok(())
}

fn eval_assignment(expr: &QExpr, stored: &Value, excluded: &Value) -> Result<Value, LiteError> {
    Ok(expr.eval_with_excluded(stored, excluded)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn literal_assignment_overwrites_field() {
        let mut row = HashMap::from([("n".to_string(), Value::Integer(1))]);
        let excluded = HashMap::from([("n".to_string(), Value::Integer(9))]);
        let bytes = zerompk::to_msgpack_vec(&Value::Integer(5)).expect("encode literal");
        let patch = vec![("n".to_string(), UpdateValue::Literal(bytes))];
        apply_patch(&mut row, &patch, &excluded).expect("apply literal");
        assert_eq!(row.get("n"), Some(&Value::Integer(5)));
    }

    #[test]
    fn expr_assignment_evaluates_against_stored_and_excluded() {
        let mut row = HashMap::from([("n".to_string(), Value::Integer(1))]);
        let excluded = HashMap::from([("n".to_string(), Value::Integer(9))]);
        let expr = QExpr::BinaryOp {
            left: Box::new(QExpr::Column("n".to_string())),
            op: nodedb_query::expr::types::BinaryOp::Add,
            right: Box::new(QExpr::Literal(Value::Integer(1))),
        };
        let patch = vec![("n".to_string(), UpdateValue::Expr(expr))];
        apply_patch(&mut row, &patch, &excluded).expect("apply expr");
        assert_eq!(row.get("n"), Some(&Value::Integer(2)));
    }

    #[test]
    fn excluded_column_resolves_to_incoming_row() {
        let mut row = HashMap::from([("n".to_string(), Value::Integer(1))]);
        let excluded = HashMap::from([("n".to_string(), Value::Integer(9))]);
        let expr = QExpr::ExcludedColumn("n".to_string());
        let patch = vec![("n".to_string(), UpdateValue::Expr(expr))];
        apply_patch(&mut row, &patch, &excluded).expect("apply excluded ref");
        assert_eq!(row.get("n"), Some(&Value::Integer(9)));
    }
}
