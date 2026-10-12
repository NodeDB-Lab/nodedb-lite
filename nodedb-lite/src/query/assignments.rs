// SPDX-License-Identifier: Apache-2.0
//! `SET` assignment conversion and resolution, shared by every write path
//! that applies assignments to a stored row: `UPDATE`, `UPDATE ... FROM`,
//! `MERGE ... WHEN MATCHED THEN UPDATE`, and `ON CONFLICT DO UPDATE`.
//!
//! A literal right-hand side is pre-encoded as msgpack
//! (`UpdateValue::Literal`). Any other right-hand side converts to an
//! evaluator expression (`UpdateValue::Expr`), as on Origin.
//!
//! Every assignment resolves against the row's pre-image. One assignment
//! never sees the result of another, so `SET a = b, b = a` swaps.

use std::collections::HashMap;

use nodedb_physical::physical_plan::document::types::UpdateValue;
use nodedb_sql::types_expr::SqlExpr;
use nodedb_types::columnar::ColumnDef;
use nodedb_types::value::Value;

use crate::error::LiteError;
use crate::query::coerce::{coerce_sql_value, coerce_value};
use crate::query::expr_convert::convert_sql_expr_with_source;
use crate::query::filter_convert::sql_value_to_value;

/// Convert `(column, expr)` assignments to `(column, UpdateValue)` pairs.
pub(crate) fn convert_assignments(
    assignments: &[(String, SqlExpr)],
) -> Result<Vec<(String, UpdateValue)>, LiteError> {
    convert_assignments_with_source(assignments, &[])
}

/// Convert assignments whose right-hand side can read a joined source row.
///
/// A column qualified by any name in `source_refs` converts to the key
/// `"<source_refs[0]>.<column>"`, the key [`joined_row`] gives that source
/// field. Any other column reads the target row.
pub(crate) fn convert_assignments_with_source(
    assignments: &[(String, SqlExpr)],
    source_refs: &[String],
) -> Result<Vec<(String, UpdateValue)>, LiteError> {
    assignments
        .iter()
        .map(|(col, expr)| Ok((col.clone(), assignment_value(expr, source_refs)?)))
        .collect()
}

/// Convert assignments on a collection with a declared schema. A literal
/// coerces to its column's type up front, so a typed literal (an exact
/// decimal, an instant) keeps its precision. Any other right-hand side
/// evaluates per row.
pub(crate) fn convert_typed_assignments(
    assignments: &[(String, SqlExpr)],
    columns: &[ColumnDef],
) -> Result<Vec<(String, UpdateValue)>, LiteError> {
    assignments
        .iter()
        .map(|(field, expr)| {
            let column = columns.iter().find(|c| c.name == *field);
            let value = match (expr, column) {
                (SqlExpr::Literal(literal), Some(column)) => {
                    literal_update_value(&coerce_sql_value(literal, &column.column_type)?)?
                }
                _ => assignment_value(expr, &[])?,
            };
            Ok((field.clone(), value))
        })
        .collect()
}

fn assignment_value(expr: &SqlExpr, source_refs: &[String]) -> Result<UpdateValue, LiteError> {
    match expr {
        SqlExpr::Literal(v) => literal_update_value(&sql_value_to_value(v)?),
        other => Ok(UpdateValue::Expr(convert_sql_expr_with_source(
            other,
            source_refs,
        )?)),
    }
}

/// Encode `value` as a literal assignment.
fn literal_update_value(value: &Value) -> Result<UpdateValue, LiteError> {
    let bytes = zerompk::to_msgpack_vec(value).map_err(|e| LiteError::Serialization {
        detail: format!("encode assignment value: {e}"),
    })?;
    Ok(UpdateValue::Literal(bytes))
}

/// Whether any assignment needs the row's pre-image to resolve.
pub(crate) fn has_expr(updates: &[(String, UpdateValue)]) -> bool {
    updates
        .iter()
        .any(|(_, update)| matches!(update, UpdateValue::Expr(_)))
}

/// Resolve every assignment against `current_row`, the row's pre-image.
///
/// A literal decodes. An expression evaluates against `current_row`. An
/// evaluation error, such as a division by zero, fails the statement.
pub(crate) fn resolve_updates(
    updates: &[(String, UpdateValue)],
    current_row: &Value,
) -> Result<HashMap<String, Value>, LiteError> {
    resolve(updates, current_row, None)
}

/// [`resolve_updates`] with `EXCLUDED.col` bound to `excluded`, the row an
/// `INSERT ... ON CONFLICT DO UPDATE` proposed.
pub(crate) fn resolve_updates_with_excluded(
    updates: &[(String, UpdateValue)],
    current_row: &Value,
    excluded: &Value,
) -> Result<HashMap<String, Value>, LiteError> {
    resolve(updates, current_row, Some(excluded))
}

fn resolve(
    updates: &[(String, UpdateValue)],
    current_row: &Value,
    excluded: Option<&Value>,
) -> Result<HashMap<String, Value>, LiteError> {
    let mut resolved = HashMap::with_capacity(updates.len());
    for (field, update) in updates {
        let value = match update {
            UpdateValue::Literal(bytes) => {
                zerompk::from_msgpack::<Value>(bytes).map_err(|e| LiteError::Serialization {
                    detail: format!("decode assignment for '{field}': {e}"),
                })?
            }
            UpdateValue::Expr(expr) => match excluded {
                Some(excluded) => expr.eval_with_excluded(current_row, excluded)?,
                None => expr.eval(current_row)?,
            },
        };
        resolved.insert(field.clone(), value);
    }
    Ok(resolved)
}

/// The row a joined assignment evaluates against: the target's fields under
/// their own names, and each source field under `"<source_alias>.<field>"`.
/// Origin's MERGE and `UPDATE ... FROM` evaluate against the same shape.
pub(crate) fn joined_row(
    target: &HashMap<String, Value>,
    source: &HashMap<String, Value>,
    source_alias: &str,
) -> HashMap<String, Value> {
    let mut row = target.clone();
    for (field, value) in source {
        row.insert(format!("{source_alias}.{field}"), value.clone());
    }
    row
}

/// Encode resolved values as literal assignments.
pub(crate) fn literal_updates(
    resolved: HashMap<String, Value>,
) -> Result<Vec<(String, UpdateValue)>, LiteError> {
    resolved
        .into_iter()
        .map(|(field, value)| Ok((field, literal_update_value(&value)?)))
        .collect()
}

/// A strict or columnar row, `values` in schema column order, as an object
/// keyed by column name.
pub(crate) fn row_object(columns: &[ColumnDef], values: &[Value]) -> Value {
    Value::Object(
        columns
            .iter()
            .zip(values)
            .map(|(col, value)| (col.name.clone(), value.clone()))
            .collect(),
    )
}

/// Coerce each resolved value to its column's type, by the rules a literal
/// of the same value takes. A field that names no column passes through, and
/// the engine refuses it.
pub(crate) fn coerce_to_columns(
    resolved: HashMap<String, Value>,
    columns: &[ColumnDef],
) -> Result<HashMap<String, Value>, LiteError> {
    resolved
        .into_iter()
        .map(|(field, value)| {
            let value = match columns.iter().find(|c| c.name == field) {
                Some(col) => coerce_value(value, &col.column_type)?,
                None => value,
            };
            Ok((field, value))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use nodedb_query::expr::SqlExpr as QExpr;
    use nodedb_query::expr::types::BinaryOp;

    use super::*;

    fn row(fields: &[(&str, Value)]) -> Value {
        Value::Object(
            fields
                .iter()
                .map(|(k, v)| ((*k).to_string(), v.clone()))
                .collect(),
        )
    }

    fn column(name: &str) -> QExpr {
        QExpr::Column(name.to_string())
    }

    #[test]
    fn expression_assignment_reads_the_pre_image() {
        let updates = vec![(
            "n".to_string(),
            UpdateValue::Expr(QExpr::BinaryOp {
                left: Box::new(column("n")),
                op: BinaryOp::Add,
                right: Box::new(QExpr::Literal(Value::Integer(1))),
            }),
        )];
        let resolved =
            resolve_updates(&updates, &row(&[("n", Value::Integer(4))])).expect("resolve n + 1");
        assert_eq!(resolved.get("n"), Some(&Value::Integer(5)));
    }

    #[test]
    fn assignments_never_see_each_other() {
        let updates = vec![
            ("a".to_string(), UpdateValue::Expr(column("b"))),
            ("b".to_string(), UpdateValue::Expr(column("a"))),
        ];
        let current = row(&[("a", Value::Integer(1)), ("b", Value::Integer(2))]);
        let resolved = resolve_updates(&updates, &current).expect("resolve swap");
        assert_eq!(resolved.get("a"), Some(&Value::Integer(2)));
        assert_eq!(resolved.get("b"), Some(&Value::Integer(1)));
    }

    #[test]
    fn undecodable_literal_names_its_field() {
        let updates = vec![("n".to_string(), UpdateValue::Literal(vec![0xC1]))];
        let err = resolve_updates(&updates, &row(&[])).expect_err("0xC1 is never valid msgpack");
        assert!(err.to_string().contains("'n'"), "{err}");
    }

    #[test]
    fn division_by_zero_fails_the_assignment() {
        let updates = vec![(
            "n".to_string(),
            UpdateValue::Expr(QExpr::BinaryOp {
                left: Box::new(QExpr::Literal(Value::Integer(1))),
                op: BinaryOp::Div,
                right: Box::new(column("d")),
            }),
        )];
        assert!(resolve_updates(&updates, &row(&[("d", Value::Integer(0))])).is_err());
    }

    #[test]
    fn joined_row_qualifies_source_fields() {
        let target = HashMap::from([("qty".to_string(), Value::Integer(1))]);
        let source = HashMap::from([("qty".to_string(), Value::Integer(5))]);
        let joined = joined_row(&target, &source, "s");
        assert_eq!(joined.get("qty"), Some(&Value::Integer(1)));
        assert_eq!(joined.get("s.qty"), Some(&Value::Integer(5)));
    }
}
