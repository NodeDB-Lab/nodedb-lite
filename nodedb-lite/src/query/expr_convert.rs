// SPDX-License-Identifier: Apache-2.0

//! Convert `nodedb_sql::types_expr::SqlExpr` to `nodedb_query::expr::types::SqlExpr`
//! for WHERE predicates, sort keys, projections and assignments.
//!
//! The evaluator has no `IN`, `BETWEEN`, `LIKE` or array-construct variant.
//! Each is lowered the way Origin's planner lowers it, so an expression
//! evaluates the same on both.

use nodedb_query::expr::types::{BinaryOp as QBinaryOp, CastType, SqlExpr as QExpr};
use nodedb_sql::types_expr::{BinaryOp as SBinaryOp, SqlExpr as SExpr, UnaryOp};

use crate::error::LiteError;
use crate::query::filter_convert::sql_value_to_value;

/// Convert a SQL-side expression to a query-side expression.
///
/// Lowerings, as on Origin:
/// - `e IN (a, b)` is `e = a OR e = b`, and `NOT IN` is `e <> a AND e <> b`.
///   An empty list is `false`, or `true` for `NOT IN`.
/// - `e BETWEEN lo AND hi` is `e >= lo AND e <= hi`, and `NOT BETWEEN` is
///   `e < lo OR e > hi`.
/// - `e LIKE p` calls `like(e, p)`, or `ilike` when case-insensitive.
///   `NOT LIKE` negates the call.
/// - An array of literals is one array literal. An array with any other
///   element calls `make_array`, which builds the array per row.
///
/// A subquery or a wildcard has no evaluator form and is a `BadRequest`.
pub(crate) fn convert_sql_expr(expr: &SExpr) -> Result<QExpr, LiteError> {
    convert(expr, &[])
}

/// [`convert_sql_expr`] for an expression that reads a joined source row.
///
/// A column qualified by any name in `source_refs` converts to
/// `Column("<source_refs[0]>.<name>")`, the key a joined row gives that
/// source field, as Origin's qualified conversion does. Any other qualifier
/// names the target and is dropped.
pub(crate) fn convert_sql_expr_with_source(
    expr: &SExpr,
    source_refs: &[String],
) -> Result<QExpr, LiteError> {
    convert(expr, source_refs)
}

fn convert(expr: &SExpr, source_refs: &[String]) -> Result<QExpr, LiteError> {
    match expr {
        // `EXCLUDED.col` (`INSERT ... ON CONFLICT DO UPDATE`) resolves
        // against the incoming row via `eval_with_excluded`, mirroring
        // Origin's `sql_expr_to_bridge_expr`.
        SExpr::Column { table, name }
            if table
                .as_deref()
                .is_some_and(|t| t.eq_ignore_ascii_case("excluded")) =>
        {
            Ok(QExpr::ExcludedColumn(name.clone()))
        }
        SExpr::Column {
            table: Some(table),
            name,
        } if source_refs.iter().any(|r| r.eq_ignore_ascii_case(table)) => {
            let alias = source_refs.first().map_or(table.as_str(), String::as_str);
            Ok(QExpr::Column(format!("{alias}.{name}")))
        }
        SExpr::Column { name, .. } => Ok(QExpr::Column(name.clone())),

        SExpr::Literal(v) => {
            let val = sql_value_to_value(v)?;
            Ok(QExpr::Literal(val))
        }

        SExpr::BinaryOp { left, op, right } => {
            let ql = convert(left, source_refs)?;
            let qr = convert(right, source_refs)?;
            let qop = convert_binary_op(*op)?;
            Ok(QExpr::BinaryOp {
                left: Box::new(ql),
                op: qop,
                right: Box::new(qr),
            })
        }

        SExpr::UnaryOp { op, expr } => {
            let inner = convert(expr, source_refs)?;
            match op {
                UnaryOp::Neg => Ok(QExpr::Negate(Box::new(inner))),
                UnaryOp::Not => Ok(QExpr::BinaryOp {
                    left: Box::new(inner),
                    op: QBinaryOp::Eq,
                    right: Box::new(QExpr::Literal(nodedb_types::Value::Bool(false))),
                }),
            }
        }

        SExpr::Function { name, args, .. } => {
            let qargs: Result<Vec<QExpr>, LiteError> =
                args.iter().map(|a| convert(a, source_refs)).collect();
            Ok(QExpr::Function {
                name: name.clone(),
                args: qargs?,
            })
        }

        SExpr::Case {
            operand,
            when_then,
            else_expr,
        } => {
            let qoperand = operand
                .as_ref()
                .map(|e| convert(e, source_refs).map(Box::new))
                .transpose()?;
            let qwhen: Result<Vec<(QExpr, QExpr)>, LiteError> = when_then
                .iter()
                .map(|(cond, val)| Ok((convert(cond, source_refs)?, convert(val, source_refs)?)))
                .collect();
            let qelse = else_expr
                .as_ref()
                .map(|e| convert(e, source_refs).map(Box::new))
                .transpose()?;
            Ok(QExpr::Case {
                operand: qoperand,
                when_thens: qwhen?,
                else_expr: qelse,
            })
        }

        SExpr::Cast { expr, to_type } => {
            let inner = convert(expr, source_refs)?;
            let ct = convert_cast_type(to_type)?;
            Ok(QExpr::Cast {
                expr: Box::new(inner),
                to_type: ct,
            })
        }

        SExpr::IsNull { expr, negated } => {
            let inner = convert(expr, source_refs)?;
            Ok(QExpr::IsNull {
                expr: Box::new(inner),
                negated: *negated,
            })
        }

        SExpr::InList {
            expr,
            list,
            negated,
        } => {
            let target = convert(expr, source_refs)?;
            let (eq_op, combine_op) = if *negated {
                (QBinaryOp::NotEq, QBinaryOp::And)
            } else {
                (QBinaryOp::Eq, QBinaryOp::Or)
            };
            let mut lowered: Option<QExpr> = None;
            for item in list {
                let test = QExpr::BinaryOp {
                    left: Box::new(target.clone()),
                    op: eq_op,
                    right: Box::new(convert(item, source_refs)?),
                };
                lowered = Some(match lowered {
                    None => test,
                    Some(acc) => QExpr::BinaryOp {
                        left: Box::new(acc),
                        op: combine_op,
                        right: Box::new(test),
                    },
                });
            }
            Ok(lowered.unwrap_or(QExpr::Literal(nodedb_types::Value::Bool(*negated))))
        }

        SExpr::Between {
            expr,
            low,
            high,
            negated,
        } => {
            let e = convert(expr, source_refs)?;
            let lo = convert(low, source_refs)?;
            let hi = convert(high, source_refs)?;
            let (low_op, high_op, combine_op) = if *negated {
                (QBinaryOp::Lt, QBinaryOp::Gt, QBinaryOp::Or)
            } else {
                (QBinaryOp::GtEq, QBinaryOp::LtEq, QBinaryOp::And)
            };
            Ok(QExpr::BinaryOp {
                left: Box::new(QExpr::BinaryOp {
                    left: Box::new(e.clone()),
                    op: low_op,
                    right: Box::new(lo),
                }),
                op: combine_op,
                right: Box::new(QExpr::BinaryOp {
                    left: Box::new(e),
                    op: high_op,
                    right: Box::new(hi),
                }),
            })
        }

        SExpr::Like {
            expr,
            pattern,
            negated,
            case_insensitive,
        } => {
            let name = if *case_insensitive { "ilike" } else { "like" };
            let call = QExpr::Function {
                name: name.into(),
                args: vec![convert(expr, source_refs)?, convert(pattern, source_refs)?],
            };
            Ok(if *negated {
                QExpr::Negate(Box::new(call))
            } else {
                call
            })
        }

        SExpr::ArrayLiteral(elems) => {
            let lowered = elems
                .iter()
                .map(|e| convert(e, source_refs))
                .collect::<Result<Vec<QExpr>, LiteError>>()?;
            let literals: Option<Vec<nodedb_types::Value>> = lowered
                .iter()
                .map(|elem| {
                    if let QExpr::Literal(v) = elem {
                        Some(v.clone())
                    } else {
                        None
                    }
                })
                .collect();
            Ok(match literals {
                Some(values) => QExpr::Literal(nodedb_types::Value::Array(values)),
                None => QExpr::Function {
                    name: "make_array".into(),
                    args: lowered,
                },
            })
        }

        SExpr::Subquery(_) => Err(LiteError::BadRequest {
            detail: "a subquery has no expression form here".to_string(),
        }),

        SExpr::Wildcard => Err(LiteError::BadRequest {
            detail: "a wildcard has no expression form here".to_string(),
        }),
    }
}

fn convert_binary_op(op: SBinaryOp) -> Result<QBinaryOp, LiteError> {
    Ok(match op {
        SBinaryOp::Add => QBinaryOp::Add,
        SBinaryOp::Sub => QBinaryOp::Sub,
        SBinaryOp::Mul => QBinaryOp::Mul,
        SBinaryOp::Div => QBinaryOp::Div,
        SBinaryOp::Mod => QBinaryOp::Mod,
        SBinaryOp::Eq => QBinaryOp::Eq,
        SBinaryOp::Ne => QBinaryOp::NotEq,
        SBinaryOp::Gt => QBinaryOp::Gt,
        SBinaryOp::Ge => QBinaryOp::GtEq,
        SBinaryOp::Lt => QBinaryOp::Lt,
        SBinaryOp::Le => QBinaryOp::LtEq,
        SBinaryOp::And => QBinaryOp::And,
        SBinaryOp::Or => QBinaryOp::Or,
        SBinaryOp::Concat => QBinaryOp::Concat,
    })
}

fn convert_cast_type(to_type: &str) -> Result<CastType, LiteError> {
    match to_type.to_uppercase().as_str() {
        "INT" | "INT64" | "INTEGER" | "BIGINT" => Ok(CastType::Int),
        "FLOAT" | "FLOAT64" | "DOUBLE" | "REAL" | "NUMERIC" | "DECIMAL" => Ok(CastType::Float),
        "TEXT" | "STRING" | "VARCHAR" | "CHAR" => Ok(CastType::String),
        "BOOL" | "BOOLEAN" => Ok(CastType::Bool),
        other => Err(LiteError::BadRequest {
            detail: format!("CAST to type '{other}' is not supported"),
        }),
    }
}

#[cfg(test)]
mod tests {
    use nodedb_sql::types_expr::SqlValue;
    use nodedb_types::Value;

    use super::*;

    fn col(name: &str) -> SExpr {
        SExpr::Column {
            table: None,
            name: name.into(),
        }
    }

    fn lit(v: i64) -> SExpr {
        SExpr::Literal(SqlValue::Int(v))
    }

    fn eval(expr: &SExpr, n: i64) -> Value {
        let doc = Value::Object(std::collections::HashMap::from([
            ("n".to_string(), Value::Integer(n)),
            ("s".to_string(), Value::String("alpha".into())),
        ]));
        convert_sql_expr(expr)
            .expect("lower")
            .eval(&doc)
            .expect("eval")
    }

    #[test]
    fn in_lowers_to_an_or_of_equalities() {
        let expr = SExpr::InList {
            expr: Box::new(col("n")),
            list: vec![lit(1), lit(3)],
            negated: false,
        };
        assert_eq!(eval(&expr, 3), Value::Bool(true));
        assert_eq!(eval(&expr, 2), Value::Bool(false));
    }

    #[test]
    fn not_in_and_an_empty_list_follow_origin() {
        let not_in = SExpr::InList {
            expr: Box::new(col("n")),
            list: vec![lit(1)],
            negated: true,
        };
        assert_eq!(eval(&not_in, 2), Value::Bool(true));
        let empty = SExpr::InList {
            expr: Box::new(col("n")),
            list: Vec::new(),
            negated: false,
        };
        assert_eq!(eval(&empty, 1), Value::Bool(false));
    }

    #[test]
    fn like_and_not_like_lower_to_the_like_function() {
        let like = |negated, case_insensitive, pattern: &str| SExpr::Like {
            expr: Box::new(col("s")),
            pattern: Box::new(SExpr::Literal(SqlValue::String(pattern.into()))),
            negated,
            case_insensitive,
        };
        assert_eq!(eval(&like(false, false, "al%"), 0), Value::Bool(true));
        assert_eq!(eval(&like(true, false, "al%"), 0), Value::Bool(false));
        assert_eq!(eval(&like(false, true, "AL%"), 0), Value::Bool(true));
        assert_eq!(eval(&like(false, false, "AL%"), 0), Value::Bool(false));
    }

    #[test]
    fn an_array_with_a_column_element_is_built_per_row() {
        let expr = SExpr::ArrayLiteral(vec![col("n"), lit(1)]);
        assert_eq!(
            eval(&expr, 7),
            Value::Array(vec![Value::Integer(7), Value::Integer(1)])
        );
        let literal = SExpr::ArrayLiteral(vec![lit(1), lit(2)]);
        assert_eq!(
            convert_sql_expr(&literal).expect("lower"),
            QExpr::Literal(Value::Array(vec![Value::Integer(1), Value::Integer(2)]))
        );
    }

    #[test]
    fn source_qualified_column_reads_the_joined_source_key() {
        let qualified = |table: &str| SExpr::Column {
            table: Some(table.into()),
            name: "qty".into(),
        };
        let refs = vec!["s".to_string(), "stock".to_string()];
        assert_eq!(
            convert_sql_expr_with_source(&qualified("stock"), &refs).expect("lower"),
            QExpr::Column("s.qty".into())
        );
        assert_eq!(
            convert_sql_expr_with_source(&qualified("t"), &refs).expect("lower"),
            QExpr::Column("qty".into())
        );
        assert_eq!(
            convert_sql_expr(&qualified("s")).expect("lower"),
            QExpr::Column("qty".into())
        );
    }

    #[test]
    fn between_is_inclusive() {
        let expr = SExpr::Between {
            expr: Box::new(col("n")),
            low: Box::new(lit(2)),
            high: Box::new(lit(4)),
            negated: false,
        };
        assert_eq!(eval(&expr, 2), Value::Bool(true));
        assert_eq!(eval(&expr, 4), Value::Bool(true));
        assert_eq!(eval(&expr, 5), Value::Bool(false));
    }
}
