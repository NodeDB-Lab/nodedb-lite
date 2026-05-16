// SPDX-License-Identifier: Apache-2.0

//! Convert `nodedb_sql::types_expr::SqlExpr` to `nodedb_query::expr::types::SqlExpr`
//! for use in sort-key evaluation and other post-processing steps.

use nodedb_query::expr::types::{BinaryOp as QBinaryOp, CastType, SqlExpr as QExpr};
use nodedb_sql::types_expr::{BinaryOp as SBinaryOp, SqlExpr as SExpr, UnaryOp};

use crate::error::LiteError;
use crate::query::filter_convert::sql_value_to_value;

/// Convert a SQL-side expression to a query-side expression.
///
/// Variants that have no meaningful query-side equivalent in a sort context
/// (Subquery, Wildcard, InList, Between, Like, ArrayLiteral) return
/// `LiteError::BadRequest` naming the unsupported variant.
pub(crate) fn convert_sql_expr(expr: &SExpr) -> Result<QExpr, LiteError> {
    match expr {
        SExpr::Column { name, .. } => Ok(QExpr::Column(name.clone())),

        SExpr::Literal(v) => {
            let val = sql_value_to_value(v)?;
            Ok(QExpr::Literal(val))
        }

        SExpr::BinaryOp { left, op, right } => {
            let ql = convert_sql_expr(left)?;
            let qr = convert_sql_expr(right)?;
            let qop = convert_binary_op(*op)?;
            Ok(QExpr::BinaryOp {
                left: Box::new(ql),
                op: qop,
                right: Box::new(qr),
            })
        }

        SExpr::UnaryOp { op, expr } => {
            let inner = convert_sql_expr(expr)?;
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
            let qargs: Result<Vec<QExpr>, LiteError> = args.iter().map(convert_sql_expr).collect();
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
                .map(|e| convert_sql_expr(e).map(Box::new))
                .transpose()?;
            let qwhen: Result<Vec<(QExpr, QExpr)>, LiteError> = when_then
                .iter()
                .map(|(cond, val)| Ok((convert_sql_expr(cond)?, convert_sql_expr(val)?)))
                .collect();
            let qelse = else_expr
                .as_ref()
                .map(|e| convert_sql_expr(e).map(Box::new))
                .transpose()?;
            Ok(QExpr::Case {
                operand: qoperand,
                when_thens: qwhen?,
                else_expr: qelse,
            })
        }

        SExpr::Cast { expr, to_type } => {
            let inner = convert_sql_expr(expr)?;
            let ct = convert_cast_type(to_type)?;
            Ok(QExpr::Cast {
                expr: Box::new(inner),
                to_type: ct,
            })
        }

        SExpr::IsNull { expr, negated } => {
            let inner = convert_sql_expr(expr)?;
            Ok(QExpr::IsNull {
                expr: Box::new(inner),
                negated: *negated,
            })
        }

        SExpr::Subquery(_) => Err(LiteError::BadRequest {
            detail: "Subquery expressions are not valid in a sort context".to_string(),
        }),

        SExpr::Wildcard => Err(LiteError::BadRequest {
            detail: "Wildcard expressions are not valid in a sort context".to_string(),
        }),

        SExpr::InList { .. } => Err(LiteError::BadRequest {
            detail: "InList expressions are not valid in a sort context".to_string(),
        }),

        SExpr::Between { .. } => Err(LiteError::BadRequest {
            detail: "Between expressions are not valid in a sort context".to_string(),
        }),

        SExpr::Like { .. } => Err(LiteError::BadRequest {
            detail: "Like expressions are not valid in a sort context".to_string(),
        }),

        SExpr::ArrayLiteral(_) => Err(LiteError::BadRequest {
            detail: "ArrayLiteral expressions are not valid in a sort context".to_string(),
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
            detail: format!("CAST to type '{other}' is not supported in a sort context"),
        }),
    }
}
