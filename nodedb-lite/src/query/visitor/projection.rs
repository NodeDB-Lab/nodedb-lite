// SPDX-License-Identifier: Apache-2.0

//! Target-list projection for a materialized scan result.
//!
//! A scan returns every stored column. This step reshapes each row to the
//! SELECT list: named columns are picked, `Computed` expressions are
//! evaluated per row, and `CpComputed` expressions first resolve their
//! sequence accessors (`nextval` / `currval` / `setval`) against the engine's
//! registry. Rows run in slice order and items in SELECT order, so a later
//! item reads an earlier alias exactly as SQL left-to-right evaluation does.

use nodedb_physical::physical_plan::query::JoinProjection;
use nodedb_query::expr::types::SqlExpr as QExpr;
use nodedb_sql::types::query::Projection;
use nodedb_types::result::QueryResult;
use nodedb_types::value::Value;

use crate::error::LiteError;
use crate::query::expr_convert::convert_sql_expr;
use crate::query::visitor::scan_post::row_to_typed_value;
use crate::sequence::LiteSequenceRegistry;

/// The output names of a target list for a path whose executor reshapes
/// rows by name (join, lateral, spatial, timeseries). A named column keeps
/// its name and a `Computed` item its alias; a star is inherited from the
/// source rows. A `CpComputed` item needs per-row sequence evaluation, which
/// only the scan path runs, so it is refused here.
pub(super) fn projection_names(
    projection: &[Projection],
    context: &str,
) -> Result<Vec<String>, LiteError> {
    let mut names = Vec::with_capacity(projection.len());
    for p in projection {
        match p {
            Projection::Column(name) => names.push(name.clone()),
            Projection::Computed { alias, .. } => names.push(alias.clone()),
            Projection::Star | Projection::QualifiedStar(_) => {}
            Projection::CpComputed { alias, .. } => {
                return Err(LiteError::BadRequest {
                    detail: format!(
                        "sequence accessor '{alias}' in a {context} target list is not \
                         supported on the Lite engine; select it from a plain scan"
                    ),
                });
            }
        }
    }
    Ok(names)
}

/// [`projection_names`] as same-named join projections.
pub(super) fn join_projections(
    projection: &[Projection],
    context: &str,
) -> Result<Vec<JoinProjection>, LiteError> {
    Ok(projection_names(projection, context)?
        .into_iter()
        .map(|name| JoinProjection {
            source: name.clone(),
            output: name,
        })
        .collect())
}

/// One SELECT-list item after conversion to the query-side expression form.
enum Item {
    /// A named column, with its qualifier stripped.
    Column { name: String, bare: String },
    /// Every scan column, in scan order.
    Star,
    /// An expression evaluated against the row document.
    Expr { expr: QExpr, alias: String },
    /// An expression whose sequence accessors resolve before evaluation.
    SequenceExpr { expr: QExpr, alias: String },
}

/// Reshape `result` to `projection`. An empty list or a bare `*` leaves the
/// scan shape untouched.
pub(crate) fn project_scan_result(
    result: &mut QueryResult,
    projection: &[Projection],
    sequences: &LiteSequenceRegistry,
) -> Result<(), LiteError> {
    if projection.is_empty()
        || projection
            .iter()
            .all(|p| matches!(p, Projection::Star | Projection::QualifiedStar(_)))
    {
        return Ok(());
    }
    let items = convert_items(projection)?;
    let scan_columns = std::mem::take(&mut result.columns);

    let mut out_columns = Vec::with_capacity(items.len());
    for item in &items {
        match item {
            Item::Column { bare, .. } => out_columns.push(bare.clone()),
            Item::Star => out_columns.extend(scan_columns.iter().cloned()),
            Item::Expr { alias, .. } | Item::SequenceExpr { alias, .. } => {
                out_columns.push(alias.clone())
            }
        }
    }

    let mut out_rows = Vec::with_capacity(result.rows.len());
    for row in std::mem::take(&mut result.rows) {
        let mut doc = row_to_typed_value(&scan_columns, &row);
        let mut out = Vec::with_capacity(out_columns.len());
        for item in &items {
            match item {
                Item::Column { name, bare } => {
                    out.push(column_value(&scan_columns, &row, &doc, name, bare));
                }
                Item::Star => out.extend(row.iter().cloned()),
                Item::Expr { expr, alias } => {
                    let value = expr.eval(&doc)?;
                    stamp_alias(&mut doc, alias, &value);
                    out.push(value);
                }
                Item::SequenceExpr { expr, alias } => {
                    let resolved = resolve_accessors(expr, &doc, sequences)?;
                    let value = resolved.eval(&doc)?;
                    stamp_alias(&mut doc, alias, &value);
                    out.push(value);
                }
            }
        }
        out_rows.push(out);
    }

    result.columns = out_columns;
    result.rows = out_rows;
    Ok(())
}

fn convert_items(projection: &[Projection]) -> Result<Vec<Item>, LiteError> {
    let mut items = Vec::with_capacity(projection.len());
    for p in projection {
        items.push(match p {
            Projection::Column(name) => Item::Column {
                name: name.clone(),
                bare: name.rsplit('.').next().unwrap_or(name).to_string(),
            },
            Projection::Star | Projection::QualifiedStar(_) => Item::Star,
            Projection::Computed { expr, alias } => Item::Expr {
                expr: convert_sql_expr(expr)?,
                alias: alias.clone(),
            },
            Projection::CpComputed { expr, alias } => Item::SequenceExpr {
                expr: convert_sql_expr(expr)?,
                alias: alias.clone(),
            },
        });
    }
    Ok(items)
}

/// The value a column reference yields: a scan column by its exact name
/// first, then a document field by the full or the bare name.
fn column_value(
    scan_columns: &[String],
    row: &[Value],
    doc: &Value,
    name: &str,
    bare: &str,
) -> Value {
    if let Some(idx) = scan_columns.iter().position(|c| c == name || c == bare)
        && let Some(v) = row.get(idx)
    {
        return v.clone();
    }
    let Value::Object(fields) = doc else {
        return Value::Null;
    };
    fields
        .get(name)
        .or_else(|| fields.get(bare))
        .cloned()
        .unwrap_or(Value::Null)
}

fn stamp_alias(doc: &mut Value, alias: &str, value: &Value) {
    if let Value::Object(fields) = doc {
        fields.insert(alias.to_string(), value.clone());
    }
}

/// Replace every sequence accessor call in `expr` with the value the
/// registry hands back for the current row. Arguments resolve before the
/// call that holds them, and sibling calls run left to right.
fn resolve_accessors(
    expr: &QExpr,
    doc: &Value,
    sequences: &LiteSequenceRegistry,
) -> Result<QExpr, LiteError> {
    match expr {
        QExpr::Column(_) | QExpr::Literal(_) | QExpr::OldColumn(_) | QExpr::ExcludedColumn(_) => {
            Ok(expr.clone())
        }
        QExpr::BinaryOp { left, op, right } => Ok(QExpr::BinaryOp {
            left: Box::new(resolve_accessors(left, doc, sequences)?),
            op: *op,
            right: Box::new(resolve_accessors(right, doc, sequences)?),
        }),
        QExpr::Negate(inner) => Ok(QExpr::Negate(Box::new(resolve_accessors(
            inner, doc, sequences,
        )?))),
        QExpr::Cast { expr, to_type } => Ok(QExpr::Cast {
            expr: Box::new(resolve_accessors(expr, doc, sequences)?),
            to_type: to_type.clone(),
        }),
        QExpr::Case {
            operand,
            when_thens,
            else_expr,
        } => Ok(QExpr::Case {
            operand: resolve_boxed(operand.as_deref(), doc, sequences)?,
            when_thens: when_thens
                .iter()
                .map(|(when, then)| {
                    Ok((
                        resolve_accessors(when, doc, sequences)?,
                        resolve_accessors(then, doc, sequences)?,
                    ))
                })
                .collect::<Result<Vec<_>, LiteError>>()?,
            else_expr: resolve_boxed(else_expr.as_deref(), doc, sequences)?,
        }),
        QExpr::Coalesce(items) => Ok(QExpr::Coalesce(resolve_list(items, doc, sequences)?)),
        QExpr::NullIf(left, right) => Ok(QExpr::NullIf(
            Box::new(resolve_accessors(left, doc, sequences)?),
            Box::new(resolve_accessors(right, doc, sequences)?),
        )),
        QExpr::IsNull { expr, negated } => Ok(QExpr::IsNull {
            expr: Box::new(resolve_accessors(expr, doc, sequences)?),
            negated: *negated,
        }),
        QExpr::Function { name, args } => {
            let args = resolve_list(args, doc, sequences)?;
            let lowered = name.to_ascii_lowercase();
            match lowered.as_str() {
                "nextval" => {
                    let sequence = sequence_name(&lowered, &args, doc)?;
                    Ok(QExpr::Literal(Value::Integer(
                        sequences.nextval(&sequence)?,
                    )))
                }
                "currval" => {
                    let sequence = sequence_name(&lowered, &args, doc)?;
                    Ok(QExpr::Literal(Value::Integer(
                        sequences.currval(&sequence)?,
                    )))
                }
                "setval" => {
                    let (sequence, value) = setval_args(&args, doc)?;
                    Ok(QExpr::Literal(Value::Integer(
                        sequences.setval(&sequence, value)?,
                    )))
                }
                _ => Ok(QExpr::Function {
                    name: name.clone(),
                    args,
                }),
            }
        }
    }
}

fn resolve_boxed(
    expr: Option<&QExpr>,
    doc: &Value,
    sequences: &LiteSequenceRegistry,
) -> Result<Option<Box<QExpr>>, LiteError> {
    expr.map(|e| resolve_accessors(e, doc, sequences).map(Box::new))
        .transpose()
}

fn resolve_list(
    items: &[QExpr],
    doc: &Value,
    sequences: &LiteSequenceRegistry,
) -> Result<Vec<QExpr>, LiteError> {
    items
        .iter()
        .map(|item| resolve_accessors(item, doc, sequences))
        .collect()
}

/// The single sequence-name argument of `nextval` / `currval`.
fn sequence_name(function: &str, args: &[QExpr], doc: &Value) -> Result<String, LiteError> {
    let [arg] = args else {
        return Err(LiteError::BadRequest {
            detail: format!(
                "{function}() takes exactly one argument, the sequence name; got {}",
                args.len()
            ),
        });
    };
    match arg.eval(doc)? {
        Value::String(name) => Ok(name),
        other => Err(LiteError::BadRequest {
            detail: format!("{function}() sequence name must be text; got {other:?}"),
        }),
    }
}

/// The `(name, value)` arguments of `setval`.
fn setval_args(args: &[QExpr], doc: &Value) -> Result<(String, i64), LiteError> {
    let [name_arg, value_arg] = args else {
        return Err(LiteError::BadRequest {
            detail: format!(
                "setval() takes exactly two arguments, the sequence name and the value; got {}",
                args.len()
            ),
        });
    };
    let name = match name_arg.eval(doc)? {
        Value::String(name) => name,
        other => {
            return Err(LiteError::BadRequest {
                detail: format!("setval() sequence name must be text; got {other:?}"),
            });
        }
    };
    let value = match value_arg.eval(doc)? {
        Value::Integer(v) => v,
        other => {
            return Err(LiteError::BadRequest {
                detail: format!("setval() value must be an integer; got {other:?}"),
            });
        }
    };
    Ok((name, value))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sequence::LiteSequenceDef;
    use nodedb_sql::types_expr::SqlExpr as SExpr;
    use nodedb_sql::types_expr::{BinaryOp, SqlValue};

    fn registry_with(name: &str) -> LiteSequenceRegistry {
        let reg = LiteSequenceRegistry::new();
        reg.register(LiteSequenceDef {
            name: name.to_string(),
            start_value: 1,
            increment: 1,
            min_value: 1,
            max_value: i64::MAX,
            cycle: false,
        });
        reg
    }

    fn strict_result() -> QueryResult {
        QueryResult {
            columns: vec!["id".into(), "price".into(), "qty".into()],
            rows: vec![
                vec![Value::Integer(1), Value::Integer(10), Value::Integer(2)],
                vec![Value::Integer(2), Value::Integer(5), Value::Integer(3)],
            ],
            rows_affected: 0,
            command: None,
        }
    }

    fn col(name: &str) -> SExpr {
        SExpr::Column {
            table: None,
            name: name.to_string(),
        }
    }

    fn accessor(name: &str, sequence: &str) -> SExpr {
        SExpr::Function {
            name: name.to_string(),
            args: vec![SExpr::Literal(SqlValue::String(sequence.to_string()))],
            distinct: false,
        }
    }

    #[test]
    fn computed_projection_is_evaluated_per_row() {
        let mut result = strict_result();
        let projection = vec![
            Projection::Column("id".into()),
            Projection::Computed {
                expr: SExpr::BinaryOp {
                    left: Box::new(col("price")),
                    op: BinaryOp::Mul,
                    right: Box::new(col("qty")),
                },
                alias: "total".into(),
            },
        ];
        project_scan_result(&mut result, &projection, &LiteSequenceRegistry::new())
            .expect("project");
        assert_eq!(result.columns, vec!["id".to_string(), "total".to_string()]);
        assert_eq!(result.rows[0], vec![Value::Integer(1), Value::Integer(20)]);
        assert_eq!(result.rows[1], vec![Value::Integer(2), Value::Integer(15)]);
    }

    #[test]
    fn schemaless_document_fields_project_by_name() {
        let mut result = QueryResult {
            columns: vec!["id".into(), "document".into()],
            rows: vec![vec![
                Value::String("d1".into()),
                Value::String(r#"{"name":"Ann","age":30}"#.into()),
            ]],
            rows_affected: 0,
            command: None,
        };
        let projection = vec![
            Projection::Column("t.name".into()),
            Projection::Column("id".into()),
        ];
        project_scan_result(&mut result, &projection, &LiteSequenceRegistry::new())
            .expect("project");
        assert_eq!(result.columns, vec!["name".to_string(), "id".to_string()]);
        assert_eq!(
            result.rows[0],
            vec![Value::String("Ann".into()), Value::String("d1".into())]
        );
    }

    #[test]
    fn nextval_advances_once_per_row_in_order() {
        let mut result = strict_result();
        let projection = vec![
            Projection::CpComputed {
                expr: accessor("nextval", "s"),
                alias: "n".into(),
            },
            Projection::Column("id".into()),
        ];
        project_scan_result(&mut result, &projection, &registry_with("s")).expect("project");
        assert_eq!(result.columns, vec!["n".to_string(), "id".to_string()]);
        assert_eq!(result.rows[0], vec![Value::Integer(1), Value::Integer(1)]);
        assert_eq!(result.rows[1], vec![Value::Integer(2), Value::Integer(2)]);
    }

    #[test]
    fn currval_reads_the_same_rows_nextval() {
        let mut result = strict_result();
        let projection = vec![
            Projection::CpComputed {
                expr: accessor("nextval", "s"),
                alias: "n".into(),
            },
            Projection::CpComputed {
                expr: accessor("currval", "s"),
                alias: "c".into(),
            },
        ];
        project_scan_result(&mut result, &projection, &registry_with("s")).expect("project");
        assert_eq!(result.rows[1], vec![Value::Integer(2), Value::Integer(2)]);
    }

    #[test]
    fn unknown_sequence_is_an_error() {
        let mut result = strict_result();
        let projection = vec![Projection::CpComputed {
            expr: accessor("nextval", "missing"),
            alias: "n".into(),
        }];
        let err = project_scan_result(&mut result, &projection, &LiteSequenceRegistry::new())
            .expect_err("unknown sequence");
        assert!(err.to_string().contains("does not exist"), "{err}");
    }

    #[test]
    fn star_alone_keeps_scan_shape() {
        let mut result = strict_result();
        project_scan_result(
            &mut result,
            &[Projection::Star],
            &LiteSequenceRegistry::new(),
        )
        .expect("project");
        assert_eq!(result.columns.len(), 3);
    }
}
