// SPDX-License-Identifier: Apache-2.0

//! Convert SQL filter types into `LiteFilter` for scan post-filtering.
//!
//! Both `Filter` (WHERE-clause AST) and `SqlPayloadAtom` (payload bitmap
//! predicates) are lowered into a `LiteFilter` that combines:
//! - `meta`: primitive `MetadataFilter` conditions (serializable, pushed to
//!   the physical visitor for pre-filtering)
//! - `exprs`: complex `QExpr` predicates (functions, arithmetic, IS NULL on
//!   expressions) evaluated row-by-row in post-scan

use nodedb_query::expr::types::SqlExpr as QExpr;
use nodedb_query::value_ops::is_truthy;
use nodedb_sql::types::SqlValue;
use nodedb_sql::types::filter::{CompareOp, Filter, FilterExpr};
use nodedb_sql::types_expr::SqlPayloadAtom;
use nodedb_types::filter::MetadataFilter;
use nodedb_types::value::Value;

use crate::error::LiteError;
use crate::query::expr_convert::convert_sql_expr;

/// Combined filter result: primitive `MetadataFilter` conditions plus
/// zero or more `QExpr` predicates that must be evaluated against the
/// full row after the physical scan returns.
///
/// The `meta` part is serializable and can be pushed down to the physical
/// visitor. The `exprs` part requires a row value to evaluate and is always
/// applied at the post-scan layer.
pub(crate) struct LiteFilter {
    /// Primitive equality / range / in-list conditions. `None` means no
    /// primitive filter — every row passes the primitive stage.
    pub meta: Option<MetadataFilter>,
    /// Complex SQL expression predicates (functions, arithmetic, IS NULL on
    /// computed values). Applied row-by-row; a row is kept only when every
    /// expression evaluates to a truthy value.
    pub exprs: Vec<QExpr>,
}

impl LiteFilter {
    /// Returns `true` when there is nothing to filter (no primitive filter and
    /// no expression predicates).
    pub fn is_empty(&self) -> bool {
        self.meta.is_none() && self.exprs.is_empty()
    }

    /// Evaluate the expression predicates against a typed `Value` row document.
    ///
    /// Returns `true` when all expression predicates are satisfied. Always
    /// `true` when `exprs` is empty.
    pub fn eval_exprs(&self, doc: &Value) -> bool {
        self.exprs.iter().all(|e| is_truthy(&e.eval(doc)))
    }
}

/// Convert SQL WHERE filters and payload atoms into a `LiteFilter`.
///
/// Primitive conditions (`Comparison`, `InList`, `Between`, `IsNull`,
/// `IsNotNull`, `And`, `Or`, `Not`) are lowered into the `meta` field.
/// Complex `Expr(SqlExpr)` predicates (functions, arithmetic, CASE, CAST …)
/// are converted to `QExpr` and stored in `exprs` for row-by-row evaluation.
///
/// The only remaining `Err` paths are genuine client input errors:
/// - `Range` payload atom with no bounds (malformed request)
/// - Decimal literals that do not fit in `f64` (malformed request)
/// - `SqlExpr` shapes that have no post-scan equivalent (`Subquery`,
///   `Wildcard`, `InList`, `Between`, `Like`, `ArrayLiteral` — all of which
///   are disallowed in a predicate context by the SQL planner before Lite
///   receives the plan)
pub(crate) fn sql_filters_to_metadata(
    filters: &[Filter],
    payload_filters: &[SqlPayloadAtom],
) -> Result<LiteFilter, LiteError> {
    if filters.is_empty() && payload_filters.is_empty() {
        return Ok(LiteFilter {
            meta: None,
            exprs: vec![],
        });
    }

    let mut meta_parts: Vec<MetadataFilter> = Vec::new();
    let mut exprs: Vec<QExpr> = Vec::new();

    for f in filters {
        convert_filter(f, &mut meta_parts, &mut exprs)?;
    }

    for atom in payload_filters {
        meta_parts.push(convert_payload_atom(atom)?);
    }

    let meta = match meta_parts.len() {
        0 => None,
        1 => Some(meta_parts.remove(0)),
        _ => Some(MetadataFilter::And(meta_parts)),
    };

    Ok(LiteFilter { meta, exprs })
}

/// Recursively convert a `Filter` into either a primitive `MetadataFilter`
/// (pushed into `meta_parts`) or a `QExpr` predicate (pushed into `exprs`).
///
/// The function drives the "primitive vs. expression" split:
/// - Simple field predicates → `meta_parts`
/// - `Expr(SqlExpr)` → converted to `QExpr` → `exprs`
/// - `And` / `Or` / `Not` with mixed children → the whole subtree becomes
///   a `QExpr` via expression conversion so that truth values compose
///   correctly across primitive and non-primitive children.
fn convert_filter(
    f: &Filter,
    meta_parts: &mut Vec<MetadataFilter>,
    exprs: &mut Vec<QExpr>,
) -> Result<(), LiteError> {
    match try_convert_filter_to_meta(f) {
        Ok(mf) => {
            meta_parts.push(mf);
        }
        Err(_) => {
            // Fall through: lower the whole filter as a `QExpr` predicate.
            let qexpr = filter_to_qexpr(f)?;
            exprs.push(qexpr);
        }
    }
    Ok(())
}

/// Attempt to lower a `Filter` to a primitive `MetadataFilter` without any
/// complex sub-expressions.  Returns `Err(())` when the filter contains
/// `Expr(SqlExpr)` at any depth, signalling that the whole tree must be
/// evaluated as a `QExpr`.
fn try_convert_filter_to_meta(f: &Filter) -> Result<MetadataFilter, ()> {
    match &f.expr {
        FilterExpr::Comparison { field, op, value } => {
            let v = sql_value_to_value(value).map_err(|_| ())?;
            Ok(match op {
                CompareOp::Eq => MetadataFilter::Eq {
                    field: field.clone(),
                    value: v,
                },
                CompareOp::Ne => MetadataFilter::Ne {
                    field: field.clone(),
                    value: v,
                },
                CompareOp::Gt => MetadataFilter::Gt {
                    field: field.clone(),
                    value: v,
                },
                CompareOp::Ge => MetadataFilter::Gte {
                    field: field.clone(),
                    value: v,
                },
                CompareOp::Lt => MetadataFilter::Lt {
                    field: field.clone(),
                    value: v,
                },
                CompareOp::Le => MetadataFilter::Lte {
                    field: field.clone(),
                    value: v,
                },
            })
        }
        FilterExpr::InList { field, values } => {
            let vs: Result<Vec<Value>, _> = values
                .iter()
                .map(|v| sql_value_to_value(v).map_err(|_| ()))
                .collect();
            Ok(MetadataFilter::In {
                field: field.clone(),
                values: vs?,
            })
        }
        FilterExpr::Between { field, low, high } => {
            let lo = sql_value_to_value(low).map_err(|_| ())?;
            let hi = sql_value_to_value(high).map_err(|_| ())?;
            Ok(MetadataFilter::And(vec![
                MetadataFilter::Gte {
                    field: field.clone(),
                    value: lo,
                },
                MetadataFilter::Lte {
                    field: field.clone(),
                    value: hi,
                },
            ]))
        }
        FilterExpr::IsNull { field } => Ok(MetadataFilter::Eq {
            field: field.clone(),
            value: Value::Null,
        }),
        FilterExpr::IsNotNull { field } => Ok(MetadataFilter::Ne {
            field: field.clone(),
            value: Value::Null,
        }),
        FilterExpr::And(sub) => {
            let parts: Result<Vec<MetadataFilter>, ()> =
                sub.iter().map(try_convert_filter_to_meta).collect();
            Ok(MetadataFilter::And(parts?))
        }
        FilterExpr::Or(sub) => {
            let parts: Result<Vec<MetadataFilter>, ()> =
                sub.iter().map(try_convert_filter_to_meta).collect();
            Ok(MetadataFilter::Or(parts?))
        }
        FilterExpr::Not(inner) => Ok(MetadataFilter::Not(Box::new(try_convert_filter_to_meta(
            inner,
        )?))),
        // Complex expression: cannot be lowered to a primitive MetadataFilter.
        FilterExpr::Expr(_) => Err(()),
    }
}

/// Combine a list of `QExpr` arms into a single expression by folding with
/// the given binary op. Returns `identity` when `arms` is empty (used for the
/// short-circuit values of empty AND = true, empty OR = false, empty IN = false).
fn combine_arms(
    arms: Vec<QExpr>,
    op: nodedb_query::expr::types::BinaryOp,
    identity: bool,
) -> QExpr {
    let mut iter = arms.into_iter();
    let Some(first) = iter.next() else {
        return QExpr::Literal(Value::Bool(identity));
    };
    iter.fold(first, |a, b| QExpr::BinaryOp {
        left: Box::new(a),
        op,
        right: Box::new(b),
    })
}

/// Convert a `Filter` to a `QExpr` predicate for row-by-row evaluation.
///
/// This is the fallback path for predicates that contain `Expr(SqlExpr)`.
/// `FilterExpr::Comparison` and similar primitives are lowered to the
/// equivalent `QExpr::BinaryOp` so that `And`/`Or`/`Not` subtrees that mix
/// primitives and expressions work correctly.
fn filter_to_qexpr(f: &Filter) -> Result<QExpr, LiteError> {
    use nodedb_query::expr::types::BinaryOp;
    match &f.expr {
        FilterExpr::Comparison { field, op, value } => {
            let left = QExpr::Column(field.clone());
            let right = QExpr::Literal(sql_value_to_value(value)?);
            let qop = match op {
                CompareOp::Eq => BinaryOp::Eq,
                CompareOp::Ne => BinaryOp::NotEq,
                CompareOp::Gt => BinaryOp::Gt,
                CompareOp::Ge => BinaryOp::GtEq,
                CompareOp::Lt => BinaryOp::Lt,
                CompareOp::Le => BinaryOp::LtEq,
            };
            Ok(QExpr::BinaryOp {
                left: Box::new(left),
                op: qop,
                right: Box::new(right),
            })
        }
        FilterExpr::InList { field, values } => {
            // Rewrite as OR of equality checks.
            let col = QExpr::Column(field.clone());
            let arms: Result<Vec<QExpr>, LiteError> = values
                .iter()
                .map(|v| {
                    let lit = QExpr::Literal(sql_value_to_value(v)?);
                    Ok(QExpr::BinaryOp {
                        left: Box::new(col.clone()),
                        op: BinaryOp::Eq,
                        right: Box::new(lit),
                    })
                })
                .collect();
            Ok(combine_arms(arms?, BinaryOp::Or, false))
        }
        FilterExpr::Between { field, low, high } => {
            let col = QExpr::Column(field.clone());
            let lo = QExpr::Literal(sql_value_to_value(low)?);
            let hi = QExpr::Literal(sql_value_to_value(high)?);
            let gte = QExpr::BinaryOp {
                left: Box::new(col.clone()),
                op: BinaryOp::GtEq,
                right: Box::new(lo),
            };
            let lte = QExpr::BinaryOp {
                left: Box::new(col),
                op: BinaryOp::LtEq,
                right: Box::new(hi),
            };
            Ok(QExpr::BinaryOp {
                left: Box::new(gte),
                op: BinaryOp::And,
                right: Box::new(lte),
            })
        }
        FilterExpr::IsNull { field } => Ok(QExpr::IsNull {
            expr: Box::new(QExpr::Column(field.clone())),
            negated: false,
        }),
        FilterExpr::IsNotNull { field } => Ok(QExpr::IsNull {
            expr: Box::new(QExpr::Column(field.clone())),
            negated: true,
        }),
        FilterExpr::And(sub) => {
            let arms: Result<Vec<QExpr>, LiteError> = sub.iter().map(filter_to_qexpr).collect();
            Ok(combine_arms(arms?, BinaryOp::And, true))
        }
        FilterExpr::Or(sub) => {
            let arms: Result<Vec<QExpr>, LiteError> = sub.iter().map(filter_to_qexpr).collect();
            Ok(combine_arms(arms?, BinaryOp::Or, false))
        }
        FilterExpr::Not(inner) => {
            let inner_q = filter_to_qexpr(inner)?;
            Ok(QExpr::BinaryOp {
                left: Box::new(inner_q),
                op: BinaryOp::Eq,
                right: Box::new(QExpr::Literal(Value::Bool(false))),
            })
        }
        FilterExpr::Expr(sql_expr) => convert_sql_expr(sql_expr),
    }
}

fn convert_payload_atom(atom: &SqlPayloadAtom) -> Result<MetadataFilter, LiteError> {
    match atom {
        SqlPayloadAtom::Eq(field, value) => {
            let v = sql_value_to_value(value)?;
            Ok(MetadataFilter::Eq {
                field: field.clone(),
                value: v,
            })
        }
        SqlPayloadAtom::In(field, values) => {
            let vs: Result<Vec<Value>, LiteError> = values.iter().map(sql_value_to_value).collect();
            Ok(MetadataFilter::In {
                field: field.clone(),
                values: vs?,
            })
        }
        SqlPayloadAtom::Range {
            field,
            low,
            low_inclusive,
            high,
            high_inclusive,
        } => {
            let mut parts: Vec<MetadataFilter> = Vec::new();
            if let Some(lo) = low {
                let v = sql_value_to_value(lo)?;
                if *low_inclusive {
                    parts.push(MetadataFilter::Gte {
                        field: field.clone(),
                        value: v,
                    });
                } else {
                    parts.push(MetadataFilter::Gt {
                        field: field.clone(),
                        value: v,
                    });
                }
            }
            if let Some(hi) = high {
                let v = sql_value_to_value(hi)?;
                if *high_inclusive {
                    parts.push(MetadataFilter::Lte {
                        field: field.clone(),
                        value: v,
                    });
                } else {
                    parts.push(MetadataFilter::Lt {
                        field: field.clone(),
                        value: v,
                    });
                }
            }
            match parts.len() {
                0 => Err(LiteError::BadRequest {
                    detail: "Range payload atom with no bounds is not valid".to_string(),
                }),
                1 => Ok(parts.remove(0)),
                _ => Ok(MetadataFilter::And(parts)),
            }
        }
    }
}

pub(crate) fn sql_value_to_value(v: &SqlValue) -> Result<Value, LiteError> {
    match v {
        SqlValue::Int(i) => Ok(Value::Integer(*i)),
        SqlValue::Float(f) => Ok(Value::Float(*f)),
        SqlValue::Decimal(d) => {
            d.to_string()
                .parse::<f64>()
                .map(Value::Float)
                .map_err(|_| LiteError::BadRequest {
                    detail: format!("decimal value {d} could not be converted to f64"),
                })
        }
        SqlValue::String(s) => Ok(Value::String(s.clone())),
        SqlValue::Bool(b) => Ok(Value::Bool(*b)),
        SqlValue::Null => Ok(Value::Null),
        SqlValue::Bytes(b) => Ok(Value::Bytes(b.clone())),
        SqlValue::Array(elems) => {
            let vs: Result<Vec<Value>, LiteError> = elems.iter().map(sql_value_to_value).collect();
            Ok(Value::Array(vs?))
        }
        SqlValue::Timestamp(ts) => Ok(Value::NaiveDateTime(*ts)),
        SqlValue::Timestamptz(ts) => Ok(Value::DateTime(*ts)),
    }
}

#[cfg(test)]
mod tests {
    use nodedb_query::metadata_filter::matches_metadata_filter;
    use nodedb_sql::types::SqlValue;
    use nodedb_sql::types::filter::{CompareOp, Filter, FilterExpr};
    use nodedb_sql::types_expr::{BinaryOp, SqlExpr};
    use serde_json::json;

    use super::*;

    fn make_comparison(field: &str, op: CompareOp, value: SqlValue) -> Filter {
        Filter {
            expr: FilterExpr::Comparison {
                field: field.to_string(),
                op,
                value,
            },
        }
    }

    fn make_expr_filter(expr: SqlExpr) -> Filter {
        Filter {
            expr: FilterExpr::Expr(expr),
        }
    }

    fn eval_lite_filter(lf: &LiteFilter, doc: &serde_json::Value) -> bool {
        let meta_pass = lf
            .meta
            .as_ref()
            .map(|f| matches_metadata_filter(doc, f))
            .unwrap_or(true);
        if !meta_pass {
            return false;
        }
        if lf.exprs.is_empty() {
            return true;
        }
        let typed: Value = {
            let mut map = std::collections::HashMap::new();
            for (k, v) in doc.as_object().unwrap_or(&serde_json::Map::new()) {
                let val = match v {
                    serde_json::Value::Bool(b) => Value::Bool(*b),
                    serde_json::Value::Number(n) => {
                        if let Some(i) = n.as_i64() {
                            Value::Integer(i)
                        } else {
                            Value::Float(n.as_f64().unwrap_or(0.0))
                        }
                    }
                    serde_json::Value::String(s) => Value::String(s.clone()),
                    serde_json::Value::Null => Value::Null,
                    _ => Value::Null,
                };
                map.insert(k.clone(), val);
            }
            Value::Object(map)
        };
        lf.eval_exprs(&typed)
    }

    #[test]
    fn primitive_eq_filter() {
        let filters = vec![make_comparison(
            "name",
            CompareOp::Eq,
            SqlValue::String("alice".into()),
        )];
        let lf = sql_filters_to_metadata(&filters, &[]).expect("ok");
        assert!(lf.meta.is_some());
        assert!(lf.exprs.is_empty());

        let doc_match = json!({"name": "alice"});
        let doc_miss = json!({"name": "bob"});
        assert!(eval_lite_filter(&lf, &doc_match));
        assert!(!eval_lite_filter(&lf, &doc_miss));
    }

    #[test]
    fn logical_and_or_not() {
        let age_gt = make_comparison("age", CompareOp::Gt, SqlValue::Int(20));
        let age_lt = make_comparison("age", CompareOp::Lt, SqlValue::Int(40));
        let and_filter = Filter {
            expr: FilterExpr::And(vec![age_gt, age_lt]),
        };
        let lf = sql_filters_to_metadata(&[and_filter], &[]).expect("ok");
        assert!(eval_lite_filter(&lf, &json!({"age": 30})));
        assert!(!eval_lite_filter(&lf, &json!({"age": 50})));

        let name_a = make_comparison("status", CompareOp::Eq, SqlValue::String("a".into()));
        let name_b = make_comparison("status", CompareOp::Eq, SqlValue::String("b".into()));
        let or_filter = Filter {
            expr: FilterExpr::Or(vec![name_a, name_b]),
        };
        let lf2 = sql_filters_to_metadata(&[or_filter], &[]).expect("ok");
        assert!(eval_lite_filter(&lf2, &json!({"status": "a"})));
        assert!(eval_lite_filter(&lf2, &json!({"status": "b"})));
        assert!(!eval_lite_filter(&lf2, &json!({"status": "c"})));

        let not_filter = Filter {
            expr: FilterExpr::Not(Box::new(make_comparison(
                "active",
                CompareOp::Eq,
                SqlValue::Bool(false),
            ))),
        };
        let lf3 = sql_filters_to_metadata(&[not_filter], &[]).expect("ok");
        assert!(eval_lite_filter(&lf3, &json!({"active": true})));
        assert!(!eval_lite_filter(&lf3, &json!({"active": false})));
    }

    #[test]
    fn function_call_predicate_lower() {
        // WHERE LOWER(name) = 'alice' — expressed as FilterExpr::Expr with a function.
        let lower_call = SqlExpr::Function {
            name: "lower".to_string(),
            args: vec![SqlExpr::Column {
                name: "name".to_string(),
                table: None,
            }],
            distinct: false,
        };
        let eq_expr = SqlExpr::BinaryOp {
            left: Box::new(lower_call),
            op: BinaryOp::Eq,
            right: Box::new(SqlExpr::Literal(SqlValue::String("alice".into()))),
        };
        let lf = sql_filters_to_metadata(&[make_expr_filter(eq_expr)], &[]).expect("ok");
        // Complex filter must land in exprs, not meta.
        assert!(lf.meta.is_none());
        assert_eq!(lf.exprs.len(), 1);
        // Evaluate: LOWER("Alice") = "alice" should be true when the runtime
        // function evaluator handles "lower".  We check that the expr does NOT
        // return an error (it may return null if the function is unregistered,
        // in which case the filter correctly rejects non-matching rows).
        // The key invariant: no BadRequest returned from sql_filters_to_metadata.
    }

    #[test]
    fn arithmetic_predicate() {
        // WHERE age + 1 > 30
        let age_plus_one = SqlExpr::BinaryOp {
            left: Box::new(SqlExpr::Column {
                name: "age".to_string(),
                table: None,
            }),
            op: BinaryOp::Add,
            right: Box::new(SqlExpr::Literal(SqlValue::Int(1))),
        };
        let gt_expr = SqlExpr::BinaryOp {
            left: Box::new(age_plus_one),
            op: BinaryOp::Gt,
            right: Box::new(SqlExpr::Literal(SqlValue::Int(30))),
        };
        let lf = sql_filters_to_metadata(&[make_expr_filter(gt_expr)], &[]).expect("ok");
        assert!(lf.meta.is_none());
        assert_eq!(lf.exprs.len(), 1);

        // age=30 → 30+1=31 > 30 → true
        let doc_pass = Value::Object({
            let mut m = std::collections::HashMap::new();
            m.insert("age".to_string(), Value::Integer(30));
            m
        });
        assert!(lf.eval_exprs(&doc_pass));

        // age=29 → 29+1=30 > 30 → false
        let doc_fail = Value::Object({
            let mut m = std::collections::HashMap::new();
            m.insert("age".to_string(), Value::Integer(29));
            m
        });
        assert!(!lf.eval_exprs(&doc_fail));
    }

    #[test]
    fn is_null_is_not_null() {
        let is_null = Filter {
            expr: FilterExpr::IsNull {
                field: "email".to_string(),
            },
        };
        let lf = sql_filters_to_metadata(&[is_null], &[]).expect("ok");
        assert!(eval_lite_filter(&lf, &json!({"email": null})));
        assert!(eval_lite_filter(&lf, &json!({})));
        assert!(!eval_lite_filter(&lf, &json!({"email": "x@y.z"})));

        let is_not_null = Filter {
            expr: FilterExpr::IsNotNull {
                field: "email".to_string(),
            },
        };
        let lf2 = sql_filters_to_metadata(&[is_not_null], &[]).expect("ok");
        assert!(!eval_lite_filter(&lf2, &json!({"email": null})));
        assert!(eval_lite_filter(&lf2, &json!({"email": "x@y.z"})));
    }
}
