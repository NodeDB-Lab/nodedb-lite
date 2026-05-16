// SPDX-License-Identifier: Apache-2.0

//! Convert SQL filter types into `MetadataFilter` for vector search post-filtering.
//!
//! Both `Filter` (WHERE-clause AST) and `SqlPayloadAtom` (payload bitmap
//! predicates) are lowered into a single `MetadataFilter::And(...)` that
//! `run_vector_search` applies after HNSW returns candidates.

use nodedb_sql::types::SqlValue;
use nodedb_sql::types::filter::{CompareOp, Filter, FilterExpr};
use nodedb_sql::types_expr::SqlPayloadAtom;
use nodedb_types::filter::MetadataFilter;
use nodedb_types::value::Value;

use crate::error::LiteError;

/// Convert SQL WHERE filters and payload atoms into a single `MetadataFilter`.
///
/// Returns `None` when both slices are empty (no filtering needed).
/// Returns `Err` when a filter variant cannot be expressed (e.g. `Expr(SqlExpr)`
/// sub-expressions that embed subqueries).
pub(crate) fn sql_filters_to_metadata(
    filters: &[Filter],
    payload_filters: &[SqlPayloadAtom],
) -> Result<Option<MetadataFilter>, LiteError> {
    if filters.is_empty() && payload_filters.is_empty() {
        return Ok(None);
    }

    let mut parts: Vec<MetadataFilter> = Vec::new();

    for f in filters {
        parts.push(convert_filter(f)?);
    }

    for atom in payload_filters {
        parts.push(convert_payload_atom(atom)?);
    }

    if parts.len() == 1 {
        Ok(Some(parts.remove(0)))
    } else {
        Ok(Some(MetadataFilter::And(parts)))
    }
}

fn convert_filter(f: &Filter) -> Result<MetadataFilter, LiteError> {
    match &f.expr {
        FilterExpr::Comparison { field, op, value } => {
            let v = sql_value_to_value(value)?;
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
            let vs: Result<Vec<Value>, LiteError> = values.iter().map(sql_value_to_value).collect();
            Ok(MetadataFilter::In {
                field: field.clone(),
                values: vs?,
            })
        }
        FilterExpr::Between { field, low, high } => {
            let lo = sql_value_to_value(low)?;
            let hi = sql_value_to_value(high)?;
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
            let parts: Result<Vec<MetadataFilter>, LiteError> =
                sub.iter().map(convert_filter).collect();
            Ok(MetadataFilter::And(parts?))
        }
        FilterExpr::Or(sub) => {
            let parts: Result<Vec<MetadataFilter>, LiteError> =
                sub.iter().map(convert_filter).collect();
            Ok(MetadataFilter::Or(parts?))
        }
        FilterExpr::Not(inner) => Ok(MetadataFilter::Not(Box::new(convert_filter(inner)?))),
        FilterExpr::Expr(_) => Err(LiteError::BadRequest {
            detail: "complex expression predicates (subqueries, functions) in vector_search \
                     WHERE clauses are not supported in 0.1.0"
                .to_string(),
        }),
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
