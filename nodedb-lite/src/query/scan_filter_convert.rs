// SPDX-License-Identifier: Apache-2.0

//! `Filter` tree → `Vec<ScanFilter>`, the predicate form a physical op
//! carries as zerompk bytes and evaluates per stored row.
//!
//! A `FilterExpr` the planner reduced to a field/op/value triple maps onto
//! one `ScanFilter`; `Not` and `Expr` become a `FilterOp::Expr` predicate
//! through the query-side expression converter.

use nodedb_query::scan_filter::{FilterOp, ScanFilter};
use nodedb_sql::types::filter::{CompareOp, Filter, FilterExpr};
use nodedb_types::value::Value;

use crate::error::LiteError;
use crate::query::filter_convert::{filter_to_qexpr, sql_value_to_value};

/// Encode `filters` as the zerompk `Vec<ScanFilter>` a physical op carries.
/// Empty input encodes as empty bytes, which every consumer reads as
/// "match all".
pub(crate) fn encode_scan_filters(filters: &[Filter]) -> Result<Vec<u8>, LiteError> {
    if filters.is_empty() {
        return Ok(Vec::new());
    }
    let scan_filters = filters_to_scan_filters(filters)?;
    zerompk::to_msgpack_vec(&scan_filters).map_err(|e| LiteError::Serialization {
        detail: format!("encode scan filters: {e}"),
    })
}

/// Decode the bytes [`encode_scan_filters`] produced. Empty bytes decode
/// to an empty list.
pub(crate) fn decode_scan_filters(bytes: &[u8]) -> Result<Vec<ScanFilter>, LiteError> {
    if bytes.is_empty() {
        return Ok(Vec::new());
    }
    zerompk::from_msgpack(bytes).map_err(|e| LiteError::Serialization {
        detail: format!("decode scan filters: {e}"),
    })
}

/// Convert an AND-list of filters to the flat `ScanFilter` AND-group.
pub(crate) fn filters_to_scan_filters(filters: &[Filter]) -> Result<Vec<ScanFilter>, LiteError> {
    let mut out = Vec::with_capacity(filters.len());
    for f in filters {
        filter_to_scan_filters(f, &mut out)?;
    }
    Ok(out)
}

fn simple(field: &str, op: FilterOp, value: Value) -> ScanFilter {
    ScanFilter {
        field: field.to_string(),
        op,
        value,
        clauses: Vec::new(),
        expr: None,
    }
}

fn filter_to_scan_filters(f: &Filter, out: &mut Vec<ScanFilter>) -> Result<(), LiteError> {
    match &f.expr {
        FilterExpr::Comparison { field, op, value } => {
            let op = match op {
                CompareOp::Eq => FilterOp::Eq,
                CompareOp::Ne => FilterOp::Ne,
                CompareOp::Gt => FilterOp::Gt,
                CompareOp::Ge => FilterOp::Gte,
                CompareOp::Lt => FilterOp::Lt,
                CompareOp::Le => FilterOp::Lte,
            };
            out.push(simple(field, op, sql_value_to_value(value)?));
        }
        FilterExpr::InList { field, values } => {
            let arr = values
                .iter()
                .map(sql_value_to_value)
                .collect::<Result<Vec<_>, LiteError>>()?;
            out.push(simple(field, FilterOp::In, Value::Array(arr)));
        }
        FilterExpr::Between { field, low, high } => {
            out.push(simple(field, FilterOp::Gte, sql_value_to_value(low)?));
            out.push(simple(field, FilterOp::Lte, sql_value_to_value(high)?));
        }
        FilterExpr::IsNull { field } => out.push(simple(field, FilterOp::IsNull, Value::Null)),
        FilterExpr::IsNotNull { field } => {
            out.push(simple(field, FilterOp::IsNotNull, Value::Null))
        }
        FilterExpr::And(filters) => {
            for inner in filters {
                filter_to_scan_filters(inner, out)?;
            }
        }
        FilterExpr::Or(filters) => {
            let mut clauses = Vec::with_capacity(filters.len());
            for inner in filters {
                let mut group = Vec::new();
                filter_to_scan_filters(inner, &mut group)?;
                clauses.push(group);
            }
            out.push(ScanFilter {
                field: String::new(),
                op: FilterOp::Or,
                value: Value::Null,
                clauses,
                expr: None,
            });
        }
        FilterExpr::Not(_) | FilterExpr::Expr(_) => out.push(ScanFilter {
            field: String::new(),
            op: FilterOp::Expr,
            value: Value::Null,
            clauses: Vec::new(),
            expr: Some(filter_to_qexpr(f)?),
        }),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use nodedb_sql::types::SqlValue;

    use super::*;

    fn doc(pairs: &[(&str, Value)]) -> Value {
        Value::Object(
            pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.clone()))
                .collect::<HashMap<_, _>>(),
        )
    }

    #[test]
    fn in_list_round_trips_and_matches() {
        let filters = vec![Filter {
            expr: FilterExpr::InList {
                field: "id".into(),
                values: vec![SqlValue::String("a".into()), SqlValue::String("b".into())],
            },
        }];
        let bytes = encode_scan_filters(&filters).expect("encode");
        let decoded = decode_scan_filters(&bytes).expect("decode");
        assert_eq!(decoded.len(), 1);
        let hit = doc(&[("id", Value::String("b".into()))]);
        let miss = doc(&[("id", Value::String("z".into()))]);
        assert!(ScanFilter::all_match_value(&decoded, &hit).expect("eval"));
        assert!(!ScanFilter::all_match_value(&decoded, &miss).expect("eval"));
    }

    #[test]
    fn between_becomes_two_bounds() {
        let filters = vec![Filter {
            expr: FilterExpr::Between {
                field: "n".into(),
                low: SqlValue::Int(1),
                high: SqlValue::Int(3),
            },
        }];
        let scan = filters_to_scan_filters(&filters).expect("convert");
        assert_eq!(scan.len(), 2);
        assert!(
            ScanFilter::all_match_value(&scan, &doc(&[("n", Value::Integer(2))])).expect("eval")
        );
        assert!(
            !ScanFilter::all_match_value(&scan, &doc(&[("n", Value::Integer(4))])).expect("eval")
        );
    }

    #[test]
    fn empty_bytes_decode_to_no_filters() {
        assert!(decode_scan_filters(&[]).expect("decode").is_empty());
    }
}
