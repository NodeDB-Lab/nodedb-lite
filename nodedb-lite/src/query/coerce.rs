//! Shared SQL → `nodedb_types::Value` coercion used by every engine DML
//! dispatcher (strict, columnar, timeseries, …).
//!
//! The coercion table is single-sourced here so adding a new `ColumnType`
//! variant or a new literal-shape rule lights up across every engine in one
//! edit instead of being copy-pasted into each `*_dml.rs`.

use std::collections::HashMap;

use nodedb_sql::types::SqlValue;
use nodedb_types::columnar::{ColumnDef, ColumnType};
use nodedb_types::datetime::NdbDateTime;
use nodedb_types::value::Value;

use crate::error::LiteError;
pub(crate) use crate::query::filter_convert::sql_value_to_value;

/// Build a `Vec<Value>` in schema column order from a `(name, SqlValue)` pair list.
///
/// Columns absent from `pairs` default to `Value::Null`. Each provided value is
/// coerced to the schema column type via [`coerce_sql_value`].
pub fn build_row(
    pairs: &[(String, SqlValue)],
    columns: &[ColumnDef],
) -> Result<Vec<Value>, LiteError> {
    let pair_map: HashMap<&str, &SqlValue> = pairs.iter().map(|(k, v)| (k.as_str(), v)).collect();

    let mut values = Vec::with_capacity(columns.len());
    for col in columns {
        let value = match pair_map.get(col.name.as_str()).copied() {
            Some(v) => coerce_sql_value(v, &col.column_type)?,
            None => Value::Null,
        };
        values.push(value);
    }
    Ok(values)
}

/// Coerce a `SqlValue` to a `Value` matching the target column type.
///
/// A column type without a dedicated rule takes the untyped conversion from
/// `sql_value_to_value`, which keeps SELECT and unconstrained literal
/// contexts working. A literal no instant can be read from is refused for a
/// timestamp column, never stored as NULL.
pub fn coerce_sql_value(v: &SqlValue, col_type: &ColumnType) -> Result<Value, LiteError> {
    if matches!(v, SqlValue::Null) {
        return Ok(Value::Null);
    }
    match col_type {
        ColumnType::Timestamp => coerce_instant(v, col_type).map(Value::NaiveDateTime),
        ColumnType::Timestamptz => coerce_instant(v, col_type).map(Value::DateTime),
        ColumnType::Int64 => match v {
            SqlValue::Int(i) => Ok(Value::Integer(*i)),
            other => sql_value_to_value(other),
        },
        ColumnType::Float64 => match v {
            SqlValue::Float(f) => Ok(Value::Float(*f)),
            SqlValue::Int(i) => Ok(Value::Float(*i as f64)),
            other => sql_value_to_value(other),
        },
        ColumnType::String => match v {
            SqlValue::String(s) => Ok(Value::String(s.clone())),
            other => sql_value_to_value(other),
        },
        ColumnType::Uuid => match v {
            SqlValue::String(s) => Ok(Value::Uuid(s.clone())),
            other => sql_value_to_value(other),
        },
        ColumnType::Bool => match v {
            SqlValue::Bool(b) => Ok(Value::Bool(*b)),
            other => sql_value_to_value(other),
        },
        ColumnType::Bytes
        | ColumnType::SystemTimestamp
        | ColumnType::Decimal { .. }
        | ColumnType::Geometry
        | ColumnType::Vector(_)
        | ColumnType::SparseVector
        | ColumnType::Json
        | ColumnType::Ulid
        | ColumnType::Duration
        | ColumnType::Array => sql_value_to_value(v),
        other => Err(LiteError::Unsupported {
            detail: format!("column type {other} has no write coercion rule in Lite"),
        }),
    }
}

/// The instant a literal denotes under a timestamp column, mirroring the
/// planner's write-side rule: a typed instant keeps its value whichever tag
/// it carries, text parses as ISO-8601, and a numeric literal is epoch
/// milliseconds with a fractional one contributing its integer part.
fn coerce_instant(v: &SqlValue, col_type: &ColumnType) -> Result<NdbDateTime, LiteError> {
    let refused = |literal: String| LiteError::BadRequest {
        detail: format!("{literal} is not representable as {col_type}"),
    };
    match v {
        SqlValue::Timestamp(at) | SqlValue::Timestamptz(at) => Ok(*at),
        SqlValue::String(s) => NdbDateTime::parse(s).ok_or_else(|| refused(format!("'{s}'"))),
        SqlValue::Int(millis) => {
            NdbDateTime::from_millis(*millis).map_err(|_| refused(millis.to_string()))
        }
        SqlValue::Float(f) => {
            let whole = f.trunc();
            if !whole.is_finite() || whole < i64::MIN as f64 || whole >= i64::MAX as f64 {
                return Err(refused(f.to_string()));
            }
            NdbDateTime::from_millis(whole as i64).map_err(|_| refused(f.to_string()))
        }
        SqlValue::Decimal(d) => d
            .trunc()
            .to_string()
            .parse::<i64>()
            .ok()
            .and_then(|millis| NdbDateTime::from_millis(millis).ok())
            .ok_or_else(|| refused(d.to_string())),
        SqlValue::Null => Err(refused("NULL".to_string())),
        SqlValue::Bool(b) => Err(refused(b.to_string())),
        SqlValue::Bytes(_) => Err(refused("a byte string".to_string())),
        SqlValue::Array(_) => Err(refused("an array".to_string())),
    }
}

/// Render a `SqlValue` as the textual primary-key form used by `parse_pk_value`.
pub fn sql_value_to_string(v: &SqlValue) -> String {
    match v {
        SqlValue::String(s) => s.clone(),
        SqlValue::Int(i) => i.to_string(),
        SqlValue::Float(f) => f.to_string(),
        SqlValue::Bool(b) => b.to_string(),
        SqlValue::Decimal(d) => d.to_string(),
        SqlValue::Timestamp(at) | SqlValue::Timestamptz(at) => at.to_iso8601(),
        SqlValue::Null | SqlValue::Bytes(_) | SqlValue::Array(_) => String::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `2024-01-01T00:00:00Z` in epoch microseconds.
    const NEW_YEAR_MICROS: i64 = 1_704_067_200_000_000;

    fn at() -> NdbDateTime {
        NdbDateTime::from_micros(NEW_YEAR_MICROS)
    }

    /// The planner's typed instant lands in a timestamp column under the
    /// column's own kind, whichever tag the literal carries.
    #[test]
    fn typed_instant_lands_in_timestamp_column() {
        assert_eq!(
            coerce_sql_value(&SqlValue::Timestamp(at()), &ColumnType::Timestamp).expect("coerce"),
            Value::NaiveDateTime(at())
        );
        assert_eq!(
            coerce_sql_value(&SqlValue::Timestamptz(at()), &ColumnType::Timestamp).expect("coerce"),
            Value::NaiveDateTime(at())
        );
        assert_eq!(
            coerce_sql_value(&SqlValue::Timestamp(at()), &ColumnType::Timestamptz).expect("coerce"),
            Value::DateTime(at())
        );
    }

    /// Text and numeric literals read as ISO-8601 and epoch milliseconds.
    #[test]
    fn text_and_millis_read_as_instants() {
        assert_eq!(
            coerce_sql_value(
                &SqlValue::String("2024-01-01 00:00:00".into()),
                &ColumnType::Timestamp
            )
            .expect("coerce"),
            Value::NaiveDateTime(at())
        );
        assert_eq!(
            coerce_sql_value(
                &SqlValue::Int(NEW_YEAR_MICROS / 1_000),
                &ColumnType::Timestamptz
            )
            .expect("coerce"),
            Value::DateTime(at())
        );
        assert_eq!(
            coerce_sql_value(
                &SqlValue::Float(NEW_YEAR_MICROS as f64 / 1_000.0 + 0.7),
                &ColumnType::Timestamp
            )
            .expect("coerce"),
            Value::NaiveDateTime(at())
        );
    }

    /// A literal no instant can be read from is refused, never stored as NULL.
    #[test]
    fn unreadable_instant_is_refused() {
        for bad in [
            SqlValue::String("not a date".into()),
            SqlValue::Bool(true),
            SqlValue::Int(i64::MAX),
        ] {
            let err = coerce_sql_value(&bad, &ColumnType::Timestamp).expect_err("refused");
            assert!(matches!(err, LiteError::BadRequest { .. }), "{err:?}");
        }
        assert_eq!(
            coerce_sql_value(&SqlValue::Null, &ColumnType::Timestamp).expect("null"),
            Value::Null
        );
    }

    /// `build_row` places a typed instant under its column and leaves absent
    /// columns NULL.
    #[test]
    fn build_row_orders_by_schema() {
        let columns = vec![
            ColumnDef::required("id", ColumnType::Int64),
            ColumnDef::required("ts", ColumnType::Timestamp),
            ColumnDef::nullable("value", ColumnType::Float64),
        ];
        let pairs = vec![
            ("ts".to_string(), SqlValue::Timestamp(at())),
            ("id".to_string(), SqlValue::Int(1)),
        ];
        let row = build_row(&pairs, &columns).expect("row");
        assert_eq!(
            row,
            vec![Value::Integer(1), Value::NaiveDateTime(at()), Value::Null]
        );
    }
}
