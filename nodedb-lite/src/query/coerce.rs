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
            Some(v) => coerce_sql_value(v, &col.column_type),
            None => Value::Null,
        };
        values.push(value);
    }
    Ok(values)
}

/// Coerce a `SqlValue` to a `Value` matching the target column type.
///
/// Falls back to [`sql_value_to_value`] for any combination not handled by the
/// explicit table — that path keeps SELECT and unconstrained literal contexts
/// working.
pub fn coerce_sql_value(v: &SqlValue, col_type: &ColumnType) -> Value {
    match (v, col_type) {
        (SqlValue::Int(i), ColumnType::Int64) => Value::Integer(*i),
        (SqlValue::Float(f), ColumnType::Float64) => Value::Float(*f),
        (SqlValue::Int(i), ColumnType::Float64) => Value::Float(*i as f64),
        (SqlValue::String(s), ColumnType::String) => Value::String(s.clone()),
        (SqlValue::String(s), ColumnType::Uuid) => Value::Uuid(s.clone()),
        (SqlValue::Bool(b), ColumnType::Bool) => Value::Bool(*b),
        (SqlValue::Null, _) => Value::Null,
        // Timestamp: integer microseconds pass through directly. String literals
        // (ISO-8601 / SQL format) parse to `NaiveDateTime`; unparseable strings
        // collapse to `Null` rather than silently inserting epoch.
        (SqlValue::Int(i), ColumnType::Timestamp | ColumnType::Timestamptz) => Value::Integer(*i),
        (SqlValue::String(s), ColumnType::Timestamp | ColumnType::Timestamptz) => {
            match NdbDateTime::parse(s) {
                Some(dt) => Value::NaiveDateTime(dt),
                None => Value::Null,
            }
        }
        _ => sql_value_to_value(v),
    }
}

/// Untyped fallback conversion used when no schema column type is available.
pub fn sql_value_to_value(v: &SqlValue) -> Value {
    match v {
        SqlValue::Int(i) => Value::Integer(*i),
        SqlValue::Float(f) => Value::Float(*f),
        SqlValue::String(s) => Value::String(s.clone()),
        SqlValue::Bool(b) => Value::Bool(*b),
        SqlValue::Null => Value::Null,
        _ => Value::Null,
    }
}

/// Render a `SqlValue` as the textual primary-key form used by `parse_pk_value`.
pub fn sql_value_to_string(v: &SqlValue) -> String {
    match v {
        SqlValue::String(s) => s.clone(),
        SqlValue::Int(i) => i.to_string(),
        SqlValue::Float(f) => f.to_string(),
        SqlValue::Bool(b) => b.to_string(),
        _ => String::new(),
    }
}
