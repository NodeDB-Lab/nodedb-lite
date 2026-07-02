// SPDX-License-Identifier: Apache-2.0

//! Conversions between `nodedb_array` cell types and `nodedb_types::Value`.

use nodedb_array::types::cell_value::value::CellValue;
use nodedb_types::value::Value;

/// Convert an array engine `CellValue` into the public `Value` type used
/// in `QueryResult` rows.
pub fn cell_value_to_value(cv: CellValue) -> Value {
    match cv {
        CellValue::Int64(i) => Value::Integer(i),
        CellValue::Float64(f) => Value::Float(f),
        CellValue::String(s) => Value::String(s),
        CellValue::Bytes(b) => Value::Bytes(b),
        CellValue::Null => Value::Null,
    }
}
