// SPDX-License-Identifier: Apache-2.0
//! Shared scalar-to-string conversions used by index keys and indexed lookups.

use nodedb_types::value::Value;

/// Convert a scalar `Value` into the canonical string form used as a
/// component of an index key. Non-scalar variants collapse to the empty
/// string so they can still produce a deterministic key segment.
pub fn value_to_string(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Integer(n) => n.to_string(),
        Value::Float(f) => f.to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Uuid(s) => s.clone(),
        Value::Null => String::new(),
        _ => String::new(),
    }
}

/// Convert a scalar `LoroValue` into the canonical string form used as a
/// component of an index key. Containers and binary blobs collapse to the
/// empty string for the same reason as `value_to_string`.
pub fn loro_value_to_string(v: &loro::LoroValue) -> String {
    match v {
        loro::LoroValue::String(s) => s.to_string(),
        loro::LoroValue::I64(n) => n.to_string(),
        loro::LoroValue::Double(f) => f.to_string(),
        loro::LoroValue::Bool(b) => b.to_string(),
        _ => String::new(),
    }
}

/// Wall-clock milliseconds since the Unix epoch (`u64`).
///
/// Mirrors `engine::array::ops::util::time::now_ms` but returns `u64` so
/// it can be added to TTL deadlines without sign-conversion clutter.
pub fn now_ms_u64() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
