// SPDX-License-Identifier: Apache-2.0

//! Shared row and assignment conversions for the DML lowerings.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use nodedb_types::result::QueryResult;
use nodedb_types::value::Value;

use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::query::value_utils::value_to_string;
use crate::query::visitor::scan_post::row_to_typed_value;
use crate::storage::engine::StorageEngine;

/// Encode the typed document object that `point_insert` decodes.
pub(super) fn row_to_msgpack(row: HashMap<String, Value>) -> Result<Vec<u8>, LiteError> {
    zerompk::to_msgpack_vec(&Value::Object(row)).map_err(|e| LiteError::Serialization {
        detail: format!("encode row msgpack: {e}"),
    })
}

/// Process-wide counter used to guarantee uniqueness within the same millisecond.
static GEN_ID_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Extract the "id" column value from a row map, or generate a synthetic key.
///
/// The fallback id combines the current millisecond timestamp with a
/// process-wide monotonic counter so two inserts in the same millisecond
/// produce distinct keys. `crate::runtime::now_millis()` is used instead of
/// `SystemTime::now()` because the latter panics on wasm32.
pub(super) fn extract_id(row: &HashMap<String, Value>) -> String {
    row.get("id").map(value_to_string).unwrap_or_else(|| {
        let ms = crate::runtime::now_millis();
        let seq = GEN_ID_COUNTER.fetch_add(1, Ordering::Relaxed);
        format!("gen-{ms:x}-{seq:x}")
    })
}

/// Convert `QueryResult` rows to field maps. A schemaless document row
/// (`id` plus one `document` JSON column) contributes its document's fields,
/// so a source column reads the same whichever engine holds the source.
pub(super) fn result_to_maps(result: QueryResult) -> Vec<HashMap<String, Value>> {
    let cols = result.columns;
    result
        .rows
        .into_iter()
        .map(|row| match row_to_typed_value(&cols, &row) {
            Value::Object(map) => map,
            _ => HashMap::new(),
        })
        .collect()
}

// ── InsertSelect ─────────────────────────────────────────────────────────────

/// The declared primary-key column of a strict target, or `None` when the
/// target is schemaless (its `id` key is implicit, not declared).
pub(super) fn declared_primary_key<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    collection: &str,
) -> Option<String> {
    engine
        .strict
        .schema(collection)?
        .columns
        .iter()
        .find(|c| c.primary_key)
        .map(|c| c.name.clone())
}

/// Convert a `nodedb_types::Value` to the nearest `SqlValue` equivalent.
pub(super) fn value_to_sql_value(v: Value) -> nodedb_sql::types::SqlValue {
    use nodedb_sql::types::SqlValue;
    match v {
        Value::String(s) => SqlValue::String(s),
        Value::Integer(i) => SqlValue::Int(i),
        Value::Float(f) => SqlValue::Float(f),
        Value::Bool(b) => SqlValue::Bool(b),
        Value::Null => SqlValue::Null,
        _ => SqlValue::Null,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn row_encoding_round_trips_as_document_value() {
        let fields = HashMap::from([
            ("id".into(), Value::String("source".into())),
            ("count".into(), Value::Integer(42)),
            ("score".into(), Value::Float(1.5)),
            ("absent".into(), Value::Null),
            ("enabled".into(), Value::Bool(true)),
            ("bytes".into(), Value::Bytes(vec![0, 130, 255])),
            (
                "items".into(),
                Value::Array(vec![Value::String("item".into()), Value::Null]),
            ),
            (
                "nested".into(),
                Value::Object(HashMap::from([
                    ("name".into(), Value::String("nested".into())),
                    ("count".into(), Value::Integer(7)),
                    ("empty".into(), Value::Null),
                    ("values".into(), Value::Array(vec![Value::Integer(3)])),
                    ("object".into(), Value::Object(HashMap::new())),
                ])),
            ),
        ]);
        let expected = Value::Object(fields.clone());
        let bytes = row_to_msgpack(fields).expect("encode document row");
        let decoded: Value = zerompk::from_msgpack(&bytes).expect("decode document value");
        assert_eq!(decoded, expected);
    }
}
