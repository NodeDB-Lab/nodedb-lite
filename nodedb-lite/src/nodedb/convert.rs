//! Value conversion helpers between `noedb_types` and `loro` value types.

use loro::LoroValue;

use nodedb_types::document::Document;
use nodedb_types::value::Value;

/// Convert `nodedb_types::Value` to `LoroValue`.
pub(crate) fn value_to_loro(v: &Value) -> LoroValue {
    match v {
        Value::Null => LoroValue::Null,
        Value::Bool(b) => LoroValue::Bool(*b),
        Value::Integer(i) => LoroValue::I64(*i),
        Value::Float(f) => LoroValue::Double(*f),
        Value::String(s) => LoroValue::String(s.clone().into()),
        Value::Bytes(b) => LoroValue::Binary(b.clone().into()),
        Value::Array(_) | Value::Object(_) | Value::Set(_) => {
            // Serialize complex values as JSON string.
            let json = sonic_rs::to_string(v).unwrap_or_default();
            LoroValue::String(json.into())
        }
        Value::Regex(p) => LoroValue::String(p.clone().into()),
        Value::Range {
            start,
            end,
            inclusive,
        } => {
            let s = start
                .as_deref()
                .map(|b| sonic_rs::to_string(b).unwrap_or_default())
                .unwrap_or_default();
            let e = end
                .as_deref()
                .map(|b| sonic_rs::to_string(b).unwrap_or_default())
                .unwrap_or_default();
            let display = if *inclusive {
                format!("{s}..={e}")
            } else {
                format!("{s}..{e}")
            };
            LoroValue::String(display.into())
        }
        Value::Record { table, id } => LoroValue::String(format!("{table}:{id}").into()),
        Value::Uuid(s) | Value::Ulid(s) => LoroValue::String(s.clone().into()),
        Value::DateTime(dt) => LoroValue::String(dt.to_iso8601().into()),
        Value::Duration(d) => LoroValue::String(d.to_human().into()),
        Value::Decimal(d) => LoroValue::String(d.to_string().into()),
        Value::Geometry(g) => LoroValue::String(sonic_rs::to_string(g).unwrap_or_default().into()),
        Value::ArrayCell(_) => LoroValue::Null,
        _ => LoroValue::Null,
    }
}

/// Convert a `LoroValue` (row) into a `Document`.
pub(crate) fn loro_value_to_document(id: &str, value: &LoroValue) -> Document {
    let mut doc = Document::new(id);
    if let LoroValue::Map(map) = value {
        for (k, v) in map.iter() {
            doc.set(k.to_string(), loro_value_to_value(v));
        }
    }
    doc
}

/// Serialize a `Document`'s fields to msgpack for storage in the history table.
///
/// Encodes the fields as a msgpack map via the JSON bridge (same path as the
/// bulk ingest writer).
pub(crate) fn document_to_msgpack(doc: &Document) -> Vec<u8> {
    let json = serde_json::to_value(&doc.fields).unwrap_or_default();
    nodedb_types::json_msgpack::json_to_msgpack_or_empty(&json)
}

/// Convert `LoroValue` to `nodedb_types::Value`.
pub(crate) fn loro_value_to_value(v: &LoroValue) -> Value {
    match v {
        LoroValue::Null => Value::Null,
        LoroValue::Bool(b) => Value::Bool(*b),
        LoroValue::I64(i) => Value::Integer(*i),
        LoroValue::Double(f) => Value::Float(*f),
        LoroValue::String(s) => Value::String(s.to_string()),
        LoroValue::Binary(b) => Value::Bytes(b.to_vec()),
        LoroValue::List(arr) => Value::Array(arr.iter().map(loro_value_to_value).collect()),
        LoroValue::Map(map) => Value::Object(
            map.iter()
                .map(|(k, v)| (k.to_string(), loro_value_to_value(v)))
                .collect(),
        ),
        _ => Value::Null,
    }
}
