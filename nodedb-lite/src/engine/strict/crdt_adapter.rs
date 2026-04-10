//! CRDT adapter for strict document mode.
//!
//! Translates between loro CRDT field-level operations and Binary Tuple
//! byte offsets. This enables CRDT sync for strict collections — field
//! mutations are mapped to tuple patches without full deserialization.
//!
//! Schemaless document mode remains the recommended path for heavy CRDT
//! workloads. This adapter is a thin compatibility layer for strict
//! collections that occasionally sync.

use std::collections::HashMap;

use nodedb_strict::{TupleDecoder, TupleEncoder};
use nodedb_types::columnar::{SchemaOps, StrictSchema};
use nodedb_types::value::Value;

/// Apply a CRDT field-level set operation to a Binary Tuple.
///
/// Reads the existing tuple, patches the specified field, and returns
/// a new tuple with the updated value. Other fields are unchanged.
///
/// `field_updates` maps column names to new values.
pub fn apply_crdt_set(
    existing_tuple: &[u8],
    schema: &StrictSchema,
    field_updates: &HashMap<String, Value>,
) -> Result<Vec<u8>, String> {
    let decoder = TupleDecoder::new(schema);
    let encoder = TupleEncoder::new(schema);

    // Extract all current values.
    let mut values = decoder
        .extract_all(existing_tuple)
        .map_err(|e| format!("decode failed: {e}"))?;

    // Apply field updates.
    for (field_name, new_value) in field_updates {
        let col_idx = schema
            .column_index(field_name)
            .ok_or_else(|| format!("unknown field: '{field_name}'"))?;
        values[col_idx] = new_value.clone();
    }

    // Re-encode with patched values.
    encoder
        .encode(&values)
        .map_err(|e| format!("encode failed: {e}"))
}

/// Merge two conflicting tuples using Last-Writer-Wins (LWW) resolution.
///
/// For each field, picks the value from whichever tuple has the higher
/// priority (determined by the caller — typically a timestamp or vector clock).
///
/// `prefer_b` indicates whether `tuple_b` should win over `tuple_a` for
/// conflicting fields. In LWW, the tuple with the later timestamp wins.
pub fn merge_tuples_lww(
    tuple_a: &[u8],
    tuple_b: &[u8],
    schema: &StrictSchema,
    prefer_b: bool,
) -> Result<Vec<u8>, String> {
    let decoder = TupleDecoder::new(schema);
    let encoder = TupleEncoder::new(schema);

    let values_a = decoder
        .extract_all(tuple_a)
        .map_err(|e| format!("decode A: {e}"))?;
    let values_b = decoder
        .extract_all(tuple_b)
        .map_err(|e| format!("decode B: {e}"))?;

    // LWW: pick the preferred tuple's value for each field.
    let merged: Vec<Value> = values_a
        .into_iter()
        .zip(values_b)
        .map(|(a, b)| {
            if prefer_b {
                if matches!(b, Value::Null) { a } else { b }
            } else {
                if matches!(a, Value::Null) { b } else { a }
            }
        })
        .collect();

    encoder
        .encode(&merged)
        .map_err(|e| format!("encode merged: {e}"))
}

/// Convert a loro LoroValue to a nodedb Value for tuple patching.
pub fn loro_to_value(loro_val: &loro::LoroValue) -> Value {
    match loro_val {
        loro::LoroValue::Null => Value::Null,
        loro::LoroValue::Bool(b) => Value::Bool(*b),
        loro::LoroValue::I64(n) => Value::Integer(*n),
        loro::LoroValue::Double(f) => Value::Float(*f),
        loro::LoroValue::String(s) => Value::String(s.to_string()),
        _ => Value::String(format!("{loro_val:?}")),
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::columnar::{ColumnDef, ColumnType};

    use super::*;

    fn test_schema() -> StrictSchema {
        StrictSchema::new(vec![
            ColumnDef::required("id", ColumnType::Int64).with_primary_key(),
            ColumnDef::required("name", ColumnType::String),
            ColumnDef::nullable("balance", ColumnType::Float64),
        ])
        .expect("valid")
    }

    #[test]
    fn apply_set_patches_field() {
        let schema = test_schema();
        let encoder = TupleEncoder::new(&schema);

        let original = encoder
            .encode(&[
                Value::Integer(1),
                Value::String("Alice".into()),
                Value::Float(100.0),
            ])
            .expect("encode");

        let mut updates = HashMap::new();
        updates.insert("balance".into(), Value::Float(200.0));

        let patched = apply_crdt_set(&original, &schema, &updates).expect("patch");

        let decoder = TupleDecoder::new(&schema);
        let values = decoder.extract_all(&patched).expect("decode");
        assert_eq!(values[0], Value::Integer(1)); // Unchanged.
        assert_eq!(values[1], Value::String("Alice".into())); // Unchanged.
        assert_eq!(values[2], Value::Float(200.0)); // Patched.
    }

    #[test]
    fn merge_lww_prefers_b() {
        let schema = test_schema();
        let encoder = TupleEncoder::new(&schema);

        let tuple_a = encoder
            .encode(&[
                Value::Integer(1),
                Value::String("Alice".into()),
                Value::Float(100.0),
            ])
            .expect("encode a");

        let tuple_b = encoder
            .encode(&[
                Value::Integer(1),
                Value::String("Alice Updated".into()),
                Value::Float(200.0),
            ])
            .expect("encode b");

        let merged = merge_tuples_lww(&tuple_a, &tuple_b, &schema, true).expect("merge");

        let decoder = TupleDecoder::new(&schema);
        let values = decoder.extract_all(&merged).expect("decode");
        assert_eq!(values[1], Value::String("Alice Updated".into()));
        assert_eq!(values[2], Value::Float(200.0));
    }

    #[test]
    fn merge_lww_prefers_a() {
        let schema = test_schema();
        let encoder = TupleEncoder::new(&schema);

        let tuple_a = encoder
            .encode(&[
                Value::Integer(1),
                Value::String("Alice".into()),
                Value::Float(100.0),
            ])
            .expect("encode a");

        let tuple_b = encoder
            .encode(&[Value::Integer(1), Value::String("Bob".into()), Value::Null])
            .expect("encode b");

        let merged = merge_tuples_lww(&tuple_a, &tuple_b, &schema, false).expect("merge");

        let decoder = TupleDecoder::new(&schema);
        let values = decoder.extract_all(&merged).expect("decode");
        assert_eq!(values[1], Value::String("Alice".into())); // A wins.
        assert_eq!(values[2], Value::Float(100.0)); // A wins (B is null).
    }
}
