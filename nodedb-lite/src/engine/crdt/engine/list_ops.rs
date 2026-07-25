// SPDX-License-Identifier: BUSL-1.1

//! LoroMovableList block operations and JSON-to-Loro value conversion.

use sonic_rs::JsonValueTrait as _;

use crate::error::LiteError;

use super::types::CrdtEngine;

impl CrdtEngine {
    /// Insert a new LoroMap block into a document's movable list at `index`.
    ///
    /// `fields` is a `sonic_rs::Value` object; each top-level key is
    /// recursively converted via [`sonic_value_to_loro`] so nested objects /
    /// arrays survive the round-trip as `LoroValue::Map` / `LoroValue::List`.
    pub fn list_insert(
        &mut self,
        collection: &str,
        document_id: &str,
        list_path: &str,
        index: usize,
        fields: &sonic_rs::Value,
    ) -> Result<(), LiteError> {
        use sonic_rs::JsonContainerTrait as _;

        // Convert the JSON object to the scalar-field slice CrdtState expects.
        // The raw LoroDoc handle stays encapsulated inside CrdtState.
        let mut field_values: Vec<(String, loro::LoroValue)> = Vec::new();
        if let Some(obj) = fields.as_object() {
            for (k, v) in obj {
                field_values.push((k.to_string(), sonic_value_to_loro(v)));
            }
        }

        self.with_delta_capture(collection, document_id, "list_insert", |state| {
            state
                .list_insert_fields(collection, document_id, list_path, index, &field_values)
                .map_err(|e| LiteError::Storage {
                    detail: format!("list_insert: {e}"),
                })
        })
    }

    /// Delete a block from a document's movable list at `index`.
    pub fn list_delete(
        &mut self,
        collection: &str,
        document_id: &str,
        list_path: &str,
        index: usize,
    ) -> Result<(), LiteError> {
        self.with_delta_capture(collection, document_id, "list_delete", |state| {
            state
                .list_delete(collection, document_id, list_path, index)
                .map_err(|e| LiteError::Storage {
                    detail: format!("list_delete: {e}"),
                })
        })
    }

    /// Move a block within a document's movable list from `from_index` to `to_index`.
    pub fn list_move(
        &mut self,
        collection: &str,
        document_id: &str,
        list_path: &str,
        from_index: usize,
        to_index: usize,
    ) -> Result<(), LiteError> {
        self.with_delta_capture(collection, document_id, "list_move", |state| {
            state
                .list_move(collection, document_id, list_path, from_index, to_index)
                .map_err(|e| LiteError::Storage {
                    detail: format!("list_move: {e}"),
                })
        })
    }
}

/// Convert a `sonic_rs::Value` to a `loro::LoroValue`, recursing into
/// objects and arrays so nested data is preserved as `LoroValue::Map` /
/// `LoroValue::List` rather than collapsed to an opaque JSON string.
///
/// Plain `LoroValue` containers (as opposed to `LoroMap` / `LoroList`
/// containers attached to the document) are value-only and have no CRDT
/// identity — that is the right shape for a field inserted onto a block
/// map: it round-trips through `read()` as a `LoroValue::Map`/`List`.
fn sonic_value_to_loro(v: &sonic_rs::Value) -> loro::LoroValue {
    use sonic_rs::JsonContainerTrait as _;

    if v.is_null() {
        loro::LoroValue::Null
    } else if let Some(b) = v.as_bool() {
        loro::LoroValue::Bool(b)
    } else if let Some(n) = v.as_i64() {
        loro::LoroValue::I64(n)
    } else if let Some(f) = v.as_f64() {
        loro::LoroValue::Double(f)
    } else if v.is_str() {
        loro::LoroValue::String(v.as_str().unwrap_or("").to_string().into())
    } else if let Some(arr) = v.as_array() {
        let items: Vec<loro::LoroValue> = arr.iter().map(sonic_value_to_loro).collect();
        loro::LoroValue::List(items.into())
    } else if let Some(obj) = v.as_object() {
        let map: std::collections::HashMap<String, loro::LoroValue> = obj
            .iter()
            .map(|(k, vv)| (k.to_string(), sonic_value_to_loro(vv)))
            .collect();
        loro::LoroValue::Map(map.into())
    } else {
        loro::LoroValue::Null
    }
}
