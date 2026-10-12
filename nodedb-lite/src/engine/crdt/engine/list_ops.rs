// SPDX-License-Identifier: BUSL-1.1

//! LoroMovableList block operations and JSON-to-Loro value conversion.

use sonic_rs::JsonValueTrait as _;

use crate::error::LiteError;

use super::rollback::Rollback;
use super::types::CrdtEngine;

impl CrdtEngine {
    /// Insert a new LoroMap block into a document's movable list at `index`.
    ///
    /// `fields` is a `sonic_rs::Value` object; each top-level key is
    /// recursively converted via [`sonic_value_to_loro`] so nested objects /
    /// arrays survive the round-trip as `LoroValue::Map` / `LoroValue::List`.
    ///
    /// Every refusal (absent row, unresolved path, plain list) comes before
    /// the first operation. Only a Loro fault fails after the block is
    /// inserted. A row image holds no nested containers, so that partial
    /// block stays and reaches the sync queue with the error.
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

        self.with_delta_capture(
            collection,
            document_id,
            "list_insert",
            Rollback::Keep,
            |state| {
                state
                    .list_insert_fields(collection, document_id, list_path, index, &field_values)
                    .map_err(|e| LiteError::crdt("list_insert", e))
            },
        )
        .map(|_| ())
    }

    /// Delete a block from a document's movable list at `index`.
    pub fn list_delete(
        &mut self,
        collection: &str,
        document_id: &str,
        list_path: &str,
        index: usize,
    ) -> Result<(), LiteError> {
        self.with_delta_capture(
            collection,
            document_id,
            "list_delete",
            Rollback::Keep,
            |state| {
                state
                    .list_delete(collection, document_id, list_path, index)
                    .map_err(|e| LiteError::crdt("list_delete", e))
            },
        )
        .map(|_| ())
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
        self.with_delta_capture(
            collection,
            document_id,
            "list_move",
            Rollback::Keep,
            |state| {
                state
                    .list_move(collection, document_id, list_path, from_index, to_index)
                    .map_err(|e| LiteError::crdt("list_move", e))
            },
        )
        .map(|_| ())
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

#[cfg(test)]
mod tests {
    use nodedb_types::error::{ErrorCode, NodeDbError};

    use super::*;

    fn engine_with_one_block() -> CrdtEngine {
        let mut engine = CrdtEngine::new(1).unwrap();
        engine
            .upsert("pages", "p", &[("a", loro::LoroValue::I64(1))])
            .unwrap();
        let block: sonic_rs::Value = sonic_rs::from_str(r#"{"t":"b0"}"#).unwrap();
        engine
            .list_insert("pages", "p", "blocks", 0, &block)
            .unwrap();
        engine
    }

    #[test]
    fn a_block_index_out_of_bounds_is_a_data_exception() {
        let mut engine = engine_with_one_block();
        let pending_before = engine.pending_count();

        let err = engine.list_delete("pages", "p", "blocks", 5).unwrap_err();

        assert!(matches!(err, LiteError::DataException { .. }), "{err:?}");
        assert_eq!(NodeDbError::from(err).code(), ErrorCode::DATA_EXCEPTION);
        assert_eq!(engine.pending_count(), pending_before);
    }

    #[test]
    fn a_block_move_out_of_bounds_is_a_data_exception() {
        let mut engine = engine_with_one_block();
        let err = engine.list_move("pages", "p", "blocks", 0, 3).unwrap_err();
        assert_eq!(NodeDbError::from(err).code(), ErrorCode::DATA_EXCEPTION);
    }

    #[test]
    fn a_block_list_on_an_absent_row_is_an_undefined_object() {
        let mut engine = engine_with_one_block();
        let err = engine
            .list_delete("pages", "missing", "blocks", 0)
            .unwrap_err();
        assert_eq!(NodeDbError::from(err).code(), ErrorCode::UNDEFINED_OBJECT);
    }
}
