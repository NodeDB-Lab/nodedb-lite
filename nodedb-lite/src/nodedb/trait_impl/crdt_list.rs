// SPDX-License-Identifier: Apache-2.0

//! CRDT movable-list operation helpers for `NodeDbLite`.
//!
//! Reuses the already-wired execution helpers in
//! `crate::query::crdt_ops::list` — the same handlers the native-protocol
//! dispatch path (`query::physical_visitor::adapter::crdt`) drives for
//! `PhysicalPlan::Crdt(CrdtOp::List*)` — instead of duplicating the Loro
//! movable-list logic here.

use std::collections::HashMap;

use nodedb_types::error::{NodeDbError, NodeDbResult};
use nodedb_types::value::Value;

use crate::nodedb::NodeDbLite;
use crate::query::crdt_ops::list;
use crate::storage::engine::StorageEngine;

/// Extract the field map from `fields`, rejecting anything that is not a
/// JSON-object-shaped `Value::Object`.
///
/// `handle_list_insert` treats `fields_json` as a JSON object whose
/// top-level keys become fields on the new list block; a scalar or array
/// here would silently produce a malformed block.
fn require_object_fields(fields: &Value) -> NodeDbResult<&HashMap<String, Value>> {
    match fields {
        Value::Object(obj) => Ok(obj),
        other => Err(NodeDbError::bad_request(format!(
            "list_insert: fields must be Value::Object, got {other:?}"
        ))),
    }
}

impl<S: StorageEngine> NodeDbLite<S> {
    pub(super) async fn list_insert_impl(
        &self,
        collection: &str,
        document_id: &str,
        list_path: &str,
        index: usize,
        fields: &Value,
    ) -> NodeDbResult<()> {
        let obj = require_object_fields(fields)?;
        let fields_json = sonic_rs::to_string(obj)
            .map_err(|e| NodeDbError::serialization("json", format!("list_insert fields: {e}")))?;

        list::handle_list_insert(
            &self.query_engine,
            collection,
            document_id,
            list_path,
            index,
            &fields_json,
        )
        .await?;
        Ok(())
    }

    pub(super) async fn list_delete_impl(
        &self,
        collection: &str,
        document_id: &str,
        list_path: &str,
        index: usize,
    ) -> NodeDbResult<()> {
        list::handle_list_delete(
            &self.query_engine,
            collection,
            document_id,
            list_path,
            index,
        )
        .await?;
        Ok(())
    }

    pub(super) async fn list_move_impl(
        &self,
        collection: &str,
        document_id: &str,
        list_path: &str,
        from_index: usize,
        to_index: usize,
    ) -> NodeDbResult<()> {
        list::handle_list_move(
            &self.query_engine,
            collection,
            document_id,
            list_path,
            from_index,
            to_index,
        )
        .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn obj_fields() -> Value {
        let mut map = HashMap::new();
        map.insert("title".to_string(), Value::String("hello".to_string()));
        map.insert("done".to_string(), Value::Bool(false));
        Value::Object(map)
    }

    #[test]
    fn require_object_fields_accepts_object() {
        let fields = obj_fields();
        let obj = require_object_fields(&fields).expect("object fields must pass");
        assert_eq!(obj.get("title"), Some(&Value::String("hello".to_string())));
        assert_eq!(obj.get("done"), Some(&Value::Bool(false)));
    }

    #[test]
    fn require_object_fields_rejects_scalar() {
        let err =
            require_object_fields(&Value::Integer(1)).expect_err("scalar fields must be rejected");
        assert!(format!("{err}").to_lowercase().contains("object"));
    }

    #[test]
    fn require_object_fields_rejects_array() {
        let err = require_object_fields(&Value::Array(vec![Value::Integer(1)]))
            .expect_err("array fields must be rejected");
        assert!(format!("{err}").to_lowercase().contains("object"));
    }
}
