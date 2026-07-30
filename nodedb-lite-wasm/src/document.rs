// SPDX-License-Identifier: Apache-2.0

//! Document-engine methods on [`NodeDbLiteWasm`].

use wasm_bindgen::prelude::*;

use nodedb_client::NodeDb;
use nodedb_types::document::Document;
use nodedb_types::value::Value;

use crate::dispatch;
use crate::types::NodeDbLiteWasm;

#[wasm_bindgen]
impl NodeDbLiteWasm {
    /// Get a document by ID. Returns JSON or null.
    #[wasm_bindgen(js_name = "documentGet")]
    pub async fn document_get(&self, collection: &str, id: &str) -> Result<JsValue, JsError> {
        let doc = dispatch!(self, db, {
            db.document_get(collection, id)
                .await
                .map_err(|e| JsError::new(&e.to_string()))
        })?;

        match doc {
            Some(d) => serde_wasm_bindgen::to_value(&d).map_err(|e| JsError::new(&e.to_string())),
            None => Ok(JsValue::NULL),
        }
    }

    /// Put (insert or update) a document. Takes a JSON string of fields.
    ///
    /// If `id` is empty, a UUIDv7 is auto-generated.
    /// Returns the document ID (useful when auto-generated).
    #[wasm_bindgen(js_name = "documentPut")]
    pub async fn document_put(
        &self,
        collection: &str,
        id: &str,
        fields_json: &str,
    ) -> Result<String, JsError> {
        let fields: std::collections::HashMap<String, Value> =
            sonic_rs::from_str(fields_json).map_err(|e| JsError::new(&e.to_string()))?;

        let doc_id = if id.is_empty() {
            nodedb_types::id_gen::uuid_v7()
        } else {
            id.to_string()
        };

        let mut doc = Document::new(&doc_id);
        for (k, v) in fields {
            doc.set(k, v);
        }

        dispatch!(self, db, {
            db.document_put(collection, doc)
                .await
                .map_err(|e| JsError::new(&e.to_string()))
        })?;

        Ok(doc_id)
    }

    /// Delete a document by ID.
    #[wasm_bindgen(js_name = "documentDelete")]
    pub async fn document_delete(&self, collection: &str, id: &str) -> Result<(), JsError> {
        dispatch!(self, db, {
            db.document_delete(collection, id)
                .await
                .map_err(|e| JsError::new(&e.to_string()))
        })
    }
}
