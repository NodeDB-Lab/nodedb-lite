// SPDX-License-Identifier: Apache-2.0

//! Vector-engine methods on [`NodeDbLiteWasm`].

use wasm_bindgen::prelude::*;

use nodedb_client::NodeDb;

use crate::dispatch;
use crate::types::NodeDbLiteWasm;

#[wasm_bindgen]
impl NodeDbLiteWasm {
    /// Insert a vector into a collection.
    #[wasm_bindgen(js_name = "vectorInsert")]
    pub async fn vector_insert(
        &self,
        collection: &str,
        id: &str,
        embedding: &[f32],
    ) -> Result<(), JsError> {
        dispatch!(self, db, {
            db.vector_insert(collection, id, embedding, None)
                .await
                .map_err(|e| JsError::new(&e.to_string()))
        })
    }

    /// Search for the k nearest vectors. Returns JSON array.
    #[wasm_bindgen(js_name = "vectorSearch")]
    pub async fn vector_search(
        &self,
        collection: &str,
        query: &[f32],
        k: usize,
    ) -> Result<JsValue, JsError> {
        let results = dispatch!(self, db, {
            db.vector_search(collection, query, k, None, None)
                .await
                .map_err(|e| JsError::new(&e.to_string()))
        })?;

        let json: Vec<serde_json::Value> = results
            .iter()
            .map(|r| serde_json::json!({"id": r.id, "distance": r.distance}))
            .collect();

        serde_wasm_bindgen::to_value(&json).map_err(|e| JsError::new(&e.to_string()))
    }

    /// Delete a vector by ID.
    #[wasm_bindgen(js_name = "vectorDelete")]
    pub async fn vector_delete(&self, collection: &str, id: &str) -> Result<(), JsError> {
        dispatch!(self, db, {
            db.vector_delete(collection, id)
                .await
                .map_err(|e| JsError::new(&e.to_string()))
        })
    }
}
