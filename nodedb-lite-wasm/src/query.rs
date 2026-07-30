// SPDX-License-Identifier: Apache-2.0

//! Full-text search and SQL execution on [`NodeDbLiteWasm`].

use wasm_bindgen::prelude::*;

use nodedb_client::NodeDb;

use crate::dispatch;
use crate::types::NodeDbLiteWasm;

#[wasm_bindgen]
impl NodeDbLiteWasm {
    /// Full-text search (BM25) against `field` in `collection`. Returns JSON array of results.
    #[wasm_bindgen(js_name = "textSearch")]
    pub async fn text_search(
        &self,
        collection: &str,
        field: &str,
        query: &str,
        top_k: usize,
    ) -> Result<JsValue, JsError> {
        let results = dispatch!(self, db, {
            db.text_search(
                collection,
                field,
                query,
                top_k,
                nodedb_types::TextSearchParams::default(),
                None,
            )
            .await
            .map_err(|e| JsError::new(&e.to_string()))
        })?;

        let json: Vec<serde_json::Value> = results
            .iter()
            .map(|r| serde_json::json!({"id": r.id, "distance": r.distance}))
            .collect();

        serde_wasm_bindgen::to_value(&json).map_err(|e| JsError::new(&e.to_string()))
    }

    /// Execute a SQL query. Returns JSON with columns and rows.
    #[wasm_bindgen(js_name = "executeSql")]
    pub async fn execute_sql(&self, sql: &str) -> Result<JsValue, JsError> {
        let result = dispatch!(self, db, {
            db.execute_sql(sql, &[])
                .await
                .map_err(|e| JsError::new(&e.to_string()))
        })?;

        let json = serde_json::json!({
            "columns": result.columns,
            "rows": result.rows,
            "rows_affected": result.rows_affected,
        });

        serde_wasm_bindgen::to_value(&json).map_err(|e| JsError::new(&e.to_string()))
    }
}
