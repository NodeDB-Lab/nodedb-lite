// SPDX-License-Identifier: Apache-2.0

//! Durability, space reclamation, and identifier generation.

use wasm_bindgen::prelude::*;

use crate::dispatch;
use crate::types::NodeDbLiteWasm;

#[wasm_bindgen]
impl NodeDbLiteWasm {
    /// Flush all in-memory state to storage.
    ///
    /// The background flush task already runs at `auto_flush_ms`; call this
    /// when a specific write must be on disk before continuing.
    #[wasm_bindgen]
    pub async fn flush(&self) -> Result<(), JsError> {
        dispatch!(self, db, {
            db.flush().await.map_err(|e| JsError::new(&e.to_string()))
        })
    }

    /// Compact the backing store, reclaiming dead pages and truncating the
    /// OPFS file to bound on-disk growth.
    ///
    /// Returns a `{ reclaimedPages, segmentsRepacked, fileBytesFreed }` object.
    /// Useful for one-commit-per-entry workloads where the file would otherwise
    /// grow without bound; a no-op for the in-memory backend.
    #[wasm_bindgen]
    pub async fn compact(&self) -> Result<JsValue, JsError> {
        dispatch!(self, db, {
            let outcome = db
                .compact()
                .await
                .map_err(|e| JsError::new(&e.to_string()))?;
            serde_wasm_bindgen::to_value(&outcome).map_err(|e| JsError::new(&e.to_string()))
        })
    }

    // ─── ID Generation ──────────────────────────────────────────────────

    /// Generate a UUIDv7 (time-sortable, recommended for primary keys).
    #[wasm_bindgen(js_name = "generateId")]
    pub fn generate_id() -> String {
        nodedb_types::id_gen::uuid_v7()
    }

    /// Generate an ID of the specified type.
    ///
    /// Supported types: "uuidv7", "uuidv4", "ulid", "cuid2", "nanoid".
    #[wasm_bindgen(js_name = "generateIdTyped")]
    pub fn generate_id_typed(id_type: &str) -> Result<String, JsError> {
        nodedb_types::id_gen::generate_by_type(id_type).ok_or_else(|| {
            JsError::new(&format!(
                "unknown ID type '{id_type}': use uuidv7, uuidv4, ulid, cuid2, or nanoid"
            ))
        })
    }
}
