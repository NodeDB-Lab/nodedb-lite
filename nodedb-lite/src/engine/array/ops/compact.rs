// SPDX-License-Identifier: Apache-2.0

//! `ArrayOp::Compact` handler for NodeDB-Lite.
//!
//! Delegates to `crate::engine::array::retention::run_retention` to merge
//! out-of-horizon tile-versions per segment and rewrite the manifest.
//! When `audit_retain_ms` is `None` the array has no retention policy and
//! compact is a no-op (returns `rows_affected = 0`).

use std::sync::{Arc, Mutex};

use nodedb_types::result::QueryResult;

use crate::engine::array::engine::ArrayEngineState;
use crate::engine::array::ops::util::time::now_ms;
use crate::error::LiteError;
use crate::storage::engine::StorageEngineSync;

/// Execute `ArrayOp::Compact` for the Lite engine.
///
/// If `audit_retain_ms` is `Some`, runs retention merge across every segment
/// in the manifest and updates the manifest. `rows_affected` is set to the
/// number of segments rewritten.
///
/// If `audit_retain_ms` is `None`, no merge is needed and the call returns
/// immediately with `rows_affected = 0`.
pub async fn compact<S: StorageEngineSync>(
    array_state: &Arc<Mutex<ArrayEngineState>>,
    storage: &Arc<S>,
    name: &str,
    audit_retain_ms: Option<i64>,
) -> Result<QueryResult, LiteError> {
    let retain_ms = match audit_retain_ms {
        Some(r) => r,
        None => {
            return Ok(QueryResult {
                columns: vec!["segments_rewritten".to_string()],
                rows: vec![vec![nodedb_types::value::Value::Integer(0)]],
                rows_affected: 0,
            });
        }
    };

    let now_ms = now_ms();

    let rewritten = {
        let mut state = array_state.lock().map_err(|_| LiteError::LockPoisoned)?;
        let arr = state
            .arrays
            .get_mut(name)
            .ok_or_else(|| LiteError::BadRequest {
                detail: format!("array '{name}' not found"),
            })?;
        let schema = arr.schema.clone();
        let schema_hash = arr.schema_hash;
        crate::engine::array::retention::run_retention(
            storage,
            name,
            &mut arr.manifest,
            &schema,
            schema_hash,
            retain_ms,
            now_ms,
        )?
    };

    Ok(QueryResult {
        columns: vec!["segments_rewritten".to_string()],
        rows: vec![vec![nodedb_types::value::Value::Integer(rewritten as i64)]],
        rows_affected: rewritten as u64,
    })
}
