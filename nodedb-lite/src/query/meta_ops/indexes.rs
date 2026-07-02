// SPDX-License-Identifier: Apache-2.0
//! Index meta-ops: RebuildIndex.

use nodedb_types::result::QueryResult;
use nodedb_types::value::Value;

use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::storage::engine::StorageEngine;

/// `RebuildIndex` — re-emit index entries by scanning collection rows.
///
/// For Lite, index backfill is already handled by the per-engine backfill
/// helpers (see `DocumentOp::BackfillIndex`). This meta-op triggers a
/// logical rebuild by delegating to the document engine's backfill path when
/// `index_name` is specified, or scanning all collections if `None`.
///
/// The `concurrent` flag is ignored on Lite (single-threaded embedded engine).
pub async fn handle_rebuild_index<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    collection: &str,
    index_name: Option<&str>,
    _concurrent: bool,
) -> Result<QueryResult, LiteError> {
    if let Some(field) = index_name {
        // Delegate to the document backfill path.
        crate::query::document_ops::indexes::backfill_index(engine, collection, field).await?;
        Ok(QueryResult {
            columns: vec!["rebuilt".into()],
            rows: vec![vec![Value::String(format!(
                "index '{field}' on '{collection}' rebuilt"
            ))]],
            rows_affected: 1,
        })
    } else {
        // No specific index: nothing to do without an explicit field name.
        Ok(QueryResult {
            columns: vec!["rebuilt".into()],
            rows: vec![vec![Value::String(format!(
                "no index name specified for '{collection}' — nothing rebuilt"
            ))]],
            rows_affected: 0,
        })
    }
}
