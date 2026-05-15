//! Columnar-engine DML dispatch for the Lite query layer.
//!
//! INSERT for columnar collections converts SQL values to `nodedb_types::Value`
//! in schema column order, then delegates to `ColumnarEngine::insert`.

use std::sync::Arc;

use nodedb_sql::types::SqlValue;
use nodedb_types::result::QueryResult;

use crate::engine::columnar::ColumnarEngine;
use crate::error::LiteError;
use crate::storage::engine::StorageEngine;

use super::coerce::build_row;

/// Insert rows into a columnar collection.
///
/// Each `row` is a list of `(column_name, SqlValue)` pairs. Values are
/// coerced to match the schema column type and ordered by schema position.
pub fn insert_columnar<S: StorageEngine>(
    columnar: &Arc<ColumnarEngine<S>>,
    collection: &str,
    rows: &[Vec<(String, SqlValue)>],
) -> Result<QueryResult, LiteError> {
    let schema = columnar
        .schema(collection)
        .ok_or_else(|| LiteError::BadRequest {
            detail: format!("columnar collection '{collection}' does not exist"),
        })?;

    let mut affected: u64 = 0;
    for row_pairs in rows {
        let values = build_row(row_pairs, &schema.columns)?;
        columnar
            .insert(collection, &values)
            .map_err(|e| LiteError::BadRequest {
                detail: format!("columnar insert: {e}"),
            })?;
        affected += 1;
    }

    Ok(QueryResult {
        columns: Vec::new(),
        rows: Vec::new(),
        rows_affected: affected,
    })
}
