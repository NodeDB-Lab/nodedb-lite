// SPDX-License-Identifier: Apache-2.0
//! Stored-row lookup by primary key and `ON CONFLICT DO UPDATE` merging.

use nodedb_types::value::Value;

use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::storage::engine::StorageEngine;

/// The stored row whose primary key equals `pk`, or `None`.
///
/// Reads the whole collection through `list_rows`: Lite's columnar engine
/// keeps its rows in memory, so the scan is bounded by the memtable plus
/// flushed segments.
pub(super) async fn find_row<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    collection: &str,
    pk: &Value,
) -> Result<Option<Vec<Value>>, LiteError> {
    let schema = match engine.columnar.schema(collection) {
        Some(s) => s,
        None => return Ok(None),
    };
    let pk_idx = schema
        .columns
        .iter()
        .position(|c| c.primary_key)
        .unwrap_or(0);
    let rows = engine.columnar.list_rows(collection).await?;
    Ok(rows
        .into_iter()
        .find(|row| row.get(pk_idx).map(|v| v == pk).unwrap_or(false)))
}

type UpdateValue = nodedb_physical::physical_plan::document::types::UpdateValue;

/// Apply `ON CONFLICT DO UPDATE` assignments to an existing row.
pub(super) fn apply_conflict_updates(
    mut existing: Vec<Value>,
    incoming: &Value,
    updates: &[(String, UpdateValue)],
    col_names: &[String],
) -> Result<Vec<Value>, LiteError> {
    for (field, update_val) in updates {
        let new_val = match update_val {
            UpdateValue::Literal(bytes) => {
                zerompk::from_msgpack::<Value>(bytes).unwrap_or(Value::Null)
            }
            UpdateValue::Expr(expr) => {
                // Evaluate expr against the existing row document. The expr
                // may reference EXCLUDED columns via the incoming object; we
                // use the incoming value for the target field as a safe fallback
                // when the expr cannot be fully resolved.
                let evaled = expr.eval(incoming)?;
                if matches!(evaled, Value::Null) {
                    incoming.get(field).cloned().unwrap_or(Value::Null)
                } else {
                    evaled
                }
            }
        };
        if let Some(col_idx) = col_names.iter().position(|n| n == field)
            && col_idx < existing.len()
        {
            existing[col_idx] = new_val;
        }
    }
    Ok(existing)
}
