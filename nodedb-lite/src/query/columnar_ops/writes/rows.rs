// SPDX-License-Identifier: Apache-2.0
//! Stored-row lookup by primary key and `ON CONFLICT DO UPDATE` merging.

use nodedb_types::value::Value;

use crate::error::LiteError;
use crate::query::assignments::resolve_updates_with_excluded;
use crate::query::engine::LiteQueryEngine;
use crate::storage::engine::StorageEngine;

use super::super::reads::row_to_object;

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
///
/// Each assignment evaluates against the existing row, with `EXCLUDED.col`
/// bound to `incoming`, the row the INSERT proposed.
pub(super) fn apply_conflict_updates(
    mut existing: Vec<Value>,
    incoming: &Value,
    updates: &[(String, UpdateValue)],
    col_names: &[String],
) -> Result<Vec<Value>, LiteError> {
    let current = row_to_object(col_names, &existing);
    let resolved = resolve_updates_with_excluded(updates, &current, incoming)?;
    for (field, value) in resolved {
        if let Some(col_idx) = col_names.iter().position(|n| *n == field)
            && let Some(slot) = existing.get_mut(col_idx)
        {
            *slot = value;
        }
    }
    Ok(existing)
}
