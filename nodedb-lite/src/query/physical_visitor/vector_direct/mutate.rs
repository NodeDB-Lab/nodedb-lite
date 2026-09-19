// SPDX-License-Identifier: Apache-2.0
//! Vector-primary `DirectDelete` and `DirectUpdate`.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use nodedb_physical::physical_plan::{UpdateValue, VectorWriteTargets};
use nodedb_query::scan_filter::ScanFilter;
use nodedb_types::result::QueryResult;
use nodedb_types::value::Value;

use crate::engine::crdt::CrdtEngine;
use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::query::scan_filter_convert::decode_scan_filters;
use crate::storage::engine::StorageEngine;

use super::super::adapter::LitePhysicalFut;
use super::common::{
    EMBEDDING_DIM_FIELD, StoredRow, apply_patch, delete_row, index_key, insert_node, read_all_rows,
    read_row, remove_durable, remove_live_node, write_row,
};

/// The stored rows a write targets, as `(doc_id, row)`.
///
/// Surrogates resolve through their text form, the only surrogate binding
/// Lite keeps; a key with no stored row matches nothing. A predicate scans
/// every stored row of the collection and keeps the ones the filters admit.
fn resolve_targets(
    crdt: &Mutex<CrdtEngine>,
    collection: &str,
    targets: &VectorWriteTargets,
) -> Result<Vec<StoredRow>, LiteError> {
    match targets {
        VectorWriteTargets::Surrogates(surrogates) => Ok(surrogates
            .iter()
            .map(|s| s.to_string())
            .filter_map(|doc_id| read_row(crdt, collection, &doc_id).map(|row| (doc_id, row)))
            .collect()),
        VectorWriteTargets::Predicate(bytes) => {
            let filters = decode_scan_filters(bytes)?;
            let mut hits = Vec::new();
            for (doc_id, row) in read_all_rows(crdt, collection) {
                let doc = Value::Object(row);
                if ScanFilter::all_match_value(&filters, &doc)?
                    && let Value::Object(row) = doc
                {
                    hits.push((doc_id, row));
                }
            }
            Ok(hits)
        }
    }
}

/// Remove the node, durable vector, and payload row of every targeted
/// surrogate. Reports the number of rows that existed.
pub(in crate::query::physical_visitor) fn vector_direct_delete<'a, S>(
    engine: &'a LiteQueryEngine<S>,
    collection: String,
    field: String,
    targets: VectorWriteTargets,
) -> LitePhysicalFut<'a>
where
    S: StorageEngine + 'a,
{
    let key = index_key(&collection, &field);
    let vector_state = Arc::clone(&engine.vector_state);
    let crdt = Arc::clone(&engine.crdt);
    Box::pin(async move {
        let hits = resolve_targets(&crdt, &collection, &targets)?;
        let mut removed = 0u64;
        for (doc_id, _) in hits {
            remove_live_node(&vector_state, &key, &doc_id);
            remove_durable(&vector_state, &key, &doc_id, "DirectDelete").await?;
            if delete_row(&crdt, &collection, &doc_id, "DirectDelete")? {
                removed += 1;
            }
        }
        Ok(QueryResult {
            columns: vec![],
            rows: vec![],
            rows_affected: removed,
        })
    })
}

/// Inputs of [`vector_direct_update`].
pub(in crate::query::physical_visitor) struct DirectUpdateArgs {
    pub collection: String,
    pub field: String,
    pub targets: VectorWriteTargets,
    /// Replacement vector, when the statement assigns the vector column.
    pub new_vector: Option<Vec<f32>>,
    /// Assignments to non-vector columns, merged into the stored row.
    pub payload_patch: Vec<(String, UpdateValue)>,
}

/// Re-embed and/or patch every targeted row. A `new_vector` rebuilds the
/// node under the same identity; the patch merges into the stored row.
/// Reports the number of rows that existed.
pub(in crate::query::physical_visitor) fn vector_direct_update<'a, S>(
    engine: &'a LiteQueryEngine<S>,
    args: DirectUpdateArgs,
) -> Result<LitePhysicalFut<'a>, LiteError>
where
    S: StorageEngine + 'a,
{
    let DirectUpdateArgs {
        collection,
        field,
        targets,
        new_vector,
        payload_patch,
    } = args;
    if let Some(vector) = &new_vector
        && vector.is_empty()
    {
        return Err(LiteError::BadRequest {
            detail: "DirectUpdate: the assigned vector is empty".into(),
        });
    }
    let key = index_key(&collection, &field);
    let vector_state = Arc::clone(&engine.vector_state);
    let crdt = Arc::clone(&engine.crdt);
    Ok(Box::pin(async move {
        let hits = resolve_targets(&crdt, &collection, &targets)?;
        let mut updated = 0u64;
        for (doc_id, mut row) in hits {
            let dim = match &new_vector {
                Some(vector) => {
                    remove_live_node(&vector_state, &key, &doc_id);
                    insert_node(&vector_state, &key, &doc_id, vector, "DirectUpdate").await?;
                    vector.len()
                }
                None => stored_dim(&row),
            };
            // The patch never targets the vector column: the planner routes
            // that assignment through `new_vector`.
            let excluded = HashMap::new();
            apply_patch(&mut row, &payload_patch, &excluded)?;
            write_row(&crdt, &collection, &doc_id, dim, &row, "DirectUpdate")?;
            updated += 1;
        }
        Ok(QueryResult {
            columns: vec![],
            rows: vec![],
            rows_affected: updated,
        })
    }))
}

/// The dimension the stored row records, or 0 when the row carries none.
fn stored_dim(row: &HashMap<String, Value>) -> usize {
    match row.get(EMBEDDING_DIM_FIELD) {
        Some(Value::Integer(n)) if *n > 0 => *n as usize,
        _ => 0,
    }
}
