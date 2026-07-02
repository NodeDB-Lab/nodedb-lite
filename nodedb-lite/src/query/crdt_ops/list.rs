// SPDX-License-Identifier: Apache-2.0
//! CRDT LoroMovableList operation handlers: insert, delete, move.

use nodedb_types::result::QueryResult;

use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::storage::engine::StorageEngine;

/// Insert a block into a document's LoroMovableList at the given index.
///
/// `fields_json` is a JSON object; each key-value pair becomes a field on
/// a new LoroMap container inserted at `index`.
pub async fn handle_list_insert<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    collection: &str,
    document_id: &str,
    list_path: &str,
    index: usize,
    fields_json: &str,
) -> Result<QueryResult, LiteError> {
    let fields: sonic_rs::Value =
        sonic_rs::from_str(fields_json).map_err(|e| LiteError::BadRequest {
            detail: format!("ListInsert: invalid fields_json: {e}"),
        })?;

    let mut crdt = engine.crdt.lock().map_err(|_| LiteError::LockPoisoned)?;
    crdt.list_insert(collection, document_id, list_path, index, &fields)
        .map_err(|e| LiteError::Storage {
            detail: format!("ListInsert: {e}"),
        })?;

    Ok(QueryResult {
        columns: vec![],
        rows: vec![],
        rows_affected: 1,
    })
}

/// Delete a block from a document's LoroMovableList at the given index.
pub async fn handle_list_delete<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    collection: &str,
    document_id: &str,
    list_path: &str,
    index: usize,
) -> Result<QueryResult, LiteError> {
    let mut crdt = engine.crdt.lock().map_err(|_| LiteError::LockPoisoned)?;
    crdt.list_delete(collection, document_id, list_path, index)
        .map_err(|e| LiteError::Storage {
            detail: format!("ListDelete: {e}"),
        })?;

    Ok(QueryResult {
        columns: vec![],
        rows: vec![],
        rows_affected: 1,
    })
}

/// Move a block within a document's LoroMovableList from one index to another.
pub async fn handle_list_move<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    collection: &str,
    document_id: &str,
    list_path: &str,
    from_index: usize,
    to_index: usize,
) -> Result<QueryResult, LiteError> {
    let mut crdt = engine.crdt.lock().map_err(|_| LiteError::LockPoisoned)?;
    crdt.list_move(collection, document_id, list_path, from_index, to_index)
        .map_err(|e| LiteError::Storage {
            detail: format!("ListMove: {e}"),
        })?;

    Ok(QueryResult {
        columns: vec![],
        rows: vec![],
        rows_affected: 1,
    })
}
