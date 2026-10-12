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
    handle_list_insert_coordinated(
        engine,
        None,
        collection,
        document_id,
        list_path,
        index,
        fields_json,
    )
    .await
}

pub(crate) async fn handle_list_insert_coordinated<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    permit: Option<&crate::engine::fts::coordinator::TextMutationPermit>,
    collection: &str,
    document_id: &str,
    list_path: &str,
    index: usize,
    fields_json: &str,
) -> Result<QueryResult, LiteError> {
    if let Some(permit) = permit {
        return handle_list_insert_admitted(
            engine,
            permit,
            collection,
            document_id,
            list_path,
            index,
            fields_json,
        )
        .await;
    }
    let guard = engine.fts_state.admit_mutation().await;
    let result = handle_list_insert_admitted(
        engine,
        guard.permit(),
        collection,
        document_id,
        list_path,
        index,
        fields_json,
    )
    .await;
    guard.finish(result)
}

pub(crate) async fn handle_list_insert_admitted<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    permit: &crate::engine::fts::coordinator::TextMutationPermit,
    collection: &str,
    document_id: &str,
    list_path: &str,
    index: usize,
    fields_json: &str,
) -> Result<QueryResult, LiteError> {
    let _permit = permit;
    let fields: sonic_rs::Value =
        sonic_rs::from_str(fields_json).map_err(|e| LiteError::BadRequest {
            detail: format!("ListInsert: invalid fields_json: {e}"),
        })?;

    let mut crdt = engine.crdt.lock().map_err(|_| LiteError::LockPoisoned)?;
    crdt.list_insert(collection, document_id, list_path, index, &fields)?;

    Ok(QueryResult {
        columns: vec![],
        rows: vec![],
        rows_affected: 1,
        command: None,
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
    handle_list_delete_coordinated(engine, None, collection, document_id, list_path, index).await
}

pub(crate) async fn handle_list_delete_coordinated<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    permit: Option<&crate::engine::fts::coordinator::TextMutationPermit>,
    collection: &str,
    document_id: &str,
    list_path: &str,
    index: usize,
) -> Result<QueryResult, LiteError> {
    if let Some(permit) = permit {
        return handle_list_delete_admitted(
            engine,
            permit,
            collection,
            document_id,
            list_path,
            index,
        )
        .await;
    }
    let guard = engine.fts_state.admit_mutation().await;
    let result = handle_list_delete_admitted(
        engine,
        guard.permit(),
        collection,
        document_id,
        list_path,
        index,
    )
    .await;
    guard.finish(result)
}

pub(crate) async fn handle_list_delete_admitted<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    permit: &crate::engine::fts::coordinator::TextMutationPermit,
    collection: &str,
    document_id: &str,
    list_path: &str,
    index: usize,
) -> Result<QueryResult, LiteError> {
    let _permit = permit;
    let mut crdt = engine.crdt.lock().map_err(|_| LiteError::LockPoisoned)?;
    crdt.list_delete(collection, document_id, list_path, index)?;

    Ok(QueryResult {
        columns: vec![],
        rows: vec![],
        rows_affected: 1,
        command: None,
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
    handle_list_move_coordinated(
        engine,
        None,
        collection,
        document_id,
        list_path,
        from_index,
        to_index,
    )
    .await
}

pub(crate) async fn handle_list_move_coordinated<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    permit: Option<&crate::engine::fts::coordinator::TextMutationPermit>,
    collection: &str,
    document_id: &str,
    list_path: &str,
    from_index: usize,
    to_index: usize,
) -> Result<QueryResult, LiteError> {
    if let Some(permit) = permit {
        return handle_list_move_admitted(
            engine,
            permit,
            collection,
            document_id,
            list_path,
            from_index,
            to_index,
        )
        .await;
    }
    let guard = engine.fts_state.admit_mutation().await;
    let result = handle_list_move_admitted(
        engine,
        guard.permit(),
        collection,
        document_id,
        list_path,
        from_index,
        to_index,
    )
    .await;
    guard.finish(result)
}

pub(crate) async fn handle_list_move_admitted<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    permit: &crate::engine::fts::coordinator::TextMutationPermit,
    collection: &str,
    document_id: &str,
    list_path: &str,
    from_index: usize,
    to_index: usize,
) -> Result<QueryResult, LiteError> {
    let _permit = permit;
    let mut crdt = engine.crdt.lock().map_err(|_| LiteError::LockPoisoned)?;
    crdt.list_move(collection, document_id, list_path, from_index, to_index)?;

    Ok(QueryResult {
        columns: vec![],
        rows: vec![],
        rows_affected: 1,
        command: None,
    })
}
