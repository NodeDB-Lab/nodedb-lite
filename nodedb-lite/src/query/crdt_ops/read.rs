// SPDX-License-Identifier: Apache-2.0
//! CRDT read and policy-read handlers.

use nodedb_types::result::QueryResult;
use nodedb_types::value::Value;

use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::storage::engine::StorageEngine;

/// Read the current CRDT state of a document.
pub async fn handle_read<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    collection: &str,
    document_id: &str,
) -> Result<QueryResult, LiteError> {
    let crdt = engine.crdt.lock().map_err(|_| LiteError::LockPoisoned)?;
    let value = crdt.read(collection, document_id);
    let row = match value {
        Some(v) => {
            let json = sonic_rs::to_string(&v).map_err(|e| LiteError::Serialization {
                detail: format!("CRDT read serialize: {e}"),
            })?;
            vec![vec![Value::String(json)]]
        }
        None => vec![],
    };
    Ok(QueryResult {
        columns: vec!["document".to_string()],
        rows: row,
        rows_affected: 0,
    })
}

/// Read the conflict resolution policy for a collection.
pub async fn handle_get_policy<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    collection: &str,
) -> Result<QueryResult, LiteError> {
    let crdt = engine.crdt.lock().map_err(|_| LiteError::LockPoisoned)?;
    let policy = crdt.policies().get_owned(collection);
    let json = sonic_rs::to_string(&policy).map_err(|e| LiteError::Serialization {
        detail: format!("GetPolicy serialize: {e}"),
    })?;
    Ok(QueryResult {
        columns: vec!["policy_json".to_string()],
        rows: vec![vec![Value::String(json)]],
        rows_affected: 0,
    })
}
