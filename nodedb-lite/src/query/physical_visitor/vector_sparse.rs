// SPDX-License-Identifier: Apache-2.0
//! Sparse inverted-index arms of `VectorOp`: insert, search, delete.

use std::sync::Arc;

use nodedb_types::SparseVector;
use nodedb_types::result::QueryResult;
use nodedb_types::value::Value;

use crate::error::LiteError;
use crate::nodedb::lock_ext::LockExt;
use crate::query::engine::LiteQueryEngine;
use crate::storage::engine::StorageEngine;

use super::adapter::LitePhysicalFut;

pub(super) fn sparse_insert<'a, S>(
    engine: &'a LiteQueryEngine<S>,
    collection: String,
    field_name: String,
    doc_id: String,
    entries: Vec<(u32, f32)>,
) -> Result<LitePhysicalFut<'a>, LiteError>
where
    S: StorageEngine + 'a,
{
    let vector = SparseVector::from_entries(entries).map_err(|e| LiteError::BadRequest {
        detail: format!("SparseInsert: {e}"),
    })?;
    let sparse_state = Arc::clone(&engine.sparse_state);
    Ok(Box::pin(async move {
        sparse_state.manager.lock_or_recover().index_document(
            &collection,
            &field_name,
            &doc_id,
            &vector,
        );
        Ok(QueryResult {
            columns: Vec::new(),
            rows: Vec::new(),
            rows_affected: 1,
        })
    }))
}

pub(super) fn sparse_search<'a, S>(
    engine: &'a LiteQueryEngine<S>,
    collection: String,
    field_name: String,
    query_entries: Vec<(u32, f32)>,
    top_k: usize,
) -> Result<LitePhysicalFut<'a>, LiteError>
where
    S: StorageEngine + 'a,
{
    let query = SparseVector::from_entries(query_entries).map_err(|e| LiteError::BadRequest {
        detail: format!("SparseSearch: {e}"),
    })?;
    let sparse_state = Arc::clone(&engine.sparse_state);
    Ok(Box::pin(async move {
        let hits =
            sparse_state
                .manager
                .lock_or_recover()
                .search(&collection, &field_name, &query, top_k);
        let rows: Vec<Vec<Value>> = hits
            .into_iter()
            .map(|h| vec![Value::String(h.doc_id), Value::Float(h.score as f64)])
            .collect();
        Ok(QueryResult {
            columns: vec!["id".to_string(), "score".to_string()],
            rows,
            rows_affected: 0,
        })
    }))
}

pub(super) fn sparse_delete<'a, S>(
    engine: &'a LiteQueryEngine<S>,
    collection: String,
    field_name: String,
    doc_id: String,
) -> LitePhysicalFut<'a>
where
    S: StorageEngine + 'a,
{
    let sparse_state = Arc::clone(&engine.sparse_state);
    Box::pin(async move {
        let removed = sparse_state.manager.lock_or_recover().remove_document(
            &collection,
            &field_name,
            &doc_id,
        );
        Ok(QueryResult {
            columns: Vec::new(),
            rows: Vec::new(),
            rows_affected: u64::from(removed),
        })
    })
}
