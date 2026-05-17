// SPDX-License-Identifier: Apache-2.0
//! Set operations for the Document engine physical visitor.

use std::collections::HashMap;

use nodedb_types::result::QueryResult;
use nodedb_types::value::Value;

use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::query::value_utils::value_to_string;
use crate::storage::engine::{StorageEngine, StorageEngineSync};

use super::is_strict;
use super::reads::loro_value_to_ndb_value;
use super::writes::batch_insert;

/// InsertSelect: copy documents from source to target collection.
///
/// Scans all documents in `source_collection` up to `source_limit`, then
/// batch-inserts them into `target_collection`. Source filters are not
/// evaluated — all documents are copied. Callers that need filtered
/// copying should apply a Scan + BatchInsert composition.
pub async fn insert_select<S: StorageEngine + StorageEngineSync>(
    engine: &LiteQueryEngine<S>,
    target_collection: &str,
    source_collection: &str,
    source_limit: usize,
) -> Result<QueryResult, LiteError> {
    let documents: Vec<(String, Vec<u8>)> = if is_strict(engine, source_collection) {
        let schema =
            engine
                .strict
                .schema(source_collection)
                .ok_or_else(|| LiteError::BadRequest {
                    detail: format!(
                        "strict source collection '{source_collection}' does not exist"
                    ),
                })?;
        let pk_idx = schema
            .columns
            .iter()
            .position(|c| c.primary_key)
            .ok_or_else(|| LiteError::BadRequest {
                detail: format!(
                    "strict source collection '{source_collection}' has no primary key"
                ),
            })?;
        let columns = schema.columns.clone();
        let all_rows = engine.strict.list_rows(source_collection).await?;
        let mut docs = Vec::with_capacity(all_rows.len().min(source_limit));
        for row in all_rows.into_iter().take(source_limit) {
            let pk = value_to_string(&row[pk_idx]);
            let map: HashMap<String, Value> = columns
                .iter()
                .enumerate()
                .filter_map(|(i, col)| {
                    if i < row.len() {
                        Some((col.name.clone(), row[i].clone()))
                    } else {
                        None
                    }
                })
                .collect();
            let bytes = zerompk::to_msgpack_vec(&Value::Object(map)).map_err(|e| {
                LiteError::Serialization {
                    detail: format!("serialize source row: {e}"),
                }
            })?;
            docs.push((pk, bytes));
        }
        docs
    } else {
        let crdt = engine.crdt.lock().map_err(|_| LiteError::LockPoisoned)?;
        let ids = crdt.list_ids(source_collection);
        let mut docs = Vec::with_capacity(ids.len().min(source_limit));
        for id in ids.into_iter().take(source_limit) {
            if let Some(val) = crdt.read(source_collection, &id) {
                let ndb_val = loro_value_to_ndb_value(&val);
                let bytes =
                    zerompk::to_msgpack_vec(&ndb_val).map_err(|e| LiteError::Serialization {
                        detail: format!("serialize crdt source row: {e}"),
                    })?;
                docs.push((id, bytes));
            }
        }
        drop(crdt);
        docs
    };

    batch_insert(engine, target_collection, &documents).await
}
