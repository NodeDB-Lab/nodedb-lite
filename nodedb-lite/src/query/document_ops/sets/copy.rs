// SPDX-License-Identifier: Apache-2.0
use super::super::is_strict;
use super::super::reads::loro_value_to_ndb_value;
use super::super::writes::batch_insert_admitted;
use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::query::value_utils::value_to_string;
use crate::storage::engine::StorageEngine;
use nodedb_types::result::QueryResult;
use nodedb_types::value::Value;
use std::collections::HashMap;
/// InsertSelect: copy documents from source to target collection.
///
/// Reads the documents of `source_collection` up to `source_limit`, then
/// batch-inserts them into `target_collection`. Every document is copied
/// unchanged. A key the target already stores refuses the whole copy.
pub async fn insert_select<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    target_collection: &str,
    source_collection: &str,
    source_limit: usize,
) -> Result<QueryResult, LiteError> {
    insert_select_coordinated(
        engine,
        None,
        target_collection,
        source_collection,
        source_limit,
    )
    .await
}

pub(crate) async fn insert_select_coordinated<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    permit: Option<&crate::engine::fts::coordinator::TextMutationPermit>,
    target_collection: &str,
    source_collection: &str,
    source_limit: usize,
) -> Result<QueryResult, LiteError> {
    if let Some(permit) = permit {
        return insert_select_admitted(
            engine,
            permit,
            target_collection,
            source_collection,
            source_limit,
        )
        .await;
    }
    let guard = engine.fts_state.admit_mutation().await;
    let result = insert_select_admitted(
        engine,
        guard.permit(),
        target_collection,
        source_collection,
        source_limit,
    )
    .await;
    guard.finish(result)
}

pub(crate) async fn insert_select_admitted<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    permit: &crate::engine::fts::coordinator::TextMutationPermit,
    target_collection: &str,
    source_collection: &str,
    source_limit: usize,
) -> Result<QueryResult, LiteError> {
    let _permit = permit;
    let documents: Vec<(String, Vec<u8>)> = if is_strict(engine, source_collection) {
        let schema = engine
            .strict
            .schema(source_collection)
            .ok_or_else(|| LiteError::collection_not_found("strict", source_collection))?;
        let pk_idx = schema
            .columns
            .iter()
            .position(|c| c.primary_key)
            .ok_or_else(|| LiteError::BadRequest {
                detail: format!(
                    "strict source collection '{source_collection}' has no primary key"
                ),
            })?;
        let columns = schema.columns;
        let all_rows = engine.strict.list_rows(source_collection).await?;
        let mut docs = Vec::with_capacity(all_rows.len().min(source_limit));
        for row in all_rows.into_iter().take(source_limit) {
            let pk = row.get(pk_idx).ok_or_else(|| LiteError::Storage {
                detail: format!("strict collection '{source_collection}' row omits primary key at column {pk_idx}"),
            })?;
            let pk = value_to_string(pk);
            let map: HashMap<String, Value> = columns
                .iter()
                .enumerate()
                .filter_map(|(i, col)| row.get(i).map(|value| (col.name.clone(), value.clone())))
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

    batch_insert_admitted(engine, permit, target_collection, &documents, false).await
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn insert_select_borrows_existing_admission() -> Result<(), Box<dyn std::error::Error>> {
        use nodedb_types::value::Value;
        let storage = crate::PagedbStorageMem::open_in_memory().await?;
        let db = crate::NodeDbLite::open(storage).await?;
        let bytes = zerompk::to_msgpack_vec(&Value::Object(std::collections::HashMap::from([(
            "body".into(),
            Value::String("copied".into()),
        )])))?;
        crate::query::document_ops::writes::point_put(&db.query_engine, "source", "one", &bytes)
            .await?;
        let guard = db.query_engine.fts_state.admit_mutation().await;
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            super::insert_select_coordinated(
                &db.query_engine,
                Some(guard.permit()),
                "target",
                "source",
                10,
            ),
        )
        .await?;
        let result = guard.finish(result)?;
        assert_eq!(result.rows_affected, 1);
        assert!(
            db.query_engine
                .crdt
                .lock()
                .map_err(|_| crate::error::LiteError::LockPoisoned)?
                .exists("target", "one")
        );
        Ok(())
    }
}
