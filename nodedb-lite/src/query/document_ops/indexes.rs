// SPDX-License-Identifier: Apache-2.0
//! Index management operations for the Document engine physical visitor.

use nodedb_types::Namespace;
use nodedb_types::result::QueryResult;

use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::query::value_utils::{loro_value_to_string, value_to_string};
use crate::storage::engine::{StorageEngine, StorageEngineSync, WriteOp};

use super::is_strict;
use super::reads::index_insert_id;

/// Register: initialize a collection in the appropriate engine.
///
/// For strict collections (`StorageMode::Strict`), the schema is persisted to
/// the strict engine. For schemaless collections, this is a no-op — CRDT
/// collections are discovered on first write.
pub async fn register<S: StorageEngine + StorageEngineSync>(
    engine: &LiteQueryEngine<S>,
    collection: &str,
    storage_mode: &nodedb_physical::physical_plan::document::types::StorageMode,
) -> Result<QueryResult, LiteError> {
    use nodedb_physical::physical_plan::document::types::StorageMode;
    match storage_mode {
        StorageMode::Strict { schema } => {
            engine
                .strict
                .create_collection(collection, schema.clone())
                .await?;
        }
        StorageMode::Schemaless => {
            // Schemaless collections are auto-discovered — no registration needed.
        }
    }
    Ok(QueryResult {
        columns: Vec::new(),
        rows: Vec::new(),
        rows_affected: 0,
    })
}

/// DropIndex: remove all sparse-index entries for a field on a collection.
pub fn drop_index<S: StorageEngine + StorageEngineSync>(
    engine: &LiteQueryEngine<S>,
    collection: &str,
    field: &str,
) -> Result<QueryResult, LiteError> {
    let prefix = format!("{collection}:{field}:");
    let entries = engine.storage.scan_range_bounded_sync(
        Namespace::Meta,
        Some(prefix.as_bytes()),
        None,
        None,
    )?;
    let mut ops: Vec<WriteOp> = Vec::with_capacity(entries.len());
    for (key, _) in entries {
        if key.starts_with(prefix.as_bytes()) {
            ops.push(WriteOp::Delete {
                ns: Namespace::Meta,
                key,
            });
        }
    }
    let count = ops.len() as u64;
    if !ops.is_empty() {
        engine.storage.batch_write_sync(&ops)?;
    }
    Ok(QueryResult {
        columns: Vec::new(),
        rows: Vec::new(),
        rows_affected: count,
    })
}

/// BackfillIndex: rebuild a secondary index from existing collection documents.
pub async fn backfill_index<S: StorageEngine + StorageEngineSync>(
    engine: &LiteQueryEngine<S>,
    collection: &str,
    path: &str,
) -> Result<QueryResult, LiteError> {
    let mut indexed: u64 = 0;

    if is_strict(engine, collection) {
        let schema = engine
            .strict
            .schema(collection)
            .ok_or_else(|| LiteError::BadRequest {
                detail: format!("strict collection '{collection}' does not exist"),
            })?;
        let bare = bare_path(path);
        let col_idx = schema
            .columns
            .iter()
            .position(|c| c.name == bare || c.name == path);
        let pk_idx = schema
            .columns
            .iter()
            .position(|c| c.primary_key)
            .ok_or_else(|| LiteError::BadRequest {
                detail: format!("strict collection '{collection}' has no primary key"),
            })?;
        let all_rows = engine.strict.list_rows(collection).await?;

        for row in &all_rows {
            let pk = value_to_string(&row[pk_idx]);
            if let Some(idx) = col_idx
                && idx < row.len()
            {
                let val_str = value_to_string(&row[idx]);
                index_insert_id(engine, collection, path, &val_str, &pk)?;
                indexed += 1;
            }
        }
    } else {
        let crdt = engine.crdt.lock().map_err(|_| LiteError::LockPoisoned)?;
        let ids = crdt.list_ids(collection);
        let bare = bare_path(path);
        let mut pairs: Vec<(String, String)> = Vec::new();
        for id in &ids {
            if let Some(val) = crdt.read(collection, id)
                && let loro::LoroValue::Map(map) = &val
                && let Some(field_val) = map.get(bare)
            {
                let val_str = loro_value_to_string(field_val);
                pairs.push((id.clone(), val_str));
            }
        }
        drop(crdt);
        for (id, val_str) in &pairs {
            index_insert_id(engine, collection, path, val_str, id)?;
            indexed += 1;
        }
    }

    Ok(QueryResult {
        columns: Vec::new(),
        rows: Vec::new(),
        rows_affected: indexed,
    })
}

/// Strip `$.` prefix from a JSON path expression to get the bare field name.
fn bare_path(path: &str) -> &str {
    path.trim_start_matches("$.").trim_start_matches('$')
}
