// SPDX-License-Identifier: Apache-2.0
//! Lifecycle meta-ops: snapshot, compact, checkpoint, unregister, rename, convert.

use nodedb_types::Namespace;
use nodedb_types::columnar::{ColumnDef, StrictSchema};
use nodedb_types::result::QueryResult;
use nodedb_types::value::Value;

use crate::error::LiteError;
use crate::query::ddl::default_convert_schema;
use crate::query::engine::LiteQueryEngine;
use crate::storage::engine::StorageEngine;

/// Reject snapshots because the Lite storage interface has no snapshot API.
pub async fn handle_create_snapshot<S: StorageEngine>(
    _engine: &LiteQueryEngine<S>,
) -> Result<QueryResult, LiteError> {
    Err(LiteError::Unsupported {
        detail: "MetaOp::CreateSnapshot requires an Origin WAL manager and snapshot API".into(),
    })
}

/// Reject compaction because the Lite storage interface has no compact API.
pub async fn handle_compact<S: StorageEngine>(
    _engine: &LiteQueryEngine<S>,
) -> Result<QueryResult, LiteError> {
    Err(LiteError::Unsupported {
        detail: "MetaOp::Compact requires an Origin compaction manager and compact API".into(),
    })
}

/// `Checkpoint` — report a logical LSN of 0 (Lite is single-node, no WAL LSN).
pub async fn handle_checkpoint<S: StorageEngine>(
    _engine: &LiteQueryEngine<S>,
) -> Result<QueryResult, LiteError> {
    Ok(QueryResult {
        columns: vec!["lsn".into()],
        rows: vec![vec![Value::Integer(0)]],
        rows_affected: 0,
        command: None,
    })
}

/// `UnregisterCollection` — drop all storage entries for a collection.
///
/// Scans `Namespace::Meta` for keys prefixed with `collection/<name>` and
/// deletes them. The collection name is the `name` field from the op.
pub async fn handle_unregister_collection<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    _tenant_id: u64,
    name: &str,
    _purge_lsn: u64,
) -> Result<QueryResult, LiteError> {
    handle_unregister_collection_coordinated(engine, None, _tenant_id, name, _purge_lsn).await
}

pub(crate) async fn handle_unregister_collection_coordinated<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    permit: Option<&crate::engine::fts::coordinator::TextMutationPermit>,
    _tenant_id: u64,
    name: &str,
    _purge_lsn: u64,
) -> Result<QueryResult, LiteError> {
    if let Some(permit) = permit {
        return handle_unregister_collection_admitted(engine, permit, _tenant_id, name, _purge_lsn)
            .await;
    }
    let guard = engine.fts_state.admit_mutation().await;
    let result =
        handle_unregister_collection_admitted(engine, guard.permit(), _tenant_id, name, _purge_lsn)
            .await;
    guard.finish(result)
}

pub(crate) async fn handle_unregister_collection_admitted<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    permit: &crate::engine::fts::coordinator::TextMutationPermit,
    _tenant_id: u64,
    name: &str,
    _purge_lsn: u64,
) -> Result<QueryResult, LiteError> {
    let _permit = permit;
    engine
        .fts_state
        .persist_collection_tombstone(&*engine.storage, name, permit)
        .await?;
    let prefix = format!("collection/{name}");
    let pairs = engine
        .storage
        .scan_prefix(Namespace::Meta, prefix.as_bytes())
        .await?;
    let mut deleted: u64 = 0;
    for (key, _) in &pairs {
        engine.storage.delete(Namespace::Meta, key).await?;
        deleted += 1;
    }
    // Also drop from CRDT engine if present.
    {
        let mut crdt = engine.crdt.lock().map_err(|_| LiteError::LockPoisoned)?;
        let ids = crdt.list_ids(name);
        for id in &ids {
            crdt.delete(name, id).map_err(|e| LiteError::Storage {
                detail: format!("UnregisterCollection: delete CRDT doc '{id}': {e}"),
            })?;
        }
    }
    // The collection's documents are gone, and so is every index over them.
    engine
        .indexes
        .drop_collection(&*engine.storage, name)
        .await?;
    engine
        .fts_state
        .manager
        .lock()
        .map_err(|_| LiteError::LockPoisoned)?
        .drop_collection(name);
    Ok(QueryResult {
        columns: vec!["deleted_entries".into()],
        rows: vec![vec![Value::Integer(deleted as i64)]],
        rows_affected: deleted,
        command: None,
    })
}

/// `UnregisterMaterializedView` — remove materialized-view metadata entries.
pub async fn handle_unregister_materialized_view<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    _tenant_id: u64,
    name: &str,
) -> Result<QueryResult, LiteError> {
    let prefix = format!("mv/{name}");
    let pairs = engine
        .storage
        .scan_prefix(Namespace::Meta, prefix.as_bytes())
        .await?;
    let mut deleted: u64 = 0;
    for (key, _) in &pairs {
        engine.storage.delete(Namespace::Meta, key).await?;
        deleted += 1;
    }
    Ok(QueryResult {
        columns: vec!["deleted_entries".into()],
        rows: vec![vec![Value::Integer(deleted as i64)]],
        rows_affected: deleted,
        command: None,
    })
}

/// `ConvertCollection` — run the same conversion the DDL path runs.
///
/// `target_type` is one of `"document_schemaless"`, `"document_strict"`,
/// `"columnar"`. `schema_json` is a JSON `Vec<ColumnDef>`; empty means the
/// default convert schema.
pub async fn handle_convert_collection<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    collection: &str,
    target_type: &str,
    schema_json: &str,
) -> Result<QueryResult, LiteError> {
    handle_convert_collection_coordinated(engine, None, collection, target_type, schema_json).await
}

pub(crate) async fn handle_convert_collection_coordinated<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    permit: Option<&crate::engine::fts::coordinator::TextMutationPermit>,
    collection: &str,
    target_type: &str,
    schema_json: &str,
) -> Result<QueryResult, LiteError> {
    if let Some(permit) = permit {
        return handle_convert_collection_admitted(
            engine,
            permit,
            collection,
            target_type,
            schema_json,
        )
        .await;
    }
    let guard = engine.fts_state.admit_mutation().await;
    let result = handle_convert_collection_admitted(
        engine,
        guard.permit(),
        collection,
        target_type,
        schema_json,
    )
    .await;
    guard.finish(result)
}

pub(crate) async fn handle_convert_collection_admitted<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    permit: &crate::engine::fts::coordinator::TextMutationPermit,
    collection: &str,
    target_type: &str,
    schema_json: &str,
) -> Result<QueryResult, LiteError> {
    let _permit = permit;
    crate::engine::fts::checkpoint::persist_checkpoint_incomplete(&*engine.storage).await?;
    let target_schema = if schema_json.is_empty() {
        default_convert_schema()
    } else {
        let columns: Vec<ColumnDef> =
            sonic_rs::from_str(schema_json).map_err(|e| LiteError::BadRequest {
                detail: format!("ConvertCollection: schema_json decode: {e}"),
            })?;
        StrictSchema {
            columns,
            version: 1,
            dropped_columns: Vec::new(),
            bitemporal: false,
        }
    };
    match target_type {
        "document_strict" | "strict" => {
            engine
                .convert_to_strict_admitted(permit, collection, target_schema)
                .await
        }
        "document_schemaless" | "document" => {
            engine
                .convert_to_document_admitted(permit, collection)
                .await
        }
        "columnar" => {
            engine
                .convert_to_columnar_admitted(permit, collection, target_schema)
                .await
        }
        other => Err(LiteError::BadRequest {
            detail: format!(
                "ConvertCollection: unsupported target_type '{other}'; \
                 accepted values are document_schemaless, document_strict, columnar"
            ),
        }),
    }
}
