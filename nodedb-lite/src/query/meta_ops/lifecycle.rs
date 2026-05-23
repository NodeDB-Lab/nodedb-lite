// SPDX-License-Identifier: Apache-2.0
//! Lifecycle meta-ops: snapshot, compact, checkpoint, unregister, rename, convert.

use nodedb_types::Namespace;
use nodedb_types::result::QueryResult;
use nodedb_types::value::Value;

use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::storage::engine::StorageEngine;

/// `CreateSnapshot` dispatch target.
///
/// `MetaOp::CreateSnapshot` is a maintenance op issued by Origin's WAL manager
/// or administrative CLI. Lite's `PlanVisitor` has no `create_snapshot` trait
/// method and exposes no SQL syntax that produces this variant. The `StorageEngine`
/// trait has no snapshot API. No valid Lite deployment-shape code path reaches here.
pub async fn handle_create_snapshot<S: StorageEngine>(
    _engine: &LiteQueryEngine<S>,
) -> Result<QueryResult, LiteError> {
    unreachable!(
        "MetaOp::CreateSnapshot is an Origin WAL-manager op; Lite's PlanVisitor \
         exposes no SQL that produces this variant and StorageEngine has no snapshot API"
    )
}

/// `Compact` dispatch target.
///
/// `MetaOp::Compact` is a maintenance op issued by Origin's compaction manager.
/// Lite's `PlanVisitor` has no `compact` trait method and exposes no SQL syntax
/// that produces this variant. The `StorageEngine` trait has no compact entry
/// point. No valid Lite deployment-shape code path reaches here.
pub async fn handle_compact<S: StorageEngine>(
    _engine: &LiteQueryEngine<S>,
) -> Result<QueryResult, LiteError> {
    unreachable!(
        "MetaOp::Compact is an Origin compaction-manager op; Lite's PlanVisitor \
         exposes no SQL that produces this variant and StorageEngine has no compact API"
    )
}

/// `Checkpoint` — report a logical LSN of 0 (Lite is single-node, no WAL LSN).
pub async fn handle_checkpoint<S: StorageEngine>(
    _engine: &LiteQueryEngine<S>,
) -> Result<QueryResult, LiteError> {
    Ok(QueryResult {
        columns: vec!["lsn".into()],
        rows: vec![vec![Value::Integer(0)]],
        rows_affected: 0,
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
    Ok(QueryResult {
        columns: vec!["deleted_entries".into()],
        rows: vec![vec![Value::Integer(deleted as i64)]],
        rows_affected: deleted,
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
    })
}

/// `RenameCollection` — rewrite all `Namespace::Meta` keys for a collection
/// from the old qualified name to the new qualified name.
pub async fn handle_rename_collection<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    _tenant_id: u64,
    old_collection: &str,
    new_collection: &str,
) -> Result<QueryResult, LiteError> {
    let old_prefix = format!("collection/{old_collection}");
    let pairs = engine
        .storage
        .scan_prefix(Namespace::Meta, old_prefix.as_bytes())
        .await?;
    let mut renamed: u64 = 0;
    for (old_key, value) in &pairs {
        let old_key_str = String::from_utf8_lossy(old_key);
        let new_key_str = old_key_str.replacen(
            &format!("collection/{old_collection}"),
            &format!("collection/{new_collection}"),
            1,
        );
        engine
            .storage
            .put(Namespace::Meta, new_key_str.as_bytes(), value)
            .await?;
        engine.storage.delete(Namespace::Meta, old_key).await?;
        renamed += 1;
    }
    Ok(QueryResult {
        columns: vec!["renamed_entries".into()],
        rows: vec![vec![Value::Integer(renamed as i64)]],
        rows_affected: renamed,
    })
}

/// `ConvertCollection` — delegate to the existing DDL convert helpers.
///
/// `target_type` is one of `"document_schemaless"`, `"document_strict"`, `"kv"`.
pub async fn handle_convert_collection<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    collection: &str,
    target_type: &str,
    _schema_json: &str,
) -> Result<QueryResult, LiteError> {
    // Build a synthetic SQL string and delegate to the DDL visitor path.
    let sql = format!("CONVERT COLLECTION {collection} TO {target_type}");
    match target_type {
        "document_strict" | "strict" => engine.handle_convert_to_strict(&sql).await,
        "document_schemaless" | "document" => engine.handle_convert_to_document(&sql).await,
        "columnar" => engine.handle_convert_to_columnar(&sql).await,
        other => Err(LiteError::BadRequest {
            detail: format!(
                "ConvertCollection: unsupported target_type '{other}'; \
                 accepted values are document_schemaless, document_strict, kv"
            ),
        }),
    }
}
