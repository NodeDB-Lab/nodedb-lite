// SPDX-License-Identifier: Apache-2.0
//! Write operations for the Document engine physical visitor.

use std::collections::HashMap;

use nodedb_types::Namespace;
use nodedb_types::result::QueryResult;
use nodedb_types::value::Value;

use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::storage::engine::{StorageEngine, StorageEngineSync, WriteOp};

use super::is_strict;
use super::reads::{loro_value_to_ndb_value, msgpack_bytes_to_crdt_fields, ndb_value_to_loro};

type UpdateValue = nodedb_physical::physical_plan::document::types::UpdateValue;

/// PointPut: unconditional overwrite (upsert semantics).
pub async fn point_put<S: StorageEngine + StorageEngineSync>(
    engine: &LiteQueryEngine<S>,
    collection: &str,
    document_id: &str,
    value_bytes: &[u8],
) -> Result<QueryResult, LiteError> {
    if is_strict(engine, collection) {
        let fields = decode_strict_fields(value_bytes)?;
        let existing_pk = Value::String(document_id.to_string());
        if engine.strict.get(collection, &existing_pk).await?.is_some() {
            let updates: HashMap<String, Value> = fields.into_iter().collect();
            engine
                .strict
                .update(collection, &existing_pk, &updates)
                .await?;
        } else {
            let schema = strict_schema(engine, collection)?;
            let values = fields_to_values(&fields, &schema.columns);
            engine.strict.insert(collection, &values).await?;
        }
    } else {
        let crdt_fields = msgpack_bytes_to_crdt_fields(value_bytes)?;
        let loro_fields: Vec<(&str, loro::LoroValue)> = crdt_fields
            .iter()
            .map(|(k, v)| (k.as_str(), v.clone()))
            .collect();
        let mut crdt = engine.crdt.lock().map_err(|_| LiteError::LockPoisoned)?;
        crdt.upsert(collection, document_id, &loro_fields)
            .map_err(|e| LiteError::Storage {
                detail: e.to_string(),
            })?;
    }
    Ok(affected(1))
}

/// PointInsert: insert-only, fail on duplicate PK (or skip if `if_absent`).
pub async fn point_insert<S: StorageEngine + StorageEngineSync>(
    engine: &LiteQueryEngine<S>,
    collection: &str,
    document_id: &str,
    value_bytes: &[u8],
    if_absent: bool,
) -> Result<QueryResult, LiteError> {
    if is_strict(engine, collection) {
        let pk = Value::String(document_id.to_string());
        if engine.strict.get(collection, &pk).await?.is_some() {
            if if_absent {
                return Ok(affected(0));
            }
            return Err(LiteError::BadRequest {
                detail: format!(
                    "duplicate key value violates unique constraint on '{collection}' (id = '{document_id}')"
                ),
            });
        }
        let fields = decode_strict_fields(value_bytes)?;
        let schema = strict_schema(engine, collection)?;
        let values = fields_to_values(&fields, &schema.columns);
        engine.strict.insert(collection, &values).await?;
    } else {
        let crdt_fields = msgpack_bytes_to_crdt_fields(value_bytes)?;
        let loro_fields: Vec<(&str, loro::LoroValue)> = crdt_fields
            .iter()
            .map(|(k, v)| (k.as_str(), v.clone()))
            .collect();
        let mut crdt = engine.crdt.lock().map_err(|_| LiteError::LockPoisoned)?;
        if crdt.exists(collection, document_id) {
            if if_absent {
                return Ok(affected(0));
            }
            return Err(LiteError::BadRequest {
                detail: format!(
                    "duplicate key value violates unique constraint on '{collection}' (id = '{document_id}')"
                ),
            });
        }
        crdt.upsert(collection, document_id, &loro_fields)
            .map_err(|e| LiteError::Storage {
                detail: e.to_string(),
            })?;
    }
    Ok(affected(1))
}

/// PointUpdate: read-modify-write with field-level changes.
pub async fn point_update<S: StorageEngine + StorageEngineSync>(
    engine: &LiteQueryEngine<S>,
    collection: &str,
    document_id: &str,
    updates: &[(String, UpdateValue)],
) -> Result<QueryResult, LiteError> {
    if is_strict(engine, collection) {
        let pk = Value::String(document_id.to_string());
        let field_updates = decode_literal_updates(updates)?;
        let updated = engine
            .strict
            .update(collection, &pk, &field_updates)
            .await?;
        Ok(affected(if updated { 1 } else { 0 }))
    } else {
        let crdt = engine.crdt.lock().map_err(|_| LiteError::LockPoisoned)?;
        if !crdt.exists(collection, document_id) {
            return Ok(affected(0));
        }
        let existing_val = crdt.read(collection, document_id);
        drop(crdt);
        let mut merged: HashMap<String, loro::LoroValue> = if let Some(val) = existing_val {
            match loro_value_to_ndb_value(&val) {
                Value::Object(map) => map
                    .into_iter()
                    .map(|(k, v)| (k, ndb_value_to_loro(v)))
                    .collect(),
                _ => HashMap::new(),
            }
        } else {
            HashMap::new()
        };
        for (field, update_val) in updates {
            if let UpdateValue::Literal(bytes) = update_val {
                let val: Value =
                    zerompk::from_msgpack(bytes).map_err(|e| LiteError::Serialization {
                        detail: format!("decode update literal: {e}"),
                    })?;
                merged.insert(field.clone(), ndb_value_to_loro(val));
            }
        }
        let loro_fields: Vec<(&str, loro::LoroValue)> = merged
            .iter()
            .map(|(k, v)| (k.as_str(), v.clone()))
            .collect();
        let mut crdt = engine.crdt.lock().map_err(|_| LiteError::LockPoisoned)?;
        crdt.upsert(collection, document_id, &loro_fields)
            .map_err(|e| LiteError::Storage {
                detail: e.to_string(),
            })?;
        Ok(affected(1))
    }
}

/// PointDelete: remove a document by ID.
pub async fn point_delete<S: StorageEngine + StorageEngineSync>(
    engine: &LiteQueryEngine<S>,
    collection: &str,
    document_id: &str,
) -> Result<QueryResult, LiteError> {
    if is_strict(engine, collection) {
        let pk = Value::String(document_id.to_string());
        let deleted = engine.strict.delete(collection, &pk).await?;
        Ok(affected(if deleted { 1 } else { 0 }))
    } else {
        let mut crdt = engine.crdt.lock().map_err(|_| LiteError::LockPoisoned)?;
        if !crdt.exists(collection, document_id) {
            return Ok(affected(0));
        }
        crdt.delete(collection, document_id)
            .map_err(|e| LiteError::Storage {
                detail: e.to_string(),
            })?;
        Ok(affected(1))
    }
}

/// BatchInsert: insert N documents in a single transaction.
pub async fn batch_insert<S: StorageEngine + StorageEngineSync>(
    engine: &LiteQueryEngine<S>,
    collection: &str,
    documents: &[(String, Vec<u8>)],
) -> Result<QueryResult, LiteError> {
    if is_strict(engine, collection) {
        let schema = strict_schema(engine, collection)?;
        let mut rows: Vec<Vec<Value>> = Vec::with_capacity(documents.len());
        for (_doc_id, value_bytes) in documents {
            let fields = decode_strict_fields(value_bytes)?;
            let values = fields_to_values(&fields, &schema.columns);
            rows.push(values);
        }
        let affected_n = rows.len() as u64;
        engine.strict.insert_batch(collection, &rows).await?;
        Ok(affected(affected_n))
    } else {
        let mut decoded: Vec<(String, Vec<(String, loro::LoroValue)>)> =
            Vec::with_capacity(documents.len());
        for (doc_id, value_bytes) in documents {
            let crdt_fields = msgpack_bytes_to_crdt_fields(value_bytes)?;
            decoded.push((doc_id.clone(), crdt_fields));
        }
        let affected_n = decoded.len() as u64;
        let mut crdt = engine.crdt.lock().map_err(|_| LiteError::LockPoisoned)?;
        for (doc_id, fields) in &decoded {
            let loro_slice: Vec<(&str, loro::LoroValue)> = fields
                .iter()
                .map(|(k, v)| (k.as_str(), v.clone()))
                .collect();
            crdt.upsert_deferred(collection, doc_id, &loro_slice)
                .map_err(|e| LiteError::Storage {
                    detail: e.to_string(),
                })?;
        }
        crdt.flush_deltas().map_err(|e| LiteError::Storage {
            detail: e.to_string(),
        })?;
        Ok(affected(affected_n))
    }
}

/// Upsert: insert or update. When `on_conflict_updates` is non-empty, applies
/// those assignments on conflict instead of merging the new value.
pub async fn upsert<S: StorageEngine + StorageEngineSync>(
    engine: &LiteQueryEngine<S>,
    collection: &str,
    document_id: &str,
    value_bytes: &[u8],
    on_conflict_updates: &[(String, UpdateValue)],
) -> Result<QueryResult, LiteError> {
    if is_strict(engine, collection) {
        let pk = Value::String(document_id.to_string());
        let existed = engine.strict.get(collection, &pk).await?.is_some();
        if existed && !on_conflict_updates.is_empty() {
            let field_updates = decode_literal_updates(on_conflict_updates)?;
            engine
                .strict
                .update(collection, &pk, &field_updates)
                .await?;
        } else {
            let fields = decode_strict_fields(value_bytes)?;
            if existed {
                let updates: HashMap<String, Value> = fields.into_iter().collect();
                engine.strict.update(collection, &pk, &updates).await?;
            } else {
                let schema = strict_schema(engine, collection)?;
                let values = fields_to_values(&fields, &schema.columns);
                engine.strict.insert(collection, &values).await?;
            }
        }
    } else {
        let crdt_fields = msgpack_bytes_to_crdt_fields(value_bytes)?;
        let loro_fields: Vec<(&str, loro::LoroValue)> = crdt_fields
            .iter()
            .map(|(k, v)| (k.as_str(), v.clone()))
            .collect();
        let mut crdt = engine.crdt.lock().map_err(|_| LiteError::LockPoisoned)?;
        crdt.upsert(collection, document_id, &loro_fields)
            .map_err(|e| LiteError::Storage {
                detail: e.to_string(),
            })?;
    }
    Ok(affected(1))
}

/// Truncate: delete ALL documents in a collection.
pub async fn truncate<S: StorageEngine + StorageEngineSync>(
    engine: &LiteQueryEngine<S>,
    collection: &str,
) -> Result<QueryResult, LiteError> {
    if is_strict(engine, collection) {
        let prefix = format!("{collection}:");
        let all_entries = engine
            .storage
            .scan_prefix(Namespace::Strict, prefix.as_bytes())
            .await?;
        let mut ops: Vec<WriteOp> = Vec::with_capacity(all_entries.len());
        for (key, _) in all_entries {
            ops.push(WriteOp::Delete {
                ns: Namespace::Strict,
                key,
            });
        }
        let affected_n = ops.len() as u64;
        if !ops.is_empty() {
            engine.storage.batch_write(&ops).await?;
        }
        Ok(affected(affected_n))
    } else {
        let mut crdt = engine.crdt.lock().map_err(|_| LiteError::LockPoisoned)?;
        let count = crdt
            .clear_collection(collection)
            .map_err(|e| LiteError::Storage {
                detail: e.to_string(),
            })?;
        Ok(affected(count as u64))
    }
}

/// BulkUpdate: scan matching documents and apply field updates to all.
///
/// Lite does not yet evaluate residual scan filters — every document in the
/// collection receives the update. Callers that need filtered bulk updates
/// should compose `Scan` + per-row `PointUpdate` at the application layer.
pub async fn bulk_update<S: StorageEngine + StorageEngineSync>(
    engine: &LiteQueryEngine<S>,
    collection: &str,
    updates: &[(String, UpdateValue)],
) -> Result<QueryResult, LiteError> {
    let field_updates = decode_literal_updates(updates)?;

    if is_strict(engine, collection) {
        let schema = strict_schema(engine, collection)?;
        let pk_idx = schema
            .columns
            .iter()
            .position(|c| c.primary_key)
            .ok_or_else(|| LiteError::BadRequest {
                detail: format!("strict collection '{collection}' has no primary key"),
            })?;
        let all_rows = engine.strict.list_rows(collection).await?;
        let mut affected_n: u64 = 0;
        for row in &all_rows {
            let pk = &row[pk_idx];
            if engine.strict.update(collection, pk, &field_updates).await? {
                affected_n += 1;
            }
        }
        Ok(affected(affected_n))
    } else {
        let loro_updates: Vec<(String, loro::LoroValue)> = field_updates
            .into_iter()
            .map(|(k, v)| (k, ndb_value_to_loro(v)))
            .collect();
        let mut crdt = engine.crdt.lock().map_err(|_| LiteError::LockPoisoned)?;
        let ids = crdt.list_ids(collection);
        let loro_slice: Vec<(&str, loro::LoroValue)> = loro_updates
            .iter()
            .map(|(k, v)| (k.as_str(), v.clone()))
            .collect();
        let mut affected_n: u64 = 0;
        for id in &ids {
            crdt.upsert_deferred(collection, id, &loro_slice)
                .map_err(|e| LiteError::Storage {
                    detail: e.to_string(),
                })?;
            affected_n += 1;
        }
        crdt.flush_deltas().map_err(|e| LiteError::Storage {
            detail: e.to_string(),
        })?;
        Ok(affected(affected_n))
    }
}

/// BulkDelete dispatch target.
///
/// `DocumentOp::BulkDelete` carries a msgpack-encoded filter predicate produced
/// by Origin's Calvin/OLLP planner. Lite's SQL visitor never emits this variant —
/// it always resolves DELETE to point-key `PointDelete` ops via `target_keys`.
/// CRDT sync plans do not include bulk-predicate deletes. No valid code path
/// in the Lite deployment shape reaches this arm.
pub async fn bulk_delete<S: StorageEngine + StorageEngineSync>(
    _engine: &LiteQueryEngine<S>,
    _collection: &str,
) -> Result<QueryResult, LiteError> {
    unreachable!(
        "DocumentOp::BulkDelete is produced only by Origin's Calvin/OLLP planner; \
         Lite's SQL visitor always resolves DELETE to PointDelete ops via target_keys \
         and CRDT sync never emits bulk-predicate deletes"
    )
}

// ─── Internal helpers ────────────────────────────────────────────────────────

fn affected(n: u64) -> QueryResult {
    QueryResult {
        columns: Vec::new(),
        rows: Vec::new(),
        rows_affected: n,
    }
}

fn strict_schema<S: StorageEngine + StorageEngineSync>(
    engine: &LiteQueryEngine<S>,
    collection: &str,
) -> Result<nodedb_types::columnar::StrictSchema, LiteError> {
    engine
        .strict
        .schema(collection)
        .ok_or_else(|| LiteError::BadRequest {
            detail: format!("strict collection '{collection}' does not exist"),
        })
}

/// Decode msgpack document bytes into `(field_name, Value)` pairs.
fn decode_strict_fields(value_bytes: &[u8]) -> Result<Vec<(String, Value)>, LiteError> {
    let val: Value = zerompk::from_msgpack(value_bytes).map_err(|e| LiteError::Serialization {
        detail: format!("decode strict document: {e}"),
    })?;
    match val {
        Value::Object(map) => Ok(map.into_iter().collect()),
        _ => Err(LiteError::BadRequest {
            detail: "strict document payload must be a msgpack-encoded object".into(),
        }),
    }
}

/// Build a `Vec<Value>` in schema column order from a field map.
fn fields_to_values(
    fields: &[(String, Value)],
    columns: &[nodedb_types::columnar::ColumnDef],
) -> Vec<Value> {
    let map: HashMap<&str, &Value> = fields.iter().map(|(k, v)| (k.as_str(), v)).collect();
    columns
        .iter()
        .map(|c| {
            map.get(c.name.as_str())
                .copied()
                .cloned()
                .unwrap_or(Value::Null)
        })
        .collect()
}

/// Decode literal-only update values; non-literal `UpdateValue::Expr` arms
/// are ignored because the Lite executor has no expression evaluator.
fn decode_literal_updates(
    updates: &[(String, UpdateValue)],
) -> Result<HashMap<String, Value>, LiteError> {
    let mut field_updates: HashMap<String, Value> = HashMap::new();
    for (field, update_val) in updates {
        if let UpdateValue::Literal(bytes) = update_val {
            let val: Value =
                zerompk::from_msgpack(bytes).map_err(|e| LiteError::Serialization {
                    detail: format!("decode update literal for '{field}': {e}"),
                })?;
            field_updates.insert(field.clone(), val);
        }
    }
    Ok(field_updates)
}
