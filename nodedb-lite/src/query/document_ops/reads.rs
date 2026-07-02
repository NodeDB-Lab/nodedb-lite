// SPDX-License-Identifier: Apache-2.0
//! Read operations for the Document engine physical visitor.

use std::collections::HashMap;

use nodedb_types::Namespace;
use nodedb_types::result::QueryResult;
use nodedb_types::value::Value;

use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::query::value_utils::value_to_string;
use crate::storage::engine::StorageEngine;

use super::is_strict;

/// PointGet: fetch a single document by ID.
pub async fn point_get<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    collection: &str,
    document_id: &str,
) -> Result<QueryResult, LiteError> {
    if is_strict(engine, collection) {
        let columns = strict_columns(engine, collection);
        let pk = Value::String(document_id.to_string());
        match engine.strict.get(collection, &pk).await? {
            Some(values) => Ok(QueryResult {
                columns,
                rows: vec![values],
                rows_affected: 0,
            }),
            None => Ok(QueryResult::empty()),
        }
    } else {
        let crdt = engine.crdt.lock().map_err(|_| LiteError::LockPoisoned)?;
        match crdt.read(collection, document_id) {
            Some(val) => {
                let bytes = crdt_value_to_msgpack(&val)?;
                drop(crdt);
                Ok(QueryResult {
                    columns: vec!["id".into(), "data".into()],
                    rows: vec![vec![
                        Value::String(document_id.to_string()),
                        Value::Bytes(bytes),
                    ]],
                    rows_affected: 0,
                })
            }
            None => Ok(QueryResult::empty()),
        }
    }
}

/// Scan: full collection scan with limit/offset.
pub async fn scan<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    collection: &str,
    limit: usize,
    offset: usize,
) -> Result<QueryResult, LiteError> {
    if is_strict(engine, collection) {
        let columns = strict_columns(engine, collection);
        let all_rows = engine.strict.list_rows(collection).await?;
        let rows: Vec<Vec<Value>> = all_rows.into_iter().skip(offset).take(limit).collect();
        Ok(QueryResult {
            columns,
            rows,
            rows_affected: 0,
        })
    } else {
        let crdt = engine.crdt.lock().map_err(|_| LiteError::LockPoisoned)?;
        let ids = crdt.list_ids(collection);
        let mut rows = Vec::with_capacity(ids.len().min(limit));
        for id in ids.iter().skip(offset).take(limit) {
            if let Some(val) = crdt.read(collection, id) {
                let bytes = crdt_value_to_msgpack(&val)?;
                rows.push(vec![Value::String(id.clone()), Value::Bytes(bytes)]);
            }
        }
        drop(crdt);
        Ok(QueryResult {
            columns: vec!["id".into(), "data".into()],
            rows,
            rows_affected: 0,
        })
    }
}

/// RangeScan: scan documents whose primary key lies within `[lower, upper]`.
///
/// For the strict path, materializes via `list_rows` once and filters by the
/// PK byte-range — avoiding the N+1 re-fetch that a `scan + get` composition
/// would incur (the strict-storage value encoding is internal to the strict
/// engine and not safe to decode here).
pub async fn range_scan<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    collection: &str,
    lower: Option<&[u8]>,
    upper: Option<&[u8]>,
    limit: usize,
) -> Result<QueryResult, LiteError> {
    if is_strict(engine, collection) {
        let schema = engine
            .strict
            .schema(collection)
            .ok_or_else(|| LiteError::BadRequest {
                detail: format!("strict collection '{collection}' does not exist"),
            })?;
        let columns: Vec<String> = schema.columns.iter().map(|c| c.name.clone()).collect();
        let pk_idx = schema
            .columns
            .iter()
            .position(|c| c.primary_key)
            .ok_or_else(|| LiteError::BadRequest {
                detail: format!("strict collection '{collection}' has no primary key"),
            })?;
        let all_rows = engine.strict.list_rows(collection).await?;
        let mut rows = Vec::new();
        for row in all_rows {
            let pk_str = value_to_string(&row[pk_idx]);
            let pk_bytes = pk_str.as_bytes();
            if let Some(lo) = lower
                && pk_bytes < lo
            {
                continue;
            }
            if let Some(hi) = upper
                && pk_bytes > hi
            {
                continue;
            }
            rows.push(row);
            if rows.len() >= limit {
                break;
            }
        }
        Ok(QueryResult {
            columns,
            rows,
            rows_affected: 0,
        })
    } else {
        let crdt = engine.crdt.lock().map_err(|_| LiteError::LockPoisoned)?;
        let all_ids = crdt.list_ids(collection);
        let mut rows = Vec::new();
        for id in all_ids.iter() {
            if let Some(lo) = lower
                && id.as_bytes() < lo
            {
                continue;
            }
            if let Some(hi) = upper
                && id.as_bytes() > hi
            {
                continue;
            }
            if let Some(val) = crdt.read(collection, id) {
                let bytes = crdt_value_to_msgpack(&val)?;
                rows.push(vec![Value::String(id.clone()), Value::Bytes(bytes)]);
                if rows.len() >= limit {
                    break;
                }
            }
        }
        drop(crdt);
        Ok(QueryResult {
            columns: vec!["id".into(), "data".into()],
            rows,
            rows_affected: 0,
        })
    }
}

/// IndexedFetch: fetch docs via secondary index, apply residual filters, and project.
pub async fn indexed_fetch<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    collection: &str,
    path: &str,
    value: &str,
    limit: usize,
    offset: usize,
) -> Result<QueryResult, LiteError> {
    let doc_ids = index_lookup_ids(engine, collection, path, value).await?;
    if is_strict(engine, collection) {
        let columns = strict_columns(engine, collection);
        let mut rows = Vec::new();
        let mut skipped = 0usize;
        for id in &doc_ids {
            let pk = Value::String(id.clone());
            if let Some(values) = engine.strict.get(collection, &pk).await? {
                if skipped < offset {
                    skipped += 1;
                    continue;
                }
                rows.push(values);
                if rows.len() >= limit {
                    break;
                }
            }
        }
        Ok(QueryResult {
            columns,
            rows,
            rows_affected: 0,
        })
    } else {
        let crdt = engine.crdt.lock().map_err(|_| LiteError::LockPoisoned)?;
        let mut rows = Vec::new();
        let mut skipped = 0usize;
        for id in &doc_ids {
            if let Some(val) = crdt.read(collection, id) {
                if skipped < offset {
                    skipped += 1;
                    continue;
                }
                let bytes = crdt_value_to_msgpack(&val)?;
                rows.push(vec![Value::String(id.clone()), Value::Bytes(bytes)]);
                if rows.len() >= limit {
                    break;
                }
            }
        }
        drop(crdt);
        Ok(QueryResult {
            columns: vec!["id".into(), "data".into()],
            rows,
            rows_affected: 0,
        })
    }
}

/// IndexLookup: return doc IDs for all documents matching field=value.
pub async fn index_lookup<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    collection: &str,
    path: &str,
    value: &str,
) -> Result<QueryResult, LiteError> {
    let ids = index_lookup_ids(engine, collection, path, value).await?;
    let rows: Vec<Vec<Value>> = ids.into_iter().map(|id| vec![Value::String(id)]).collect();
    Ok(QueryResult {
        columns: vec!["document_id".into()],
        rows,
        rows_affected: 0,
    })
}

/// EstimateCount: exact document count for the collection.
pub async fn estimate_count<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    collection: &str,
) -> Result<QueryResult, LiteError> {
    let count: u64 = if is_strict(engine, collection) {
        engine.strict.count(collection).await? as u64
    } else {
        let crdt = engine.crdt.lock().map_err(|_| LiteError::LockPoisoned)?;
        crdt.list_ids(collection).len() as u64
    };
    Ok(QueryResult {
        columns: vec!["count".into()],
        rows: vec![vec![Value::Integer(count as i64)]],
        rows_affected: 0,
    })
}

// ─── Internal helpers ────────────────────────────────────────────────────────

fn strict_columns<S: StorageEngine>(engine: &LiteQueryEngine<S>, collection: &str) -> Vec<String> {
    engine
        .strict
        .schema(collection)
        .map(|s| s.columns.iter().map(|c| c.name.clone()).collect())
        .unwrap_or_default()
}

/// Sparse-index lookup: return all doc IDs where `path == value`.
pub(super) async fn index_lookup_ids<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    collection: &str,
    path: &str,
    value: &str,
) -> Result<Vec<String>, LiteError> {
    let index_key = format!("{collection}:{path}:{value}");
    let stored = engine
        .storage
        .get(Namespace::Meta, index_key.as_bytes())
        .await
        .map_err(|e| LiteError::Storage {
            detail: e.to_string(),
        })?;
    match stored {
        Some(bytes) => {
            let ids: Vec<String> =
                zerompk::from_msgpack(&bytes).map_err(|e| LiteError::Serialization {
                    detail: format!("decode index entry: {e}"),
                })?;
            Ok(ids)
        }
        None => Ok(Vec::new()),
    }
}

/// Write a doc-ID into the sparse index for `(collection, path, value)`.
pub(super) async fn index_insert_id<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    collection: &str,
    path: &str,
    value: &str,
    doc_id: &str,
) -> Result<(), LiteError> {
    let index_key = format!("{collection}:{path}:{value}");
    let mut ids: Vec<String> = if let Some(bytes) = engine
        .storage
        .get(Namespace::Meta, index_key.as_bytes())
        .await
        .map_err(|e| LiteError::Storage {
            detail: e.to_string(),
        })? {
        zerompk::from_msgpack(&bytes).map_err(|e| LiteError::Serialization {
            detail: format!("decode index entry: {e}"),
        })?
    } else {
        Vec::new()
    };
    if !ids.contains(&doc_id.to_string()) {
        ids.push(doc_id.to_string());
        let bytes = zerompk::to_msgpack_vec(&ids).map_err(|e| LiteError::Serialization {
            detail: format!("encode index entry: {e}"),
        })?;
        engine
            .storage
            .put(Namespace::Meta, index_key.as_bytes(), &bytes)
            .await
            .map_err(|e| LiteError::Storage {
                detail: e.to_string(),
            })?;
    }
    Ok(())
}

fn crdt_value_to_msgpack(val: &loro::LoroValue) -> Result<Vec<u8>, LiteError> {
    let ndb_val = loro_value_to_ndb_value(val);
    zerompk::to_msgpack_vec(&ndb_val).map_err(|e| LiteError::Serialization {
        detail: format!("serialize crdt value: {e}"),
    })
}

pub(crate) fn loro_value_to_ndb_value(v: &loro::LoroValue) -> Value {
    match v {
        loro::LoroValue::Null => Value::Null,
        loro::LoroValue::Bool(b) => Value::Bool(*b),
        loro::LoroValue::I64(n) => Value::Integer(*n),
        loro::LoroValue::Double(f) => Value::Float(*f),
        loro::LoroValue::String(s) => Value::String(s.to_string()),
        loro::LoroValue::Binary(b) => Value::Bytes(b.to_vec()),
        loro::LoroValue::Map(m) => {
            let mut map = HashMap::new();
            for (k, v) in m.iter() {
                map.insert(k.to_string(), loro_value_to_ndb_value(v));
            }
            Value::Object(map)
        }
        loro::LoroValue::List(arr) => {
            Value::Array(arr.iter().map(loro_value_to_ndb_value).collect())
        }
        _ => Value::Null,
    }
}

pub(super) fn msgpack_bytes_to_crdt_fields(
    bytes: &[u8],
) -> Result<Vec<(String, loro::LoroValue)>, LiteError> {
    let val: Value = zerompk::from_msgpack(bytes).map_err(|e| LiteError::Serialization {
        detail: format!("decode document bytes: {e}"),
    })?;
    match val {
        Value::Object(map) => Ok(map
            .into_iter()
            .map(|(k, v)| (k, ndb_value_to_loro(v)))
            .collect()),
        _ => Err(LiteError::BadRequest {
            detail: "document payload must be a msgpack-encoded object".into(),
        }),
    }
}

pub(super) fn ndb_value_to_loro(v: Value) -> loro::LoroValue {
    match v {
        Value::Null => loro::LoroValue::Null,
        Value::Bool(b) => loro::LoroValue::Bool(b),
        Value::Integer(n) => loro::LoroValue::I64(n),
        Value::Float(f) => loro::LoroValue::Double(f),
        Value::String(s) => loro::LoroValue::String(s.into()),
        Value::Bytes(b) => loro::LoroValue::Binary(b.into()),
        Value::Object(map) => {
            let loro_map: HashMap<String, loro::LoroValue> = map
                .into_iter()
                .map(|(k, v)| (k, ndb_value_to_loro(v)))
                .collect();
            loro::LoroValue::Map(loro_map.into())
        }
        Value::Array(arr) => {
            let list: Vec<loro::LoroValue> = arr.into_iter().map(ndb_value_to_loro).collect();
            loro::LoroValue::List(list.into())
        }
        _ => loro::LoroValue::Null,
    }
}
