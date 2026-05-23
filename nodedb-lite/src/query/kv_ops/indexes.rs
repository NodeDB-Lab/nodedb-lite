// SPDX-License-Identifier: Apache-2.0
//! Secondary index management for the KV engine physical visitor.
//!
//! Secondary indexes are stored in the Meta namespace with keys of the form:
//! `kv:{collection}:{field}:{field_value}` → msgpack-encoded `Vec<String>` of primary keys.

use nodedb_types::Namespace;
use nodedb_types::result::QueryResult;

use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::query::value_utils::value_to_string;
use crate::storage::engine::{StorageEngine, WriteOp};

use super::reads::{decode_value, is_expired, split_kv_key};

/// Index key prefix in Meta namespace.
fn meta_prefix(collection: &str, field: &str) -> String {
    format!("kv:{collection}:{field}:")
}

/// Full meta key for a given (collection, field, value).
fn meta_key(collection: &str, field: &str, field_value: &str) -> String {
    format!("kv:{collection}:{field}:{field_value}")
}

/// RegisterIndex: register a secondary index and optionally backfill it.
pub async fn kv_register_index<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    collection: &str,
    field: &str,
    backfill: bool,
) -> Result<QueryResult, LiteError> {
    if !backfill {
        return Ok(QueryResult {
            columns: vec![],
            rows: vec![],
            rows_affected: 0,
        });
    }

    // Backfill: scan all entries in the collection and build the index.
    let col_prefix = {
        let mut p = collection.as_bytes().to_vec();
        p.push(0);
        p
    };
    let entries = engine
        .storage
        .scan_range_bounded(Namespace::Kv, Some(&col_prefix), None, None)
        .await
        .map_err(|e| LiteError::Storage {
            detail: e.to_string(),
        })?;

    let mut indexed: u64 = 0;
    for (composite_key, raw_value) in &entries {
        let Some((coll, user_key_bytes)) = split_kv_key(composite_key) else {
            continue;
        };
        if coll != collection {
            break;
        }
        let Some((deadline, user_bytes)) = decode_value(raw_value) else {
            continue;
        };
        if is_expired(deadline) {
            continue;
        }

        let map: std::collections::HashMap<String, nodedb_types::value::Value> =
            zerompk::from_msgpack(user_bytes).map_err(|e| LiteError::Serialization {
                detail: format!(
                    "kv_register_index backfill: decode value for collection '{collection}': {e}"
                ),
            })?;

        if let Some(field_val) = map.get(field) {
            let field_str = value_to_string(field_val);
            let pk = String::from_utf8_lossy(user_key_bytes).into_owned();
            index_insert_pk(engine, collection, field, &field_str, &pk).await?;
            indexed += 1;
        }
    }

    Ok(QueryResult {
        columns: vec![],
        rows: vec![],
        rows_affected: indexed,
    })
}

/// DropIndex: remove all index entries for a given field on a collection.
pub async fn kv_drop_index<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    collection: &str,
    field: &str,
) -> Result<QueryResult, LiteError> {
    let prefix = meta_prefix(collection, field);
    let entries = engine
        .storage
        .scan_range_bounded(Namespace::Meta, Some(prefix.as_bytes()), None, None)
        .await
        .map_err(|e| LiteError::Storage {
            detail: e.to_string(),
        })?;

    let mut ops: Vec<WriteOp> = Vec::with_capacity(entries.len());
    for (key, _) in &entries {
        if key.starts_with(prefix.as_bytes()) {
            ops.push(WriteOp::Delete {
                ns: Namespace::Meta,
                key: key.clone(),
            });
        }
    }
    let count = ops.len() as u64;
    if !ops.is_empty() {
        engine
            .storage
            .batch_write(&ops)
            .await
            .map_err(|e| LiteError::Storage {
                detail: e.to_string(),
            })?;
    }
    Ok(QueryResult {
        columns: vec![],
        rows: vec![],
        rows_affected: count,
    })
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

/// Insert a primary key into the inverted index for (collection, field, field_value).
async fn index_insert_pk<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    collection: &str,
    field: &str,
    field_value: &str,
    pk: &str,
) -> Result<(), LiteError> {
    let mk = meta_key(collection, field, field_value);
    let mut ids: Vec<String> = if let Some(bytes) = engine
        .storage
        .get(Namespace::Meta, mk.as_bytes())
        .await
        .map_err(|e| LiteError::Storage {
            detail: e.to_string(),
        })? {
        zerompk::from_msgpack(&bytes).map_err(|e| LiteError::Serialization {
            detail: format!("decode KV index entry: {e}"),
        })?
    } else {
        Vec::new()
    };
    if !ids.contains(&pk.to_string()) {
        ids.push(pk.to_string());
        let bytes = zerompk::to_msgpack_vec(&ids).map_err(|e| LiteError::Serialization {
            detail: format!("encode KV index entry: {e}"),
        })?;
        engine
            .storage
            .put(Namespace::Meta, mk.as_bytes(), &bytes)
            .await
            .map_err(|e| LiteError::Storage {
                detail: e.to_string(),
            })?;
    }
    Ok(())
}
