// SPDX-License-Identifier: Apache-2.0
//! Write operations for the KV engine physical visitor.

use nodedb_types::Namespace;
use nodedb_types::result::QueryResult;
use nodedb_types::value::Value;

use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::storage::engine::{StorageEngine, WriteOp};

use super::reads::{decode_value, encode_value, is_expired, kv_key, now_ms, split_kv_key};

// ─── Point writes ────────────────────────────────────────────────────────────

/// Put: unconditional upsert.
pub async fn kv_put<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    collection: &str,
    key: &[u8],
    value: &[u8],
    ttl_ms: u64,
) -> Result<QueryResult, LiteError> {
    let deadline = if ttl_ms > 0 {
        now_ms().saturating_add(ttl_ms)
    } else {
        0
    };
    let rkey = kv_key(collection, key);
    let encoded = encode_value(deadline, value);
    engine
        .storage
        .put(Namespace::Kv, &rkey, &encoded)
        .await
        .map_err(|e| LiteError::Storage {
            detail: e.to_string(),
        })?;
    Ok(QueryResult {
        columns: vec![],
        rows: vec![],
        rows_affected: 1,
    })
}

/// Insert: write only if key absent; error on duplicate.
pub async fn kv_insert<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    collection: &str,
    key: &[u8],
    value: &[u8],
    ttl_ms: u64,
) -> Result<QueryResult, LiteError> {
    let rkey = kv_key(collection, key);
    let existing = engine
        .storage
        .get(Namespace::Kv, &rkey)
        .await
        .map_err(|e| LiteError::Storage {
            detail: e.to_string(),
        })?;
    if let Some(raw) = existing
        && let Some((deadline, _)) = decode_value(&raw)
        && !is_expired(deadline)
    {
        return Err(LiteError::BadRequest {
            detail: format!("unique_violation: key already exists in collection '{collection}'"),
        });
    }
    kv_put(engine, collection, key, value, ttl_ms).await
}

/// InsertIfAbsent: write if absent, silently no-op on duplicate.
pub async fn kv_insert_if_absent<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    collection: &str,
    key: &[u8],
    value: &[u8],
    ttl_ms: u64,
) -> Result<QueryResult, LiteError> {
    let rkey = kv_key(collection, key);
    let existing = engine
        .storage
        .get(Namespace::Kv, &rkey)
        .await
        .map_err(|e| LiteError::Storage {
            detail: e.to_string(),
        })?;
    if let Some(raw) = existing
        && let Some((deadline, _)) = decode_value(&raw)
        && !is_expired(deadline)
    {
        return Ok(QueryResult {
            columns: vec![],
            rows: vec![],
            rows_affected: 0,
        });
    }
    kv_put(engine, collection, key, value, ttl_ms).await
}

/// InsertOnConflictUpdate: write if absent; on conflict apply field updates.
pub async fn kv_insert_on_conflict_update<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    collection: &str,
    key: &[u8],
    value: &[u8],
    ttl_ms: u64,
    updates: &[(
        String,
        nodedb_physical::physical_plan::document::UpdateValue,
    )],
) -> Result<QueryResult, LiteError> {
    let rkey = kv_key(collection, key);
    let existing = engine
        .storage
        .get(Namespace::Kv, &rkey)
        .await
        .map_err(|e| LiteError::Storage {
            detail: e.to_string(),
        })?;

    let raw = match existing {
        None => return kv_put(engine, collection, key, value, ttl_ms).await,
        Some(raw) => match decode_value(&raw) {
            None => return kv_put(engine, collection, key, value, ttl_ms).await,
            Some((deadline, _)) if is_expired(deadline) => {
                return kv_put(engine, collection, key, value, ttl_ms).await;
            }
            Some(_) => raw,
        },
    };

    let (old_deadline, old_user_bytes) = decode_value(&raw).ok_or_else(|| LiteError::Storage {
        detail: "corrupt KV entry".into(),
    })?;

    let mut map: std::collections::HashMap<String, nodedb_types::value::Value> =
        zerompk::from_msgpack(old_user_bytes).map_err(|e| LiteError::Serialization {
            detail: format!("InsertOnConflictUpdate: decode existing value: {e}"),
        })?;

    for (field, update_val) in updates {
        use nodedb_physical::physical_plan::document::UpdateValue;
        match update_val {
            UpdateValue::Literal(bytes) => {
                let v: nodedb_types::value::Value =
                    zerompk::from_msgpack(bytes).map_err(|e| LiteError::Serialization {
                        detail: format!(
                            "InsertOnConflictUpdate: decode update literal for '{field}': {e}"
                        ),
                    })?;
                map.insert(field.clone(), v);
            }
            UpdateValue::Expr(_) => {
                unreachable!(
                    "UpdateValue::Expr on KV InsertOnConflictUpdate: Lite's KV SQL \
                     visitor always converts ON CONFLICT DO UPDATE assignments to \
                     UpdateValue::Literal before building KvOp; no Lite code path \
                     emits Expr here"
                );
            }
        }
    }

    let new_user_bytes = zerompk::to_msgpack_vec(&map).map_err(|e| LiteError::Serialization {
        detail: format!("encode updated KV value: {e}"),
    })?;

    let keep_deadline = if ttl_ms > 0 {
        now_ms().saturating_add(ttl_ms)
    } else {
        old_deadline
    };

    let encoded = encode_value(keep_deadline, &new_user_bytes);
    engine
        .storage
        .put(Namespace::Kv, &rkey, &encoded)
        .await
        .map_err(|e| LiteError::Storage {
            detail: e.to_string(),
        })?;

    Ok(QueryResult {
        columns: vec![],
        rows: vec![],
        rows_affected: 1,
    })
}

/// Delete: remove keys by primary key list.
pub async fn kv_delete<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    collection: &str,
    keys: &[Vec<u8>],
) -> Result<QueryResult, LiteError> {
    let ops: Vec<WriteOp> = keys
        .iter()
        .map(|k| WriteOp::Delete {
            ns: Namespace::Kv,
            key: kv_key(collection, k),
        })
        .collect();
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

/// BatchPut: atomically insert/update multiple key-value pairs.
pub async fn kv_batch_put<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    collection: &str,
    entries: &[(Vec<u8>, Vec<u8>)],
    ttl_ms: u64,
) -> Result<QueryResult, LiteError> {
    let deadline = if ttl_ms > 0 {
        now_ms().saturating_add(ttl_ms)
    } else {
        0
    };
    let ops: Vec<WriteOp> = entries
        .iter()
        .map(|(k, v)| WriteOp::Put {
            ns: Namespace::Kv,
            key: kv_key(collection, k),
            value: encode_value(deadline, v),
        })
        .collect();
    let count = ops.len() as u64;
    engine
        .storage
        .batch_write(&ops)
        .await
        .map_err(|e| LiteError::Storage {
            detail: e.to_string(),
        })?;
    Ok(QueryResult {
        columns: vec![],
        rows: vec![],
        rows_affected: count,
    })
}

/// Expire: set or update TTL on an existing key.
pub async fn kv_expire<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    collection: &str,
    key: &[u8],
    ttl_ms: u64,
) -> Result<QueryResult, LiteError> {
    let rkey = kv_key(collection, key);
    let stored = engine
        .storage
        .get(Namespace::Kv, &rkey)
        .await
        .map_err(|e| LiteError::Storage {
            detail: e.to_string(),
        })?;

    match stored {
        None => Ok(QueryResult {
            columns: vec![],
            rows: vec![],
            rows_affected: 0,
        }),
        Some(raw) => {
            let (_, user_bytes) = decode_value(&raw).ok_or_else(|| LiteError::Storage {
                detail: "corrupt KV entry".into(),
            })?;
            let deadline = now_ms().saturating_add(ttl_ms);
            let encoded = encode_value(deadline, user_bytes);
            engine
                .storage
                .put(Namespace::Kv, &rkey, &encoded)
                .await
                .map_err(|e| LiteError::Storage {
                    detail: e.to_string(),
                })?;
            Ok(QueryResult {
                columns: vec![],
                rows: vec![],
                rows_affected: 1,
            })
        }
    }
}

/// Persist: remove TTL from an existing key (make it permanent).
pub async fn kv_persist<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    collection: &str,
    key: &[u8],
) -> Result<QueryResult, LiteError> {
    let rkey = kv_key(collection, key);
    let stored = engine
        .storage
        .get(Namespace::Kv, &rkey)
        .await
        .map_err(|e| LiteError::Storage {
            detail: e.to_string(),
        })?;

    match stored {
        None => Ok(QueryResult {
            columns: vec![],
            rows: vec![],
            rows_affected: 0,
        }),
        Some(raw) => {
            let (_, user_bytes) = decode_value(&raw).ok_or_else(|| LiteError::Storage {
                detail: "corrupt KV entry".into(),
            })?;
            let encoded = encode_value(0, user_bytes);
            engine
                .storage
                .put(Namespace::Kv, &rkey, &encoded)
                .await
                .map_err(|e| LiteError::Storage {
                    detail: e.to_string(),
                })?;
            Ok(QueryResult {
                columns: vec![],
                rows: vec![],
                rows_affected: 1,
            })
        }
    }
}

/// Truncate: delete ALL entries in a KV collection.
pub async fn kv_truncate<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    collection: &str,
) -> Result<QueryResult, LiteError> {
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

    let mut ops: Vec<WriteOp> = Vec::with_capacity(entries.len());
    for (composite_key, _) in &entries {
        let Some((coll, _)) = split_kv_key(composite_key) else {
            continue;
        };
        if coll != collection {
            break;
        }
        ops.push(WriteOp::Delete {
            ns: Namespace::Kv,
            key: composite_key.clone(),
        });
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

/// Incr: atomic integer counter increment.
///
/// Initialises to 0 if the key does not exist, then adds delta.
/// Returns the new value. Fails with TypeMismatch if the stored value is
/// not a plain i64.
pub async fn kv_incr<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    collection: &str,
    key: &[u8],
    delta: i64,
    ttl_ms: u64,
) -> Result<QueryResult, LiteError> {
    let rkey = kv_key(collection, key);
    let stored = engine
        .storage
        .get(Namespace::Kv, &rkey)
        .await
        .map_err(|e| LiteError::Storage {
            detail: e.to_string(),
        })?;

    let (current, old_deadline) = match stored {
        None => (0i64, 0u64),
        Some(raw) => {
            let (deadline, user_bytes) = decode_value(&raw).ok_or_else(|| LiteError::Storage {
                detail: "corrupt KV entry".into(),
            })?;
            if is_expired(deadline) {
                (0i64, 0u64)
            } else {
                let v: i64 =
                    zerompk::from_msgpack(user_bytes).map_err(|_| LiteError::BadRequest {
                        detail: "Incr: stored value is not an integer".into(),
                    })?;
                (v, deadline)
            }
        }
    };

    let new_val = current
        .checked_add(delta)
        .ok_or_else(|| LiteError::BadRequest {
            detail: "Incr: integer overflow".into(),
        })?;

    let new_user_bytes =
        zerompk::to_msgpack_vec(&new_val).map_err(|e| LiteError::Serialization {
            detail: format!("Incr encode: {e}"),
        })?;

    let deadline = if ttl_ms > 0 {
        now_ms().saturating_add(ttl_ms)
    } else {
        old_deadline
    };

    let encoded = encode_value(deadline, &new_user_bytes);
    engine
        .storage
        .put(Namespace::Kv, &rkey, &encoded)
        .await
        .map_err(|e| LiteError::Storage {
            detail: e.to_string(),
        })?;

    Ok(QueryResult {
        columns: vec!["value".into()],
        rows: vec![vec![Value::Integer(new_val)]],
        rows_affected: 1,
    })
}

/// IncrFloat: atomic f64 increment.
pub async fn kv_incr_float<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    collection: &str,
    key: &[u8],
    delta: f64,
) -> Result<QueryResult, LiteError> {
    let rkey = kv_key(collection, key);
    let stored = engine
        .storage
        .get(Namespace::Kv, &rkey)
        .await
        .map_err(|e| LiteError::Storage {
            detail: e.to_string(),
        })?;

    let (current, old_deadline) = match stored {
        None => (0.0f64, 0u64),
        Some(raw) => {
            let (deadline, user_bytes) = decode_value(&raw).ok_or_else(|| LiteError::Storage {
                detail: "corrupt KV entry".into(),
            })?;
            if is_expired(deadline) {
                (0.0f64, 0u64)
            } else {
                let v: f64 =
                    zerompk::from_msgpack(user_bytes).map_err(|_| LiteError::BadRequest {
                        detail: "IncrFloat: stored value is not a float".into(),
                    })?;
                (v, deadline)
            }
        }
    };

    let new_val = current + delta;
    let new_user_bytes =
        zerompk::to_msgpack_vec(&new_val).map_err(|e| LiteError::Serialization {
            detail: format!("IncrFloat encode: {e}"),
        })?;
    let encoded = encode_value(old_deadline, &new_user_bytes);
    engine
        .storage
        .put(Namespace::Kv, &rkey, &encoded)
        .await
        .map_err(|e| LiteError::Storage {
            detail: e.to_string(),
        })?;

    Ok(QueryResult {
        columns: vec!["value".into()],
        rows: vec![vec![Value::Float(new_val)]],
        rows_affected: 1,
    })
}

/// Cas: compare-and-swap.
///
/// Sets `new_value` only if current bytes equal `expected`.
/// If key doesn't exist and `expected` is empty, creates the key.
pub async fn kv_cas<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    collection: &str,
    key: &[u8],
    expected: &[u8],
    new_value: &[u8],
) -> Result<QueryResult, LiteError> {
    let rkey = kv_key(collection, key);
    let stored = engine
        .storage
        .get(Namespace::Kv, &rkey)
        .await
        .map_err(|e| LiteError::Storage {
            detail: e.to_string(),
        })?;

    let (current_bytes, old_deadline) = match stored {
        None => (Vec::new(), 0u64),
        Some(raw) => match decode_value(&raw) {
            None => (Vec::new(), 0u64),
            Some((deadline, user_bytes)) => {
                if is_expired(deadline) {
                    (Vec::new(), 0u64)
                } else {
                    (user_bytes.to_vec(), deadline)
                }
            }
        },
    };

    let success = current_bytes == expected;
    if success {
        let encoded = encode_value(old_deadline, new_value);
        engine
            .storage
            .put(Namespace::Kv, &rkey, &encoded)
            .await
            .map_err(|e| LiteError::Storage {
                detail: e.to_string(),
            })?;
    }

    Ok(QueryResult {
        columns: vec!["success".into(), "current_value".into()],
        rows: vec![vec![Value::Bool(success), Value::Bytes(current_bytes)]],
        rows_affected: if success { 1 } else { 0 },
    })
}

/// GetSet: atomically set new value and return old value.
pub async fn kv_get_set<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    collection: &str,
    key: &[u8],
    new_value: &[u8],
) -> Result<QueryResult, LiteError> {
    let rkey = kv_key(collection, key);
    let stored = engine
        .storage
        .get(Namespace::Kv, &rkey)
        .await
        .map_err(|e| LiteError::Storage {
            detail: e.to_string(),
        })?;

    let (old_val, old_deadline) = match stored {
        None => (Value::Null, 0u64),
        Some(raw) => match decode_value(&raw) {
            None => (Value::Null, 0u64),
            Some((deadline, user_bytes)) => {
                let v = if is_expired(deadline) {
                    Value::Null
                } else {
                    Value::Bytes(user_bytes.to_vec())
                };
                (v, deadline)
            }
        },
    };

    let encoded = encode_value(old_deadline, new_value);
    engine
        .storage
        .put(Namespace::Kv, &rkey, &encoded)
        .await
        .map_err(|e| LiteError::Storage {
            detail: e.to_string(),
        })?;

    Ok(QueryResult {
        columns: vec!["old_value".into()],
        rows: vec![vec![old_val]],
        rows_affected: 1,
    })
}

/// FieldSet: read-modify-write on named fields of a MessagePack map value.
pub async fn kv_field_set<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    collection: &str,
    key: &[u8],
    field_updates: &[(String, Vec<u8>)],
) -> Result<QueryResult, LiteError> {
    let rkey = kv_key(collection, key);
    let stored = engine
        .storage
        .get(Namespace::Kv, &rkey)
        .await
        .map_err(|e| LiteError::Storage {
            detail: e.to_string(),
        })?;

    let (old_deadline, mut map) = match stored {
        None => (
            0u64,
            std::collections::HashMap::<String, nodedb_types::value::Value>::new(),
        ),
        Some(raw) => {
            let (deadline, user_bytes) = decode_value(&raw).ok_or_else(|| LiteError::Storage {
                detail: "corrupt KV entry".into(),
            })?;
            if is_expired(deadline) {
                (
                    0u64,
                    std::collections::HashMap::<String, nodedb_types::value::Value>::new(),
                )
            } else {
                let m: std::collections::HashMap<String, nodedb_types::value::Value> =
                    zerompk::from_msgpack(user_bytes).map_err(|e| LiteError::Serialization {
                        detail: format!("FieldSet: decode existing value: {e}"),
                    })?;
                (deadline, m)
            }
        }
    };

    for (field, val_bytes) in field_updates {
        let v: nodedb_types::value::Value =
            zerompk::from_msgpack(val_bytes).map_err(|e| LiteError::Serialization {
                detail: format!("FieldSet decode field '{field}': {e}"),
            })?;
        map.insert(field.clone(), v);
    }

    let new_user_bytes = zerompk::to_msgpack_vec(&map).map_err(|e| LiteError::Serialization {
        detail: format!("FieldSet encode: {e}"),
    })?;
    let encoded = encode_value(old_deadline, &new_user_bytes);
    engine
        .storage
        .put(Namespace::Kv, &rkey, &encoded)
        .await
        .map_err(|e| LiteError::Storage {
            detail: e.to_string(),
        })?;

    Ok(QueryResult {
        columns: vec![],
        rows: vec![],
        rows_affected: 1,
    })
}

/// Transfer: atomic fungible transfer between two keys in the same collection.
///
/// Reads source and dest, validates source.field >= amount, writes both back.
pub async fn kv_transfer<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    collection: &str,
    source_key: &[u8],
    dest_key: &[u8],
    field: &str,
    amount: f64,
) -> Result<QueryResult, LiteError> {
    let src_rkey = kv_key(collection, source_key);
    let dst_rkey = kv_key(collection, dest_key);

    let src_raw = engine
        .storage
        .get(Namespace::Kv, &src_rkey)
        .await
        .map_err(|e| LiteError::Storage {
            detail: e.to_string(),
        })?
        .ok_or_else(|| LiteError::BadRequest {
            detail: format!("Transfer: source key not found in '{collection}'"),
        })?;

    let (src_deadline, src_user_bytes) =
        decode_value(&src_raw).ok_or_else(|| LiteError::Storage {
            detail: "corrupt KV entry: source".into(),
        })?;
    if is_expired(src_deadline) {
        return Err(LiteError::BadRequest {
            detail: "Transfer: source key is expired".into(),
        });
    }

    let mut src_map: std::collections::HashMap<String, nodedb_types::value::Value> =
        zerompk::from_msgpack(src_user_bytes).map_err(|e| LiteError::Serialization {
            detail: format!("Transfer: decode source: {e}"),
        })?;

    let src_balance = extract_f64(&src_map, field)?;
    if src_balance < amount {
        return Err(LiteError::BadRequest {
            detail: format!(
                "Transfer: insufficient balance ({src_balance} < {amount}) in field '{field}'"
            ),
        });
    }

    let dst_raw = engine
        .storage
        .get(Namespace::Kv, &dst_rkey)
        .await
        .map_err(|e| LiteError::Storage {
            detail: e.to_string(),
        })?;

    let (dst_deadline, mut dst_map) = match dst_raw {
        None => (
            0u64,
            std::collections::HashMap::<String, nodedb_types::value::Value>::new(),
        ),
        Some(raw) => {
            let (dl, user_bytes) = decode_value(&raw).ok_or_else(|| LiteError::Storage {
                detail: "corrupt KV entry: dest".into(),
            })?;
            let m: std::collections::HashMap<String, nodedb_types::value::Value> =
                zerompk::from_msgpack(user_bytes).map_err(|e| LiteError::Serialization {
                    detail: format!("Transfer: decode destination value: {e}"),
                })?;
            (dl, m)
        }
    };

    let dst_balance = extract_f64(&dst_map, field).unwrap_or(0.0);

    src_map.insert(
        field.to_string(),
        nodedb_types::value::Value::Float(src_balance - amount),
    );
    dst_map.insert(
        field.to_string(),
        nodedb_types::value::Value::Float(dst_balance + amount),
    );

    let src_bytes = zerompk::to_msgpack_vec(&src_map).map_err(|e| LiteError::Serialization {
        detail: format!("Transfer encode source: {e}"),
    })?;
    let dst_bytes = zerompk::to_msgpack_vec(&dst_map).map_err(|e| LiteError::Serialization {
        detail: format!("Transfer encode dest: {e}"),
    })?;

    let ops = vec![
        WriteOp::Put {
            ns: Namespace::Kv,
            key: src_rkey,
            value: encode_value(src_deadline, &src_bytes),
        },
        WriteOp::Put {
            ns: Namespace::Kv,
            key: dst_rkey,
            value: encode_value(dst_deadline, &dst_bytes),
        },
    ];
    engine
        .storage
        .batch_write(&ops)
        .await
        .map_err(|e| LiteError::Storage {
            detail: e.to_string(),
        })?;

    Ok(QueryResult {
        columns: vec![],
        rows: vec![],
        rows_affected: 2,
    })
}

/// TransferItem: atomic non-fungible item transfer between two collections.
///
/// Deletes item from source collection and inserts at dest collection key.
pub async fn kv_transfer_item<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    source_collection: &str,
    dest_collection: &str,
    item_key: &[u8],
    dest_key: &[u8],
) -> Result<QueryResult, LiteError> {
    let src_rkey = kv_key(source_collection, item_key);
    let src_raw = engine
        .storage
        .get(Namespace::Kv, &src_rkey)
        .await
        .map_err(|e| LiteError::Storage {
            detail: e.to_string(),
        })?
        .ok_or_else(|| LiteError::BadRequest {
            detail: format!(
                "TransferItem: item not found in source collection '{source_collection}'"
            ),
        })?;

    let (src_deadline, src_user_bytes) =
        decode_value(&src_raw).ok_or_else(|| LiteError::Storage {
            detail: "corrupt KV entry: source item".into(),
        })?;
    if is_expired(src_deadline) {
        return Err(LiteError::BadRequest {
            detail: "TransferItem: source item is expired".into(),
        });
    }
    let item_bytes = src_user_bytes.to_vec();

    let dst_rkey = kv_key(dest_collection, dest_key);
    let ops = vec![
        WriteOp::Delete {
            ns: Namespace::Kv,
            key: src_rkey,
        },
        WriteOp::Put {
            ns: Namespace::Kv,
            key: dst_rkey,
            value: encode_value(0, &item_bytes),
        },
    ];
    engine
        .storage
        .batch_write(&ops)
        .await
        .map_err(|e| LiteError::Storage {
            detail: e.to_string(),
        })?;

    Ok(QueryResult {
        columns: vec![],
        rows: vec![],
        rows_affected: 1,
    })
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

fn extract_f64(
    map: &std::collections::HashMap<String, nodedb_types::value::Value>,
    field: &str,
) -> Result<f64, LiteError> {
    match map.get(field) {
        Some(nodedb_types::value::Value::Float(f)) => Ok(*f),
        Some(nodedb_types::value::Value::Integer(i)) => Ok(*i as f64),
        Some(_) => Err(LiteError::BadRequest {
            detail: format!("Transfer: field '{field}' is not numeric"),
        }),
        None => Ok(0.0),
    }
}
