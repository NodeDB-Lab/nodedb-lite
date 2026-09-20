// SPDX-License-Identifier: Apache-2.0
//! Numeric and compare-and-swap writes for the KV engine: incr, incr_float,
//! cas, get_set.

use nodedb_types::Namespace;
use nodedb_types::result::QueryResult;
use nodedb_types::value::Value;

use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::storage::engine::StorageEngine;

use super::super::reads::{decode_value, encode_value, is_expired, kv_key};

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
        crate::runtime::now_millis().saturating_add(ttl_ms)
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
        command: None,
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
        command: None,
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
        command: None,
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
        command: None,
    })
}
