// SPDX-License-Identifier: Apache-2.0
//! Field-level and cross-key writes for the KV engine: field_set, transfer,
//! transfer_item.

use nodedb_types::Namespace;
use nodedb_types::result::QueryResult;

use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::storage::engine::{StorageEngine, WriteOp};

use super::super::reads::{decode_value, encode_value, is_expired, kv_key};

/// FieldSet: read-modify-write on named fields of a MessagePack map value.
///
/// `if_present` is the SQL `UPDATE` contract: an absent or expired key is
/// `UPDATE 0` and no row is created. `false` is the RESP hash-set contract,
/// which creates the row from nothing.
pub async fn kv_field_set<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    collection: &str,
    key: &[u8],
    field_updates: &[(String, Vec<u8>)],
    if_present: bool,
) -> Result<QueryResult, LiteError> {
    let rkey = kv_key(collection, key);
    let stored = engine
        .storage
        .get(Namespace::Kv, &rkey)
        .await
        .map_err(|e| LiteError::Storage {
            detail: e.to_string(),
        })?;

    let live = match stored {
        None => None,
        Some(raw) => {
            let (deadline, user_bytes) = decode_value(&raw).ok_or_else(|| LiteError::Storage {
                detail: "corrupt KV entry".into(),
            })?;
            if is_expired(deadline) {
                None
            } else {
                let m: std::collections::HashMap<String, nodedb_types::value::Value> =
                    zerompk::from_msgpack(user_bytes).map_err(|e| LiteError::Serialization {
                        detail: format!("FieldSet: decode existing value: {e}"),
                    })?;
                Some((deadline, m))
            }
        }
    };
    let (old_deadline, mut map) = match live {
        Some(live) => live,
        None if if_present => {
            return Ok(QueryResult {
                columns: vec![],
                rows: vec![],
                rows_affected: 0,
            });
        }
        None => (0u64, std::collections::HashMap::new()),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::engine::test_engine;
    use crate::query::kv_ops::reads::kv_get;

    fn update(field: &str, value: i64) -> (String, Vec<u8>) {
        let bytes = zerompk::to_msgpack_vec(&nodedb_types::value::Value::Integer(value))
            .expect("encode field");
        (field.to_string(), bytes)
    }

    #[tokio::test]
    async fn field_set_if_present_on_absent_key_is_a_no_op() {
        let engine = test_engine().await;
        let r = kv_field_set(&engine, "kvfs", b"missing", &[update("n", 1)], true)
            .await
            .expect("field set");
        assert_eq!(r.rows_affected, 0);
        let stored = kv_get(&engine, "kvfs", b"missing", None)
            .await
            .expect("get");
        assert!(stored.rows.is_empty(), "no row is created");
    }

    #[tokio::test]
    async fn field_set_without_if_present_creates_the_row() {
        let engine = test_engine().await;
        let r = kv_field_set(&engine, "kvfs", b"fresh", &[update("n", 1)], false)
            .await
            .expect("field set");
        assert_eq!(r.rows_affected, 1);
        let stored = kv_get(&engine, "kvfs", b"fresh", None).await.expect("get");
        assert_eq!(stored.rows.len(), 1);
    }

    #[tokio::test]
    async fn field_set_if_present_merges_into_an_existing_row() {
        let engine = test_engine().await;
        kv_field_set(&engine, "kvfs", b"k", &[update("a", 1)], false)
            .await
            .expect("seed");
        let r = kv_field_set(&engine, "kvfs", b"k", &[update("b", 2)], true)
            .await
            .expect("field set");
        assert_eq!(r.rows_affected, 1);
    }
}
