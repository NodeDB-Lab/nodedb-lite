// SPDX-License-Identifier: Apache-2.0
//! Point writes for the KV engine: put, insert variants, delete, batch put,
//! expire, persist, truncate.

use std::collections::HashMap;

use nodedb_physical::physical_plan::document::UpdateValue;
use nodedb_types::Namespace;
use nodedb_types::result::QueryResult;
use nodedb_types::value::Value;

use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::query::on_conflict::apply_patch;
use crate::query::truncate::truncated;
use crate::storage::engine::{StorageEngine, WriteOp};

use super::super::reads::{decode_value, encode_value, is_expired, kv_key, split_kv_key};

/// `kv_put`, tagged `INSERT` instead of `UPSERT`. Backs every insert variant
/// that falls through to an unconditional put once absence is confirmed.
async fn insert_via_put<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    collection: &str,
    key: &[u8],
    value: &[u8],
    ttl_ms: u64,
) -> Result<QueryResult, LiteError> {
    let mut result = kv_put(engine, collection, key, value, ttl_ms).await?;
    result.command = Some("INSERT".into());
    Ok(result)
}

/// Put: unconditional upsert.
pub async fn kv_put<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    collection: &str,
    key: &[u8],
    value: &[u8],
    ttl_ms: u64,
) -> Result<QueryResult, LiteError> {
    let deadline = if ttl_ms > 0 {
        crate::runtime::now_millis().saturating_add(ttl_ms)
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
        command: Some("UPSERT".into()),
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
    insert_via_put(engine, collection, key, value, ttl_ms).await
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
            command: Some("INSERT".into()),
        });
    }
    insert_via_put(engine, collection, key, value, ttl_ms).await
}

/// InsertOnConflictUpdate: write if absent; on conflict apply field updates.
///
/// `updates` carries `UpdateValue`: a `Literal` decodes and overwrites the
/// field directly; an `Expr` (`n + 1`, `EXCLUDED.n`, ...) evaluates via
/// `query::on_conflict::apply_patch` against the existing stored row, with
/// `EXCLUDED.col` bound to `value` (the row that would have been inserted).
/// A stored value that decodes to a non-object (the single-`value`-column
/// plain KV row) has no named fields to merge onto, so it starts from an
/// empty row — the same fallback Origin's `apply_on_conflict_updates` takes.
pub async fn kv_insert_on_conflict_update<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    collection: &str,
    key: &[u8],
    value: &[u8],
    ttl_ms: u64,
    updates: &[(String, UpdateValue)],
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
        None => return insert_via_put(engine, collection, key, value, ttl_ms).await,
        Some(raw) => match decode_value(&raw) {
            None => return insert_via_put(engine, collection, key, value, ttl_ms).await,
            Some((deadline, _)) if is_expired(deadline) => {
                return insert_via_put(engine, collection, key, value, ttl_ms).await;
            }
            Some(_) => raw,
        },
    };

    let (old_deadline, old_user_bytes) = decode_value(&raw).ok_or_else(|| LiteError::Storage {
        detail: "corrupt KV entry".into(),
    })?;

    // A multi-column KV row is the zerompk map `encode_kv_value` writes, so
    // it decodes as that map; the single-`value` raw form is not a map and
    // reports a decode error, the same outcome the Origin handler has.
    let mut map: HashMap<String, Value> =
        zerompk::from_msgpack(old_user_bytes).map_err(|e| LiteError::Serialization {
            detail: format!("InsertOnConflictUpdate: decode existing value: {e}"),
        })?;
    let excluded: HashMap<String, Value> =
        zerompk::from_msgpack(value).map_err(|e| LiteError::Serialization {
            detail: format!("InsertOnConflictUpdate: decode incoming value: {e}"),
        })?;

    apply_patch(&mut map, updates, &excluded)?;

    let new_user_bytes = zerompk::to_msgpack_vec(&map).map_err(|e| LiteError::Serialization {
        detail: format!("encode updated KV value: {e}"),
    })?;

    let keep_deadline = if ttl_ms > 0 {
        crate::runtime::now_millis().saturating_add(ttl_ms)
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
        command: Some("UPDATE".into()),
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
        command: Some("DELETE".into()),
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
        crate::runtime::now_millis().saturating_add(ttl_ms)
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
        command: Some("UPSERT".into()),
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
            command: None,
        }),
        Some(raw) => {
            let (_, user_bytes) = decode_value(&raw).ok_or_else(|| LiteError::Storage {
                detail: "corrupt KV entry".into(),
            })?;
            let deadline = crate::runtime::now_millis().saturating_add(ttl_ms);
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
                command: None,
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
            command: None,
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
                command: None,
            })
        }
    }
}

/// Truncate: delete ALL entries in a KV collection and every secondary
/// index posting they produced. The collection stays registered. Buffered
/// writes and cached values of the public KV API are forgotten first, so a
/// pending put cannot resurrect a row and a cached value is not served past
/// the clear.
pub async fn kv_truncate<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    collection: &str,
) -> Result<QueryResult, LiteError> {
    engine.kv_local.forget_collection(collection);
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

    // Secondary index postings: `kv:{collection}:{field}:{value}` in Meta.
    let index_prefix = super::super::indexes::collection_index_prefix(collection);
    let postings = engine
        .storage
        .scan_range_bounded(Namespace::Meta, Some(index_prefix.as_bytes()), None, None)
        .await
        .map_err(|e| LiteError::Storage {
            detail: e.to_string(),
        })?;
    for (key, _) in &postings {
        if !key.starts_with(index_prefix.as_bytes()) {
            break;
        }
        ops.push(WriteOp::Delete {
            ns: Namespace::Meta,
            key: key.clone(),
        });
    }

    if !ops.is_empty() {
        engine
            .storage
            .batch_write(&ops)
            .await
            .map_err(|e| LiteError::Storage {
                detail: e.to_string(),
            })?;
    }
    Ok(truncated())
}

#[cfg(test)]
mod tests {
    use nodedb_query::expr::types::{BinaryOp, SqlExpr as QExpr};

    use super::*;
    use crate::query::engine::test_engine;
    use crate::query::kv_ops::reads::kv_get;

    /// Encode a `{field: value}` map, the shape `encode_kv_value` produces
    /// for a multi-column KV row.
    fn row_bytes(fields: &[(&str, i64)]) -> Vec<u8> {
        let map: HashMap<String, Value> = fields
            .iter()
            .map(|(k, v)| (k.to_string(), Value::Integer(*v)))
            .collect();
        zerompk::to_msgpack_vec(&map).expect("encode row")
    }

    fn literal(n: i64) -> UpdateValue {
        UpdateValue::Literal(zerompk::to_msgpack_vec(&Value::Integer(n)).expect("encode literal"))
    }

    /// Read back a stored KV row's `n` field.
    async fn stored_n<S: StorageEngine>(engine: &LiteQueryEngine<S>, key: &[u8]) -> i64 {
        let r = kv_get(engine, "kvoc", key, None).await.expect("get");
        let Value::Bytes(bytes) = &r.rows[0][1] else {
            panic!("kv_get value column is not bytes");
        };
        let map: HashMap<String, Value> = zerompk::from_msgpack(bytes).expect("decode row");
        match map.get("n") {
            Some(Value::Integer(n)) => *n,
            other => panic!("expected integer 'n', got {other:?}"),
        }
    }

    #[tokio::test]
    async fn literal_assignment_overwrites_field() {
        let engine = test_engine().await;
        kv_put(&engine, "kvoc", b"k", &row_bytes(&[("n", 1)]), 0)
            .await
            .expect("seed");
        let updates = vec![("n".to_string(), literal(5))];
        let r = kv_insert_on_conflict_update(
            &engine,
            "kvoc",
            b"k",
            &row_bytes(&[("n", 99)]),
            0,
            &updates,
        )
        .await
        .expect("on conflict update");
        assert_eq!(r.rows_affected, 1);
        assert_eq!(stored_n(&engine, b"k").await, 5);
    }

    #[tokio::test]
    async fn expr_assignment_evaluates_against_existing_row() {
        let engine = test_engine().await;
        kv_put(&engine, "kvoc", b"k2", &row_bytes(&[("n", 1)]), 0)
            .await
            .expect("seed");
        let expr = QExpr::BinaryOp {
            left: Box::new(QExpr::Column("n".to_string())),
            op: BinaryOp::Add,
            right: Box::new(QExpr::Literal(Value::Integer(1))),
        };
        let updates = vec![("n".to_string(), UpdateValue::Expr(expr))];
        let r = kv_insert_on_conflict_update(
            &engine,
            "kvoc",
            b"k2",
            &row_bytes(&[("n", 99)]),
            0,
            &updates,
        )
        .await
        .expect("on conflict update");
        assert_eq!(r.rows_affected, 1);
        // `n + 1` against the existing row (1), not the incoming row (99).
        assert_eq!(stored_n(&engine, b"k2").await, 2);
    }

    #[tokio::test]
    async fn excluded_assignment_resolves_to_incoming_row() {
        let engine = test_engine().await;
        kv_put(&engine, "kvoc", b"k3", &row_bytes(&[("n", 1)]), 0)
            .await
            .expect("seed");
        let expr = QExpr::ExcludedColumn("n".to_string());
        let updates = vec![("n".to_string(), UpdateValue::Expr(expr))];
        let r = kv_insert_on_conflict_update(
            &engine,
            "kvoc",
            b"k3",
            &row_bytes(&[("n", 42)]),
            0,
            &updates,
        )
        .await
        .expect("on conflict update");
        assert_eq!(r.rows_affected, 1);
        assert_eq!(stored_n(&engine, b"k3").await, 42);
    }

    #[tokio::test]
    async fn absent_key_writes_the_incoming_row_unmerged() {
        let engine = test_engine().await;
        let updates = vec![("n".to_string(), literal(5))];
        let r = kv_insert_on_conflict_update(
            &engine,
            "kvoc",
            b"missing",
            &row_bytes(&[("n", 7)]),
            0,
            &updates,
        )
        .await
        .expect("insert");
        assert_eq!(r.rows_affected, 1);
        assert_eq!(stored_n(&engine, b"missing").await, 7);
    }

    #[tokio::test]
    async fn truncate_removes_rows_and_index_postings() {
        use crate::query::kv_ops::indexes::kv_register_index;
        let engine = test_engine().await;
        for (k, n) in [("a", 1), ("b", 2), ("c", 3)] {
            kv_put(&engine, "kvt", k.as_bytes(), &row_bytes(&[("n", n)]), 0)
                .await
                .expect("seed");
        }
        kv_put(&engine, "kvoc", b"z", &row_bytes(&[("n", 9)]), 0)
            .await
            .expect("seed other");
        kv_register_index(&engine, "kvt", "n", true)
            .await
            .expect("register index");
        let posting_prefix = b"kv:kvt:";
        let before = engine
            .storage
            .scan_range_bounded(Namespace::Meta, Some(posting_prefix), None, None)
            .await
            .expect("scan");
        assert_eq!(before.len(), 3, "backfill wrote one posting per value");

        let r = kv_truncate(&engine, "kvt").await.expect("truncate");
        assert_eq!(r.rows_affected, 0);
        assert_eq!(r.command.as_deref(), Some("TRUNCATE"));
        for k in ["a", "b", "c"] {
            let got = kv_get(&engine, "kvt", k.as_bytes(), None)
                .await
                .expect("get");
            assert!(got.rows.is_empty(), "{k} survives truncate");
        }
        let after = engine
            .storage
            .scan_range_bounded(Namespace::Meta, Some(posting_prefix), None, None)
            .await
            .expect("scan");
        assert!(
            after.iter().all(|(k, _)| !k.starts_with(posting_prefix)),
            "index postings survive truncate"
        );
        assert_eq!(
            stored_n(&engine, b"z").await,
            9,
            "other collection untouched"
        );
    }
}
