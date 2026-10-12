// SPDX-License-Identifier: Apache-2.0
//! Multi-key writes for the KV engine: delete, batch put, truncate.

use nodedb_types::Namespace;
use nodedb_types::result::QueryResult;

use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::query::truncate::truncated;
use crate::storage::engine::{StorageEngine, WriteOp};

use super::super::indexes::kv_truncate_commit;
use super::super::reads::{encode_value, kv_key};

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
    engine.kv_local.commit(&*engine.storage, ops).await?;
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
    engine.kv_local.commit(&*engine.storage, ops).await?;
    Ok(QueryResult {
        columns: vec![],
        rows: vec![],
        rows_affected: count,
        command: Some("UPSERT".into()),
    })
}

/// Truncate: delete ALL entries in a KV collection and every secondary
/// index entry they produced, and drop the collection's sorted indexes. The
/// collection and its secondary index definitions stay registered. Buffered
/// writes and cached values of the public KV API are forgotten first, so a
/// pending put cannot resurrect a row and a cached value is not served past
/// the clear.
pub async fn kv_truncate<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    collection: &str,
) -> Result<QueryResult, LiteError> {
    engine.kv_local.forget_collection(collection);
    kv_truncate_commit(&*engine.storage, &engine.indexes, collection).await?;
    Ok(truncated())
}
