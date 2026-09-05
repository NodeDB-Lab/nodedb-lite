// SPDX-License-Identifier: Apache-2.0
//! Read-only `KvOp` arms: point lookup, scan, TTL peek, batch/field get.

use nodedb_types::{QualifiedCollection, RlsWriteCheck};

use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::query::kv_ops;
use crate::storage::engine::StorageEngine;

use super::super::LitePhysicalFut;
use super::super::policy::deny_policy;

pub(super) fn get<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    collection: &QualifiedCollection,
    key: &[u8],
    rls_filters: &[u8],
    surrogate_ceiling: Option<u32>,
) -> Result<LitePhysicalFut<'a>, LiteError> {
    // Get carries no write-check slot: it never writes.
    deny_policy(
        "KvOp::Get",
        None,
        &[rls_filters],
        &RlsWriteCheck::NoPolicyApplies,
    )?;
    let col = collection.clone();
    let k = key.to_vec();
    Ok(Box::pin(async move {
        kv_ops::reads::kv_get(engine, col.as_str(), &k, surrogate_ceiling).await
    }))
}

pub(super) fn scan<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    collection: &QualifiedCollection,
    cursor: &[u8],
    count: usize,
    match_pattern: Option<&str>,
    surrogate_ceiling: Option<u32>,
) -> Result<LitePhysicalFut<'a>, LiteError> {
    let col = collection.clone();
    let cur = cursor.to_vec();
    let pattern = match_pattern.map(str::to_owned);
    Ok(Box::pin(async move {
        kv_ops::reads::kv_scan(
            engine,
            col.as_str(),
            &cur,
            count,
            pattern.as_deref(),
            surrogate_ceiling,
        )
        .await
    }))
}

pub(super) fn get_ttl<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    collection: &QualifiedCollection,
    key: &[u8],
) -> Result<LitePhysicalFut<'a>, LiteError> {
    let col = collection.clone();
    let k = key.to_vec();
    Ok(Box::pin(async move {
        kv_ops::reads::kv_get_ttl(engine, col.as_str(), &k).await
    }))
}

pub(super) fn batch_get<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    collection: &QualifiedCollection,
    keys: &[Vec<u8>],
    rls_filters: &[u8],
) -> Result<LitePhysicalFut<'a>, LiteError> {
    // BatchGet carries no write-check slot: it never writes.
    deny_policy(
        "KvOp::BatchGet",
        None,
        &[rls_filters],
        &RlsWriteCheck::NoPolicyApplies,
    )?;
    let col = collection.clone();
    let ks = keys.to_vec();
    Ok(Box::pin(async move {
        kv_ops::reads::kv_batch_get(engine, col.as_str(), &ks).await
    }))
}

pub(super) fn field_get<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    collection: &QualifiedCollection,
    key: &[u8],
    fields: &[String],
    rls_filters: &[u8],
) -> Result<LitePhysicalFut<'a>, LiteError> {
    // FieldGet carries no write-check slot: it never writes.
    deny_policy(
        "KvOp::FieldGet",
        None,
        &[rls_filters],
        &RlsWriteCheck::NoPolicyApplies,
    )?;
    let col = collection.clone();
    let k = key.to_vec();
    let flds = fields.to_vec();
    Ok(Box::pin(async move {
        kv_ops::reads::kv_field_get(engine, col.as_str(), &k, &flds).await
    }))
}

pub(super) fn materialize_scan<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    collection: &QualifiedCollection,
    cursor: &[u8],
    count: usize,
) -> Result<LitePhysicalFut<'a>, LiteError> {
    let col = collection.clone();
    let cur = cursor.to_vec();
    Ok(Box::pin(async move {
        kv_ops::reads::kv_materialize_scan(engine, col.as_str(), &cur, count, None).await
    }))
}
