// SPDX-License-Identifier: Apache-2.0
//! Mutating `KvOp` arms: put/insert/delete/increment/CAS/transfer.

use nodedb_physical::physical_plan::{ReturningSpec, UpdateValue};
use nodedb_types::{QualifiedCollection, RlsWriteCheck};

use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::query::kv_ops;
use crate::storage::engine::StorageEngine;

use super::super::LitePhysicalFut;
use super::super::policy::deny_policy;

pub(super) fn put<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    collection: &QualifiedCollection,
    key: &[u8],
    value: &[u8],
    ttl_ms: u64,
    returning: &Option<ReturningSpec>,
    rls_filters: &[u8],
) -> Result<LitePhysicalFut<'a>, LiteError> {
    // Put has no rls_write_check slot: SET semantics carry no write gate.
    deny_policy(
        "KvOp::Put",
        returning.as_ref(),
        &[rls_filters],
        &RlsWriteCheck::NoPolicyApplies,
    )?;
    let col = collection.clone();
    let k = key.to_vec();
    let v = value.to_vec();
    Ok(Box::pin(async move {
        kv_ops::writes::kv_put(engine, col.as_str(), &k, &v, ttl_ms).await
    }))
}

pub(super) fn insert<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    collection: &QualifiedCollection,
    key: &[u8],
    value: &[u8],
    ttl_ms: u64,
    returning: &Option<ReturningSpec>,
    rls_filters: &[u8],
) -> Result<LitePhysicalFut<'a>, LiteError> {
    // Insert has no rls_write_check slot: an unconditional first write
    // carries no write gate.
    deny_policy(
        "KvOp::Insert",
        returning.as_ref(),
        &[rls_filters],
        &RlsWriteCheck::NoPolicyApplies,
    )?;
    let col = collection.clone();
    let k = key.to_vec();
    let v = value.to_vec();
    Ok(Box::pin(async move {
        kv_ops::writes::kv_insert(engine, col.as_str(), &k, &v, ttl_ms).await
    }))
}

pub(super) fn insert_if_absent<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    collection: &QualifiedCollection,
    key: &[u8],
    value: &[u8],
    ttl_ms: u64,
    returning: &Option<ReturningSpec>,
    rls_filters: &[u8],
) -> Result<LitePhysicalFut<'a>, LiteError> {
    // InsertIfAbsent has no rls_write_check slot: an unconditional
    // first write carries no write gate.
    deny_policy(
        "KvOp::InsertIfAbsent",
        returning.as_ref(),
        &[rls_filters],
        &RlsWriteCheck::NoPolicyApplies,
    )?;
    let col = collection.clone();
    let k = key.to_vec();
    let v = value.to_vec();
    Ok(Box::pin(async move {
        kv_ops::writes::kv_insert_if_absent(engine, col.as_str(), &k, &v, ttl_ms).await
    }))
}

/// Grouped tail fields of `KvOp::InsertOnConflictUpdate`, kept out of the
/// function signature so it stays under clippy's argument-count lint.
pub(super) struct ConflictUpdateArgs<'a> {
    pub value: &'a [u8],
    pub ttl_ms: u64,
    pub updates: &'a [(String, UpdateValue)],
    pub rls_write_check: &'a RlsWriteCheck,
    pub returning: &'a Option<ReturningSpec>,
    pub rls_filters: &'a [u8],
}

pub(super) fn insert_on_conflict_update<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    collection: &QualifiedCollection,
    key: &[u8],
    args: ConflictUpdateArgs<'_>,
) -> Result<LitePhysicalFut<'a>, LiteError> {
    deny_policy(
        "KvOp::InsertOnConflictUpdate",
        args.returning.as_ref(),
        &[args.rls_filters],
        args.rls_write_check,
    )?;
    let col = collection.clone();
    let k = key.to_vec();
    let v = args.value.to_vec();
    let ttl_ms = args.ttl_ms;
    let upd = args.updates.to_vec();
    Ok(Box::pin(async move {
        kv_ops::writes::kv_insert_on_conflict_update(engine, col.as_str(), &k, &v, ttl_ms, &upd)
            .await
    }))
}

pub(super) fn delete<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    collection: &QualifiedCollection,
    keys: &[Vec<u8>],
    rls_write_check: &RlsWriteCheck,
) -> Result<LitePhysicalFut<'a>, LiteError> {
    deny_policy("KvOp::Delete", None, &[], rls_write_check)?;
    let col = collection.clone();
    let ks = keys.to_vec();
    Ok(Box::pin(async move {
        kv_ops::writes::kv_delete(engine, col.as_str(), &ks).await
    }))
}

pub(super) fn batch_put<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    collection: &QualifiedCollection,
    entries: &[(Vec<u8>, Vec<u8>)],
    ttl_ms: u64,
    returning: &Option<ReturningSpec>,
    rls_filters: &[u8],
) -> Result<LitePhysicalFut<'a>, LiteError> {
    // BatchPut has no rls_write_check slot: SET semantics carry no
    // write gate.
    deny_policy(
        "KvOp::BatchPut",
        returning.as_ref(),
        &[rls_filters],
        &RlsWriteCheck::NoPolicyApplies,
    )?;
    let col = collection.clone();
    let ents = entries.to_vec();
    Ok(Box::pin(async move {
        kv_ops::writes::kv_batch_put(engine, col.as_str(), &ents, ttl_ms).await
    }))
}

pub(super) fn expire<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    collection: &QualifiedCollection,
    key: &[u8],
    ttl_ms: u64,
    rls_write_check: &RlsWriteCheck,
) -> Result<LitePhysicalFut<'a>, LiteError> {
    deny_policy("KvOp::Expire", None, &[], rls_write_check)?;
    let col = collection.clone();
    let k = key.to_vec();
    Ok(Box::pin(async move {
        kv_ops::writes::kv_expire(engine, col.as_str(), &k, ttl_ms).await
    }))
}

pub(super) fn persist<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    collection: &QualifiedCollection,
    key: &[u8],
    rls_write_check: &RlsWriteCheck,
) -> Result<LitePhysicalFut<'a>, LiteError> {
    deny_policy("KvOp::Persist", None, &[], rls_write_check)?;
    let col = collection.clone();
    let k = key.to_vec();
    Ok(Box::pin(async move {
        kv_ops::writes::kv_persist(engine, col.as_str(), &k).await
    }))
}

pub(super) fn truncate<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    collection: &QualifiedCollection,
) -> Result<LitePhysicalFut<'a>, LiteError> {
    let col = collection.clone();
    Ok(Box::pin(async move {
        kv_ops::writes::kv_truncate(engine, col.as_str()).await
    }))
}

pub(super) fn incr<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    collection: &QualifiedCollection,
    key: &[u8],
    delta: i64,
    ttl_ms: u64,
    rls_write_check: &RlsWriteCheck,
) -> Result<LitePhysicalFut<'a>, LiteError> {
    deny_policy("KvOp::Incr", None, &[], rls_write_check)?;
    let col = collection.clone();
    let k = key.to_vec();
    Ok(Box::pin(async move {
        kv_ops::writes::kv_incr(engine, col.as_str(), &k, delta, ttl_ms).await
    }))
}

pub(super) fn incr_float<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    collection: &QualifiedCollection,
    key: &[u8],
    delta: f64,
    rls_write_check: &RlsWriteCheck,
) -> Result<LitePhysicalFut<'a>, LiteError> {
    deny_policy("KvOp::IncrFloat", None, &[], rls_write_check)?;
    let col = collection.clone();
    let k = key.to_vec();
    Ok(Box::pin(async move {
        kv_ops::writes::kv_incr_float(engine, col.as_str(), &k, delta).await
    }))
}

pub(super) fn cas<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    collection: &QualifiedCollection,
    key: &[u8],
    expected: &[u8],
    new_value: &[u8],
    rls_write_check: &RlsWriteCheck,
) -> Result<LitePhysicalFut<'a>, LiteError> {
    deny_policy("KvOp::Cas", None, &[], rls_write_check)?;
    let col = collection.clone();
    let k = key.to_vec();
    let exp = expected.to_vec();
    let nv = new_value.to_vec();
    Ok(Box::pin(async move {
        kv_ops::writes::kv_cas(engine, col.as_str(), &k, &exp, &nv).await
    }))
}

pub(super) fn get_set<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    collection: &QualifiedCollection,
    key: &[u8],
    new_value: &[u8],
    rls_filters: &[u8],
    rls_write_check: &RlsWriteCheck,
) -> Result<LitePhysicalFut<'a>, LiteError> {
    deny_policy("KvOp::GetSet", None, &[rls_filters], rls_write_check)?;
    let col = collection.clone();
    let k = key.to_vec();
    let nv = new_value.to_vec();
    Ok(Box::pin(async move {
        kv_ops::writes::kv_get_set(engine, col.as_str(), &k, &nv).await
    }))
}

pub(super) fn field_set<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    collection: &QualifiedCollection,
    key: &[u8],
    updates: &[(String, Vec<u8>)],
    rls_write_check: &RlsWriteCheck,
) -> Result<LitePhysicalFut<'a>, LiteError> {
    deny_policy("KvOp::FieldSet", None, &[], rls_write_check)?;
    let col = collection.clone();
    let k = key.to_vec();
    let upd = updates.to_vec();
    Ok(Box::pin(async move {
        kv_ops::writes::kv_field_set(engine, col.as_str(), &k, &upd).await
    }))
}

pub(super) fn transfer<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    collection: &QualifiedCollection,
    source_key: &[u8],
    dest_key: &[u8],
    field: &str,
    amount: f64,
    rls_write_check: &RlsWriteCheck,
) -> Result<LitePhysicalFut<'a>, LiteError> {
    deny_policy("KvOp::Transfer", None, &[], rls_write_check)?;
    let col = collection.clone();
    let src = source_key.to_vec();
    let dst = dest_key.to_vec();
    let fld = field.to_owned();
    Ok(Box::pin(async move {
        kv_ops::writes::kv_transfer(engine, col.as_str(), &src, &dst, &fld, amount).await
    }))
}

pub(super) fn transfer_item<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    source_collection: &QualifiedCollection,
    dest_collection: &QualifiedCollection,
    item_key: &[u8],
    dest_key: &[u8],
    source_rls_write_check: &RlsWriteCheck,
    dest_rls_write_check: &RlsWriteCheck,
) -> Result<LitePhysicalFut<'a>, LiteError> {
    // Source and destination carry independent write gates: an
    // identity may give a row up but not receive it, so both check.
    deny_policy("KvOp::TransferItem", None, &[], source_rls_write_check)?;
    deny_policy("KvOp::TransferItem", None, &[], dest_rls_write_check)?;
    let src_col = source_collection.clone();
    let dst_col = dest_collection.clone();
    let ik = item_key.to_vec();
    let dk = dest_key.to_vec();
    Ok(Box::pin(async move {
        kv_ops::writes::kv_transfer_item(engine, src_col.as_str(), dst_col.as_str(), &ik, &dk)
            .await
    }))
}
