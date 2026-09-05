// SPDX-License-Identifier: Apache-2.0
//! Secondary-index and sorted-index (leaderboard) `KvOp` arms.

use nodedb_types::QualifiedCollection;

use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::query::kv_ops;
use crate::storage::engine::StorageEngine;

use super::super::LitePhysicalFut;

pub(super) fn register_index<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    collection: &QualifiedCollection,
    field: &str,
    backfill: bool,
) -> Result<LitePhysicalFut<'a>, LiteError> {
    let col = collection.clone();
    let fld = field.to_owned();
    Ok(Box::pin(async move {
        kv_ops::indexes::kv_register_index(engine, col.as_str(), &fld, backfill).await
    }))
}

pub(super) fn drop_index<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    collection: &QualifiedCollection,
    field: &str,
) -> Result<LitePhysicalFut<'a>, LiteError> {
    let col = collection.clone();
    let fld = field.to_owned();
    Ok(Box::pin(async move {
        kv_ops::indexes::kv_drop_index(engine, col.as_str(), &fld).await
    }))
}

pub(super) fn register_sorted_index<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    index_name: &str,
    window_type: &str,
    window_timestamp_column: &str,
    window_start_ms: u64,
    window_end_ms: u64,
) -> Result<LitePhysicalFut<'a>, LiteError> {
    let name = index_name.to_owned();
    let wt = window_type.to_owned();
    let ts_col = window_timestamp_column.to_owned();
    Ok(Box::pin(async move {
        kv_ops::sorted::kv_register_sorted_index(
            engine,
            &name,
            &wt,
            &ts_col,
            window_start_ms,
            window_end_ms,
        )
        .await
    }))
}

pub(super) fn drop_sorted_index<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    index_name: &str,
) -> Result<LitePhysicalFut<'a>, LiteError> {
    let name = index_name.to_owned();
    Ok(Box::pin(async move {
        kv_ops::sorted::kv_drop_sorted_index(engine, &name).await
    }))
}

pub(super) fn sorted_index_rank<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    index_name: &str,
    primary_key: &[u8],
) -> Result<LitePhysicalFut<'a>, LiteError> {
    let name = index_name.to_owned();
    let pk = primary_key.to_vec();
    Ok(Box::pin(async move {
        kv_ops::sorted::kv_sorted_index_rank(engine, &name, &pk).await
    }))
}

pub(super) fn sorted_index_top_k<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    index_name: &str,
    k: u32,
) -> Result<LitePhysicalFut<'a>, LiteError> {
    let name = index_name.to_owned();
    Ok(Box::pin(async move {
        kv_ops::sorted::kv_sorted_index_top_k(engine, &name, k).await
    }))
}

pub(super) fn sorted_index_range<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    index_name: &str,
    score_min: Option<&[u8]>,
    score_max: Option<&[u8]>,
) -> Result<LitePhysicalFut<'a>, LiteError> {
    let name = index_name.to_owned();
    let smin = score_min.map(<[u8]>::to_vec);
    let smax = score_max.map(<[u8]>::to_vec);
    Ok(Box::pin(async move {
        kv_ops::sorted::kv_sorted_index_range(engine, &name, smin.as_deref(), smax.as_deref()).await
    }))
}

pub(super) fn sorted_index_count<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    index_name: &str,
) -> Result<LitePhysicalFut<'a>, LiteError> {
    let name = index_name.to_owned();
    Ok(Box::pin(async move {
        kv_ops::sorted::kv_sorted_index_count(engine, &name).await
    }))
}

pub(super) fn sorted_index_score<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    index_name: &str,
    primary_key: &[u8],
) -> Result<LitePhysicalFut<'a>, LiteError> {
    let name = index_name.to_owned();
    let pk = primary_key.to_vec();
    Ok(Box::pin(async move {
        kv_ops::sorted::kv_sorted_index_score(engine, &name, &pk).await
    }))
}
