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

/// The fields of a `KvOp::RegisterSortedIndex`, borrowed from the plan.
pub(super) struct RegisterSortedIndexArgs<'p> {
    pub collection: &'p QualifiedCollection,
    pub index_name: &'p str,
    pub sort_columns: &'p [(String, String)],
    pub key_column: &'p str,
    pub window_type: &'p str,
    pub window_timestamp_column: &'p str,
    pub window_start_ms: u64,
    pub window_end_ms: u64,
}

pub(super) fn register_sorted_index<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    args: RegisterSortedIndexArgs<'_>,
) -> Result<LitePhysicalFut<'a>, LiteError> {
    let collection = args.collection.as_str().to_owned();
    let index_name = args.index_name.to_owned();
    let sort_columns = args.sort_columns.to_vec();
    let key_column = args.key_column.to_owned();
    let window_type = args.window_type.to_owned();
    let window_timestamp_column = args.window_timestamp_column.to_owned();
    let (window_start_ms, window_end_ms) = (args.window_start_ms, args.window_end_ms);
    Ok(Box::pin(async move {
        kv_ops::sorted::kv_register_sorted_index(
            engine,
            kv_ops::sorted::RegisterSortedIndex {
                collection: &collection,
                index_name: &index_name,
                sort_columns: &sort_columns,
                key_column: &key_column,
                window_type: &window_type,
                window_timestamp_column: &window_timestamp_column,
                window_start_ms,
                window_end_ms,
            },
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
