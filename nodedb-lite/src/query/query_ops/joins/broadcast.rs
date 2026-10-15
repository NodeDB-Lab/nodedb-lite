// SPDX-License-Identifier: Apache-2.0
//! BroadcastJoin: small side is pre-materialized in `broadcast_data` or scanned
//! from `small_collection` if `broadcast_data` is empty.

use std::collections::HashMap;

use nodedb_types::result::QueryResult;
use nodedb_types::value::Value;

use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::storage::engine::StorageEngine;

use super::common::{
    apply_filters, apply_projection, decode_filters, hash_join, maps_to_result, scan_collection,
};
use super::spec::{CollectionSide, JoinSpec};

pub async fn execute_broadcast_join<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    large: CollectionSide<'_>,
    small: CollectionSide<'_>,
    broadcast_data: &[u8],
    spec: &JoinSpec<'_>,
) -> Result<QueryResult, LiteError> {
    let large_rows = scan_collection(engine, large.collection).await?;

    let small_rows: Vec<HashMap<String, Value>> = if broadcast_data.is_empty() {
        scan_collection(engine, small.collection).await?
    } else {
        zerompk::from_msgpack(broadcast_data).map_err(|e| LiteError::Serialization {
            detail: format!("decode broadcast data: {e}"),
        })?
    };

    let (large_keys, small_keys) = spec.split_keys();

    // Build on small side, probe with large side. The large alias is already
    // in the field names when the planner prefixed it.
    let joined = hash_join(
        small_rows,
        large_rows,
        &small_keys,
        &large_keys,
        spec.join_type,
        small.alias,
        spec.effective_limit(),
    );

    let pf = decode_filters(spec.post_filters)?;
    let joined = apply_filters(joined, &pf)?;
    let joined = apply_projection(joined, spec.projection);

    Ok(maps_to_result(joined))
}
