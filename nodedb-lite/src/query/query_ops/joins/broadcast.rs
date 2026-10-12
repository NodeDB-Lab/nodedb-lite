// SPDX-License-Identifier: Apache-2.0
//! BroadcastJoin: small side is pre-materialized in `broadcast_data` or scanned
//! from `small_collection` if `broadcast_data` is empty.

use std::collections::HashMap;

use nodedb_physical::physical_plan::query::AggregateSpec;
use nodedb_query::expr::GroupKeySpec;
use nodedb_types::result::QueryResult;
use nodedb_types::value::Value;

use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::storage::engine::StorageEngine;

use super::common::{
    apply_filters, apply_projection, decode_filters, hash_join, maps_to_result, scan_collection,
};
use super::spec::{CollectionSide, JoinSpec, PostAggregate};
use crate::query::query_ops::aggregate::execute_aggregate;

pub async fn execute_broadcast_join<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    large: CollectionSide<'_>,
    small: CollectionSide<'_>,
    broadcast_data: &[u8],
    spec: &JoinSpec<'_>,
    post: &PostAggregate<'_>,
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

    if !post.group_by.is_empty() || !post.aggregates.is_empty() {
        let agg_specs: Vec<AggregateSpec> = post
            .aggregates
            .iter()
            .map(|(func, field)| AggregateSpec {
                function: func.clone(),
                alias: format!("{func}_{field}"),
                user_alias: None,
                field: field.clone(),
                expr: None,
            })
            .collect();
        // Post-join grouping is by bare output column name, so the lift is lossless.
        let group_specs: Vec<GroupKeySpec> =
            post.group_by.iter().map(GroupKeySpec::column).collect();
        return execute_aggregate(joined, &group_specs, &agg_specs, &[], &[], &[], &[]);
    }

    Ok(maps_to_result(joined))
}
