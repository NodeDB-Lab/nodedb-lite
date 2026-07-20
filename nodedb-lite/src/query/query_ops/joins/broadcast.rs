// SPDX-License-Identifier: Apache-2.0
//! BroadcastJoin: small side is pre-materialized in `broadcast_data` or scanned
//! from `small_collection` if `broadcast_data` is empty.

use std::collections::HashMap;

use nodedb_physical::physical_plan::query::{AggregateSpec, JoinProjection};
use nodedb_query::expr::GroupKeySpec;
use nodedb_types::result::QueryResult;
use nodedb_types::value::Value;

use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::storage::engine::StorageEngine;

use super::common::{
    apply_filters, apply_projection, decode_filters, hash_join, maps_to_result, scan_collection,
};
use crate::query::query_ops::aggregate::execute_aggregate;

#[allow(clippy::too_many_arguments)]
pub async fn execute_broadcast_join<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    large_collection: &str,
    small_collection: &str,
    large_alias: Option<&str>,
    small_alias: Option<&str>,
    broadcast_data: &[u8],
    on: &[(String, String)],
    join_type: &str,
    limit: usize,
    post_group_by: &[String],
    post_aggregates: &[(String, String)],
    projection: &[JoinProjection],
    post_filters: &[u8],
) -> Result<QueryResult, LiteError> {
    let large_rows = scan_collection(engine, large_collection).await?;

    let small_rows: Vec<HashMap<String, Value>> = if broadcast_data.is_empty() {
        scan_collection(engine, small_collection).await?
    } else {
        zerompk::from_msgpack(broadcast_data).map_err(|e| LiteError::Serialization {
            detail: format!("decode broadcast data: {e}"),
        })?
    };

    let large_keys: Vec<String> = on.iter().map(|(l, _)| l.clone()).collect();
    let small_keys: Vec<String> = on.iter().map(|(_, r)| r.clone()).collect();

    let effective_limit = if limit == 0 { usize::MAX } else { limit };

    // Build on small side, probe with large side.
    let joined = hash_join(
        small_rows,
        large_rows,
        &small_keys,
        &large_keys,
        join_type,
        small_alias,
        effective_limit,
    );

    let _ = large_alias; // alias already in field names if prefixed by planner

    let pf = decode_filters(post_filters)?;
    let joined = apply_filters(joined, &pf);
    let joined = apply_projection(joined, projection);

    if !post_group_by.is_empty() || !post_aggregates.is_empty() {
        let agg_specs: Vec<AggregateSpec> = post_aggregates
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
            post_group_by.iter().map(GroupKeySpec::column).collect();
        return execute_aggregate(joined, &group_specs, &agg_specs, &[], &[], &[], &[]);
    }

    Ok(maps_to_result(joined))
}
