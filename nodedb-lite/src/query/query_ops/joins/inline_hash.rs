// SPDX-License-Identifier: Apache-2.0
//! InlineHashJoin: both sides are pre-materialized msgpack bytes.

use std::collections::HashMap;

use nodedb_physical::physical_plan::query::JoinProjection;
use nodedb_types::result::QueryResult;
use nodedb_types::value::Value;

use crate::error::LiteError;

use super::common::{apply_filters, apply_projection, decode_filters, hash_join, maps_to_result};

#[allow(clippy::too_many_arguments)]
pub fn execute_inline_hash_join(
    left_data: &[u8],
    right_data: &[u8],
    right_alias: Option<&str>,
    on: &[(String, String)],
    join_type: &str,
    limit: usize,
    projection: &[JoinProjection],
    post_filters: &[u8],
) -> Result<QueryResult, LiteError> {
    let left_rows = decode_msgpack_rows(left_data)?;
    let right_rows = decode_msgpack_rows(right_data)?;

    let left_keys: Vec<String> = on.iter().map(|(l, _)| l.clone()).collect();
    let right_keys: Vec<String> = on.iter().map(|(_, r)| r.clone()).collect();

    let effective_limit = if limit == 0 { usize::MAX } else { limit };

    let joined = hash_join(
        right_rows,
        left_rows,
        &right_keys,
        &left_keys,
        join_type,
        right_alias,
        effective_limit,
    );

    let pf = decode_filters(post_filters)?;
    let joined = apply_filters(joined, &pf);
    let joined = apply_projection(joined, projection);
    Ok(maps_to_result(joined))
}

fn decode_msgpack_rows(bytes: &[u8]) -> Result<Vec<HashMap<String, Value>>, LiteError> {
    if bytes.is_empty() {
        return Ok(Vec::new());
    }
    zerompk::from_msgpack(bytes).map_err(|e| LiteError::Serialization {
        detail: format!("decode inline join data: {e}"),
    })
}
