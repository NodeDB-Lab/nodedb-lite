// SPDX-License-Identifier: Apache-2.0
//! InlineHashJoin: both sides are pre-materialized msgpack bytes.

use std::collections::HashMap;

use nodedb_types::result::QueryResult;
use nodedb_types::value::Value;

use crate::error::LiteError;

use super::common::{apply_filters, apply_projection, decode_filters, hash_join, maps_to_result};
use super::spec::JoinSpec;

pub fn execute_inline_hash_join(
    left_data: &[u8],
    right_data: &[u8],
    right_alias: Option<&str>,
    spec: &JoinSpec<'_>,
) -> Result<QueryResult, LiteError> {
    let left_rows = decode_msgpack_rows(left_data)?;
    let right_rows = decode_msgpack_rows(right_data)?;

    let (left_keys, right_keys) = spec.split_keys();

    let joined = hash_join(
        right_rows,
        left_rows,
        &right_keys,
        &left_keys,
        spec.join_type,
        right_alias,
        spec.effective_limit(),
    );

    let pf = decode_filters(spec.post_filters)?;
    let joined = apply_filters(joined, &pf)?;
    let joined = apply_projection(joined, spec.projection);
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
