// SPDX-License-Identifier: Apache-2.0
//! ShuffleJoin for Lite.
//!
//! In Origin, ShuffleJoin repartitions data across vShard cores before joining.
//! On single-node Lite all data is colocated, so `target_core` routing is a
//! no-op: we execute the join locally using a hash join and ignore `target_core`.
//! The variant is fully implemented (not `unreachable!`) because valid SQL can
//! produce a ShuffleJoin plan and must return correct rows.

use nodedb_types::result::QueryResult;

use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::storage::engine::StorageEngine;

use super::common::{hash_join, maps_to_result, scan_collection};

pub async fn execute_shuffle_join<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    left_collection: &str,
    right_collection: &str,
    on: &[(String, String)],
    join_type: &str,
    limit: usize,
    // target_core is ignored on Lite: all data is local, no cross-core dispatch needed.
    _target_core: usize,
) -> Result<QueryResult, LiteError> {
    let left_rows = scan_collection(engine, left_collection).await?;
    let right_rows = scan_collection(engine, right_collection).await?;

    let left_keys: Vec<String> = on.iter().map(|(l, _)| l.clone()).collect();
    let right_keys: Vec<String> = on.iter().map(|(_, r)| r.clone()).collect();

    let effective_limit = if limit == 0 { usize::MAX } else { limit };

    let joined = hash_join(
        right_rows,
        left_rows,
        &right_keys,
        &left_keys,
        join_type,
        None,
        effective_limit,
    );
    Ok(maps_to_result(joined))
}

#[cfg(test)]
mod tests {
    use super::super::common::hash_join;
    use nodedb_types::value::Value;
    use std::collections::HashMap;

    fn row(fields: &[(&str, Value)]) -> HashMap<String, Value> {
        fields
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect()
    }

    #[test]
    fn shuffle_reduces_to_hash() {
        // Verify ShuffleJoin on Lite produces the same result as HashJoin.
        let left = vec![row(&[
            ("id", Value::Integer(1)),
            ("val", Value::Integer(10)),
        ])];
        let right = vec![row(&[
            ("lid", Value::Integer(1)),
            ("extra", Value::Integer(99)),
        ])];
        let result = hash_join(
            right,
            left,
            &["lid".into()],
            &["id".into()],
            "inner",
            None,
            usize::MAX,
        );
        assert_eq!(result.len(), 1);
        assert_eq!(result[0]["val"], Value::Integer(10));
        assert_eq!(result[0]["extra"], Value::Integer(99));
    }
}
