// SPDX-License-Identifier: Apache-2.0
//! HashJoin implementation for Lite.
//!
//! Supports Inner/Left/Right/Full/Semi/Anti join types. Post-join filters
//! and projection are applied after the join.

use nodedb_types::result::QueryResult;

use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::storage::engine::StorageEngine;

use super::common::{
    apply_filters, apply_projection, decode_filters, hash_join, maps_to_result, scan_collection,
};
use super::spec::{CollectionSide, JoinSpec};

pub async fn execute_hash_join<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    left: CollectionSide<'_>,
    right: CollectionSide<'_>,
    spec: &JoinSpec<'_>,
) -> Result<QueryResult, LiteError> {
    let left_rows = scan_collection(engine, left.collection).await?;
    let right_rows = scan_collection(engine, right.collection).await?;

    let (left_keys, right_keys) = spec.split_keys();

    let joined = hash_join(
        right_rows,
        left_rows,
        &right_keys,
        &left_keys,
        spec.join_type,
        right.alias,
        spec.effective_limit(),
    );

    // Apply left alias prefix to left columns (post-join).
    let joined = if let Some(alias) = left.alias {
        joined
            .into_iter()
            .map(|row| {
                row.into_iter()
                    .map(|(k, v)| {
                        // Only prefix keys that don't already have a dot (right alias already applied).
                        if !k.contains('.') {
                            (format!("{alias}.{k}"), v)
                        } else {
                            (k, v)
                        }
                    })
                    .collect()
            })
            .collect()
    } else {
        joined
    };

    let pf = decode_filters(spec.post_filters)?;
    let joined = apply_filters(joined, &pf)?;

    let joined = apply_projection(joined, spec.projection);

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
    fn inner_join() {
        let left = vec![
            row(&[
                ("id", Value::Integer(1)),
                ("name", Value::String("Alice".into())),
            ]),
            row(&[
                ("id", Value::Integer(2)),
                ("name", Value::String("Bob".into())),
            ]),
        ];
        let right = vec![
            row(&[
                ("user_id", Value::Integer(1)),
                ("score", Value::Integer(90)),
            ]),
            row(&[
                ("user_id", Value::Integer(3)),
                ("score", Value::Integer(80)),
            ]),
        ];
        let result = hash_join(
            right,
            left,
            &["user_id".into()],
            &["id".into()],
            "inner",
            None,
            usize::MAX,
        );
        assert_eq!(result.len(), 1);
        assert_eq!(result[0]["name"], Value::String("Alice".into()));
    }

    #[test]
    fn left_join_preserves_unmatched() {
        let left = vec![
            row(&[("id", Value::Integer(1))]),
            row(&[("id", Value::Integer(99))]),
        ];
        let right = vec![row(&[
            ("user_id", Value::Integer(1)),
            ("val", Value::Integer(10)),
        ])];
        let result = hash_join(
            right,
            left,
            &["user_id".into()],
            &["id".into()],
            "left",
            None,
            usize::MAX,
        );
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn semi_join() {
        let left = vec![
            row(&[("id", Value::Integer(1))]),
            row(&[("id", Value::Integer(2))]),
        ];
        let right = vec![row(&[("user_id", Value::Integer(1))])];
        let result = hash_join(
            right,
            left,
            &["user_id".into()],
            &["id".into()],
            "semi",
            None,
            usize::MAX,
        );
        assert_eq!(result.len(), 1);
        assert!(result[0].contains_key("id"));
        // Semi join must not include right-side columns.
        assert!(!result[0].contains_key("user_id"));
    }
}
