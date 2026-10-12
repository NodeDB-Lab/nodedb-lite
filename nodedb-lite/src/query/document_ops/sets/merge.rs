// SPDX-License-Identifier: Apache-2.0
use super::DocumentJoin;
use super::actions::{MergeWrite, apply_merge_write, plan_merge_write};
use super::source::{build_join_map, collect_ids, extract_field_str, fetch_document_value};
use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::query::value_utils::value_to_string;
use crate::storage::engine::StorageEngine;
use nodedb_physical::physical_plan::document::merge_types::{
    MergeActionOp, MergeClauseKind, MergeClauseOp,
};
use nodedb_types::result::QueryResult;
use nodedb_types::value::Value;
use std::collections::HashMap;
/// Merge: SQL MERGE INTO target USING source ON cond WHEN ... .
///
/// Execution:
/// 1. Scan source; build join map keyed by `source_join_col`.
/// 2. For each target row: if matched → apply first matching WHEN MATCHED arm.
/// 3. For each source row with no target match: apply first WHEN NOT MATCHED arm.
/// 4. For each target row with no source match: apply WHEN NOT MATCHED BY SOURCE arm.
///
/// All writes are within the same logical operation (per-row calls to point_*).
pub async fn merge<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    target_collection: &str,
    source_collection: &str,
    source_alias: &str,
    target_join_col: &str,
    source_join_col: &str,
    clauses: &[MergeClauseOp],
) -> Result<QueryResult, LiteError> {
    merge_coordinated(
        engine,
        None,
        DocumentJoin {
            target_collection,
            source_collection,
            source_alias,
            target_join_col,
            source_join_col,
        },
        clauses,
    )
    .await
}

pub(crate) async fn merge_coordinated<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    permit: Option<&crate::engine::fts::coordinator::TextMutationPermit>,
    join: DocumentJoin<'_>,
    clauses: &[MergeClauseOp],
) -> Result<QueryResult, LiteError> {
    if let Some(permit) = permit {
        return merge_admitted(engine, permit, join, clauses).await;
    }
    let guard = engine.fts_state.admit_mutation().await;
    let result = merge_admitted(engine, guard.permit(), join, clauses).await;
    guard.finish(result)
}

pub(crate) async fn merge_admitted<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    permit: &crate::engine::fts::coordinator::TextMutationPermit,
    join: DocumentJoin<'_>,
    clauses: &[MergeClauseOp],
) -> Result<QueryResult, LiteError> {
    let DocumentJoin {
        target_collection,
        source_collection,
        source_alias,
        target_join_col,
        source_join_col,
    } = join;
    // Build source join map: source_join_col_value → document value map.
    let source_map = build_join_map(engine, source_collection, source_join_col).await?;

    // Scan target rows and track which source keys were matched. Every
    // arm's write is resolved before the first one applies, so an
    // assignment that fails leaves every row unchanged.
    let target_ids = collect_ids(engine, target_collection).await?;
    let mut matched_source_keys: std::collections::HashSet<String> =
        std::collections::HashSet::new();
    let mut planned: Vec<(String, MergeWrite)> = Vec::new();
    let no_row: HashMap<String, Value> = HashMap::new();

    for doc_id in &target_ids {
        let target_val = fetch_document_value(engine, target_collection, doc_id).await?;
        let join_key = extract_field_str(&target_val, target_join_col);

        match join_key {
            Some(ref key) if source_map.contains_key(key.as_str()) => {
                let source_val = &source_map[key.as_str()];
                matched_source_keys.insert(key.clone());

                // Find the first WHEN MATCHED arm whose extra_predicate passes.
                let arm = clauses
                    .iter()
                    .find(|c| c.kind == MergeClauseKind::Matched && c.extra_predicate.is_empty());
                if let Some(arm) = arm {
                    let write =
                        plan_merge_write(&arm.action, &target_val, source_val, source_alias)?;
                    planned.push((doc_id.clone(), write));
                }
            }
            _ => {
                // Target row has no matching source row — WHEN NOT MATCHED BY SOURCE.
                let arm = clauses.iter().find(|c| {
                    c.kind == MergeClauseKind::NotMatchedBySource && c.extra_predicate.is_empty()
                });
                if let Some(arm) = arm {
                    let write = plan_merge_write(&arm.action, &target_val, &no_row, source_alias)?;
                    planned.push((doc_id.clone(), write));
                }
            }
        }
    }

    // Source rows with no target match — WHEN NOT MATCHED (INSERT).
    let not_matched = clauses
        .iter()
        .find(|c| c.kind == MergeClauseKind::NotMatched && c.extra_predicate.is_empty());
    if let Some(arm) = not_matched
        && matches!(arm.action, MergeActionOp::Insert { .. })
    {
        for (key, source_val) in &source_map {
            if matched_source_keys.contains(key) {
                continue;
            }
            let doc_id = source_val
                .get(source_join_col)
                .map(value_to_string)
                .unwrap_or_else(|| key.clone());
            let write = plan_merge_write(&arm.action, &no_row, source_val, source_alias)?;
            planned.push((doc_id, write));
        }
    }

    let affected_n = planned.len() as u64;
    for (doc_id, write) in planned {
        apply_merge_write(engine, permit, target_collection, &doc_id, write).await?;
    }

    Ok(QueryResult {
        columns: Vec::new(),
        rows: Vec::new(),
        rows_affected: affected_n,
        command: Some("MERGE".into()),
    })
}

// ─── Internal helpers ────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use crate::NodeDbLite;
    use crate::PagedbStorageMem;

    async fn make_db() -> std::sync::Arc<NodeDbLite<PagedbStorageMem>> {
        let storage = PagedbStorageMem::open_in_memory().await.unwrap();
        NodeDbLite::open(storage).await.unwrap()
    }

    /// merge with no clauses and empty collections returns 0 rows_affected without error.
    #[tokio::test]
    async fn merge_empty_collections_no_clauses() {
        let db = make_db().await;
        let result = super::merge(
            &db.query_engine,
            "target_mg",
            "source_mg",
            "s",
            "tid",
            "sid",
            &[],
        )
        .await
        .unwrap();
        assert_eq!(result.rows_affected, 0);
    }

    #[tokio::test]
    async fn merge_nested_writes_share_admission() -> Result<(), Box<dyn std::error::Error>> {
        use crate::query::document_ops::writes::point_put;
        use nodedb_physical::physical_plan::document::merge_types::{
            MergeActionOp, MergeClauseKind, MergeClauseOp,
        };
        use nodedb_types::value::Value;
        let db = make_db().await;
        for (collection, id, key) in [
            ("target", "matched", "matched"),
            ("target", "removed", "removed"),
            ("source", "matched", "matched"),
            ("source", "inserted", "inserted"),
        ] {
            let bytes =
                zerompk::to_msgpack_vec(&Value::Object(std::collections::HashMap::from([
                    ("id".into(), Value::String(key.into())),
                    ("body".into(), Value::String("original".into())),
                ])))?;
            point_put(&db.query_engine, collection, id, &bytes).await?;
        }
        let literal = |text: &str| zerompk::to_msgpack_vec(&Value::String(text.into()));
        let clauses = vec![
            MergeClauseOp {
                kind: MergeClauseKind::Matched,
                extra_predicate: Vec::new(),
                action: MergeActionOp::Update {
                    updates: vec![(
                        "body".into(),
                        super::super::UpdateValue::Literal(literal("updated")?),
                    )],
                },
            },
            MergeClauseOp {
                kind: MergeClauseKind::NotMatchedBySource,
                extra_predicate: Vec::new(),
                action: MergeActionOp::Delete,
            },
            MergeClauseOp {
                kind: MergeClauseKind::NotMatched,
                extra_predicate: Vec::new(),
                action: MergeActionOp::Insert {
                    columns: vec!["id".into(), "body".into()],
                    values: vec![
                        super::super::UpdateValue::Literal(literal("inserted")?),
                        super::super::UpdateValue::Literal(literal("created")?),
                    ],
                },
            },
        ];
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            super::merge(
                &db.query_engine,
                "target",
                "source",
                "s",
                "id",
                "id",
                &clauses,
            ),
        )
        .await??;
        assert_eq!(result.rows_affected, 3);
        let crdt = db
            .query_engine
            .crdt
            .lock()
            .map_err(|_| crate::error::LiteError::LockPoisoned)?;
        assert!(!crdt.exists("target", "removed"));
        assert!(crdt.exists("target", "inserted"));
        let updated = crdt.read("target", "matched");
        drop(crdt);
        let Some(value) = updated else {
            return Err("matched document is absent".into());
        };
        let Value::Object(fields) =
            crate::query::document_ops::reads::loro_value_to_ndb_value(&value)
        else {
            return Err("matched document excludes its field map".into());
        };
        assert_eq!(fields.get("body"), Some(&Value::String("updated".into())));
        Ok(())
    }
}
