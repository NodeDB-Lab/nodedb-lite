// SPDX-License-Identifier: Apache-2.0

//! `MERGE` lowering.

use std::collections::HashMap;
use std::collections::HashSet;

use nodedb_physical::physical_plan::document::merge_types::{
    MergeActionOp, MergeClauseKind, MergeClauseOp,
};
use nodedb_sql::types::SqlPlan;
use nodedb_sql::types::plan::{MergeClauseKind as SqlMergeKind, MergePlanAction, MergePlanClause};
use nodedb_sql::types::query::EngineType;
use nodedb_types::result::QueryResult;
use nodedb_types::value::Value;

use crate::error::LiteError;
use crate::query::assignments::convert_assignments_with_source;
use crate::query::document_ops::sets::{
    MergeWrite, apply_merge_write, collect_ids_pub, fetch_document_value_pub, plan_merge_write,
};
use crate::query::engine::LiteQueryEngine;
use crate::query::value_utils::value_to_string;
use crate::storage::engine::StorageEngine;

use crate::query::visitor::adapter::LiteFut;

use super::rows::{extract_id, result_to_maps};

// ── Merge ────────────────────────────────────────────────────────────────────

/// `MERGE INTO target USING source ON ... WHEN ...`
///
/// An UPDATE arm evaluates against the matched target row, with source
/// fields readable as `<source_alias>.<field>`. Every arm's write is
/// resolved before the first one applies, so an assignment that fails
/// leaves every row unchanged.
#[allow(clippy::too_many_arguments)]
pub(in crate::query::visitor) fn lower_merge<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    permit: &'a crate::engine::fts::coordinator::TextMutationPermit,
    target: &str,
    _engine_type: EngineType,
    source: &SqlPlan,
    target_join_col: &str,
    source_join_col: &str,
    source_alias: &str,
    clauses: &[MergePlanClause],
    _returning: bool,
) -> Result<LiteFut<'a>, LiteError> {
    let target = target.to_string();
    let source = source.clone();
    let t_join = target_join_col.to_string();
    let s_join = source_join_col.to_string();
    let alias = source_alias.to_string();
    let phys_clauses = convert_merge_clauses(clauses, std::slice::from_ref(&alias))?;

    Ok(Box::pin(async move {
        let source_result = engine.execute_plan_admitted(&source, Some(permit)).await?;
        let source_maps = result_to_maps(source_result);

        let mut source_index: HashMap<String, HashMap<String, Value>> = HashMap::new();
        for row in source_maps {
            if let Some(key_val) = row.get(&s_join) {
                source_index.insert(value_to_string(key_val), row);
            }
        }

        let target_ids = collect_ids_pub(engine, &target).await?;
        let mut matched_source_keys: HashSet<String> = HashSet::new();
        let mut planned: Vec<(String, MergeWrite)> = Vec::new();
        let no_row: HashMap<String, Value> = HashMap::new();

        for doc_id in &target_ids {
            let target_val = fetch_document_value_pub(engine, &target, doc_id).await?;
            let Some(join_key) = target_val.get(&t_join).map(value_to_string) else {
                continue;
            };

            if let Some(source_row) = source_index.get(&join_key) {
                matched_source_keys.insert(join_key);
                let arm = phys_clauses
                    .iter()
                    .find(|c| c.kind == MergeClauseKind::Matched);
                if let Some(arm) = arm {
                    let write = plan_merge_write(&arm.action, &target_val, source_row, &alias)?;
                    planned.push((doc_id.clone(), write));
                }
            } else {
                let arm = phys_clauses
                    .iter()
                    .find(|c| c.kind == MergeClauseKind::NotMatchedBySource);
                if let Some(arm) = arm {
                    // No source row for this target — NOT MATCHED BY SOURCE arms
                    // are UPDATE/DELETE only, so an empty source suffices.
                    let write = plan_merge_write(&arm.action, &target_val, &no_row, &alias)?;
                    planned.push((doc_id.clone(), write));
                }
            }
        }

        // Unmatched source rows → WHEN NOT MATCHED.
        let not_matched_arm = phys_clauses
            .iter()
            .find(|c| c.kind == MergeClauseKind::NotMatched);
        if let Some(arm) = not_matched_arm {
            for (source_key, source_row) in &source_index {
                if matched_source_keys.contains(source_key) {
                    continue;
                }
                let write = plan_merge_write(&arm.action, &no_row, source_row, &alias)?;
                // An inserted row is keyed by its own `id`.
                let doc_id = match &write {
                    MergeWrite::Insert(row) => extract_id(row),
                    _ => extract_id(source_row),
                };
                planned.push((doc_id, write));
            }
        }

        let affected = planned.len() as u64;
        for (doc_id, write) in planned {
            apply_merge_write(engine, permit, &target, &doc_id, write).await?;
        }

        Ok(QueryResult {
            columns: Vec::new(),
            rows: Vec::new(),
            rows_affected: affected,
            command: Some("MERGE".into()),
        })
    }))
}

/// Convert MERGE arms. A column qualified by a name in `source_refs` reads
/// the source row.
pub(super) fn convert_merge_clauses(
    clauses: &[MergePlanClause],
    source_refs: &[String],
) -> Result<Vec<MergeClauseOp>, LiteError> {
    clauses
        .iter()
        .map(|clause| convert_one_clause(clause, source_refs))
        .collect()
}

pub(super) fn convert_one_clause(
    clause: &MergePlanClause,
    source_refs: &[String],
) -> Result<MergeClauseOp, LiteError> {
    let kind = match clause.kind {
        SqlMergeKind::Matched => MergeClauseKind::Matched,
        SqlMergeKind::NotMatched => MergeClauseKind::NotMatched,
        SqlMergeKind::NotMatchedBySource => MergeClauseKind::NotMatchedBySource,
    };
    let action = convert_merge_action(&clause.action, source_refs)?;
    Ok(MergeClauseOp {
        kind,
        extra_predicate: Vec::new(),
        action,
    })
}

pub(super) fn convert_merge_action(
    action: &MergePlanAction,
    source_refs: &[String],
) -> Result<MergeActionOp, LiteError> {
    match action {
        MergePlanAction::Update { assignments } => {
            let updates = convert_assignments_with_source(assignments, source_refs)?;
            Ok(MergeActionOp::Update { updates })
        }
        MergePlanAction::Delete => Ok(MergeActionOp::Delete),
        MergePlanAction::Insert { columns, values } => {
            // Literal values are pre-encoded; source-referencing expressions
            // (`s.new_embedding`, `s.qty * 2`) are carried as `UpdateValue::Expr`
            // and evaluated against the source row at apply time.
            let pairs: Vec<(String, nodedb_sql::types_expr::SqlExpr)> = columns
                .iter()
                .cloned()
                .zip(values.iter().cloned())
                .collect();
            let encoded = convert_assignments_with_source(&pairs, source_refs)?
                .into_iter()
                .map(|(_, value)| value)
                .collect();
            Ok(MergeActionOp::Insert {
                columns: columns.clone(),
                values: encoded,
            })
        }
        MergePlanAction::DoNothing => Ok(MergeActionOp::DoNothing),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::document_ops::writes::point_put;
    use crate::query::engine::test_engine;

    #[tokio::test]
    async fn merge_inserts_unmatched_schemaless_source_row()
    -> Result<(), Box<dyn std::error::Error>> {
        let engine = test_engine().await;
        engine.execute_sql("CREATE COLLECTION merge_target").await?;
        let fields = HashMap::from([
            ("id".into(), Value::String("source-1".into())),
            ("label".into(), Value::String("copied source".into())),
            ("quantity".into(), Value::Integer(7)),
        ]);
        let bytes = zerompk::to_msgpack_vec(&Value::Object(fields.clone()))?;
        point_put(&engine, "merge_source", "source-1", &bytes).await?;

        let result = engine
            .execute_sql(
                "MERGE INTO merge_target AS t \
             USING (SELECT id, label, quantity FROM merge_source) AS s \
             ON t.id = s.id \
             WHEN NOT MATCHED THEN INSERT (id, label, quantity) \
             VALUES (s.id, s.label, s.quantity)",
            )
            .await?;

        assert_eq!(result.rows_affected, 1);
        assert_eq!(result.command.as_deref(), Some("MERGE"));
        assert_eq!(
            collect_ids_pub(&engine, "merge_target").await?,
            vec!["source-1"]
        );
        let inserted = fetch_document_value_pub(&engine, "merge_target", "source-1").await?;
        assert_eq!(inserted.get("id"), fields.get("id"));
        assert_eq!(inserted.get("label"), fields.get("label"));
        assert_eq!(inserted.get("quantity"), fields.get("quantity"));
        Ok(())
    }
}
