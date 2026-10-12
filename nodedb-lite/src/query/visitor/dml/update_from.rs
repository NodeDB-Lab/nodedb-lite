// SPDX-License-Identifier: Apache-2.0

//! `UPDATE ... FROM` lowering.

use std::collections::HashMap;

use nodedb_physical::physical_plan::document::types::UpdateValue;
use nodedb_sql::types::SqlPlan;
use nodedb_sql::types::filter::Filter;
use nodedb_sql::types::query::EngineType;
use nodedb_sql::types_expr::SqlExpr;
use nodedb_types::result::QueryResult;
use nodedb_types::value::Value;

use crate::error::LiteError;
use crate::query::assignments::{
    convert_assignments_with_source, joined_row, literal_updates, resolve_updates,
};
use crate::query::document_ops::sets::{collect_ids_pub, fetch_document_value_pub};
use crate::query::document_ops::writes::point_update_admitted;
use crate::query::engine::LiteQueryEngine;
use crate::query::value_utils::value_to_string;
use crate::storage::engine::StorageEngine;

use crate::query::visitor::adapter::LiteFut;

use super::rows::result_to_maps;

// ── UpdateFrom ───────────────────────────────────────────────────────────────

/// `UPDATE target SET ... FROM source WHERE target.col = source.col`.
///
/// Each assignment evaluates against the matched target row, with source
/// fields readable by their qualified name. Every post-image is resolved
/// before the first write, so an assignment that fails leaves every row
/// unchanged.
#[allow(clippy::too_many_arguments)]
pub(in crate::query::visitor) fn lower_update_from<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    permit: &'a crate::engine::fts::coordinator::TextMutationPermit,
    collection: &str,
    _engine_type: EngineType,
    source: &SqlPlan,
    target_join_col: &str,
    source_join_col: &str,
    assignments: &[(String, SqlExpr)],
    _target_filters: &[Filter],
    _returning: bool,
) -> Result<LiteFut<'a>, LiteError> {
    let target = collection.to_string();
    let source_refs = source_refs(source);
    let source_alias = source_refs.first().cloned().unwrap_or_default();
    let source = source.clone();
    let t_join = target_join_col.to_string();
    let s_join = source_join_col.to_string();
    let updates = convert_assignments_with_source(assignments, &source_refs)?;

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
        let mut planned: Vec<(String, Vec<(String, UpdateValue)>)> = Vec::new();
        for doc_id in &target_ids {
            let target_val = fetch_document_value_pub(engine, &target, doc_id).await?;
            let Some(join_key) = target_val.get(&t_join).map(value_to_string) else {
                continue;
            };
            let Some(source_val) = source_index.get(&join_key) else {
                continue;
            };
            let row = update_from_row(&target_val, source_val, &source_alias);
            let resolved = resolve_updates(&updates, &Value::Object(row))?;
            planned.push((doc_id.clone(), literal_updates(resolved)?));
        }

        for (doc_id, resolved) in &planned {
            point_update_admitted(engine, permit, &target, doc_id, resolved).await?;
        }

        Ok(QueryResult {
            columns: Vec::new(),
            rows: Vec::new(),
            rows_affected: planned.len() as u64,
            command: Some("UPDATE".into()),
        })
    }))
}

/// The names a column qualifier can use for the source relation: its alias
/// first, then its collection. Empty for a source plan that names neither.
fn source_refs(plan: &SqlPlan) -> Vec<String> {
    match plan {
        SqlPlan::Scan {
            collection, alias, ..
        } => alias
            .iter()
            .cloned()
            .chain(std::iter::once(collection.clone()))
            .collect(),
        SqlPlan::TimeseriesScan(scan) => vec![scan.collection.clone()],
        _ => Vec::new(),
    }
}

/// The row an `UPDATE ... FROM` assignment evaluates against: the
/// [`joined_row`], plus each source field the target lacks under its bare
/// name. An unqualified column the target does not hold names the source.
fn update_from_row(
    target: &HashMap<String, Value>,
    source: &HashMap<String, Value>,
    source_alias: &str,
) -> HashMap<String, Value> {
    let mut row = joined_row(target, source, source_alias);
    for (field, value) in source {
        row.entry(field.clone()).or_insert_with(|| value.clone());
    }
    row
}
