// SPDX-License-Identifier: Apache-2.0
use super::super::writes::{point_delete_admitted, point_insert_admitted, point_update_admitted};
use super::UpdateValue;
use crate::error::LiteError;
use crate::query::assignments::{joined_row, literal_updates, resolve_updates};
use crate::query::engine::LiteQueryEngine;
use crate::storage::engine::StorageEngine;
use nodedb_physical::physical_plan::document::merge_types::MergeActionOp;
use nodedb_types::value::Value;
use std::collections::HashMap;

/// Resolve a parallel (columns, values) pair into a field map, evaluating each
/// value against the source row.
///
/// `UpdateValue::Literal` arms decode the pre-encoded msgpack directly.
/// `UpdateValue::Expr` arms (e.g. `s.new_embedding`, `s.qty * 2`) evaluate
/// against the source fields keyed `"<source_alias>.<field>"`, as on Origin.
/// There is no target row for an insert, so a bare column is NULL. The result
/// is stored under the *target* column name.
fn build_insert_map(
    columns: &[String],
    values: &[UpdateValue],
    source_val: &HashMap<String, Value>,
    source_alias: &str,
) -> Result<HashMap<String, Value>, LiteError> {
    let source = Value::Object(joined_row(&HashMap::new(), source_val, source_alias));
    let assignments: Vec<(String, UpdateValue)> = columns
        .iter()
        .cloned()
        .zip(values.iter().cloned())
        .collect();
    resolve_updates(&assignments, &source)
}

/// One MERGE arm's write for one row, resolved before any row is written.
pub(in crate::query) enum MergeWrite {
    /// Literal assignments resolved against the row's pre-image.
    Update(Vec<(String, UpdateValue)>),
    Delete,
    /// The inserted row, keyed by target column.
    Insert(HashMap<String, Value>),
    Nothing,
}

/// Resolve one MERGE arm against the matched rows. `target_val` is the
/// target row's pre-image, empty for `WHEN NOT MATCHED`. `source_val` is the
/// source row, empty for `WHEN NOT MATCHED BY SOURCE`. An UPDATE assignment
/// reads target fields by name and source fields as
/// `"<source_alias>.<field>"`.
pub(in crate::query) fn plan_merge_write(
    action: &MergeActionOp,
    target_val: &HashMap<String, Value>,
    source_val: &HashMap<String, Value>,
    source_alias: &str,
) -> Result<MergeWrite, LiteError> {
    Ok(match action {
        MergeActionOp::Update { updates } => {
            let row = Value::Object(joined_row(target_val, source_val, source_alias));
            MergeWrite::Update(literal_updates(resolve_updates(updates, &row)?)?)
        }
        MergeActionOp::Delete => MergeWrite::Delete,
        MergeActionOp::Insert { columns, values } => {
            MergeWrite::Insert(build_insert_map(columns, values, source_val, source_alias)?)
        }
        MergeActionOp::DoNothing => MergeWrite::Nothing,
    })
}

/// Apply one planned MERGE write to the target document `doc_id`.
pub(in crate::query) async fn apply_merge_write<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    permit: &crate::engine::fts::coordinator::TextMutationPermit,
    collection: &str,
    doc_id: &str,
    write: MergeWrite,
) -> Result<(), LiteError> {
    match write {
        MergeWrite::Update(updates) => {
            point_update_admitted(engine, permit, collection, doc_id, &updates).await?;
        }
        MergeWrite::Delete => {
            point_delete_admitted(engine, permit, collection, doc_id).await?;
        }
        MergeWrite::Insert(map) => {
            let bytes = zerompk::to_msgpack_vec(&Value::Object(map)).map_err(|e| {
                LiteError::Serialization {
                    detail: format!("merge action insert serialize: {e}"),
                }
            })?;
            point_insert_admitted(engine, permit, collection, doc_id, &bytes, true).await?;
        }
        MergeWrite::Nothing => {}
    }
    Ok(())
}
