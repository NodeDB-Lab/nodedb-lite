// SPDX-License-Identifier: Apache-2.0
use super::super::is_strict;
use super::super::reads::{loro_value_to_ndb_value, ndb_value_to_loro};
use super::super::write_helpers::{affected, strict_schema};
use super::UpdateValue;
use crate::engine::crdt::{CrdtRowOp, CrdtRowWrite};
use crate::error::LiteError;
use crate::query::assignments::{coerce_to_columns, has_expr, resolve_updates, row_object};
use crate::query::engine::LiteQueryEngine;
use crate::query::text_index::{reindex_documents, reindex_strict_rows};
use crate::storage::engine::StorageEngine;
use nodedb_types::result::QueryResult;
use nodedb_types::value::Value;
use std::collections::HashMap;

/// BulkUpdate: scan matching documents and apply field updates to all.
///
/// Lite does not yet evaluate residual scan filters — every document in the
/// collection receives the update. Callers that need filtered bulk updates
/// should compose `Scan` + per-row `PointUpdate` at the application layer.
pub async fn bulk_update<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    collection: &str,
    updates: &[(String, UpdateValue)],
) -> Result<QueryResult, LiteError> {
    bulk_update_coordinated(engine, None, collection, updates).await
}

pub(crate) async fn bulk_update_coordinated<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    permit: Option<&crate::engine::fts::coordinator::TextMutationPermit>,
    collection: &str,
    updates: &[(String, UpdateValue)],
) -> Result<QueryResult, LiteError> {
    if let Some(permit) = permit {
        return bulk_update_admitted(engine, permit, collection, updates).await;
    }
    let guard = engine.fts_state.admit_mutation().await;
    let result = bulk_update_admitted(engine, guard.permit(), collection, updates).await;
    guard.finish(result)
}

pub(crate) async fn bulk_update_admitted<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    permit: &crate::engine::fts::coordinator::TextMutationPermit,
    collection: &str,
    updates: &[(String, UpdateValue)],
) -> Result<QueryResult, LiteError> {
    let _permit = permit;
    // A literal-only update resolves once. An expression resolves per row
    // against that row's pre-image. Every post-image is built before the
    // first write, so an assignment that fails leaves every row unchanged.
    let shared = if has_expr(updates) {
        None
    } else {
        Some(resolve_updates(updates, &Value::Object(HashMap::new()))?)
    };

    if is_strict(engine, collection) {
        let schema = strict_schema(engine, collection)?;
        let pk_idx = schema
            .columns
            .iter()
            .position(|c| c.primary_key)
            .ok_or_else(|| LiteError::BadRequest {
                detail: format!("strict collection '{collection}' has no primary key"),
            })?;
        let all_rows = engine.strict.list_rows(collection).await?;
        let mut changes: Vec<(Value, HashMap<String, Value>)> = Vec::with_capacity(all_rows.len());
        for row in &all_rows {
            let Some(pk) = row.get(pk_idx) else {
                continue;
            };
            let resolved = match &shared {
                Some(resolved) => resolved.clone(),
                None => resolve_updates(updates, &row_object(&schema.columns, row))?,
            };
            changes.push((pk.clone(), coerce_to_columns(resolved, &schema.columns)?));
        }
        // One batch for the statement: a unique index it would break refuses
        // every row.
        let affected_n = engine.strict.update_many(collection, &changes).await?;
        let pks: Vec<Value> = changes.into_iter().map(|(pk, _)| pk).collect();
        reindex_strict_rows(engine, collection, &pks).await?;
        Ok(affected(affected_n, "UPDATE"))
    } else {
        let mut crdt = engine.crdt.lock().map_err(|_| LiteError::LockPoisoned)?;
        let ids = crdt.list_ids(collection);
        let mut row_fields: Vec<Vec<(String, loro::LoroValue)>> = Vec::with_capacity(ids.len());
        for id in &ids {
            let resolved = match &shared {
                Some(resolved) => resolved.clone(),
                None => {
                    let current = match crdt.read(collection, id) {
                        Some(val) => loro_value_to_ndb_value(&val),
                        None => Value::Object(HashMap::new()),
                    };
                    resolve_updates(updates, &current)?
                }
            };
            row_fields.push(
                resolved
                    .into_iter()
                    .map(|(k, v)| (k, ndb_value_to_loro(v)))
                    .collect(),
            );
        }
        let slices: Vec<Vec<(&str, loro::LoroValue)>> = row_fields
            .iter()
            .map(|fields| {
                fields
                    .iter()
                    .map(|(k, v)| (k.as_str(), v.clone()))
                    .collect()
            })
            .collect();
        let rows: Vec<CrdtRowOp<'_>> = ids
            .iter()
            .zip(&slices)
            .map(|(id, fields)| {
                (
                    CrdtRowWrite::SetFields,
                    collection,
                    id.as_str(),
                    fields.as_slice(),
                )
            })
            .collect();
        crdt.check_unique_writes(&rows)?;
        let mut affected_n: u64 = 0;
        for (id, fields) in ids.iter().zip(&slices) {
            // A merge: fields the update does not assign keep their values.
            crdt.set_fields_deferred(collection, id, fields)?;
            affected_n += 1;
        }
        crdt.flush_deltas().map_err(|e| LiteError::Storage {
            detail: e.to_string(),
        })?;
        drop(crdt);
        reindex_documents(engine, collection, ids.iter().map(String::as_str))?;
        Ok(affected(affected_n, "UPDATE"))
    }
}
