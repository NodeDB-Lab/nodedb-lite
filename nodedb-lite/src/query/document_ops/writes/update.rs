// SPDX-License-Identifier: Apache-2.0
use super::super::is_strict;
use super::super::reads::{loro_value_to_ndb_value, ndb_value_to_loro};
use super::super::write_helpers::{affected, strict_schema};
use super::UpdateValue;
use crate::error::LiteError;
use crate::query::assignments::{coerce_to_columns, resolve_updates, row_object};
use crate::query::engine::LiteQueryEngine;
use crate::query::text_index::{reindex_documents, reindex_strict_rows};
use crate::storage::engine::StorageEngine;
use nodedb_types::result::QueryResult;
use nodedb_types::value::Value;
use std::collections::HashMap;

/// PointUpdate: read-modify-write with field-level changes.
pub async fn point_update<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    collection: &str,
    document_id: &str,
    updates: &[(String, UpdateValue)],
) -> Result<QueryResult, LiteError> {
    point_update_coordinated(engine, None, collection, document_id, updates).await
}

pub(crate) async fn point_update_coordinated<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    permit: Option<&crate::engine::fts::coordinator::TextMutationPermit>,
    collection: &str,
    document_id: &str,
    updates: &[(String, UpdateValue)],
) -> Result<QueryResult, LiteError> {
    if let Some(permit) = permit {
        return point_update_admitted(engine, permit, collection, document_id, updates).await;
    }
    let guard = engine.fts_state.admit_mutation().await;
    let result =
        point_update_admitted(engine, guard.permit(), collection, document_id, updates).await;
    guard.finish(result)
}

pub(crate) async fn point_update_admitted<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    permit: &crate::engine::fts::coordinator::TextMutationPermit,
    collection: &str,
    document_id: &str,
    updates: &[(String, UpdateValue)],
) -> Result<QueryResult, LiteError> {
    let _permit = permit;
    if is_strict(engine, collection) {
        let pk = Value::String(document_id.to_string());
        let schema = strict_schema(engine, collection)?;
        let Some(row) = engine.strict.get(collection, &pk).await? else {
            return Ok(affected(0, "UPDATE"));
        };
        let current = row_object(&schema.columns, &row);
        let resolved = resolve_updates(updates, &current)?;
        let field_updates = coerce_to_columns(resolved, &schema.columns)?;
        let updated = engine
            .strict
            .update(collection, &pk, &field_updates)
            .await?;
        reindex_strict_rows(engine, collection, &[pk]).await?;
        Ok(affected(if updated { 1 } else { 0 }, "UPDATE"))
    } else {
        let crdt = engine.crdt.lock().map_err(|_| LiteError::LockPoisoned)?;
        if !crdt.exists(collection, document_id) {
            return Ok(affected(0, "UPDATE"));
        }
        let existing_val = crdt.read(collection, document_id);
        drop(crdt);
        let current = match existing_val {
            Some(val) => loro_value_to_ndb_value(&val),
            None => Value::Object(HashMap::new()),
        };
        // Every assignment reads the pre-image, so resolve before merging.
        let resolved = resolve_updates(updates, &current)?;
        let mut merged: HashMap<String, loro::LoroValue> = match current {
            Value::Object(map) => map
                .into_iter()
                .map(|(k, v)| (k, ndb_value_to_loro(v)))
                .collect(),
            _ => HashMap::new(),
        };
        for (field, val) in resolved {
            merged.insert(field, ndb_value_to_loro(val));
        }
        let loro_fields: Vec<(&str, loro::LoroValue)> = merged
            .iter()
            .map(|(k, v)| (k.as_str(), v.clone()))
            .collect();
        engine
            .crdt
            .lock()
            .map_err(|_| LiteError::LockPoisoned)?
            .upsert(collection, document_id, &loro_fields)?;
        reindex_documents(engine, collection, [document_id])?;
        Ok(affected(1, "UPDATE"))
    }
}
