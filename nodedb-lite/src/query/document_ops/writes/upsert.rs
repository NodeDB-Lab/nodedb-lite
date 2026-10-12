// SPDX-License-Identifier: Apache-2.0
use super::super::is_strict;
use super::super::reads::{
    loro_value_to_ndb_value, msgpack_bytes_to_crdt_fields, ndb_value_to_loro,
};
use super::super::write_helpers::affected;
use super::super::write_helpers::{decode_strict_fields, fields_to_values, strict_schema};
use super::UpdateValue;
use crate::error::LiteError;
use crate::query::assignments::{coerce_to_columns, resolve_updates_with_excluded, row_object};
use crate::query::engine::LiteQueryEngine;
use crate::query::text_index::{reindex_documents, reindex_strict_rows};
use crate::storage::engine::StorageEngine;
use nodedb_types::result::QueryResult;
use nodedb_types::value::Value;
use std::collections::HashMap;

/// Upsert: insert or update. When `on_conflict_updates` is non-empty, applies
/// those assignments on conflict instead of merging the new value.
pub async fn upsert<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    collection: &str,
    document_id: &str,
    value_bytes: &[u8],
    on_conflict_updates: &[(String, UpdateValue)],
) -> Result<QueryResult, LiteError> {
    upsert_coordinated(
        engine,
        None,
        collection,
        document_id,
        value_bytes,
        on_conflict_updates,
    )
    .await
}

pub(crate) async fn upsert_coordinated<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    permit: Option<&crate::engine::fts::coordinator::TextMutationPermit>,
    collection: &str,
    document_id: &str,
    value_bytes: &[u8],
    on_conflict_updates: &[(String, UpdateValue)],
) -> Result<QueryResult, LiteError> {
    if let Some(permit) = permit {
        return upsert_admitted(
            engine,
            permit,
            collection,
            document_id,
            value_bytes,
            on_conflict_updates,
        )
        .await;
    }
    let guard = engine.fts_state.admit_mutation().await;
    let result = upsert_admitted(
        engine,
        guard.permit(),
        collection,
        document_id,
        value_bytes,
        on_conflict_updates,
    )
    .await;
    guard.finish(result)
}

pub(crate) async fn upsert_admitted<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    permit: &crate::engine::fts::coordinator::TextMutationPermit,
    collection: &str,
    document_id: &str,
    value_bytes: &[u8],
    on_conflict_updates: &[(String, UpdateValue)],
) -> Result<QueryResult, LiteError> {
    let _permit = permit;
    if is_strict(engine, collection) {
        let pk = Value::String(document_id.to_string());
        let schema = strict_schema(engine, collection)?;
        match engine.strict.get(collection, &pk).await? {
            Some(row) if !on_conflict_updates.is_empty() => {
                // Each assignment reads the stored row, with `EXCLUDED.col`
                // bound to the incoming row.
                let incoming: HashMap<String, Value> =
                    decode_strict_fields(value_bytes)?.into_iter().collect();
                let resolved = resolve_updates_with_excluded(
                    on_conflict_updates,
                    &row_object(&schema.columns, &row),
                    &Value::Object(incoming),
                )?;
                let field_updates = coerce_to_columns(resolved, &schema.columns)?;
                engine
                    .strict
                    .update(collection, &pk, &field_updates)
                    .await?;
            }
            Some(_) => {
                let updates: HashMap<String, Value> =
                    decode_strict_fields(value_bytes)?.into_iter().collect();
                engine.strict.update(collection, &pk, &updates).await?;
            }
            None => {
                let fields = decode_strict_fields(value_bytes)?;
                let values = fields_to_values(&fields, &schema.columns);
                engine.strict.insert(collection, &values).await?;
            }
        }
        reindex_strict_rows(engine, collection, &[pk]).await?;
    } else {
        let mut crdt = engine.crdt.lock().map_err(|_| LiteError::LockPoisoned)?;
        let stored = if on_conflict_updates.is_empty() {
            None
        } else {
            crdt.read(collection, document_id)
        };
        match stored {
            Some(stored) => {
                // Each assignment reads the stored row, with `EXCLUDED.col`
                // bound to the incoming row. Unassigned fields keep their values.
                let incoming: HashMap<String, Value> =
                    decode_strict_fields(value_bytes)?.into_iter().collect();
                let resolved = resolve_updates_with_excluded(
                    on_conflict_updates,
                    &loro_value_to_ndb_value(&stored),
                    &Value::Object(incoming),
                )?;
                let assigned: Vec<(String, loro::LoroValue)> = resolved
                    .into_iter()
                    .map(|(k, v)| (k, ndb_value_to_loro(v)))
                    .collect();
                let fields: Vec<(&str, loro::LoroValue)> = assigned
                    .iter()
                    .map(|(k, v)| (k.as_str(), v.clone()))
                    .collect();
                crdt.set_fields(collection, document_id, &fields)?;
            }
            None => {
                let crdt_fields = msgpack_bytes_to_crdt_fields(value_bytes)?;
                let loro_fields: Vec<(&str, loro::LoroValue)> = crdt_fields
                    .iter()
                    .map(|(k, v)| (k.as_str(), v.clone()))
                    .collect();
                crdt.upsert(collection, document_id, &loro_fields)?;
            }
        }
        drop(crdt);
        reindex_documents(engine, collection, [document_id])?;
    }
    Ok(affected(1, "UPSERT"))
}
