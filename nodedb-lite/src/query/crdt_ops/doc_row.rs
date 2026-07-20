// SPDX-License-Identifier: Apache-2.0
//! Document-row CRDT ops: `CrdtOp::DocUpsert` and `CrdtOp::DocDelete`.
//!
//! These back `WITH (crdt=true)` document collections, whose SQL DML routes to
//! the CRDT path instead of the plain document path. The Control Plane has no
//! `LoroDoc`, so it ships the row's fields as a JSON object and the executor
//! builds the Loro mutation — the same intent-carrying contract the list ops
//! use.
//!
//! Unlike Origin, Lite needs no dual write: its schemaless scan reads straight
//! out of the Loro store (`query/document_ops/reads.rs::scan` walks
//! `list_ids` + `read`), so mutating the CRDT state IS the materialization.

use loro::LoroValue;
use nodedb_physical::physical_plan::document::{ReturningColumns, ReturningSpec};
use nodedb_types::result::QueryResult;
use nodedb_types::value::Value;

use crate::error::LiteError;
use crate::nodedb::collection::import::json_to_loro;
use crate::query::document_ops::reads::loro_value_to_ndb_value;
use crate::query::engine::LiteQueryEngine;
use crate::storage::engine::StorageEngine;

/// Insert-or-replace / partial-update a document row's scalar fields.
///
/// `partial = false` is a full-projection replace: scalar keys absent from
/// `fields_json` are pruned. `partial = true` is UPDATE SET: only the provided
/// fields are written and untouched keys survive. Both semantics live upstream
/// in `nodedb_crdt` (`CrdtState::upsert` vs `CrdtState::set_fields`) — this
/// picks between them rather than reimplementing the prune.
pub async fn handle_doc_upsert<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    collection: &str,
    document_id: &str,
    fields_json: &str,
    partial: bool,
    returning: Option<&ReturningSpec>,
) -> Result<QueryResult, LiteError> {
    let fields = parse_fields(fields_json)?;
    let field_refs: Vec<(&str, LoroValue)> = fields
        .iter()
        .map(|(k, v)| (k.as_str(), v.clone()))
        .collect();

    {
        let mut crdt = engine.crdt.lock().map_err(|_| LiteError::LockPoisoned)?;
        if partial {
            crdt.set_fields(collection, document_id, &field_refs)?;
        } else {
            crdt.upsert(collection, document_id, &field_refs)?;
        }
    }

    project_returning(engine, collection, document_id, returning, 1)
}

/// Delete a document row, tombstoning it in the collection's Loro map.
///
/// A RETURNING projection is resolved against the row as it stood BEFORE the
/// delete — afterwards there is nothing left to read.
pub async fn handle_doc_delete<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    collection: &str,
    document_id: &str,
    returning: Option<&ReturningSpec>,
) -> Result<QueryResult, LiteError> {
    let pre_delete = if returning.is_some() {
        let crdt = engine.crdt.lock().map_err(|_| LiteError::LockPoisoned)?;
        crdt.read(collection, document_id)
    } else {
        None
    };

    let existed = {
        let mut crdt = engine.crdt.lock().map_err(|_| LiteError::LockPoisoned)?;
        let existed = crdt.exists(collection, document_id);
        if existed {
            crdt.delete(collection, document_id)?;
        }
        existed
    };

    let affected = u64::from(existed);
    match (returning, pre_delete) {
        (Some(spec), Some(row)) => Ok(rows_from_value(&row, document_id, spec, affected)),
        (Some(spec), None) => Ok(QueryResult {
            columns: returning_columns(spec, &[]),
            rows: Vec::new(),
            rows_affected: affected,
        }),
        (None, _) => Ok(QueryResult {
            columns: Vec::new(),
            rows: Vec::new(),
            rows_affected: affected,
        }),
    }
}

/// Parse the Control-Plane-built `fields_json` object into Loro field values.
fn parse_fields(fields_json: &str) -> Result<Vec<(String, LoroValue)>, LiteError> {
    let parsed: serde_json::Value =
        sonic_rs::from_str(fields_json).map_err(|e| LiteError::BadRequest {
            detail: format!("invalid DocUpsert fields_json: {e}"),
        })?;

    let obj = parsed.as_object().ok_or_else(|| LiteError::BadRequest {
        detail: "DocUpsert fields_json must be a JSON object".to_string(),
    })?;

    Ok(obj
        .iter()
        .map(|(k, v)| (k.clone(), json_to_loro(v)))
        .collect())
}

/// Read the row back and project it for RETURNING, or return a bare count.
fn project_returning<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    collection: &str,
    document_id: &str,
    returning: Option<&ReturningSpec>,
    affected: u64,
) -> Result<QueryResult, LiteError> {
    let Some(spec) = returning else {
        return Ok(QueryResult {
            columns: Vec::new(),
            rows: Vec::new(),
            rows_affected: affected,
        });
    };

    let row = {
        let crdt = engine.crdt.lock().map_err(|_| LiteError::LockPoisoned)?;
        crdt.read(collection, document_id)
    };

    match row {
        Some(v) => Ok(rows_from_value(&v, document_id, spec, affected)),
        None => Ok(QueryResult {
            columns: returning_columns(spec, &[]),
            rows: Vec::new(),
            rows_affected: affected,
        }),
    }
}

/// Build the RETURNING result for one row.
///
/// `RETURNING *` emits the row's own field order with `id` first; a named list
/// emits exactly the requested columns under their aliases, with absent fields
/// resolving to `Null` rather than being dropped, so every result row has the
/// same arity as the column header.
fn rows_from_value(
    value: &LoroValue,
    document_id: &str,
    spec: &ReturningSpec,
    affected: u64,
) -> QueryResult {
    let ndb = loro_value_to_ndb_value(value);
    let fields: Vec<(String, Value)> = match ndb {
        Value::Object(map) => {
            let mut pairs: Vec<(String, Value)> = map.into_iter().collect();
            pairs.sort_by(|a, b| a.0.cmp(&b.0));
            pairs
        }
        other => vec![("value".to_string(), other)],
    };

    match &spec.columns {
        ReturningColumns::Star => {
            let mut columns = vec!["id".to_string()];
            let mut row = vec![Value::String(document_id.to_string())];
            for (k, v) in fields {
                columns.push(k);
                row.push(v);
            }
            QueryResult {
                columns,
                rows: vec![row],
                rows_affected: affected,
            }
        }
        ReturningColumns::Named(items) => {
            let mut columns = Vec::with_capacity(items.len());
            let mut row = Vec::with_capacity(items.len());
            for item in items {
                columns.push(item.alias.clone().unwrap_or_else(|| item.name.clone()));
                if item.name == "id" {
                    row.push(Value::String(document_id.to_string()));
                } else {
                    let found = fields
                        .iter()
                        .find(|(k, _)| *k == item.name)
                        .map(|(_, v)| v.clone());
                    row.push(found.unwrap_or(Value::Null));
                }
            }
            QueryResult {
                columns,
                rows: vec![row],
                rows_affected: affected,
            }
        }
    }
}

/// Column header for a RETURNING projection with no row to emit.
fn returning_columns(spec: &ReturningSpec, star_fields: &[String]) -> Vec<String> {
    match &spec.columns {
        ReturningColumns::Star => {
            let mut cols = vec!["id".to_string()];
            cols.extend(star_fields.iter().cloned());
            cols
        }
        ReturningColumns::Named(items) => items
            .iter()
            .map(|item| item.alias.clone().unwrap_or_else(|| item.name.clone()))
            .collect(),
    }
}
