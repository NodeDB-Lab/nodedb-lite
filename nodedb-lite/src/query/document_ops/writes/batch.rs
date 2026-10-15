// SPDX-License-Identifier: Apache-2.0
use std::collections::HashSet;

use super::super::is_strict;
use super::super::reads::msgpack_bytes_to_crdt_fields;
use super::super::write_helpers::affected;
use super::super::write_helpers::{decode_strict_fields, fields_to_values, strict_schema};
use crate::engine::crdt::{CrdtRowOp, CrdtRowWrite};
use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::query::text_index::{index_strict_rows, reindex_documents};
use crate::storage::engine::StorageEngine;
use nodedb_types::result::QueryResult;
use nodedb_types::value::Value;

/// BatchInsert: insert N documents in a single transaction.
///
/// Each row is judged as a point insert judges it. A key already stored, or
/// one the batch names twice, refuses the whole batch, and nothing is
/// written. With `if_absent` (`ON CONFLICT DO NOTHING`) such a row is
/// skipped instead.
pub async fn batch_insert<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    collection: &str,
    documents: &[(String, Vec<u8>)],
    if_absent: bool,
) -> Result<QueryResult, LiteError> {
    batch_insert_coordinated(engine, None, collection, documents, if_absent).await
}

pub(crate) async fn batch_insert_coordinated<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    permit: Option<&crate::engine::fts::coordinator::TextMutationPermit>,
    collection: &str,
    documents: &[(String, Vec<u8>)],
    if_absent: bool,
) -> Result<QueryResult, LiteError> {
    if let Some(permit) = permit {
        return batch_insert_admitted(engine, permit, collection, documents, if_absent).await;
    }
    let guard = engine.fts_state.admit_mutation().await;
    let result =
        batch_insert_admitted(engine, guard.permit(), collection, documents, if_absent).await;
    guard.finish(result)
}

pub(crate) async fn batch_insert_admitted<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    permit: &crate::engine::fts::coordinator::TextMutationPermit,
    collection: &str,
    documents: &[(String, Vec<u8>)],
    if_absent: bool,
) -> Result<QueryResult, LiteError> {
    let _permit = permit;
    if is_strict(engine, collection) {
        let schema = strict_schema(engine, collection)?;
        let mut seen: HashSet<&str> = HashSet::with_capacity(documents.len());
        let mut rows: Vec<Vec<Value>> = Vec::with_capacity(documents.len());
        for (doc_id, value_bytes) in documents {
            if if_absent {
                let repeated = !seen.insert(doc_id.as_str());
                if repeated
                    || engine
                        .strict
                        .get(collection, &Value::String(doc_id.clone()))
                        .await?
                        .is_some()
                {
                    continue;
                }
            }
            let fields = decode_strict_fields(value_bytes)?;
            rows.push(fields_to_values(&fields, &schema.columns));
        }
        let affected_n = rows.len() as u64;
        if !rows.is_empty() {
            // A primary key that is stored or repeats in `rows` refuses
            // every row.
            engine.strict.insert_rows(collection, &rows).await?;
            index_strict_rows(engine, collection, rows.iter().map(Vec::as_slice))?;
        }
        Ok(affected(affected_n, "INSERT"))
    } else {
        let mut decoded: Vec<(&str, Vec<(String, loro::LoroValue)>)> =
            Vec::with_capacity(documents.len());
        for (doc_id, value_bytes) in documents {
            let crdt_fields = msgpack_bytes_to_crdt_fields(value_bytes)?;
            decoded.push((doc_id.as_str(), crdt_fields));
        }
        let mut crdt = engine.crdt.lock().map_err(|_| LiteError::LockPoisoned)?;
        // Every key is judged before any document is written.
        let mut seen: HashSet<&str> = HashSet::with_capacity(decoded.len());
        let mut kept: Vec<&(&str, Vec<(String, loro::LoroValue)>)> =
            Vec::with_capacity(decoded.len());
        for entry in &decoded {
            let doc_id = entry.0;
            let taken = !seen.insert(doc_id) || crdt.exists(collection, doc_id);
            if !taken {
                kept.push(entry);
            } else if !if_absent {
                return Err(LiteError::BadRequest {
                    detail: format!(
                        "duplicate key value violates unique constraint on '{collection}' (id = '{doc_id}')"
                    ),
                });
            }
        }
        let affected_n = kept.len() as u64;
        let slices: Vec<Vec<(&str, loro::LoroValue)>> = kept
            .iter()
            .map(|(_, fields)| {
                fields
                    .iter()
                    .map(|(k, v)| (k.as_str(), v.clone()))
                    .collect()
            })
            .collect();
        let rows: Vec<CrdtRowOp<'_>> = kept
            .iter()
            .zip(&slices)
            .map(|((doc_id, _), f)| (CrdtRowWrite::Upsert, collection, *doc_id, f.as_slice()))
            .collect();
        // The whole batch is checked before any document is written.
        crdt.check_unique_writes(&rows)?;
        for &(_, _, doc_id, fields) in &rows {
            crdt.upsert_deferred(collection, doc_id, fields)?;
        }
        crdt.flush_deltas().map_err(|e| LiteError::Storage {
            detail: e.to_string(),
        })?;
        drop(crdt);
        reindex_documents(engine, collection, kept.iter().map(|(doc_id, _)| *doc_id))?;
        Ok(affected(affected_n, "INSERT"))
    }
}
