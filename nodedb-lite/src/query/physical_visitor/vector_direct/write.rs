// SPDX-License-Identifier: Apache-2.0
//! The vector-primary insert family: `DirectInsert`, `DirectInsertIfAbsent`,
//! `DirectUpsert`.

use std::sync::Arc;

use nodedb_physical::physical_plan::{UpdateValue, VectorDirectWriteIntent};
use nodedb_types::collection_config::VectorPrimaryConfig;
use nodedb_types::result::QueryResult;
use nodedb_types::vector_dtype::VectorStorageDtype;
use nodedb_types::{Surrogate, VectorQuantization};

use crate::error::LiteError;
use crate::nodedb::LockExt;
use crate::query::engine::LiteQueryEngine;
use crate::storage::engine::StorageEngine;

use super::super::adapter::LitePhysicalFut;
use super::common::{
    apply_patch, decode_payload, doc_id_for, index_key, insert_node, read_row, remove_live_node,
    write_row,
};

/// One row of the insert family, as the three ops carry it.
pub(in crate::query::physical_visitor) struct DirectWriteArgs {
    pub collection: String,
    pub field: String,
    pub surrogate: Surrogate,
    pub pk_bytes: Vec<u8>,
    pub vector: Vec<f32>,
    pub payload: Vec<u8>,
    pub quantization: VectorQuantization,
    pub storage_dtype: VectorStorageDtype,
    pub intent: VectorDirectWriteIntent,
    /// `ON CONFLICT (pk) DO UPDATE SET` assignments; only `Upsert` carries
    /// any. Empty means whole-row replace.
    pub on_conflict_updates: Vec<(String, UpdateValue)>,
}

/// Write one vector-primary row by `intent`.
///
/// `payload_indexes` have no Lite bitmap index: every payload predicate is
/// evaluated by brute force against the stored row, so the row itself is
/// the whole index.
pub(in crate::query::physical_visitor) fn vector_direct_write<'a, S>(
    engine: &'a LiteQueryEngine<S>,
    args: DirectWriteArgs,
) -> Result<LitePhysicalFut<'a>, LiteError>
where
    S: StorageEngine + 'a,
{
    let DirectWriteArgs {
        collection,
        field,
        surrogate,
        pk_bytes,
        vector,
        payload,
        quantization,
        storage_dtype,
        intent,
        on_conflict_updates,
    } = args;
    let op_name = match intent {
        VectorDirectWriteIntent::Insert => "DirectInsert",
        VectorDirectWriteIntent::InsertIfAbsent => "DirectInsertIfAbsent",
        VectorDirectWriteIntent::Upsert => "DirectUpsert",
    };
    if vector.is_empty() {
        return Err(LiteError::BadRequest {
            detail: format!("{op_name}: the vector column is empty"),
        });
    }
    let doc_id = doc_id_for(&pk_bytes, surrogate)?;
    let incoming = decode_payload(&payload)?;
    let key = index_key(&collection, &field);
    let vector_state = Arc::clone(&engine.vector_state);
    let crdt = Arc::clone(&engine.crdt);

    Ok(Box::pin(async move {
        let dim = vector.len();
        // First-insert config (quantization + storage dtype) when absent.
        vector_state
            .per_index_config
            .lock_or_recover()
            .entry(key.clone())
            .or_insert_with(|| VectorPrimaryConfig {
                vector_field: field.clone(),
                dim: dim as u32,
                quantization,
                storage_dtype,
                ..VectorPrimaryConfig::default()
            });

        let existing = read_row(&crdt, &collection, &doc_id);
        let row = match (intent, existing) {
            (VectorDirectWriteIntent::Insert, Some(_)) => {
                return Err(LiteError::UniqueViolation {
                    collection: collection.clone(),
                    detail: format!("key '{doc_id}' already exists"),
                });
            }
            (VectorDirectWriteIntent::InsertIfAbsent, Some(_)) => {
                return Ok(QueryResult {
                    columns: vec![],
                    rows: vec![],
                    rows_affected: 0,
                });
            }
            (VectorDirectWriteIntent::Upsert, Some(mut stored))
                if !on_conflict_updates.is_empty() =>
            {
                apply_patch(&mut stored, &on_conflict_updates, &incoming)?;
                stored
            }
            (VectorDirectWriteIntent::Upsert, Some(_)) => incoming,
            (
                VectorDirectWriteIntent::Insert
                | VectorDirectWriteIntent::InsertIfAbsent
                | VectorDirectWriteIntent::Upsert,
                None,
            ) => incoming,
        };

        // A stored row keeps exactly one live node: the old one goes before
        // the new vector is bound under the same identity.
        remove_live_node(&vector_state, &key, &doc_id);
        insert_node(&vector_state, &key, &doc_id, &vector, op_name).await?;
        write_row(&crdt, &collection, &doc_id, dim, &row, op_name)?;

        Ok(QueryResult {
            columns: vec![],
            rows: vec![],
            rows_affected: 1,
        })
    }))
}
