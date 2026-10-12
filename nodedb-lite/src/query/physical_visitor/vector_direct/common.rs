// SPDX-License-Identifier: Apache-2.0
//! Row primitives shared by every vector-primary direct write.
//!
//! A vector-primary row on Lite is three things keyed by one `doc_id`: the
//! durable vector (`engine::vector::durable`), the live HNSW node reached
//! through `vector_id_map`, and the payload row in the CRDT store, which is
//! what `SELECT`, RETURNING, and predicate targeting read.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use nodedb_types::Surrogate;
use nodedb_types::value::Value;

use crate::engine::crdt::CrdtEngine;
use crate::engine::vector::VectorState;
use crate::engine::vector::nodes::{encode_sidecar, unbind_node, upsert_node};
use crate::engine::vector::resident::{check_insert_widths, lock_resident};
use crate::error::LiteError;
use crate::nodedb::LockExt;
use crate::nodedb::convert::{loro_value_to_document, value_to_loro};
use crate::storage::engine::StorageEngine;

pub(super) use crate::query::on_conflict::apply_patch;

/// A stored vector-primary row: its identity and payload fields.
pub(super) type StoredRow = (String, HashMap<String, Value>);

/// The sidecar field that records the vector's dimension.
pub(super) use crate::engine::vector::row::EMBEDDING_DIM_FIELD;

/// The HNSW bucket a `(collection, field)` pair writes into.
pub(super) fn index_key(collection: &str, field: &str) -> String {
    if field.is_empty() {
        collection.to_string()
    } else {
        format!("{collection}:{field}")
    }
}

/// The row identity a direct write carries. Lite binds no surrogate to a
/// primary key, so the key bytes are the identity; an op with no key falls
/// back to the surrogate's text form, which `DeleteBySurrogate` also uses.
pub(super) fn doc_id_for(pk_bytes: &[u8], surrogate: Surrogate) -> Result<String, LiteError> {
    if pk_bytes.is_empty() {
        return Ok(surrogate.to_string());
    }
    String::from_utf8(pk_bytes.to_vec()).map_err(|e| LiteError::BadRequest {
        detail: format!("vector-primary key is not UTF-8: {e}"),
    })
}

/// Decode a payload image into its field map. Empty bytes are an empty row.
pub(super) fn decode_payload(payload: &[u8]) -> Result<HashMap<String, Value>, LiteError> {
    if payload.is_empty() {
        return Ok(HashMap::new());
    }
    zerompk::from_msgpack(payload).map_err(|e| LiteError::Serialization {
        detail: format!("decode vector-primary payload: {e}"),
    })
}

/// Tombstone the live node bound to `doc_id`, drop its id-map entry and
/// codec sidecar code. An evicted index is loaded back first: a tombstone it
/// missed would bring the vector back. Returns whether a live node existed.
/// Fails when loading the index fails.
pub(in crate::query::physical_visitor) async fn remove_live_node<S: StorageEngine>(
    vector_state: &Arc<VectorState<S>>,
    index_key: &str,
    doc_id: &str,
) -> Result<bool, LiteError> {
    // Lock order matches the search path: indices, then the id map.
    let mut indices = lock_resident(vector_state, index_key).await?;
    let Some(index) = indices.get_mut(index_key) else {
        return Ok(false);
    };
    Ok(unbind_node(vector_state, Some(index), index_key, doc_id).is_some())
}

/// Make `embedding` durable for `doc_id`, insert it into the HNSW index,
/// bind the node in the id map, and encode it into the codec sidecar when
/// one is installed. The node `doc_id` held before is tombstoned, so the id
/// keeps one live node. Returns the internal node id.
pub(super) async fn insert_node<S: StorageEngine>(
    vector_state: &Arc<VectorState<S>>,
    index_key: &str,
    doc_id: &str,
    embedding: &[f32],
    op_name: &str,
) -> Result<u32, LiteError> {
    // A vector of another width than the index, loaded back if it was
    // evicted, is refused before its durable row is written.
    check_insert_widths(vector_state, index_key, [embedding.len()]).await?;
    // Durable row first: it is the source of truth both the in-memory index
    // and the pagedb segment are rebuilt from.
    let op = crate::engine::vector::durable::put_op(index_key, doc_id, embedding);
    vector_state
        .storage
        .batch_write(std::slice::from_ref(&op))
        .await
        .map_err(|e| LiteError::Storage {
            detail: format!("{op_name}: durable vector write failed: {e}"),
        })?;
    let internal_id = upsert_node(vector_state, index_key, doc_id, embedding).await?;
    encode_sidecar(vector_state, index_key, internal_id, embedding).map_err(|e| {
        LiteError::BadRequest {
            detail: format!("{op_name}: sidecar install failed: {e}"),
        }
    })?;
    Ok(internal_id)
}

/// Remove the durable vector of `doc_id`.
pub(super) async fn remove_durable<S: StorageEngine>(
    vector_state: &VectorState<S>,
    index_key: &str,
    doc_id: &str,
    op_name: &str,
) -> Result<(), LiteError> {
    crate::engine::vector::durable::remove(&*vector_state.storage, index_key, doc_id)
        .await
        .map_err(|e| LiteError::Storage {
            detail: format!("{op_name}: durable vector remove failed: {e}"),
        })
}

/// The stored payload row of `doc_id`, or `None` when no row exists.
pub(super) fn read_row(
    crdt: &Mutex<CrdtEngine>,
    collection: &str,
    doc_id: &str,
) -> Option<HashMap<String, Value>> {
    let guard = crdt.lock_or_recover();
    let value = guard.read(collection, doc_id)?;
    Some(loro_value_to_document(doc_id, &value).fields)
}

/// Every stored `(doc_id, row)` of `collection`.
pub(super) fn read_all_rows(crdt: &Mutex<CrdtEngine>, collection: &str) -> Vec<StoredRow> {
    let guard = crdt.lock_or_recover();
    guard
        .list_ids(collection)
        .into_iter()
        .filter_map(|id| {
            let value = guard.read(collection, &id)?;
            let fields = loro_value_to_document(&id, &value).fields;
            Some((id, fields))
        })
        .collect()
}

/// Write `fields` plus the embedding dimension as the whole payload row of
/// `doc_id`, replacing any stored row.
pub(super) fn write_row(
    crdt: &Mutex<CrdtEngine>,
    collection: &str,
    doc_id: &str,
    dim: usize,
    fields: &HashMap<String, Value>,
    op_name: &str,
) -> Result<(), LiteError> {
    let mut loro_fields: Vec<(&str, loro::LoroValue)> = fields
        .iter()
        .filter(|(k, _)| k.as_str() != EMBEDDING_DIM_FIELD)
        .map(|(k, v)| (k.as_str(), value_to_loro(v)))
        .collect();
    loro_fields.push((EMBEDDING_DIM_FIELD, loro::LoroValue::I64(dim as i64)));
    let mut guard = crdt.lock_or_recover();
    if guard.exists(collection, doc_id) {
        guard
            .delete(collection, doc_id)
            .map_err(|e| e.within(&format!("{op_name}: CRDT row replace")))?;
    }
    guard
        .upsert(collection, doc_id, &loro_fields)
        .map_err(|e| e.within(&format!("{op_name}: CRDT upsert")))?;
    Ok(())
}

/// Remove the payload row of `doc_id`. Returns whether a row existed.
pub(super) fn delete_row(
    crdt: &Mutex<CrdtEngine>,
    collection: &str,
    doc_id: &str,
    op_name: &str,
) -> Result<bool, LiteError> {
    let mut guard = crdt.lock_or_recover();
    if !guard.exists(collection, doc_id) {
        return Ok(false);
    }
    guard
        .delete(collection, doc_id)
        .map_err(|e| e.within(&format!("{op_name}: CRDT delete")))?;
    Ok(true)
}
