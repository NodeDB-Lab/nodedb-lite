// SPDX-License-Identifier: Apache-2.0
//! Write-path and config-path implementations for wired `VectorOp` variants.
//!
//! Each function corresponds to one variant routed here from `vector_op.rs`.
//! Every index access goes through `engine::vector::resident`, which loads an
//! evicted index back first.

use std::sync::Arc;

use nodedb_types::Surrogate;
use nodedb_types::collection_config::VectorPrimaryConfig;
use nodedb_types::result::QueryResult;
use nodedb_types::value::Value;
use nodedb_types::vector_distance::DistanceMetric;

use crate::engine::vector::nodes::{encode_sidecar, unbind_node, upsert_node};
use crate::engine::vector::resident::{check_insert_widths, lock_resident};
use crate::engine::vector::row::{
    EMBEDDING_DIM_FIELD, VectorSlot, detach_vector_row, is_vector_primary,
};
use crate::error::LiteError;
use crate::nodedb::LockExt;
use crate::query::engine::LiteQueryEngine;
use crate::storage::engine::StorageEngine;

use super::adapter::LitePhysicalFut;
use super::vector_direct::remove_live_node;

/// Resolve a string metric name (from `SetParams::metric`) to `DistanceMetric`.
pub(super) fn parse_metric(s: &str) -> Result<DistanceMetric, LiteError> {
    match s.to_lowercase().as_str() {
        "l2" | "euclidean" => Ok(DistanceMetric::L2),
        "cosine" => Ok(DistanceMetric::Cosine),
        "innerproduct" | "inner_product" | "dot" => Ok(DistanceMetric::InnerProduct),
        "manhattan" | "l1" => Ok(DistanceMetric::Manhattan),
        "chebyshev" | "linf" => Ok(DistanceMetric::Chebyshev),
        "hamming" => Ok(DistanceMetric::Hamming),
        "jaccard" => Ok(DistanceMetric::Jaccard),
        "pearson" => Ok(DistanceMetric::Pearson),
        other => Err(LiteError::BadRequest {
            detail: format!(
                "SetParams: unknown metric '{other}'; expected l2, cosine, inner_product, \
                 manhattan, chebyshev, hamming, jaccard, or pearson"
            ),
        }),
    }
}

/// Insert a vector into the HNSW index and merge its dimension into the CRDT
/// row with the same id, keeping the row's other fields. Re-inserting an id
/// replaces its vector.
pub(super) fn vector_insert<'a, S>(
    engine: &'a LiteQueryEngine<S>,
    collection: String,
    embedding: Vec<f32>,
    field_name: String,
    doc_id: String,
) -> LitePhysicalFut<'a>
where
    S: StorageEngine + 'a,
{
    let vector_state = Arc::clone(&engine.vector_state);
    let crdt = Arc::clone(&engine.crdt);
    Box::pin(async move {
        let index_key = if field_name.is_empty() {
            collection.clone()
        } else {
            format!("{collection}:{field_name}")
        };
        // A vector of another width than the index, loaded back if it was
        // evicted, is refused before its durable row is written.
        check_insert_widths(&vector_state, &index_key, [embedding.len()]).await?;
        // Durable row first: it is the source of truth that both the in-memory
        // index and the pagedb segment are derived from, so it must never be the
        // copy missing after a crash — and a flush that finds no durable row for
        // a collection writes no segment for it at all.
        if !embedding.is_empty() {
            let op = crate::engine::vector::durable::put_op(&index_key, &doc_id, &embedding);
            vector_state
                .storage
                .batch_write(std::slice::from_ref(&op))
                .await
                .map_err(|e| LiteError::Storage {
                    detail: format!("Insert: durable vector write failed: {e}"),
                })?;
        }
        // Replaces the node `doc_id` held before, so the id stays one node.
        let internal_id = upsert_node(&vector_state, &index_key, &doc_id, &embedding).await?;
        encode_sidecar(&vector_state, &index_key, internal_id, &embedding).map_err(|e| {
            LiteError::BadRequest {
                detail: format!("Insert: sidecar install failed: {e}"),
            }
        })?;
        {
            let mut crdt = crdt.lock_or_recover();
            crdt.set_fields(
                &collection,
                &doc_id,
                &[(
                    EMBEDDING_DIM_FIELD,
                    loro::LoroValue::I64(embedding.len() as i64),
                )],
            )
            .map_err(|e| e.within("Insert: CRDT field merge"))?;
        }
        Ok(QueryResult {
            columns: vec![],
            rows: vec![],
            rows_affected: 1,
            command: None,
        })
    })
}

/// Delete a vector by internal node id and detach it from its CRDT row.
/// The whole row goes when the collection is vector-primary or no document
/// field remains.
pub(super) fn vector_delete_by_id<'a, S>(
    engine: &'a LiteQueryEngine<S>,
    collection: String,
    vector_id: u32,
) -> LitePhysicalFut<'a>
where
    S: StorageEngine + 'a,
{
    let vector_state = Arc::clone(&engine.vector_state);
    let crdt = Arc::clone(&engine.crdt);
    Box::pin(async move {
        let doc_id = vector_state
            .vector_id_map
            .lock_or_recover()
            .doc_id(&collection, vector_id)
            .map(str::to_owned)
            .ok_or_else(|| LiteError::BadRequest {
                detail: format!(
                    "Delete: vector_id {vector_id} not found in collection '{collection}'"
                ),
            })?;
        // Durable row first: left behind, it would bring the vector back on
        // the next rebuild.
        crate::engine::vector::durable::remove(&*vector_state.storage, &collection, &doc_id)
            .await?;
        let remaining = {
            // The index, loaded back if it was evicted, takes the tombstone.
            let mut indices = lock_resident(&vector_state, &collection).await?;
            unbind_node(
                &vector_state,
                indices.get_mut(&collection),
                &collection,
                &doc_id,
            );
            vector_state
                .vector_id_map
                .lock_or_recover()
                .attached_vectors(&collection, &doc_id)
        };
        let vector_primary = is_vector_primary(&*vector_state.storage, &collection).await?;
        {
            let mut crdt = crdt.lock_or_recover();
            detach_vector_row(
                &mut crdt,
                &collection,
                &doc_id,
                VectorSlot::Base,
                &remaining,
                vector_primary,
            )
            .map_err(|e| e.within("Delete: CRDT detach"))?;
        }
        Ok(QueryResult {
            columns: vec![],
            rows: vec![],
            rows_affected: 1,
            command: None,
        })
    })
}

/// Delete a vector by surrogate: tombstone its live node and detach it from
/// its payload row, as a delete by id does. Reports 1 when a row or node
/// existed, else 0. A missing surrogate reports 0.
pub(super) fn vector_delete_by_surrogate<'a, S>(
    engine: &'a LiteQueryEngine<S>,
    collection: String,
    surrogate: Option<Surrogate>,
    field_name: String,
) -> LitePhysicalFut<'a>
where
    S: StorageEngine + 'a,
{
    let Some(surrogate) = surrogate else {
        return Box::pin(async move {
            Ok(QueryResult {
                columns: vec![],
                rows: vec![],
                rows_affected: 0,
                command: None,
            })
        });
    };
    let doc_id = surrogate.to_string();
    let vector_state = Arc::clone(&engine.vector_state);
    let crdt = Arc::clone(&engine.crdt);
    Box::pin(async move {
        let index_key = if field_name.is_empty() {
            collection.clone()
        } else {
            format!("{collection}:{field_name}")
        };
        let had_node = remove_live_node(&vector_state, &index_key, &doc_id).await?;
        let remaining = vector_state
            .vector_id_map
            .lock_or_recover()
            .attached_vectors(&collection, &doc_id);
        let slot = if field_name.is_empty() {
            VectorSlot::Base
        } else {
            VectorSlot::Named(&field_name)
        };
        let vector_primary = is_vector_primary(&*vector_state.storage, &collection).await?;
        let had_row = {
            let mut crdt = crdt.lock_or_recover();
            let had_row = crdt.exists(&collection, &doc_id);
            detach_vector_row(
                &mut crdt,
                &collection,
                &doc_id,
                slot,
                &remaining,
                vector_primary,
            )
            .map_err(|e| e.within("DeleteBySurrogate: CRDT detach"))?;
            had_row
        };
        Ok(QueryResult {
            columns: vec![],
            rows: vec![],
            rows_affected: u64::from(had_node || had_row),
            command: None,
        })
    })
}

/// Write HNSW params to `per_index_config`; error if index already exists.
pub(super) fn vector_set_params<'a, S>(
    engine: &'a LiteQueryEngine<S>,
    index_key: String,
    m: usize,
    ef_construction: usize,
    metric_str: String,
) -> Result<LitePhysicalFut<'a>, LiteError>
where
    S: StorageEngine + 'a,
{
    let metric = parse_metric(&metric_str)?;
    let vector_state = Arc::clone(&engine.vector_state);
    Ok(Box::pin(async move {
        {
            // An evicted index still exists: it is loaded back to be seen.
            let indices = lock_resident(&vector_state, &index_key).await?;
            if indices.contains_key(&index_key) {
                return Err(LiteError::BadRequest {
                    detail: format!(
                        "SetParams: Lite HnswIndex parameters are fixed at index creation; \
                         index '{index_key}' already exists. Drop and recreate to change params."
                    ),
                });
            }
        }
        {
            let mut configs = vector_state.per_index_config.lock_or_recover();
            let cfg = configs
                .entry(index_key.clone())
                .or_insert_with(VectorPrimaryConfig::default);
            cfg.m = m as u8;
            cfg.ef_construction = ef_construction as u16;
            cfg.metric = metric;
            // PQ/IVF settings have no Lite mapping; intentionally not persisted.
        }
        Ok(QueryResult {
            columns: vec![],
            rows: vec![],
            rows_affected: 0,
            command: None,
        })
    }))
}

/// Return minimal live stats from the in-memory HNSW index.
pub(super) fn vector_query_stats<'a, S>(
    engine: &'a LiteQueryEngine<S>,
    index_key: String,
) -> LitePhysicalFut<'a>
where
    S: StorageEngine + 'a,
{
    let vector_state = Arc::clone(&engine.vector_state);
    Box::pin(async move {
        let columns = vec![
            "node_count".to_string(),
            "dim".to_string(),
            "dtype".to_string(),
            "metric".to_string(),
        ];
        let indices = lock_resident(&vector_state, &index_key).await?;
        let rows = if let Some(idx) = indices.get(&index_key) {
            let p = idx.params();
            vec![vec![
                Value::Integer(idx.len() as i64),
                Value::Integer(idx.dim() as i64),
                Value::String(format!("{:?}", p.dtype)),
                Value::String(format!("{:?}", p.metric)),
            ]]
        } else {
            vec![]
        };
        Ok(QueryResult {
            columns,
            rows,
            rows_affected: 0,
            command: None,
        })
    })
}

/// Tear down one vector index: its in-memory graph, id-map entries, build
/// params, codec sidecar, persisted checkpoint, and the durable per-document
/// vectors it was built from.
///
/// The durable rows MUST go too. They are the source of truth the index is
/// rebuilt from, so leaving them behind would make the next open silently
/// resurrect the index that was just dropped.
pub(super) fn vector_drop_index<'a, S>(
    engine: &'a LiteQueryEngine<S>,
    index_key: String,
) -> Result<LitePhysicalFut<'a>, LiteError>
where
    S: StorageEngine + 'a,
{
    let vector_state = Arc::clone(&engine.vector_state);
    Ok(Box::pin(async move {
        // An evicted index exists too: its checkpoint is in storage.
        let existed = {
            let mut indices = vector_state.hnsw_indices.lock_or_recover();
            let evicted = vector_state.evicted.lock_or_recover().remove(&index_key);
            indices.remove(&index_key).is_some() || evicted
        };

        vector_state
            .vector_id_map
            .lock_or_recover()
            .remove_index(&index_key);
        vector_state
            .per_index_config
            .lock_or_recover()
            .remove(&index_key);
        vector_state
            .codec_sidecars
            .lock_or_recover()
            .remove(&index_key);

        // Persisted checkpoint, then the durable per-document vectors — see
        // the doc comment above. Either one left behind resurrects the index
        // on the next open, so a failure fails the drop.
        vector_state
            .storage
            .delete(
                nodedb_types::Namespace::Vector,
                format!("hnsw:{index_key}").as_bytes(),
            )
            .await?;
        let rows =
            crate::engine::vector::durable::load_collection(&*vector_state.storage, &index_key)
                .await?;
        for (doc_id, _) in rows {
            crate::engine::vector::durable::remove(&*vector_state.storage, &index_key, &doc_id)
                .await?;
        }

        tracing::info!(index_key, existed, "vector index dropped");
        Ok(nodedb_types::result::QueryResult {
            columns: Vec::new(),
            rows: Vec::new(),
            rows_affected: u64::from(existed),
            command: None,
        })
    }))
}
