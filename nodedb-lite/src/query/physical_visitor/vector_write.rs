// SPDX-License-Identifier: Apache-2.0
//! Write-path and config-path implementations for wired `VectorOp` variants.
//!
//! Each function corresponds to one variant routed here from `vector_op.rs`.
//! `parse_metric` lives here; `ensure_hnsw` lives in `engine::vector::state`.

use std::sync::Arc;

use nodedb_types::collection_config::VectorPrimaryConfig;
use nodedb_types::result::QueryResult;
use nodedb_types::value::Value;
use nodedb_types::vector_distance::DistanceMetric;
use nodedb_types::vector_dtype::VectorStorageDtype;
use nodedb_types::{Surrogate, VectorQuantization};

use crate::engine::vector::state::ensure_hnsw;
use crate::error::LiteError;
use crate::nodedb::LockExt;
use crate::query::engine::LiteQueryEngine;
use crate::storage::engine::{StorageEngine, StorageEngineSync};

use super::adapter::LitePhysicalFut;

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

/// Insert a vector into the HNSW index and persist its doc_id to CRDT.
pub(super) fn vector_insert<'a, S>(
    engine: &'a LiteQueryEngine<S>,
    collection: String,
    embedding: Vec<f32>,
    field_name: String,
    doc_id: String,
) -> LitePhysicalFut<'a>
where
    S: StorageEngine + StorageEngineSync + 'a,
{
    let vector_state = Arc::clone(&engine.vector_state);
    let crdt = Arc::clone(&engine.crdt);
    Box::pin(async move {
        let index_key = if field_name.is_empty() {
            collection.clone()
        } else {
            format!("{collection}:{field_name}")
        };
        let internal_id = {
            let dtype = {
                let configs = vector_state.per_index_config.lock_or_recover();
                configs
                    .get(&index_key)
                    .map(|c| c.storage_dtype)
                    .unwrap_or(VectorStorageDtype::F32)
            };
            let mut indices = vector_state.hnsw_indices.lock_or_recover();
            let index = ensure_hnsw(&mut indices, &index_key, embedding.len(), dtype);
            let id_before = index.len() as u32;
            index
                .insert(embedding.clone())
                .map_err(|e| LiteError::BadRequest {
                    detail: format!("Insert: HNSW insert failed: {e}"),
                })?;
            id_before
        };
        {
            let mut id_map = vector_state.vector_id_map.lock_or_recover();
            id_map.insert(
                format!("{index_key}:{internal_id}"),
                (doc_id.clone(), internal_id),
            );
        }
        match crate::engine::vector::sidecar::ensure_sidecar(&vector_state, &index_key) {
            Ok(true) => {
                let mut sidecars = vector_state.codec_sidecars.lock_or_recover();
                if let Some(sidecar) = sidecars.get_mut(&index_key)
                    && let Err(e) = sidecar.encode_and_insert(internal_id, &embedding)
                {
                    tracing::warn!(
                        index_key = %index_key, id = internal_id, error = %e,
                        "Insert: sidecar encode failed; row falls back to FP32 rerank"
                    );
                }
            }
            Ok(false) => {}
            Err(e) => {
                return Err(LiteError::BadRequest {
                    detail: format!("Insert: sidecar install failed: {e}"),
                });
            }
        }
        {
            let mut crdt = crdt.lock_or_recover();
            crdt.upsert(
                &collection,
                &doc_id,
                &[(
                    "embedding_dim",
                    loro::LoroValue::I64(embedding.len() as i64),
                )],
            )
            .map_err(|e| LiteError::Storage {
                detail: format!("Insert: CRDT upsert failed: {e}"),
            })?;
        }
        Ok(QueryResult {
            columns: vec![],
            rows: vec![],
            rows_affected: 1,
        })
    })
}

/// Delete a vector by internal node id; reverse-scans `vector_id_map`.
pub(super) fn vector_delete_by_id<'a, S>(
    engine: &'a LiteQueryEngine<S>,
    collection: String,
    vector_id: u32,
) -> LitePhysicalFut<'a>
where
    S: StorageEngine + StorageEngineSync + 'a,
{
    let vector_state = Arc::clone(&engine.vector_state);
    let crdt = Arc::clone(&engine.crdt);
    Box::pin(async move {
        let doc_id = {
            let id_map = vector_state.vector_id_map.lock_or_recover();
            id_map
                .iter()
                .find(|(_, (_, iid))| *iid == vector_id)
                .map(|(_, (did, _))| did.clone())
        };
        let doc_id = doc_id.ok_or_else(|| LiteError::BadRequest {
            detail: format!("Delete: vector_id {vector_id} not found in collection '{collection}'"),
        })?;
        {
            let mut indices = vector_state.hnsw_indices.lock_or_recover();
            if let Some(index) = indices.get_mut(&collection) {
                index.delete(vector_id);
            }
        }
        {
            let mut crdt = crdt.lock_or_recover();
            crdt.delete(&collection, &doc_id)
                .map_err(|e| LiteError::Storage {
                    detail: format!("Delete: CRDT delete failed: {e}"),
                })?;
        }
        Ok(QueryResult {
            columns: vec![],
            rows: vec![],
            rows_affected: 1,
        })
    })
}

/// Delete a vector by surrogate string (idempotent — no-op if not found in HNSW).
pub(super) fn vector_delete_by_surrogate<'a, S>(
    engine: &'a LiteQueryEngine<S>,
    collection: String,
    surrogate: Surrogate,
    field_name: String,
) -> LitePhysicalFut<'a>
where
    S: StorageEngine + StorageEngineSync + 'a,
{
    let doc_id = surrogate.to_string();
    let vector_state = Arc::clone(&engine.vector_state);
    let crdt = Arc::clone(&engine.crdt);
    Box::pin(async move {
        let index_key = if field_name.is_empty() {
            collection.clone()
        } else {
            format!("{collection}:{field_name}")
        };
        let internal_id = {
            let id_map = vector_state.vector_id_map.lock_or_recover();
            id_map
                .iter()
                .find(|(_, (did, _))| did == &doc_id)
                .map(|(_, (_, iid))| *iid)
        };
        if let Some(iid) = internal_id {
            let mut indices = vector_state.hnsw_indices.lock_or_recover();
            if let Some(index) = indices.get_mut(&index_key) {
                index.delete(iid);
            }
        }
        {
            let mut crdt = crdt.lock_or_recover();
            crdt.delete(&collection, &doc_id)
                .map_err(|e| LiteError::Storage {
                    detail: format!("DeleteBySurrogate: CRDT delete failed: {e}"),
                })?;
        }
        Ok(QueryResult {
            columns: vec![],
            rows: vec![],
            rows_affected: 1,
        })
    })
}

/// Write first-insert config then insert into HNSW + CRDT (DirectUpsert path).
/// `payload_indexes` have no Lite bitmap index path; they are intentionally ignored.
pub(super) fn vector_direct_upsert<'a, S>(
    engine: &'a LiteQueryEngine<S>,
    collection: String,
    field: String,
    doc_id: String,
    embedding: Vec<f32>,
    quantization: VectorQuantization,
    storage_dtype: VectorStorageDtype,
) -> LitePhysicalFut<'a>
where
    S: StorageEngine + StorageEngineSync + 'a,
{
    let vector_state = Arc::clone(&engine.vector_state);
    let crdt = Arc::clone(&engine.crdt);
    Box::pin(async move {
        let index_key = if field.is_empty() {
            collection.clone()
        } else {
            format!("{collection}:{field}")
        };
        let dim = embedding.len();
        // Write first-insert config (quantization + storage_dtype) if absent.
        {
            let mut configs = vector_state.per_index_config.lock_or_recover();
            configs
                .entry(index_key.clone())
                .or_insert_with(|| VectorPrimaryConfig {
                    vector_field: field.clone(),
                    dim: dim as u32,
                    quantization,
                    storage_dtype,
                    ..VectorPrimaryConfig::default()
                });
        }
        let internal_id = {
            let dtype = {
                let configs = vector_state.per_index_config.lock_or_recover();
                configs
                    .get(&index_key)
                    .map(|c| c.storage_dtype)
                    .unwrap_or(VectorStorageDtype::F32)
            };
            let mut indices = vector_state.hnsw_indices.lock_or_recover();
            let index = ensure_hnsw(&mut indices, &index_key, dim, dtype);
            let id_before = index.len() as u32;
            index
                .insert(embedding.clone())
                .map_err(|e| LiteError::BadRequest {
                    detail: format!("DirectUpsert: HNSW insert failed: {e}"),
                })?;
            id_before
        };
        {
            let mut id_map = vector_state.vector_id_map.lock_or_recover();
            id_map.insert(
                format!("{index_key}:{internal_id}"),
                (doc_id.clone(), internal_id),
            );
        }
        match crate::engine::vector::sidecar::ensure_sidecar(&vector_state, &index_key) {
            Ok(true) => {
                let mut sidecars = vector_state.codec_sidecars.lock_or_recover();
                if let Some(sidecar) = sidecars.get_mut(&index_key)
                    && let Err(e) = sidecar.encode_and_insert(internal_id, &embedding)
                {
                    tracing::warn!(
                        index_key = %index_key, id = internal_id, error = %e,
                        "DirectUpsert: sidecar encode failed; row falls back to FP32"
                    );
                }
            }
            Ok(false) => {}
            Err(e) => {
                return Err(LiteError::BadRequest {
                    detail: format!("DirectUpsert: sidecar install failed: {e}"),
                });
            }
        }
        {
            let mut crdt = crdt.lock_or_recover();
            crdt.upsert(
                &collection,
                &doc_id,
                &[("embedding_dim", loro::LoroValue::I64(dim as i64))],
            )
            .map_err(|e| LiteError::Storage {
                detail: format!("DirectUpsert: CRDT upsert failed: {e}"),
            })?;
        }
        Ok(QueryResult {
            columns: vec![],
            rows: vec![],
            rows_affected: 1,
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
    S: StorageEngine + StorageEngineSync + 'a,
{
    let metric = parse_metric(&metric_str)?;
    let vector_state = Arc::clone(&engine.vector_state);
    Ok(Box::pin(async move {
        {
            let indices = vector_state.hnsw_indices.lock_or_recover();
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
        })
    }))
}

/// Return minimal live stats from the in-memory HNSW index.
pub(super) fn vector_query_stats<'a, S>(
    engine: &'a LiteQueryEngine<S>,
    index_key: String,
) -> LitePhysicalFut<'a>
where
    S: StorageEngine + StorageEngineSync + 'a,
{
    let vector_state = Arc::clone(&engine.vector_state);
    Box::pin(async move {
        let columns = vec![
            "node_count".to_string(),
            "dim".to_string(),
            "dtype".to_string(),
            "metric".to_string(),
        ];
        let indices = vector_state.hnsw_indices.lock_or_recover();
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
        })
    })
}
