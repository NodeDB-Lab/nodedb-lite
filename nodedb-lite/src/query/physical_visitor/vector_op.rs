// SPDX-License-Identifier: Apache-2.0
//! Dispatch logic for all 18 `VectorOp` variants on the Lite executor.
//!
//! Variants that Lite can serve are wired to helpers in `vector_write`;
//! variants that require Origin-only infrastructure return
//! `LiteError::BadRequest` with a precise architectural-mismatch message.
//! No `_ =>` catchall — match is exhaustive over all 18 variants.

use std::sync::Arc;

use nodedb_physical::physical_plan::VectorOp;
use nodedb_types::SparseVector;
use nodedb_types::result::QueryResult;
use nodedb_types::value::Value;

use crate::engine::vector::search::run_vector_search;
use crate::error::LiteError;
use crate::nodedb::lock_ext::LockExt;
use crate::query::engine::LiteQueryEngine;
use crate::storage::engine::StorageEngine;

use super::adapter::LitePhysicalFut;
use super::vector_write::{
    vector_delete_by_id, vector_delete_by_surrogate, vector_direct_upsert, vector_insert,
    vector_query_stats, vector_set_params,
};

/// Entry point called by `LiteDataPlaneVisitor::vector()`.
pub(super) fn execute_vector_op<'a, S>(
    engine: &'a LiteQueryEngine<S>,
    op: &VectorOp,
) -> Result<LitePhysicalFut<'a>, LiteError>
where
    S: StorageEngine + 'a,
{
    match op {
        // ── A. Wired ─────────────────────────────────────────────────────────
        VectorOp::Search {
            collection,
            field_name,
            query_vector,
            top_k,
            ef_search,
            rls_filters,
            metric,
            skip_payload_fetch,
            ..
        } => {
            let index_key = if field_name.is_empty() {
                collection.clone()
            } else {
                format!("{collection}:{field_name}")
            };
            let collection = collection.clone();
            let query = query_vector.clone();
            let k = *top_k;
            let ef = *ef_search;
            let metric = *metric;
            let skip_payload_fetch = *skip_payload_fetch;
            let metadata_filter: Option<nodedb_types::filter::MetadataFilter> =
                if rls_filters.is_empty() {
                    None
                } else {
                    Some(zerompk::from_msgpack(rls_filters).map_err(|e| {
                        LiteError::Serialization {
                            detail: format!("decode MetadataFilter: {e}"),
                        }
                    })?)
                };
            let vector_state = Arc::clone(&engine.vector_state);
            let crdt = Arc::clone(&engine.crdt);
            Ok(Box::pin(async move {
                let results = run_vector_search(
                    &vector_state,
                    &crdt,
                    &index_key,
                    &collection,
                    &query,
                    k,
                    metadata_filter.as_ref(),
                    &[],
                    None,
                    None,
                    skip_payload_fetch,
                    Some(metric),
                    Some(ef),
                )
                .await
                .map_err(|e| LiteError::Query(e.to_string()))?;

                let columns = vec!["id".to_string(), "distance".to_string()];
                let rows: Vec<Vec<Value>> = results
                    .into_iter()
                    .map(|r| vec![Value::String(r.id), Value::Float(r.distance as f64)])
                    .collect();
                Ok(QueryResult {
                    columns,
                    rows,
                    rows_affected: 0,
                })
            }))
        }

        VectorOp::Insert {
            collection,
            vector,
            dim,
            field_name,
            surrogate,
            pk_bytes: _,
            provenance: _,
        } => {
            if vector.len() != *dim {
                return Err(LiteError::BadRequest {
                    detail: format!(
                        "Insert: declared dim={} but embedding has {} elements",
                        dim,
                        vector.len()
                    ),
                });
            }
            Ok(vector_insert(
                engine,
                collection.clone(),
                vector.clone(),
                field_name.clone(),
                surrogate.to_string(),
            ))
        }

        VectorOp::Delete {
            collection,
            vector_id,
        } => Ok(vector_delete_by_id(engine, collection.clone(), *vector_id)),

        VectorOp::DeleteBySurrogate {
            collection,
            surrogate,
            field_name,
            provenance: _,
        } => Ok(vector_delete_by_surrogate(
            engine,
            collection.clone(),
            *surrogate,
            field_name.clone(),
        )),

        // ── B. Wired with config write ────────────────────────────────────────
        VectorOp::SetParams {
            collection,
            field_name,
            m,
            ef_construction,
            metric,
            // index_type, pq_m, ivf_cells, ivf_nprobe: no Lite counterpart.
            ..
        } => {
            let index_key = if field_name.is_empty() {
                collection.clone()
            } else {
                format!("{collection}:{field_name}")
            };
            vector_set_params(engine, index_key, *m, *ef_construction, metric.clone())
        }

        // ── C. DirectUpsert ───────────────────────────────────────────────────

        // payload and payload_indexes: Lite has no bitmap index implementation;
        // payload bytes are not decoded or stored here.
        VectorOp::DirectUpsert {
            collection,
            field,
            surrogate,
            vector,
            quantization,
            storage_dtype,
            ..
        } => Ok(vector_direct_upsert(
            engine,
            collection.clone(),
            field.clone(),
            surrogate.to_string(),
            vector.clone(),
            *quantization,
            *storage_dtype,
        )),

        VectorOp::QueryStats {
            collection,
            field_name,
        } => {
            let index_key = if field_name.is_empty() {
                collection.clone()
            } else {
                format!("{collection}:{field_name}")
            };
            Ok(vector_query_stats(engine, index_key))
        }

        // ── D. Architectural-mismatch BadRequest ──────────────────────────────
        VectorOp::BatchInsert { .. } => Err(LiteError::BadRequest {
            detail: "BatchInsert: Lite has no batched surrogate allocator; \
                     use repeated Insert calls instead."
                .to_string(),
        }),

        VectorOp::MultiSearch { .. } => Err(LiteError::BadRequest {
            detail: "MultiSearch: Lite has no multi-field RRF fusion path; \
                     query each field separately and fuse in the client."
                .to_string(),
        }),

        VectorOp::Seal { .. } => Err(LiteError::BadRequest {
            detail: "Seal: Lite is segmentless; HNSW lives entirely in-memory and is \
                     checkpointed atomically. Seal is a no-op concept on Lite and \
                     indicates targeting the wrong deployment."
                .to_string(),
        }),

        VectorOp::CompactIndex { .. } => Err(LiteError::BadRequest {
            detail: "CompactIndex: Lite is segmentless; there are no sealed segments to \
                     compact. This operation requires Origin's segmented index lifecycle."
                .to_string(),
        }),

        VectorOp::Rebuild { .. } => Err(LiteError::BadRequest {
            detail: "Rebuild: Lite HnswIndex parameters are fixed at index creation; \
                     drop and recreate to change parameters. Rebuild requires Origin's \
                     segmented index lifecycle."
                .to_string(),
        }),

        // ── E. Sparse inverted index ──────────────────────────────────────────
        VectorOp::SparseInsert {
            collection,
            field_name,
            doc_id,
            entries,
        } => {
            let vector =
                SparseVector::from_entries(entries.clone()).map_err(|e| LiteError::BadRequest {
                    detail: format!("SparseInsert: {e}"),
                })?;
            let sparse_state = Arc::clone(&engine.sparse_state);
            let collection = collection.clone();
            let field_name = field_name.clone();
            let doc_id = doc_id.clone();
            Ok(Box::pin(async move {
                sparse_state.manager.lock_or_recover().index_document(
                    &collection,
                    &field_name,
                    &doc_id,
                    &vector,
                );
                Ok(QueryResult {
                    columns: Vec::new(),
                    rows: Vec::new(),
                    rows_affected: 1,
                })
            }))
        }

        VectorOp::SparseSearch {
            collection,
            field_name,
            query_entries,
            top_k,
        } => {
            let query = SparseVector::from_entries(query_entries.clone()).map_err(|e| {
                LiteError::BadRequest {
                    detail: format!("SparseSearch: {e}"),
                }
            })?;
            let sparse_state = Arc::clone(&engine.sparse_state);
            let collection = collection.clone();
            let field_name = field_name.clone();
            let k = *top_k;
            Ok(Box::pin(async move {
                let hits = sparse_state.manager.lock_or_recover().search(
                    &collection,
                    &field_name,
                    &query,
                    k,
                );
                let rows: Vec<Vec<Value>> = hits
                    .into_iter()
                    .map(|h| vec![Value::String(h.doc_id), Value::Float(h.score as f64)])
                    .collect();
                Ok(QueryResult {
                    columns: vec!["id".to_string(), "score".to_string()],
                    rows,
                    rows_affected: 0,
                })
            }))
        }

        VectorOp::SparseDelete {
            collection,
            field_name,
            doc_id,
        } => {
            let sparse_state = Arc::clone(&engine.sparse_state);
            let collection = collection.clone();
            let field_name = field_name.clone();
            let doc_id = doc_id.clone();
            Ok(Box::pin(async move {
                let removed = sparse_state.manager.lock_or_recover().remove_document(
                    &collection,
                    &field_name,
                    &doc_id,
                );
                Ok(QueryResult {
                    columns: Vec::new(),
                    rows: Vec::new(),
                    rows_affected: u64::from(removed),
                })
            }))
        }

        VectorOp::MultiVectorInsert { .. } => Err(LiteError::BadRequest {
            detail: "MultiVectorInsert: Lite has no multi-vector (ColBERT-style) HNSW; \
                     these operations are unsupported on Lite."
                .to_string(),
        }),

        VectorOp::MultiVectorDelete { .. } => Err(LiteError::BadRequest {
            detail: "MultiVectorDelete: Lite has no multi-vector (ColBERT-style) HNSW; \
                     these operations are unsupported on Lite."
                .to_string(),
        }),

        VectorOp::MultiVectorScoreSearch { .. } => Err(LiteError::BadRequest {
            detail: "MultiVectorScoreSearch: Lite has no multi-vector (ColBERT-style) HNSW; \
                     these operations are unsupported on Lite."
                .to_string(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use nodedb_physical::physical_plan::VectorOp;
    use nodedb_types::Surrogate;
    use nodedb_types::result::QueryResult;
    use nodedb_types::value::Value;

    use crate::PagedbStorageMem;
    use crate::engine::array::ArrayEngineState;
    use crate::engine::columnar::ColumnarEngine;
    use crate::engine::crdt::CrdtEngine;
    use crate::engine::fts::FtsState;
    use crate::engine::htap::HtapBridge;
    use crate::engine::strict::StrictEngine;
    use crate::engine::vector::VectorState;
    use crate::error::LiteError;
    use crate::query::engine::LiteQueryEngine;

    async fn make_engine() -> LiteQueryEngine<PagedbStorageMem> {
        use std::sync::Mutex;
        let storage = Arc::new(
            PagedbStorageMem::open_in_memory()
                .await
                .expect("in-memory pagedb"),
        );
        let crdt = Arc::new(Mutex::new(CrdtEngine::new(1).expect("CrdtEngine init")));
        let strict = Arc::new(StrictEngine::new(Arc::clone(&storage)));
        let columnar = Arc::new(ColumnarEngine::new(Arc::clone(&storage)));
        let htap = Arc::new(HtapBridge::new());
        let timeseries = Arc::new(Mutex::new(
            crate::engine::timeseries::engine::TimeseriesEngine::new(),
        ));
        let vector_state = Arc::new(VectorState::new(Arc::clone(&storage), 100));
        let array_state = Arc::new(tokio::sync::Mutex::new(
            ArrayEngineState::open(&storage)
                .await
                .expect("ArrayEngineState::open"),
        ));
        let fts_state = Arc::new(FtsState::new());
        let spatial = Arc::new(Mutex::new(
            crate::engine::spatial::SpatialIndexManager::new(),
        ));
        LiteQueryEngine::new(
            crdt,
            strict,
            columnar,
            htap,
            storage,
            timeseries,
            vector_state,
            array_state,
            fts_state,
            Arc::new(crate::engine::sparse_vector::SparseVectorState::new()),
            spatial,
            Arc::new(Mutex::new(std::collections::HashMap::new())),
        )
    }

    #[tokio::test]
    async fn vector_op_seal_returns_bad_request() {
        let engine = make_engine().await;
        let op = VectorOp::Seal {
            collection: "col".to_string(),
            field_name: String::new(),
        };
        match super::execute_vector_op(&engine, &op) {
            Err(LiteError::BadRequest { detail }) => {
                assert!(
                    detail.contains("segmentless") || detail.contains("Lite"),
                    "expected 'segmentless' or 'Lite' in message, got: {detail}"
                );
            }
            Err(other) => panic!("expected BadRequest, got Err({other})"),
            Ok(_) => panic!("expected BadRequest, got Ok"),
        }
    }

    async fn run_op(engine: &LiteQueryEngine<PagedbStorageMem>, op: VectorOp) -> QueryResult {
        super::execute_vector_op(engine, &op)
            .unwrap_or_else(|e| panic!("execute_vector_op failed synchronously: {e}"))
            .await
            .unwrap_or_else(|e| panic!("vector op future failed: {e}"))
    }

    fn sparse_insert(doc_id: &str, entries: Vec<(u32, f32)>) -> VectorOp {
        VectorOp::SparseInsert {
            collection: "col".to_string(),
            field_name: "sparse".to_string(),
            doc_id: doc_id.to_string(),
            entries,
        }
    }

    #[tokio::test]
    async fn vector_op_sparse_insert_then_search_ranks_by_dot_product() {
        let engine = make_engine().await;
        assert_eq!(
            run_op(&engine, sparse_insert("low", vec![(1, 0.5)]))
                .await
                .rows_affected,
            1
        );
        run_op(&engine, sparse_insert("high", vec![(1, 4.0)])).await;
        run_op(&engine, sparse_insert("disjoint", vec![(99, 9.0)])).await;

        let result = run_op(
            &engine,
            VectorOp::SparseSearch {
                collection: "col".to_string(),
                field_name: "sparse".to_string(),
                query_entries: vec![(1, 1.0)],
                top_k: 10,
            },
        )
        .await;

        assert_eq!(result.columns, vec!["id".to_string(), "score".to_string()]);
        assert_eq!(result.rows.len(), 2, "disjoint document must be excluded");
        assert_eq!(result.rows[0][0], Value::String("high".to_string()));
        assert_eq!(result.rows[1][0], Value::String("low".to_string()));
    }

    #[tokio::test]
    async fn vector_op_sparse_delete_removes_document() {
        let engine = make_engine().await;
        run_op(&engine, sparse_insert("d1", vec![(1, 1.0)])).await;

        let delete = VectorOp::SparseDelete {
            collection: "col".to_string(),
            field_name: "sparse".to_string(),
            doc_id: "d1".to_string(),
        };
        assert_eq!(run_op(&engine, delete.clone()).await.rows_affected, 1);
        assert_eq!(
            run_op(&engine, delete).await.rows_affected,
            0,
            "deleting an absent document affects no rows"
        );

        let result = run_op(
            &engine,
            VectorOp::SparseSearch {
                collection: "col".to_string(),
                field_name: "sparse".to_string(),
                query_entries: vec![(1, 1.0)],
                top_k: 10,
            },
        )
        .await;
        assert!(result.rows.is_empty());
    }

    #[tokio::test]
    async fn vector_op_sparse_search_on_missing_index_is_empty_not_error() {
        let engine = make_engine().await;
        let result = run_op(
            &engine,
            VectorOp::SparseSearch {
                collection: "never_written".to_string(),
                field_name: "sparse".to_string(),
                query_entries: vec![(1, 1.0)],
                top_k: 10,
            },
        )
        .await;
        assert!(result.rows.is_empty());
    }

    #[tokio::test]
    async fn vector_op_sparse_insert_rejects_non_finite_weight() {
        let engine = make_engine().await;
        match super::execute_vector_op(&engine, &sparse_insert("d1", vec![(1, f32::NAN)])) {
            Err(LiteError::BadRequest { detail }) => {
                assert!(detail.contains("SparseInsert"), "got: {detail}");
            }
            Err(other) => panic!("expected BadRequest, got Err({other})"),
            Ok(_) => panic!("expected BadRequest, got Ok"),
        }
    }

    #[tokio::test]
    async fn vector_op_multi_vector_score_search_returns_bad_request() {
        let engine = make_engine().await;
        let op = VectorOp::MultiVectorScoreSearch {
            collection: "col".to_string(),
            field_name: String::new(),
            query_vector: vec![1.0, 2.0],
            top_k: 5,
            ef_search: 0,
            mode: "max_sim".to_string(),
        };
        match super::execute_vector_op(&engine, &op) {
            Err(LiteError::BadRequest { detail }) => {
                assert!(
                    detail.contains("ColBERT") || detail.contains("Lite"),
                    "expected ColBERT or Lite in message, got: {detail}"
                );
            }
            Err(other) => panic!("expected BadRequest, got Err({other})"),
            Ok(_) => panic!("expected BadRequest, got Ok"),
        }
    }

    #[tokio::test]
    async fn vector_op_insert_routes_to_vector_insert_impl() {
        let engine = make_engine().await;
        let op = VectorOp::Insert {
            collection: "col".to_string(),
            vector: vec![1.0f32, 0.0, 0.0, 0.0],
            dim: 4,
            field_name: String::new(),
            surrogate: Surrogate::new(1u32),
            pk_bytes: None,
            provenance: None,
        };
        let fut = super::execute_vector_op(&engine, &op)
            .unwrap_or_else(|e| panic!("execute_vector_op should not fail synchronously: {e}"));
        let result = fut.await.expect("Insert should succeed");
        assert_eq!(result.rows_affected, 1);
        let indices = engine.vector_state.hnsw_indices.lock().unwrap();
        let idx = indices.get("col").expect("index 'col' should exist");
        assert_eq!(idx.len(), 1, "HNSW index should have exactly one node");
    }

    #[tokio::test]
    async fn vector_op_delete_by_vector_id_round_trip() {
        let engine = make_engine().await;
        // Insert first.
        let insert_op = VectorOp::Insert {
            collection: "col".to_string(),
            vector: vec![1.0f32, 0.0, 0.0, 0.0],
            dim: 4,
            field_name: String::new(),
            surrogate: Surrogate::new(42u32),
            pk_bytes: None,
            provenance: None,
        };
        super::execute_vector_op(&engine, &insert_op)
            .unwrap()
            .await
            .unwrap();

        // The HNSW node id for the first insert is 0.
        let delete_op = VectorOp::Delete {
            collection: "col".to_string(),
            vector_id: 0u32,
        };
        let result = super::execute_vector_op(&engine, &delete_op)
            .unwrap()
            .await
            .expect("Delete should succeed");
        assert_eq!(result.rows_affected, 1);

        let indices = engine.vector_state.hnsw_indices.lock().unwrap();
        let idx = indices.get("col").expect("index 'col' must still exist");
        assert_eq!(
            idx.live_count(),
            0,
            "no live nodes should remain after delete"
        );
    }
}
