// SPDX-License-Identifier: Apache-2.0

//! Physical execution of `TextOp` variants for the Lite data plane.

use std::sync::Arc;

use nodedb_graph::Direction;
use nodedb_graph::traversal::DEFAULT_MAX_VISITED;
use nodedb_physical::physical_plan::TextOp;
use nodedb_query::fusion::{FusedResult, RankedResult, reciprocal_rank_fusion_weighted};
use nodedb_types::result::QueryResult;
use nodedb_types::text_search::{QueryMode, TextSearchParams};
use nodedb_types::value::Value;

use crate::engine::fts::run_text_search;
use crate::engine::vector::search::run_vector_search;
use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::storage::engine::StorageEngine;

use super::adapter::LitePhysicalFut;

/// Dispatch a `TextOp` to the appropriate Lite execution path.
///
/// Returns a pinned future that resolves to a `QueryResult`.
pub(super) fn execute_text_op<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    op: &TextOp,
) -> Result<LitePhysicalFut<'a>, LiteError> {
    match op {
        TextOp::Search {
            collection,
            query,
            top_k,
            fuzzy,
            rls_filters,
            ..
        } => {
            let collection = collection.clone();
            let query = query.clone();
            let top_k = *top_k;
            let fuzzy = *fuzzy;
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
            let fts_state = Arc::clone(&engine.fts_state);
            let crdt = Arc::clone(&engine.crdt);
            Ok(Box::pin(async move {
                let params = TextSearchParams {
                    fuzzy,
                    mode: QueryMode::Or,
                };
                let mut results =
                    run_text_search(&fts_state, &crdt, &collection, &query, top_k, &params, None)
                        .map_err(|e| LiteError::Query(e.to_string()))?;
                if let Some(filter) = metadata_filter {
                    results.retain(|r| {
                        let json_doc = serde_json::to_value(&r.metadata).unwrap_or_default();
                        nodedb_query::metadata_filter::matches_metadata_filter(&json_doc, &filter)
                    });
                }
                let columns = vec!["id".to_string(), "score".to_string()];
                let rows: Vec<Vec<Value>> = results
                    .into_iter()
                    .map(|r| vec![Value::String(r.id), Value::Float((1.0 - r.distance) as f64)])
                    .collect();
                Ok(QueryResult {
                    columns,
                    rows,
                    rows_affected: 0,
                })
            }))
        }

        TextOp::BM25ScoreScan {
            collection,
            query,
            score_alias,
            fuzzy,
        } => {
            let collection = collection.clone();
            let query = query.clone();
            let score_alias = score_alias.clone();
            let fuzzy = *fuzzy;
            let fts_state = Arc::clone(&engine.fts_state);
            Ok(Box::pin(async move {
                let params = TextSearchParams {
                    fuzzy,
                    mode: QueryMode::Or,
                };
                let scored = fts_state
                    .manager
                    .lock()
                    .map_err(|_| LiteError::LockPoisoned)?
                    .scan_all_with_scores(&collection, &query, &params);
                let columns = vec!["id".to_string(), score_alias];
                let rows: Vec<Vec<Value>> = scored
                    .into_iter()
                    .map(|(doc_id, score)| vec![Value::String(doc_id), Value::Float(score as f64)])
                    .collect();
                Ok(QueryResult {
                    columns,
                    rows,
                    rows_affected: 0,
                })
            }))
        }

        TextOp::PhraseSearch {
            collection,
            terms,
            top_k,
            ..
        } => {
            let collection = collection.clone();
            let terms = terms.clone();
            let top_k = *top_k;
            let fts_state = Arc::clone(&engine.fts_state);
            Ok(Box::pin(async move {
                let params = TextSearchParams {
                    fuzzy: false,
                    mode: QueryMode::Or,
                };
                let results = fts_state
                    .manager
                    .lock()
                    .map_err(|_| LiteError::LockPoisoned)?
                    .phrase_search(&collection, &terms, top_k, &params);
                let columns = vec!["id".to_string(), "score".to_string()];
                let rows: Vec<Vec<Value>> = results
                    .into_iter()
                    .map(|r| vec![Value::String(r.doc_id), Value::Float(r.score as f64)])
                    .collect();
                Ok(QueryResult {
                    columns,
                    rows,
                    rows_affected: 0,
                })
            }))
        }

        TextOp::HybridSearch {
            collection,
            query_vector,
            query_text,
            top_k,
            fuzzy,
            vector_weight,
            rls_filters,
            score_alias,
            ..
        } => {
            let collection = collection.clone();
            let query_vector = query_vector.clone();
            let query_text = query_text.clone();
            let top_k = *top_k;
            let fuzzy = *fuzzy;
            let vector_weight = *vector_weight;
            let score_alias = score_alias
                .clone()
                .unwrap_or_else(|| "rrf_score".to_string());
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
            let fts_state = Arc::clone(&engine.fts_state);
            let crdt = Arc::clone(&engine.crdt);
            let vector_state = Arc::clone(&engine.vector_state);
            Ok(Box::pin(async move {
                let text_params = TextSearchParams {
                    fuzzy,
                    mode: QueryMode::Or,
                };
                let text_results = run_text_search(
                    &fts_state,
                    &crdt,
                    &collection,
                    &query_text,
                    top_k * 3,
                    &text_params,
                    None,
                )
                .map_err(|e| LiteError::Query(e.to_string()))?;
                let vector_results = run_vector_search(
                    &vector_state,
                    &crdt,
                    &collection,
                    &collection,
                    &query_vector,
                    top_k * 3,
                    metadata_filter.as_ref(),
                    &[],
                    None,
                    None,
                    false,
                    None,
                    None,
                )
                .await
                .map_err(|e| LiteError::Query(e.to_string()))?;

                let text_ranked: Vec<nodedb_query::fusion::RankedResult> = text_results
                    .iter()
                    .enumerate()
                    .map(|(i, r)| nodedb_query::fusion::RankedResult {
                        document_id: r.id.clone(),
                        rank: i,
                        score: 1.0 - r.distance,
                        source: "text",
                    })
                    .collect();
                let vector_ranked: Vec<nodedb_query::fusion::RankedResult> = vector_results
                    .iter()
                    .enumerate()
                    .map(|(i, r)| nodedb_query::fusion::RankedResult {
                        document_id: r.id.clone(),
                        rank: i,
                        score: 1.0 - r.distance,
                        source: "vector",
                    })
                    .collect();

                let text_k = 60.0 * (1.0 - vector_weight as f64);
                let vector_k = 60.0 * vector_weight as f64;
                let fused = nodedb_query::fusion::reciprocal_rank_fusion_weighted(
                    &[vector_ranked, text_ranked],
                    &[vector_k, text_k],
                    top_k,
                );

                let columns = vec!["id".to_string(), score_alias];
                let rows: Vec<Vec<Value>> = fused
                    .into_iter()
                    .map(|r| vec![Value::String(r.document_id), Value::Float(r.rrf_score)])
                    .collect();
                Ok(QueryResult {
                    columns,
                    rows,
                    rows_affected: 0,
                })
            }))
        }

        TextOp::HybridSearchTriple {
            collection,
            query_vector,
            query_text,
            graph_seed_id,
            graph_depth,
            graph_edge_label,
            top_k,
            fuzzy,
            rrf_k,
            rls_filters,
            score_alias,
            ..
        } => {
            let collection = collection.clone();
            let query_vector = query_vector.clone();
            let query_text = query_text.clone();
            let graph_seed_id = graph_seed_id.clone();
            let graph_depth = *graph_depth;
            let graph_edge_label = graph_edge_label.clone();
            let top_k = *top_k;
            let fuzzy = *fuzzy;
            let rrf_k = *rrf_k;
            let score_alias = score_alias
                .clone()
                .unwrap_or_else(|| "rrf_score".to_string());
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
            let fts_state = Arc::clone(&engine.fts_state);
            let crdt = Arc::clone(&engine.crdt);
            let vector_state = Arc::clone(&engine.vector_state);
            let csr = Arc::clone(&engine.csr);
            Ok(Box::pin(async move {
                let text_params = TextSearchParams {
                    fuzzy,
                    mode: QueryMode::Or,
                };
                // Leg 1: text search.
                let text_results = run_text_search(
                    &fts_state,
                    &crdt,
                    &collection,
                    &query_text,
                    top_k * 3,
                    &text_params,
                    None,
                )
                .map_err(|e| LiteError::Query(e.to_string()))?;

                // Leg 2: vector search.
                let vector_results = run_vector_search(
                    &vector_state,
                    &crdt,
                    &collection,
                    &collection,
                    &query_vector,
                    top_k * 3,
                    metadata_filter.as_ref(),
                    &[],
                    None,
                    None,
                    false,
                    None,
                    None,
                )
                .await
                .map_err(|e| LiteError::Query(e.to_string()))?;

                // Leg 3: graph BFS from seed node.
                let graph_ranked: Vec<RankedResult> = if graph_depth > 0 {
                    let csr_guard = csr.lock().map_err(|_| LiteError::LockPoisoned)?;
                    if let Some(csr_idx) = csr_guard.get(&collection) {
                        let edge_label = graph_edge_label.as_deref();
                        let max_vis = graph_depth
                            .saturating_mul(top_k * 3)
                            .max(DEFAULT_MAX_VISITED);
                        let expanded = csr_idx.traverse_bfs(
                            nodedb_graph::BfsParams {
                                start_nodes: &[graph_seed_id.as_str()],
                                label_filter: edge_label,
                                direction: Direction::Out,
                                max_depth: graph_depth,
                                max_visited: max_vis,
                                frontier_bitmap: None,
                            },
                            None,
                        );
                        expanded
                            .into_iter()
                            .enumerate()
                            .map(|(rank, id)| RankedResult {
                                document_id: id,
                                rank,
                                score: 0.0,
                                source: "graph",
                            })
                            .collect()
                    } else {
                        Vec::new()
                    }
                } else {
                    Vec::new()
                };

                let text_ranked: Vec<RankedResult> = text_results
                    .iter()
                    .enumerate()
                    .map(|(i, r)| RankedResult {
                        document_id: r.id.clone(),
                        rank: i,
                        score: 1.0 - r.distance,
                        source: "text",
                    })
                    .collect();

                let vector_ranked: Vec<RankedResult> = vector_results
                    .iter()
                    .enumerate()
                    .map(|(i, r)| RankedResult {
                        document_id: r.id.clone(),
                        rank: i,
                        score: 1.0 - r.distance,
                        source: "vector",
                    })
                    .collect();

                let (kv, kt, kg) = rrf_k;
                let fused: Vec<FusedResult> = reciprocal_rank_fusion_weighted(
                    &[vector_ranked, text_ranked, graph_ranked],
                    &[kv, kt, kg],
                    top_k,
                );

                let columns = vec!["id".to_string(), score_alias];
                let rows: Vec<Vec<Value>> = fused
                    .into_iter()
                    .map(|f| vec![Value::String(f.document_id), Value::Float(f.rrf_score)])
                    .collect();
                Ok(QueryResult {
                    columns,
                    rows,
                    rows_affected: 0,
                })
            }))
        }

        TextOp::FtsIndexDoc {
            collection,
            surrogate,
            text,
            provenance: _,
        } => {
            let collection = collection.clone();
            let text = text.clone();
            let surrogate = *surrogate;
            let fts_state = Arc::clone(&engine.fts_state);
            #[cfg(not(target_arch = "wasm32"))]
            let fts_outbound = engine.fts_outbound.as_ref().map(Arc::clone);
            Ok(Box::pin(async move {
                // On Lite the surrogate space is internal to FtsCollectionManager.
                // We use `text` as the string doc_id (stable across frames for the
                // same document). We also register the Origin surrogate → Lite doc_id
                // mapping so FtsDeleteDoc can resolve it precisely.
                let mut mgr = fts_state
                    .manager
                    .lock()
                    .map_err(|_| LiteError::LockPoisoned)?;
                mgr.index_document(&collection, &text, &text);
                mgr.register_origin_surrogate(surrogate, &text);
                drop(mgr);
                // Stage for durable sync outbound (SQL path — no await needed).
                #[cfg(not(target_arch = "wasm32"))]
                if let Some(q) = fts_outbound {
                    q.stage_index(&collection, &text, text.clone());
                }
                Ok(QueryResult {
                    columns: vec![],
                    rows: vec![],
                    rows_affected: 1,
                })
            }))
        }

        TextOp::FtsDeleteDoc {
            collection,
            surrogate,
            provenance: _,
        } => {
            let collection = collection.clone();
            let surrogate = *surrogate;
            let fts_state = Arc::clone(&engine.fts_state);
            #[cfg(not(target_arch = "wasm32"))]
            let fts_outbound = engine.fts_outbound.as_ref().map(Arc::clone);
            Ok(Box::pin(async move {
                let mut mgr = fts_state
                    .manager
                    .lock()
                    .map_err(|_| LiteError::LockPoisoned)?;
                let removed_doc_id = mgr.remove_by_origin_surrogate(&collection, surrogate);
                drop(mgr);
                // Stage delete for durable sync outbound (SQL path — no await needed).
                #[cfg(not(target_arch = "wasm32"))]
                if let (Some(q), Some(doc_id)) = (fts_outbound, removed_doc_id.as_deref()) {
                    q.stage_delete(&collection, doc_id);
                }
                Ok(QueryResult {
                    columns: vec![],
                    rows: vec![],
                    rows_affected: if removed_doc_id.is_some() { 1 } else { 0 },
                })
            }))
        }

        TextOp::SetAnalyzer {
            collection,
            analyzer_name,
        } => {
            let collection = collection.clone();
            let analyzer_name = analyzer_name.clone();
            let fts_state = Arc::clone(&engine.fts_state);
            Ok(Box::pin(async move {
                let mut mgr = fts_state
                    .manager
                    .lock()
                    .map_err(|_| LiteError::LockPoisoned)?;
                mgr.set_collection_analyzer(&collection, &analyzer_name);
                Ok(QueryResult {
                    columns: vec![],
                    rows: vec![],
                    rows_affected: 0,
                })
            }))
        }
    }
}
