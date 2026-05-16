// SPDX-License-Identifier: Apache-2.0

//! Physical execution of `TextOp` variants for the Lite data plane.

use std::sync::Arc;

use nodedb_physical::physical_plan::TextOp;
use nodedb_types::result::QueryResult;
use nodedb_types::text_search::{QueryMode, TextSearchParams};
use nodedb_types::value::Value;

use crate::engine::fts::run_text_search;
use crate::engine::vector::search::run_vector_search;
use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::storage::engine::{StorageEngine, StorageEngineSync};

use super::adapter::LitePhysicalFut;

/// Dispatch a `TextOp` to the appropriate Lite execution path.
///
/// Returns a pinned future that resolves to a `QueryResult`.
pub(super) fn execute_text_op<'a, S: StorageEngine + StorageEngineSync + 'a>(
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
                    run_text_search(&fts_state, &crdt, &collection, &query, top_k, &params)
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

        TextOp::BM25ScoreScan { .. } => Ok(Box::pin(async {
            unimplemented!(
                "Lite FTS engine does not yet support TextOp::BM25ScoreScan; \
                 add a `scan_all_with_scores` method to FtsCollectionManager"
            )
        })),

        TextOp::PhraseSearch { .. } => Ok(Box::pin(async {
            unimplemented!(
                "Lite FTS engine does not yet support TextOp::PhraseSearch; \
                 add a `phrase_search` method to FtsCollectionManager"
            )
        })),

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

        TextOp::HybridSearchTriple { .. } => Ok(Box::pin(async {
            unimplemented!(
                "Lite TextOp::HybridSearchTriple requires graph engine access via a \
                 GraphState extraction (parallel to VectorState/FtsState) — \
                 add GraphState first"
            )
        })),

        TextOp::FtsIndexDoc {
            collection,
            surrogate: _,
            text,
        } => {
            let collection = collection.clone();
            let text = text.clone();
            let fts_state = Arc::clone(&engine.fts_state);
            Ok(Box::pin(async move {
                // On Lite the surrogate is managed internally by FtsCollectionManager,
                // so we index using the text as both key and content.
                // The sync path on Lite routes via document_put, not this op — this arm
                // covers the case where Origin dispatches an FtsIndexDoc frame to Lite.
                fts_state
                    .manager
                    .lock()
                    .map_err(|_| LiteError::LockPoisoned)?
                    .index_document(&collection, &text, &text);
                Ok(QueryResult {
                    columns: vec![],
                    rows: vec![],
                    rows_affected: 1,
                })
            }))
        }

        TextOp::FtsDeleteDoc {
            collection,
            surrogate: _,
        } => {
            let collection = collection.clone();
            let fts_state = Arc::clone(&engine.fts_state);
            Ok(Box::pin(async move {
                // Lite FtsCollectionManager uses string doc_ids, not u32 surrogates.
                // The Origin-side surrogate cannot be mapped back to a string doc_id
                // without a reverse lookup that Lite does not maintain across processes.
                // Documents are removed via document_delete on the CRDT path instead.
                // Drop the collection's entire index as a conservative fallback.
                fts_state
                    .manager
                    .lock()
                    .map_err(|_| LiteError::LockPoisoned)?
                    .drop_collection(&collection);
                Ok(QueryResult {
                    columns: vec![],
                    rows: vec![],
                    rows_affected: 0,
                })
            }))
        }
    }
}
