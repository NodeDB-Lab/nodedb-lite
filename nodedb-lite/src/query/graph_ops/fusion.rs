// SPDX-License-Identifier: Apache-2.0

//! RagFusion: vector search → BFS graph expansion → RRF ranking.
//!
//! Supported combinations:
//! - Three-source (vector + BM25 text + graph expansion): activated when
//!   `bm25_query` and `bm25_field` are both `Some`.
//! - Two-source (vector + graph expansion): `bm25_query` is `None`.
//! - Degenerate (pure vector top-k): `expansion_depth == 0` and no BM25.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use nodedb_graph::traversal::DEFAULT_MAX_VISITED;
use nodedb_graph::{CsrIndex, Direction};
use nodedb_query::fusion::{FusedResult, RankedResult, reciprocal_rank_fusion_weighted};
use nodedb_types::TextSearchParams;
use nodedb_types::result::QueryResult;
use nodedb_types::value::Value;

use crate::engine::crdt::CrdtEngine;
use crate::engine::fts::search::run_text_search;
use crate::engine::fts::state::FtsState;
use crate::engine::vector::VectorState;
use crate::engine::vector::search::run_vector_search;
use crate::error::LiteError;
use crate::storage::engine::StorageEngine;

/// Execute a `GraphOp::RagFusion` against the Lite engine state.
///
/// Steps:
/// 1. ANN vector search for `vector_top_k` candidates.
/// 2. Optional BM25 text search if `bm25_query` is set.
/// 3. BFS graph expansion from vector-result nodes up to `expansion_depth` hops.
/// 4. RRF merge of all active rankings.
/// 5. Truncate to `final_top_k`.
#[allow(clippy::too_many_arguments)]
pub async fn rag_fusion<S: StorageEngine>(
    vector_state: &Arc<VectorState<S>>,
    crdt: &Arc<Mutex<CrdtEngine>>,
    fts_state: &Arc<FtsState>,
    csr_map: &Arc<Mutex<HashMap<String, CsrIndex>>>,
    collection: &str,
    query_vector: &[f32],
    vector_field: &str,
    vector_top_k: usize,
    edge_label: Option<&str>,
    direction: Direction,
    expansion_depth: usize,
    final_top_k: usize,
    rrf_k: (f64, f64),
    rrf_k_triple: Option<(f64, f64, f64)>,
    bm25_query: Option<&str>,
    bm25_field: Option<&str>,
) -> Result<QueryResult, LiteError> {
    // Pure vector degenerate path: no expansion and no BM25.
    if expansion_depth == 0 && bm25_query.is_none() {
        return pure_vector_path(
            vector_state,
            crdt,
            collection,
            vector_field,
            query_vector,
            final_top_k,
        )
        .await;
    }

    let index_key = if vector_field.is_empty() {
        collection.to_string()
    } else {
        format!("{collection}:{vector_field}")
    };

    // Step 1: ANN vector search.
    let vector_results = run_vector_search(
        vector_state,
        crdt,
        &index_key,
        collection,
        query_vector,
        vector_top_k,
        None,
        &[],
        None,
        None,
        true, // skip_payload_fetch — RRF only needs IDs
        None,
        None,
    )
    .await
    .map_err(|e| LiteError::Query(e.to_string()))?;

    let vector_ranked: Vec<RankedResult> = vector_results
        .iter()
        .enumerate()
        .map(|(rank, r)| RankedResult {
            document_id: r.id.clone(),
            rank,
            score: r.distance,
            source: "vector",
        })
        .collect();

    // Step 2: Optional BM25 text search.
    let bm25_ranked: Option<Vec<RankedResult>> = match (bm25_query, bm25_field) {
        (Some(q), Some(_field)) => {
            let text_results = run_text_search(
                fts_state,
                crdt,
                collection,
                q,
                vector_top_k,
                &TextSearchParams::default(),
            )
            .map_err(|e| LiteError::Query(e.to_string()))?;
            let ranked: Vec<RankedResult> = text_results
                .iter()
                .enumerate()
                .map(|(rank, r)| RankedResult {
                    document_id: r.id.clone(),
                    rank,
                    score: r.distance,
                    source: "bm25",
                })
                .collect();
            Some(ranked)
        }
        _ => None,
    };

    // Step 3: BFS graph expansion from vector-result node IDs.
    let expansion_ranked: Vec<RankedResult> = if expansion_depth > 0 {
        let map = csr_map.lock().map_err(|_| LiteError::LockPoisoned)?;
        let csr_opt = map.get(collection);

        if let Some(csr) = csr_opt {
            let starts: Vec<&str> = vector_results.iter().map(|r| r.id.as_str()).collect();
            let max_vis = expansion_depth
                .saturating_mul(vector_top_k)
                .max(DEFAULT_MAX_VISITED);
            let expanded = csr.traverse_bfs(
                &starts,
                edge_label,
                direction,
                expansion_depth,
                max_vis,
                None,
            );

            // Rank expanded nodes; skip nodes that were already vector results.
            let vector_ids: std::collections::HashSet<&str> =
                vector_results.iter().map(|r| r.id.as_str()).collect();

            expanded
                .into_iter()
                .filter(|id| !vector_ids.contains(id.as_str()))
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

    // Step 4: RRF merge.
    let fused: Vec<FusedResult> = match bm25_ranked {
        Some(bm25) if !bm25.is_empty() => {
            // Three-source fusion.
            let (kv, kt, kg) = rrf_k_triple.unwrap_or((rrf_k.0, rrf_k.1, rrf_k.0));
            reciprocal_rank_fusion_weighted(
                &[vector_ranked, bm25, expansion_ranked],
                &[kv, kt, kg],
                final_top_k,
            )
        }
        _ => {
            // Two-source fusion: vector + graph.
            if expansion_ranked.is_empty() {
                reciprocal_rank_fusion_weighted(&[vector_ranked], &[rrf_k.0], final_top_k)
            } else {
                reciprocal_rank_fusion_weighted(
                    &[vector_ranked, expansion_ranked],
                    &[rrf_k.0, rrf_k.1],
                    final_top_k,
                )
            }
        }
    };

    let rows: Vec<Vec<Value>> = fused
        .into_iter()
        .map(|f| vec![Value::String(f.document_id), Value::Float(f.rrf_score)])
        .collect();

    Ok(QueryResult {
        columns: vec!["surrogate".to_string(), "score".to_string()],
        rows,
        rows_affected: 0,
    })
}

/// Pure vector top-k path — no graph expansion, no BM25.
async fn pure_vector_path<S: StorageEngine>(
    vector_state: &Arc<VectorState<S>>,
    crdt: &Arc<Mutex<CrdtEngine>>,
    collection: &str,
    vector_field: &str,
    query_vector: &[f32],
    final_top_k: usize,
) -> Result<QueryResult, LiteError> {
    let index_key = if vector_field.is_empty() {
        collection.to_string()
    } else {
        format!("{collection}:{vector_field}")
    };

    let results = run_vector_search(
        vector_state,
        crdt,
        &index_key,
        collection,
        query_vector,
        final_top_k,
        None,
        &[],
        None,
        None,
        true,
        None,
        None,
    )
    .await
    .map_err(|e| LiteError::Query(e.to_string()))?;

    let rows: Vec<Vec<Value>> = results
        .into_iter()
        .map(|r| vec![Value::String(r.id), Value::Float(r.distance as f64)])
        .collect();

    Ok(QueryResult {
        columns: vec!["surrogate".to_string(), "score".to_string()],
        rows,
        rows_affected: 0,
    })
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use nodedb_graph::Direction;

    use crate::engine::array::ArrayEngineState;
    use crate::engine::columnar::ColumnarEngine;
    use crate::engine::crdt::CrdtEngine;
    use crate::engine::fts::FtsState;
    use crate::engine::htap::HtapBridge;
    use crate::engine::strict::StrictEngine;
    use crate::engine::vector::VectorState;
    use crate::query::engine::LiteQueryEngine;
    use crate::storage::redb_storage::RedbStorage;

    fn make_engine() -> LiteQueryEngine<RedbStorage> {
        let storage = Arc::new(RedbStorage::open_in_memory().expect("in-memory redb"));
        let crdt = Arc::new(Mutex::new(CrdtEngine::new(1).expect("CrdtEngine init")));
        let strict = Arc::new(StrictEngine::new(Arc::clone(&storage)));
        let columnar = Arc::new(ColumnarEngine::new(Arc::clone(&storage)));
        let htap = Arc::new(HtapBridge::new());
        let timeseries = Arc::new(Mutex::new(
            crate::engine::timeseries::engine::TimeseriesEngine::new(),
        ));
        let vector_state = Arc::new(VectorState::new(Arc::clone(&storage), 100));
        let array_state = Arc::new(Mutex::new(
            ArrayEngineState::open(&storage).expect("ArrayEngineState::open"),
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
            spatial,
            Arc::new(Mutex::new(std::collections::HashMap::new())),
        )
    }

    /// Pure vector degenerate path: expansion_depth=0, no BM25.
    /// With an empty HNSW index the result set is empty — no panic.
    #[tokio::test]
    async fn rag_fusion_pure_vector_empty_index() {
        let engine = make_engine();
        let result = super::rag_fusion(
            &engine.vector_state,
            &engine.crdt,
            &engine.fts_state,
            &engine.csr,
            "col",
            &[1.0_f32, 0.0, 0.0, 0.0],
            "",
            5,
            None,
            Direction::Out,
            0,
            5,
            (60.0, 60.0),
            None,
            None,
            None,
        )
        .await
        .expect("pure vector path must not error on empty index");
        assert!(result.rows.is_empty());
        assert_eq!(result.columns[0], "surrogate");
    }

    /// Two-source fusion (vector + graph): empty graph gives vector-only result.
    #[tokio::test]
    async fn rag_fusion_two_source_empty_graph() {
        let engine = make_engine();
        let result = super::rag_fusion(
            &engine.vector_state,
            &engine.crdt,
            &engine.fts_state,
            &engine.csr,
            "col",
            &[1.0_f32, 0.0, 0.0, 0.0],
            "",
            5,
            Some("KNOWS"),
            Direction::Out,
            2,
            5,
            (60.0, 60.0),
            None,
            None,
            None,
        )
        .await
        .expect("two-source path must not error");
        // No vectors inserted — empty result.
        assert!(result.rows.is_empty());
    }

    /// Three-source fusion: bm25_query set, returns columns correctly.
    #[tokio::test]
    async fn rag_fusion_three_source_returns_correct_columns() {
        let engine = make_engine();
        let result = super::rag_fusion(
            &engine.vector_state,
            &engine.crdt,
            &engine.fts_state,
            &engine.csr,
            "col",
            &[1.0_f32, 0.0, 0.0, 0.0],
            "",
            5,
            Some("KNOWS"),
            Direction::Out,
            2,
            5,
            (60.0, 60.0),
            Some((60.0, 60.0, 60.0)),
            Some("what is retrieval"),
            Some("content"),
        )
        .await
        .expect("three-source path must not error");
        assert_eq!(result.columns[0], "surrogate");
        assert_eq!(result.columns[1], "score");
    }

    /// RRF scoring logic: rank-0 score > rank-1 score for default k=60.
    #[test]
    fn rrf_k60_rank0_beats_rank1() {
        let k = 60.0_f64;
        let rank0 = 1.0 / (k + 0.0 + 1.0);
        let rank1 = 1.0 / (k + 1.0 + 1.0);
        assert!(rank0 > rank1, "rank-0 must beat rank-1 in RRF");
    }
}
