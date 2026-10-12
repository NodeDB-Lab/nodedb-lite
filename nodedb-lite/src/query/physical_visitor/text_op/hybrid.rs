// SPDX-License-Identifier: Apache-2.0

//! Hybrid reads: vector + BM25 text fused via RRF, with an optional graph
//! BFS leg.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use nodedb_graph::Direction;
use nodedb_graph::traversal::DEFAULT_MAX_VISITED;
use nodedb_query::fusion::{FusedResult, RankedResult, reciprocal_rank_fusion_weighted};
use nodedb_types::result::{QueryResult, SearchResult};
use nodedb_types::text_search::{QueryMode, TextSearchParams};
use nodedb_types::value::Value;

use crate::engine::crdt::CrdtEngine;
use crate::engine::fts::state::FtsState;
use crate::engine::fts::{TextSearchRequest, run_text_search};
use crate::engine::vector::search::{
    AnnTuning, SearchFilter, VectorSearchRequest, run_vector_search,
};
use crate::engine::vector::state::VectorState;
use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::storage::engine::StorageEngine;

use super::super::adapter::LitePhysicalFut;
use super::rows::{TextRowGate, admitted_ids, is_eligible};

/// `TextOp::HybridSearch` inputs.
pub(super) struct HybridArgs<'o> {
    pub collection: &'o str,
    /// Vector column the vector leg searches. Empty searches the
    /// collection's default vector index.
    pub vector_field: &'o str,
    /// Field index the text leg reads. Empty reads the whole-document index.
    pub text_field: &'o str,
    pub query_vector: &'o [f32],
    pub query_text: &'o str,
    pub filters: &'o [u8],
    pub top_k: usize,
    pub mode: QueryMode,
    pub fuzzy: bool,
    pub vector_weight: f32,
    pub rls_filters: &'o [u8],
    pub score_alias: Option<&'o str>,
}

/// `TextOp::HybridSearchTriple` inputs.
pub(super) struct HybridTripleArgs<'o> {
    pub collection: &'o str,
    /// Vector column the vector leg searches. Empty searches the
    /// collection's default vector index.
    pub vector_field: &'o str,
    /// Field index the text leg reads. Empty reads the whole-document index.
    pub text_field: &'o str,
    pub query_vector: &'o [f32],
    pub query_text: &'o str,
    pub filters: &'o [u8],
    pub graph_seed_id: &'o str,
    pub graph_depth: usize,
    pub graph_edge_label: Option<&'o str>,
    pub top_k: usize,
    pub mode: QueryMode,
    pub fuzzy: bool,
    pub rrf_k: (f64, f64, f64),
    pub rls_filters: &'o [u8],
    pub score_alias: Option<&'o str>,
}

/// The text and vector legs shared by both hybrid reads.
struct Legs {
    collection: String,
    vector_field: String,
    text_field: String,
    query_vector: Vec<f32>,
    query_text: String,
    /// Hits each leg fetches before fusion.
    leg_k: usize,
    params: TextSearchParams,
}

/// The vector index key of `field` in `collection`: the bare collection for
/// the default index, else `"{collection}:{field}"`.
fn vector_index_key(collection: &str, field: &str) -> String {
    if field.is_empty() {
        collection.to_owned()
    } else {
        format!("{collection}:{field}")
    }
}

impl Legs {
    /// Run the text leg and the vector leg. Each leg keeps only the rows in
    /// `admitted`. `None` admits every row.
    async fn run<S: StorageEngine>(
        &self,
        fts_state: &Arc<FtsState>,
        vector_state: &Arc<VectorState<S>>,
        crdt: &Arc<Mutex<CrdtEngine>>,
        admitted: Option<&HashSet<String>>,
    ) -> Result<(Vec<SearchResult>, Vec<SearchResult>), LiteError> {
        let text_results = run_text_search(
            fts_state,
            crdt,
            TextSearchRequest {
                collection: &self.collection,
                field: &self.text_field,
                query: &self.query_text,
                top_k: self.leg_k,
                params: &self.params,
                allowed_ids: admitted,
            },
        )?;
        let mut vector_results = run_vector_search(
            vector_state,
            crdt,
            VectorSearchRequest {
                index_key: &vector_index_key(&self.collection, &self.vector_field),
                collection: &self.collection,
                query: &self.query_vector,
                k: self.leg_k,
                filter: SearchFilter::default(),
                tuning: AnnTuning::default(),
                skip_payload_fetch: false,
            },
        )
        .await
        .map_err(|e| LiteError::Query(e.to_string()))?;
        vector_results.retain(|r| is_eligible(admitted, &r.id));
        Ok((text_results, vector_results))
    }
}

/// Rank search results in their returned order under `source`.
fn ranked(results: &[SearchResult], source: &'static str) -> Vec<RankedResult> {
    results
        .iter()
        .enumerate()
        .map(|(rank, r)| RankedResult {
            document_id: r.id.clone(),
            rank,
            score: 1.0 - r.distance,
            source,
        })
        .collect()
}

/// `id` + fused-score rows.
fn fused_result(score_alias: Option<String>, fused: Vec<FusedResult>) -> QueryResult {
    QueryResult {
        columns: vec![
            "id".to_string(),
            score_alias.unwrap_or_else(|| "rrf_score".to_string()),
        ],
        rows: fused
            .into_iter()
            .map(|f| vec![Value::String(f.document_id), Value::Float(f.rrf_score)])
            .collect(),
        rows_affected: 0,
        command: None,
    }
}

/// `TextOp::HybridSearch`: vector and text legs fused with weighted RRF.
/// The filters restrict both legs before fusion.
pub(super) fn hybrid_search<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    args: HybridArgs<'_>,
) -> Result<LitePhysicalFut<'a>, LiteError> {
    let gate = TextRowGate::new(args.filters, args.rls_filters)?;
    let legs = Legs {
        collection: args.collection.to_owned(),
        vector_field: args.vector_field.to_owned(),
        text_field: args.text_field.to_owned(),
        query_vector: args.query_vector.to_vec(),
        query_text: args.query_text.to_owned(),
        leg_k: args.top_k.saturating_mul(3),
        params: TextSearchParams {
            mode: args.mode,
            fuzzy: args.fuzzy,
        },
    };
    let top_k = args.top_k;
    let vector_weight = args.vector_weight;
    let score_alias = args.score_alias.map(str::to_owned);
    let fts_state = Arc::clone(&engine.fts_state);
    let crdt = Arc::clone(&engine.crdt);
    let vector_state = Arc::clone(&engine.vector_state);
    Ok(Box::pin(async move {
        let admitted = admitted_ids(&crdt, &legs.collection, &gate)?;
        let (text_results, vector_results) = legs
            .run(&fts_state, &vector_state, &crdt, admitted.as_ref())
            .await?;

        let text_k = 60.0 * (1.0 - vector_weight as f64);
        let vector_k = 60.0 * vector_weight as f64;
        let fused = reciprocal_rank_fusion_weighted(
            &[
                ranked(&vector_results, "vector"),
                ranked(&text_results, "text"),
            ],
            &[vector_k, text_k],
            top_k,
        );
        Ok(fused_result(score_alias, fused))
    }))
}

/// `TextOp::HybridSearchTriple`: vector, text, and graph BFS legs fused with
/// per-source RRF constants. The filters restrict every leg before fusion:
/// the graph walk reaches rows regardless of them, and only admitted rows
/// enter its ranked list.
pub(super) fn hybrid_search_triple<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    args: HybridTripleArgs<'_>,
) -> Result<LitePhysicalFut<'a>, LiteError> {
    let gate = TextRowGate::new(args.filters, args.rls_filters)?;
    let legs = Legs {
        collection: args.collection.to_owned(),
        vector_field: args.vector_field.to_owned(),
        text_field: args.text_field.to_owned(),
        query_vector: args.query_vector.to_vec(),
        query_text: args.query_text.to_owned(),
        leg_k: args.top_k.saturating_mul(3),
        params: TextSearchParams {
            mode: args.mode,
            fuzzy: args.fuzzy,
        },
    };
    let graph_seed_id = args.graph_seed_id.to_owned();
    let graph_depth = args.graph_depth;
    let graph_edge_label = args.graph_edge_label.map(str::to_owned);
    let top_k = args.top_k;
    let (kv, kt, kg) = args.rrf_k;
    let score_alias = args.score_alias.map(str::to_owned);
    let fts_state = Arc::clone(&engine.fts_state);
    let crdt = Arc::clone(&engine.crdt);
    let vector_state = Arc::clone(&engine.vector_state);
    let csr = Arc::clone(&engine.csr);
    Ok(Box::pin(async move {
        let admitted = admitted_ids(&crdt, &legs.collection, &gate)?;
        // Legs 1 and 2: text search and vector search.
        let (text_results, vector_results) = legs
            .run(&fts_state, &vector_state, &crdt, admitted.as_ref())
            .await?;

        // Leg 3: graph BFS from seed node.
        let graph_ranked: Vec<RankedResult> = if graph_depth > 0 {
            let csr_guard = csr.lock().map_err(|_| LiteError::LockPoisoned)?;
            match csr_guard.get(legs.collection.as_str()) {
                Some(csr_idx) => {
                    let max_vis = graph_depth
                        .saturating_mul(legs.leg_k)
                        .max(DEFAULT_MAX_VISITED);
                    let label = graph_edge_label.as_deref();
                    csr_idx
                        .traverse_bfs(
                            nodedb_graph::BfsParams {
                                start_nodes: &[graph_seed_id.as_str()],
                                label_filter: label.as_slice(),
                                direction: Direction::Out,
                                max_depth: graph_depth,
                                max_visited: max_vis,
                                frontier_bitmap: None,
                            },
                            None,
                        )
                        .into_iter()
                        .filter(|id| is_eligible(admitted.as_ref(), id))
                        .enumerate()
                        .map(|(rank, id)| RankedResult {
                            document_id: id,
                            rank,
                            score: 0.0,
                            source: "graph",
                        })
                        .collect()
                }
                None => Vec::new(),
            }
        } else {
            Vec::new()
        };

        let fused = reciprocal_rank_fusion_weighted(
            &[
                ranked(&vector_results, "vector"),
                ranked(&text_results, "text"),
                graph_ranked,
            ],
            &[kv, kt, kg],
            top_k,
        );
        Ok(fused_result(score_alias, fused))
    }))
}
