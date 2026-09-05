// SPDX-License-Identifier: Apache-2.0
//! GraphRAG fusion and MATCH-pattern `GraphOp` arms.

use std::sync::Arc;

use nodedb_graph::Direction;

use crate::query::engine::LiteQueryEngine;
use crate::query::graph_ops::{fusion, match_engine};
use crate::storage::engine::StorageEngine;

use super::dispatch::GraphFut;

/// Grouped tail fields of `GraphOp::RagFusion`, kept out of the function
/// signature so it stays under clippy's argument-count lint.
pub(super) struct RagFusionArgs<'a> {
    pub query_vector: &'a [f32],
    pub vector_top_k: usize,
    pub edge_label: Option<&'a str>,
    pub direction: Direction,
    pub expansion_depth: usize,
    pub final_top_k: usize,
    pub rrf_k: (f64, f64),
    pub rrf_k_triple: Option<(f64, f64, f64)>,
    pub vector_field: &'a str,
    pub bm25_query: Option<&'a str>,
    pub bm25_field: Option<&'a str>,
}

pub(super) fn rag_fusion<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    collection: &str,
    args: RagFusionArgs<'_>,
) -> GraphFut<'a> {
    let vector_state = Arc::clone(&engine.vector_state);
    let crdt = Arc::clone(&engine.crdt);
    let fts_state = Arc::clone(&engine.fts_state);
    let csr_map = Arc::clone(&engine.csr);
    let collection = collection.to_owned();
    let query_vector = args.query_vector.to_vec();
    let vector_top_k = args.vector_top_k;
    let edge_label = args.edge_label.map(str::to_owned);
    let direction = args.direction;
    let expansion_depth = args.expansion_depth;
    let final_top_k = args.final_top_k;
    let rrf_k = args.rrf_k;
    let rrf_k_triple = args.rrf_k_triple;
    let vector_field = args.vector_field.to_owned();
    let bm25_query = args.bm25_query.map(str::to_owned);
    let bm25_field = args.bm25_field.map(str::to_owned);
    Box::pin(async move {
        fusion::rag_fusion(
            &vector_state,
            &crdt,
            &fts_state,
            &csr_map,
            collection.as_str(),
            &query_vector,
            &vector_field,
            vector_top_k,
            edge_label.as_deref(),
            direction,
            expansion_depth,
            final_top_k,
            rrf_k,
            rrf_k_triple,
            bm25_query.as_deref(),
            bm25_field.as_deref(),
        )
        .await
    })
}

pub(super) fn graph_match<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    query: &[u8],
    frontier_bitmap: Option<&nodedb_types::SurrogateBitmap>,
) -> GraphFut<'a> {
    let csr_map = Arc::clone(&engine.csr);
    let crdt = Arc::clone(&engine.crdt);
    let query = query.to_vec();
    let frontier_bitmap = frontier_bitmap.cloned();
    Box::pin(async move {
        match_engine::graph_match(&csr_map, &query, frontier_bitmap.as_ref(), Some(&crdt)).await
    })
}
