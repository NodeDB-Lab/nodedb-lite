// SPDX-License-Identifier: Apache-2.0
//! `GraphOp` dispatch entry point for the Lite physical visitor.
//!
//! Exhaustively matches all 22 `GraphOp` variants. `RagFusion` and `Match`
//! are wired to their writer-2 placeholder stubs. `MatchContinuation`,
//! `MatchVarLenResume`, `BspSuperstep`, and `WccSuperstep` are cross-shard
//! distributed primitives with no single-node equivalent and return
//! `LiteError::Unsupported`.

use std::future::Future;
use std::pin::Pin;

use nodedb_physical::physical_plan::GraphOp;
use nodedb_types::result::QueryResult;

use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::storage::engine::StorageEngine;

use super::{analytics, edges, fusion_match, traversal, unsupported};

#[cfg(not(target_arch = "wasm32"))]
pub(in super::super) type GraphFut<'a> =
    Pin<Box<dyn Future<Output = Result<QueryResult, LiteError>> + Send + 'a>>;

#[cfg(target_arch = "wasm32")]
pub(in super::super) type GraphFut<'a> =
    Pin<Box<dyn Future<Output = Result<QueryResult, LiteError>> + 'a>>;

/// Dispatch a `GraphOp` to the correct Lite handler.
pub(crate) fn dispatch<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    op: &GraphOp,
) -> Result<GraphFut<'a>, LiteError> {
    let fut: GraphFut<'a> = match op {
        GraphOp::EdgePut {
            collection,
            src_id,
            label,
            dst_id,
            properties,
            ..
        } => edges::edge_put(engine, collection, src_id, label, dst_id, properties),

        GraphOp::EdgePutBatch { edges: batch_edges } => edges::edge_put_batch(engine, batch_edges),

        GraphOp::EdgeDelete {
            collection,
            src_id,
            label,
            dst_id,
            rls_write_check,
            ..
        } => edges::edge_delete(engine, collection, src_id, label, dst_id, rls_write_check)?,

        GraphOp::ResolveEdgeDelete(_) => edges::resolve_edge_delete(),

        GraphOp::EdgeDeleteBatch { edges: batch_edges } => {
            edges::edge_delete_batch(engine, batch_edges)
        }

        GraphOp::Hop {
            start_nodes,
            edge_label,
            direction,
            depth,
            options,
            frontier_bitmap,
            rls_filters,
            ..
        } => traversal::hop(
            engine,
            start_nodes,
            traversal::HopArgs {
                edge_label: edge_label.as_deref(),
                direction: *direction,
                depth: *depth,
                options,
                frontier_bitmap: frontier_bitmap.as_ref(),
                rls_filters,
            },
        )?,

        GraphOp::Neighbors {
            node_id,
            edge_label,
            direction,
            rls_filters,
            ..
        } => traversal::neighbors(
            engine,
            node_id,
            edge_label.as_deref(),
            *direction,
            rls_filters,
        )?,

        GraphOp::NeighborsMulti {
            node_ids,
            edge_label,
            direction,
            max_results,
            rls_filters,
            ..
        } => traversal::neighbors_multi(
            engine,
            node_ids,
            edge_label.as_deref(),
            *direction,
            *max_results,
            rls_filters,
        )?,

        GraphOp::Path {
            src,
            dst,
            edge_label,
            max_depth,
            options,
            frontier_bitmap,
            rls_filters,
            ..
        } => traversal::path(
            engine,
            src,
            dst,
            traversal::PathArgs {
                edge_label: edge_label.as_deref(),
                max_depth: *max_depth,
                options,
                frontier_bitmap: frontier_bitmap.as_ref(),
                rls_filters,
            },
        )?,

        GraphOp::Subgraph {
            start_nodes,
            edge_label,
            depth,
            options,
            rls_filters,
            ..
        } => traversal::subgraph(
            engine,
            start_nodes,
            edge_label.as_deref(),
            *depth,
            options,
            rls_filters,
        )?,

        GraphOp::Algo { algorithm, params } => analytics::algo(engine, *algorithm, params),

        GraphOp::SetNodeLabels { node_id, labels } => {
            analytics::set_node_labels(engine, node_id, labels)
        }

        GraphOp::RemoveNodeLabels { node_id, labels } => {
            analytics::remove_node_labels(engine, node_id, labels)
        }

        GraphOp::TemporalNeighbors {
            collection,
            node_id,
            edge_label,
            direction,
            system_time,
            valid_at_ms,
            ..
        } => analytics::temporal_neighbors(
            engine,
            collection.as_str(),
            analytics::TemporalNeighborsArgs {
                node_id,
                edge_label: edge_label.as_deref(),
                direction: *direction,
                system_time,
                valid_at_ms: *valid_at_ms,
            },
        )?,

        GraphOp::TemporalAlgorithm {
            algorithm,
            params,
            system_time,
        } => analytics::temporal_algorithm(engine, *algorithm, params, system_time)?,

        GraphOp::Stats { collection, as_of } => {
            analytics::graph_stats(engine, collection.as_ref().map(|c| c.as_str()), *as_of)
        }

        GraphOp::RagFusion {
            collection,
            query_vector,
            vector_top_k,
            edge_label,
            direction,
            expansion_depth,
            final_top_k,
            rrf_k,
            rrf_k_triple,
            vector_field,
            options: _,
            bm25_query,
            bm25_field,
        } => fusion_match::rag_fusion(
            engine,
            collection.as_str(),
            fusion_match::RagFusionArgs {
                query_vector,
                vector_top_k: *vector_top_k,
                edge_label: edge_label.as_deref(),
                direction: *direction,
                expansion_depth: *expansion_depth,
                final_top_k: *final_top_k,
                rrf_k: *rrf_k,
                rrf_k_triple: *rrf_k_triple,
                vector_field,
                bm25_query: bm25_query.as_deref(),
                bm25_field: bm25_field.as_deref(),
            },
        ),

        // Cross-shard MATCH continuation / var-len resume and the BSP
        // superstep primitives (PageRank/WCC) exist to let a distributed
        // coordinator round-trip partial state across owning shards. Lite is
        // single-node — there are no shards to resume on or stitch together
        // — so these have no local execution path.
        GraphOp::MatchContinuation { .. } => {
            Box::pin(async move { Err(unsupported::match_continuation()) })
        }

        GraphOp::MatchVarLenResume { .. } => {
            Box::pin(async move { Err(unsupported::match_var_len_resume()) })
        }

        GraphOp::BspSuperstep(_) => Box::pin(async move { Err(unsupported::bsp_superstep()) }),

        GraphOp::WccSuperstep(_) => Box::pin(async move { Err(unsupported::wcc_superstep()) }),

        GraphOp::Match {
            query,
            frontier_bitmap,
            ..
        } => fusion_match::graph_match(engine, query, frontier_bitmap.as_ref()),
    };

    Ok(fut)
}
