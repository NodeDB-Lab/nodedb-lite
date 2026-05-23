// SPDX-License-Identifier: Apache-2.0

//! Graph operation dispatcher for the Lite physical visitor.
//!
//! Exhaustively matches all 17 `GraphOp` variants. `RagFusion` and `Match`
//! are wired to their writer-2 placeholder stubs.

use std::future::Future;
use std::pin::Pin;

use nodedb_physical::physical_plan::GraphOp;
use nodedb_types::result::QueryResult;

use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::query::graph_ops::{
    algorithms, edges, fusion, labels, match_engine, stats, temporal, traversal,
};
use crate::storage::engine::StorageEngine;

#[cfg(not(target_arch = "wasm32"))]
pub(crate) type GraphFut<'a> =
    Pin<Box<dyn Future<Output = Result<QueryResult, LiteError>> + Send + 'a>>;

#[cfg(target_arch = "wasm32")]
pub(crate) type GraphFut<'a> = Pin<Box<dyn Future<Output = Result<QueryResult, LiteError>> + 'a>>;

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
        } => {
            let storage = engine.storage.clone();
            let csr_map = engine.csr.clone();
            let collection = collection.clone();
            let src_id = src_id.clone();
            let label = label.clone();
            let dst_id = dst_id.clone();
            let properties = properties.clone();
            Box::pin(async move {
                edges::edge_put(
                    &storage,
                    &csr_map,
                    &collection,
                    &src_id,
                    &label,
                    &dst_id,
                    &properties,
                )
                .await
            })
        }

        GraphOp::EdgePutBatch { edges: batch_edges } => {
            let storage = engine.storage.clone();
            let csr_map = engine.csr.clone();
            let batch_edges = batch_edges.clone();
            Box::pin(async move { edges::edge_put_batch(&storage, &csr_map, &batch_edges).await })
        }

        GraphOp::EdgeDelete {
            collection,
            src_id,
            label,
            dst_id,
        } => {
            let storage = engine.storage.clone();
            let csr_map = engine.csr.clone();
            let collection = collection.clone();
            let src_id = src_id.clone();
            let label = label.clone();
            let dst_id = dst_id.clone();
            Box::pin(async move {
                edges::edge_delete(&storage, &csr_map, &collection, &src_id, &label, &dst_id).await
            })
        }

        GraphOp::EdgeDeleteBatch { edges: batch_edges } => {
            let storage = engine.storage.clone();
            let csr_map = engine.csr.clone();
            let batch_edges = batch_edges.clone();
            Box::pin(
                async move { edges::edge_delete_batch(&storage, &csr_map, &batch_edges).await },
            )
        }

        GraphOp::Hop {
            start_nodes,
            edge_label,
            direction,
            depth,
            options,
            frontier_bitmap,
            ..
        } => {
            let csr_map = engine.csr.clone();
            // Hop is scoped to a single collection; collection is implicit in Lite
            // as all edges share the same CSR map keyed by collection. The caller
            // must pass start_nodes that are collection-scoped. We use a default
            // sentinel to indicate "traverse the first collection" — but in practice
            // the collection is embedded in the node keys when the caller is the
            // Origin SQL planner. For Lite, use a special lookup in the first key
            // found in start_nodes against the CSR map.
            //
            // Because `GraphOp::Hop` carries no explicit collection field, Lite
            // resolves the collection by iterating csr_map entries for the first
            // collection that contains any of the start nodes.
            let start_nodes = start_nodes.clone();
            let edge_label = edge_label.clone();
            let direction = *direction;
            let depth = *depth;
            let options = options.clone();
            let frontier_bitmap = frontier_bitmap.clone();
            Box::pin(async move {
                // Resolve collection from csr_map.
                let collection = resolve_collection_for_nodes(&csr_map, &start_nodes);
                traversal::hop(
                    &csr_map,
                    &collection,
                    &start_nodes,
                    edge_label.as_deref(),
                    direction,
                    depth,
                    &options,
                    frontier_bitmap.as_ref(),
                )
            })
        }

        GraphOp::Neighbors {
            node_id,
            edge_label,
            direction,
            ..
        } => {
            let csr_map = engine.csr.clone();
            let node_id = node_id.clone();
            let edge_label = edge_label.clone();
            let direction = *direction;
            Box::pin(async move {
                let collection =
                    resolve_collection_for_nodes(&csr_map, std::slice::from_ref(&node_id));
                traversal::neighbors(
                    &csr_map,
                    &collection,
                    &node_id,
                    edge_label.as_deref(),
                    direction,
                )
            })
        }

        GraphOp::NeighborsMulti {
            node_ids,
            edge_label,
            direction,
            max_results,
            ..
        } => {
            let csr_map = engine.csr.clone();
            let node_ids = node_ids.clone();
            let edge_label = edge_label.clone();
            let direction = *direction;
            let max_results = *max_results;
            Box::pin(async move {
                let collection = resolve_collection_for_nodes(&csr_map, &node_ids);
                traversal::neighbors_multi(
                    &csr_map,
                    &collection,
                    &node_ids,
                    edge_label.as_deref(),
                    direction,
                    max_results,
                )
            })
        }

        GraphOp::Path {
            src,
            dst,
            edge_label,
            max_depth,
            options,
            frontier_bitmap,
            ..
        } => {
            let csr_map = engine.csr.clone();
            let src = src.clone();
            let dst = dst.clone();
            let edge_label = edge_label.clone();
            let max_depth = *max_depth;
            let options = options.clone();
            let frontier_bitmap = frontier_bitmap.clone();
            Box::pin(async move {
                let collection =
                    resolve_collection_for_nodes(&csr_map, &[src.clone(), dst.clone()]);
                traversal::path(
                    &csr_map,
                    &collection,
                    &src,
                    &dst,
                    edge_label.as_deref(),
                    max_depth,
                    &options,
                    frontier_bitmap.as_ref(),
                )
            })
        }

        GraphOp::Subgraph {
            start_nodes,
            edge_label,
            depth,
            options,
            ..
        } => {
            let csr_map = engine.csr.clone();
            let start_nodes = start_nodes.clone();
            let edge_label = edge_label.clone();
            let depth = *depth;
            let options = options.clone();
            Box::pin(async move {
                let collection = resolve_collection_for_nodes(&csr_map, &start_nodes);
                traversal::subgraph(
                    &csr_map,
                    &collection,
                    &start_nodes,
                    edge_label.as_deref(),
                    depth,
                    &options,
                )
            })
        }

        GraphOp::Algo { algorithm, params } => {
            let csr_map = engine.csr.clone();
            let algorithm = *algorithm;
            let params = params.clone();
            Box::pin(async move { algorithms::run_algo(&csr_map, algorithm, &params) })
        }

        GraphOp::SetNodeLabels { node_id, labels } => {
            let csr_map = engine.csr.clone();
            let node_id = node_id.clone();
            let labels = labels.clone();
            Box::pin(async move {
                // SetNodeLabels carries no collection field; resolve via node presence.
                let collection =
                    resolve_collection_for_nodes(&csr_map, std::slice::from_ref(&node_id));
                labels::set_node_labels(&csr_map, &collection, &node_id, &labels)
            })
        }

        GraphOp::RemoveNodeLabels { node_id, labels } => {
            let csr_map = engine.csr.clone();
            let node_id = node_id.clone();
            let labels = labels.clone();
            Box::pin(async move {
                let collection =
                    resolve_collection_for_nodes(&csr_map, std::slice::from_ref(&node_id));
                labels::remove_node_labels(&csr_map, &collection, &node_id, &labels)
            })
        }

        GraphOp::TemporalNeighbors {
            collection,
            node_id,
            edge_label,
            direction,
            system_as_of_ms,
            valid_at_ms,
            ..
        } => {
            let storage = engine.storage.clone();
            let csr_map = engine.csr.clone();
            let collection = collection.clone();
            let node_id = node_id.clone();
            let edge_label = edge_label.clone();
            let direction = *direction;
            let system_as_of_ms = *system_as_of_ms;
            let valid_at_ms = *valid_at_ms;
            Box::pin(async move {
                temporal::temporal_neighbors(
                    &storage,
                    &csr_map,
                    &collection,
                    &node_id,
                    edge_label.as_deref(),
                    direction,
                    system_as_of_ms,
                    valid_at_ms,
                )
                .await
            })
        }

        GraphOp::TemporalAlgorithm {
            algorithm,
            params,
            system_as_of_ms,
        } => {
            let storage = engine.storage.clone();
            let csr_map = engine.csr.clone();
            let algorithm = *algorithm;
            let params = params.clone();
            let system_as_of_ms = *system_as_of_ms;
            Box::pin(async move {
                temporal::temporal_algorithm(
                    &storage,
                    &csr_map,
                    algorithm,
                    &params,
                    system_as_of_ms,
                )
                .await
            })
        }

        GraphOp::Stats { collection, as_of } => {
            let storage = engine.storage.clone();
            let csr_map = engine.csr.clone();
            let collection = collection.clone();
            let as_of = *as_of;
            Box::pin(async move {
                stats::graph_stats(&storage, &csr_map, collection.as_deref(), as_of).await
            })
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
        } => {
            let vector_state = Arc::clone(&engine.vector_state);
            let crdt = Arc::clone(&engine.crdt);
            let fts_state = Arc::clone(&engine.fts_state);
            let csr_map = Arc::clone(&engine.csr);
            let collection = collection.clone();
            let query_vector = query_vector.clone();
            let vector_top_k = *vector_top_k;
            let edge_label = edge_label.clone();
            let direction = *direction;
            let expansion_depth = *expansion_depth;
            let final_top_k = *final_top_k;
            let rrf_k = *rrf_k;
            let rrf_k_triple = *rrf_k_triple;
            let vector_field = vector_field.clone();
            let bm25_query = bm25_query.clone();
            let bm25_field = bm25_field.clone();
            Box::pin(async move {
                fusion::rag_fusion(
                    &vector_state,
                    &crdt,
                    &fts_state,
                    &csr_map,
                    &collection,
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

        GraphOp::Match {
            query,
            frontier_bitmap,
        } => {
            let csr_map = Arc::clone(&engine.csr);
            let crdt = Arc::clone(&engine.crdt);
            let query = query.clone();
            let frontier_bitmap = frontier_bitmap.clone();
            Box::pin(async move {
                match_engine::graph_match(&csr_map, &query, frontier_bitmap.as_ref(), Some(&crdt))
                    .await
            })
        }
    };

    Ok(fut)
}

/// Resolve which collection a set of node IDs belongs to by scanning the CSR map.
///
/// Returns the first collection that contains any of the given nodes, or an
/// empty string when none is found (which will produce an empty result set
/// rather than an error — correct for "no such graph" semantics).
fn resolve_collection_for_nodes(
    csr_map: &Arc<
        std::sync::Mutex<std::collections::HashMap<String, crate::engine::graph::index::CsrIndex>>,
    >,
    node_ids: &[String],
) -> String {
    let Ok(map) = csr_map.lock() else {
        return String::new();
    };
    for (coll, csr) in map.iter() {
        for node in node_ids {
            if csr.contains_node(node) {
                return coll.clone();
            }
        }
    }
    // Fall back to the first collection in the map.
    map.keys().next().cloned().unwrap_or_default()
}

use std::sync::Arc;
