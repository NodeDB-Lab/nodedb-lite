// SPDX-License-Identifier: Apache-2.0
//! Read-only traversal `GraphOp` arms: hop, neighbors, path, subgraph.

use nodedb_graph::{Direction, GraphTraversalOptions};
use nodedb_types::filter::MetadataFilter;
use nodedb_types::{RlsWriteCheck, SurrogateBitmap};

use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::query::graph_ops::traversal;
use crate::storage::engine::StorageEngine;

use super::super::graph_resolve::resolve_collection_for_nodes;
use super::super::policy::deny_policy;
use super::dispatch::GraphFut;

/// Grouped tail fields of `GraphOp::Hop`, kept out of the function
/// signature so it stays under clippy's argument-count lint.
pub(super) struct HopArgs<'a> {
    pub edge_labels: &'a [String],
    pub direction: Direction,
    pub depth: usize,
    pub options: &'a GraphTraversalOptions,
    pub frontier_bitmap: Option<&'a SurrogateBitmap>,
    pub rls_filters: &'a [u8],
}

/// Borrow each owned label as `&str`.
pub(super) fn label_refs(labels: &[String]) -> Vec<&str> {
    labels.iter().map(String::as_str).collect()
}

/// Reject the `NeighborsMulti` fields Lite cannot evaluate. Lite stores
/// edge properties outside the CSR index, so neither an edge predicate nor
/// property projection has a local execution path.
pub(super) fn check_neighbors_multi_supported(
    edge_predicate: &[MetadataFilter],
    with_properties: bool,
) -> Result<(), LiteError> {
    if !edge_predicate.is_empty() {
        return Err(LiteError::Unsupported {
            detail: "GraphOp::NeighborsMulti edge_predicate is unsupported on the Lite engine"
                .into(),
        });
    }
    if with_properties {
        return Err(LiteError::Unsupported {
            detail: "GraphOp::NeighborsMulti with_properties is unsupported on the Lite engine"
                .into(),
        });
    }
    Ok(())
}

pub(super) fn hop<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    start_nodes: &[String],
    args: HopArgs<'_>,
) -> Result<GraphFut<'a>, LiteError> {
    // Lite resolves the collection by node presence (see below); the
    // node-visibility filter still cannot be dropped.
    deny_policy(
        "GraphOp::Hop",
        None,
        &[args.rls_filters],
        &RlsWriteCheck::NoPolicyApplies,
    )?;
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
    let start_nodes = start_nodes.to_vec();
    let direction = args.direction;
    let depth = args.depth;
    let edge_labels = args.edge_labels.to_vec();
    let options = args.options.clone();
    let frontier_bitmap = args.frontier_bitmap.cloned();
    Ok(Box::pin(async move {
        // Resolve collection from csr_map.
        let collection = resolve_collection_for_nodes(&csr_map, &start_nodes);
        let labels = label_refs(&edge_labels);
        traversal::hop(
            &csr_map,
            &collection,
            traversal::HopSpec {
                start_nodes: &start_nodes,
                edge_labels: &labels,
                direction,
                depth,
                options: &options,
                frontier_bitmap: frontier_bitmap.as_ref(),
            },
        )
    }))
}

pub(super) fn neighbors<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    node_id: &str,
    edge_labels: &[String],
    direction: Direction,
    rls_filters: &[u8],
) -> Result<GraphFut<'a>, LiteError> {
    deny_policy(
        "GraphOp::Neighbors",
        None,
        &[rls_filters],
        &RlsWriteCheck::NoPolicyApplies,
    )?;
    let csr_map = engine.csr.clone();
    let node_id = node_id.to_owned();
    let edge_labels = edge_labels.to_vec();
    Ok(Box::pin(async move {
        let collection = resolve_collection_for_nodes(&csr_map, std::slice::from_ref(&node_id));
        let labels = label_refs(&edge_labels);
        traversal::neighbors(&csr_map, &collection, &node_id, &labels, direction)
    }))
}

pub(super) fn neighbors_multi<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    node_ids: &[String],
    edge_labels: &[String],
    direction: Direction,
    max_results: u32,
    rls_filters: &[u8],
) -> Result<GraphFut<'a>, LiteError> {
    deny_policy(
        "GraphOp::NeighborsMulti",
        None,
        &[rls_filters],
        &RlsWriteCheck::NoPolicyApplies,
    )?;
    let csr_map = engine.csr.clone();
    let node_ids = node_ids.to_vec();
    let edge_labels = edge_labels.to_vec();
    Ok(Box::pin(async move {
        let collection = resolve_collection_for_nodes(&csr_map, &node_ids);
        let labels = label_refs(&edge_labels);
        traversal::neighbors_multi(
            &csr_map,
            &collection,
            &node_ids,
            &labels,
            direction,
            max_results,
        )
    }))
}

/// Grouped tail fields of `GraphOp::Path`, kept out of the function
/// signature so it stays under clippy's argument-count lint.
pub(super) struct PathArgs<'a> {
    pub edge_labels: &'a [String],
    pub max_depth: usize,
    pub options: &'a GraphTraversalOptions,
    pub frontier_bitmap: Option<&'a SurrogateBitmap>,
    pub rls_filters: &'a [u8],
}

pub(super) fn path<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    src: &str,
    dst: &str,
    args: PathArgs<'_>,
) -> Result<GraphFut<'a>, LiteError> {
    deny_policy(
        "GraphOp::Path",
        None,
        &[args.rls_filters],
        &RlsWriteCheck::NoPolicyApplies,
    )?;
    let csr_map = engine.csr.clone();
    let src = src.to_owned();
    let dst = dst.to_owned();
    let edge_labels = args.edge_labels.to_vec();
    let max_depth = args.max_depth;
    let options = args.options.clone();
    let frontier_bitmap = args.frontier_bitmap.cloned();
    Ok(Box::pin(async move {
        let collection = resolve_collection_for_nodes(&csr_map, &[src.clone(), dst.clone()]);
        let labels = label_refs(&edge_labels);
        traversal::path(
            &csr_map,
            &collection,
            traversal::PathSpec {
                src: &src,
                dst: &dst,
                edge_labels: &labels,
                max_depth,
                options: &options,
                frontier_bitmap: frontier_bitmap.as_ref(),
            },
        )
    }))
}

pub(super) fn subgraph<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    start_nodes: &[String],
    edge_labels: &[String],
    depth: usize,
    options: &GraphTraversalOptions,
    rls_filters: &[u8],
) -> Result<GraphFut<'a>, LiteError> {
    deny_policy(
        "GraphOp::Subgraph",
        None,
        &[rls_filters],
        &RlsWriteCheck::NoPolicyApplies,
    )?;
    let csr_map = engine.csr.clone();
    let start_nodes = start_nodes.to_vec();
    let edge_labels = edge_labels.to_vec();
    let options = options.clone();
    Ok(Box::pin(async move {
        let collection = resolve_collection_for_nodes(&csr_map, &start_nodes);
        let labels = label_refs(&edge_labels);
        traversal::subgraph(
            &csr_map,
            &collection,
            &start_nodes,
            &labels,
            depth,
            &options,
        )
    }))
}
