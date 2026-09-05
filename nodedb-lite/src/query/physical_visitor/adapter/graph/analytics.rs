// SPDX-License-Identifier: Apache-2.0
//! Algorithm, label, temporal, and stats `GraphOp` arms.

use std::sync::Arc;

use nodedb_graph::{AlgoParams, Direction, GraphAlgorithm};
use nodedb_mem::{EngineId, ScopedMemory};
use nodedb_types::{DatabaseId, SystemTimeScope, TenantId};

use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::query::graph_ops::{algorithms, labels, stats, temporal};
use crate::storage::engine::StorageEngine;

use super::super::graph_resolve::resolve_collection_for_nodes;
use super::dispatch::GraphFut;

pub(super) fn algo<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    algorithm: GraphAlgorithm,
    params: &AlgoParams,
) -> GraphFut<'a> {
    let csr_map = engine.csr.clone();
    let params = params.clone();
    Box::pin(async move { algorithms::run_algo(&csr_map, algorithm, &params) })
}

pub(super) fn set_node_labels<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    node_id: &str,
    labels: &[String],
) -> GraphFut<'a> {
    let csr_map = engine.csr.clone();
    let memory = ScopedMemory::new(
        Arc::clone(&engine.governor),
        DatabaseId::DEFAULT,
        TenantId::new(0),
        EngineId::Graph,
    );
    let node_id = node_id.to_owned();
    let labels = labels.to_vec();
    Box::pin(async move {
        // SetNodeLabels carries no collection field; resolve via node presence.
        let collection = resolve_collection_for_nodes(&csr_map, std::slice::from_ref(&node_id));
        labels::set_node_labels(&csr_map, &memory, &collection, &node_id, &labels)
    })
}

pub(super) fn remove_node_labels<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    node_id: &str,
    labels: &[String],
) -> GraphFut<'a> {
    let csr_map = engine.csr.clone();
    let node_id = node_id.to_owned();
    let labels = labels.to_vec();
    Box::pin(async move {
        let collection = resolve_collection_for_nodes(&csr_map, std::slice::from_ref(&node_id));
        labels::remove_node_labels(&csr_map, &collection, &node_id, &labels)
    })
}

/// Grouped tail fields of `GraphOp::TemporalNeighbors`, kept out of the
/// function signature so it stays under clippy's argument-count lint.
pub(super) struct TemporalNeighborsArgs<'a> {
    pub node_id: &'a str,
    pub edge_label: Option<&'a str>,
    pub direction: Direction,
    pub system_time: &'a SystemTimeScope,
    pub valid_at_ms: Option<i64>,
}

pub(super) fn temporal_neighbors<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    collection: &str,
    args: TemporalNeighborsArgs<'_>,
) -> Result<GraphFut<'a>, LiteError> {
    // Mirror Origin: AllVersions is not supported on the graph engine.
    if args.system_time.is_all_versions() {
        return Err(LiteError::Unsupported {
            detail: "AS OF SYSTEM TIME NULL (all-versions) is not supported on \
                     the graph engine in Lite"
                .into(),
        });
    }
    let storage = engine.storage.clone();
    let csr_map = engine.csr.clone();
    let memory = ScopedMemory::new(
        Arc::clone(&engine.governor),
        DatabaseId::DEFAULT,
        TenantId::new(0),
        EngineId::Graph,
    );
    let collection = collection.to_owned();
    let node_id = args.node_id.to_owned();
    let edge_label = args.edge_label.map(str::to_owned);
    let direction = args.direction;
    // Only an explicit `AS OF SYSTEM TIME <ts>` narrows the read; every
    // other scope (`Current`, and the all-versions case already rejected
    // above) means "no system-time filter" → read the latest version.
    let system_as_of_ms: Option<i64> = match args.system_time {
        SystemTimeScope::AsOf(ms) => Some(*ms),
        _ => None,
    };
    let valid_at_ms = args.valid_at_ms;
    Ok(Box::pin(async move {
        temporal::temporal_neighbors(
            &storage,
            &csr_map,
            &memory,
            temporal::TemporalNeighborsParams {
                collection: collection.as_str(),
                node_id: &node_id,
                edge_label: edge_label.as_deref(),
                direction,
                system_as_of_ms,
                valid_at_ms,
            },
        )
        .await
    }))
}

pub(super) fn temporal_algorithm<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    algorithm: GraphAlgorithm,
    params: &AlgoParams,
    system_time: &SystemTimeScope,
) -> Result<GraphFut<'a>, LiteError> {
    // Mirror Origin: AllVersions is not supported on the graph engine.
    if system_time.is_all_versions() {
        return Err(LiteError::Unsupported {
            detail: "AS OF SYSTEM TIME NULL (all-versions) is not supported on \
                     the graph engine in Lite"
                .into(),
        });
    }
    let storage = engine.storage.clone();
    let csr_map = engine.csr.clone();
    let memory = ScopedMemory::new(
        Arc::clone(&engine.governor),
        DatabaseId::DEFAULT,
        TenantId::new(0),
        EngineId::Graph,
    );
    let params = params.clone();
    // Only an explicit `AS OF SYSTEM TIME <ts>` narrows the read; every
    // other scope (`Current`, and the all-versions case already rejected
    // above) means "no system-time filter" → read the latest version.
    let system_as_of_ms: Option<i64> = match system_time {
        SystemTimeScope::AsOf(ms) => Some(*ms),
        _ => None,
    };
    Ok(Box::pin(async move {
        temporal::temporal_algorithm(
            &storage,
            &csr_map,
            &memory,
            algorithm,
            &params,
            system_as_of_ms,
        )
        .await
    }))
}

pub(super) fn graph_stats<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    collection: Option<&str>,
    as_of: Option<i64>,
) -> GraphFut<'a> {
    let storage = engine.storage.clone();
    let csr_map = engine.csr.clone();
    let memory = ScopedMemory::new(
        Arc::clone(&engine.governor),
        DatabaseId::DEFAULT,
        TenantId::new(0),
        EngineId::Graph,
    );
    let collection = collection.map(str::to_owned);
    Box::pin(async move {
        stats::graph_stats(&storage, &csr_map, &memory, collection.as_deref(), as_of).await
    })
}
