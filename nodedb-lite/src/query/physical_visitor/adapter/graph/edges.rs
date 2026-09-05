// SPDX-License-Identifier: Apache-2.0
//! Edge mutation `GraphOp` arms: put/delete, single and batched.

use std::sync::Arc;

use nodedb_mem::{EngineId, ScopedMemory};
use nodedb_physical::physical_plan::BatchEdge;
use nodedb_types::{DatabaseId, QualifiedCollection, RlsWriteCheck, TenantId};

use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::query::graph_ops::edges::{self, EdgePutArgs};
use crate::storage::engine::StorageEngine;

use super::super::policy::deny_policy;
use super::dispatch::GraphFut;

pub(super) fn edge_put<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    collection: &QualifiedCollection,
    src_id: &str,
    label: &str,
    dst_id: &str,
    properties: &[u8],
) -> GraphFut<'a> {
    let storage = engine.storage.clone();
    let csr_map = engine.csr.clone();
    let memory = ScopedMemory::new(
        Arc::clone(&engine.governor),
        DatabaseId::DEFAULT,
        TenantId::new(0),
        EngineId::Graph,
    );
    let collection = collection.clone();
    let src_id = src_id.to_owned();
    let label = label.to_owned();
    let dst_id = dst_id.to_owned();
    let properties = properties.to_vec();
    Box::pin(async move {
        edges::edge_put(
            &storage,
            &csr_map,
            &memory,
            EdgePutArgs {
                collection: collection.as_str(),
                src_id: &src_id,
                label: &label,
                dst_id: &dst_id,
                properties: &properties,
            },
        )
        .await
    })
}

pub(super) fn edge_put_batch<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    batch_edges: &[BatchEdge],
) -> GraphFut<'a> {
    let storage = engine.storage.clone();
    let csr_map = engine.csr.clone();
    let memory = ScopedMemory::new(
        Arc::clone(&engine.governor),
        DatabaseId::DEFAULT,
        TenantId::new(0),
        EngineId::Graph,
    );
    let batch_edges = batch_edges.to_vec();
    Box::pin(async move { edges::edge_put_batch(&storage, &csr_map, &memory, &batch_edges).await })
}

pub(super) fn edge_delete<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    collection: &QualifiedCollection,
    src_id: &str,
    label: &str,
    dst_id: &str,
    rls_write_check: &RlsWriteCheck,
) -> Result<GraphFut<'a>, LiteError> {
    deny_policy("GraphOp::EdgeDelete", None, &[], rls_write_check)?;
    let storage = engine.storage.clone();
    let csr_map = engine.csr.clone();
    let collection = collection.clone();
    let src_id = src_id.to_owned();
    let label = label.to_owned();
    let dst_id = dst_id.to_owned();
    Ok(Box::pin(async move {
        edges::edge_delete(
            &storage,
            &csr_map,
            collection.as_str(),
            &src_id,
            &label,
            &dst_id,
        )
        .await
    }))
}

/// Resolve pass for a governed `EdgeDelete`: lets a follower decide the
/// write policy against the edge's live property object instead of
/// re-judging a predicate it cannot evaluate. Lite has no follower to
/// resolve for — it decides and applies `EdgeDelete` in one step.
pub(super) fn resolve_edge_delete<'a>() -> GraphFut<'a> {
    Box::pin(async move {
        Err(LiteError::Unsupported {
            detail: "GraphOp::ResolveEdgeDelete is the resolve pass of a governed \
                     edge delete Origin replays across replicas; Lite's single-node \
                     engine decides and applies EdgeDelete directly"
                .into(),
        })
    })
}

pub(super) fn edge_delete_batch<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    batch_edges: &[BatchEdge],
) -> GraphFut<'a> {
    let storage = engine.storage.clone();
    let csr_map = engine.csr.clone();
    let batch_edges = batch_edges.to_vec();
    Box::pin(async move { edges::edge_delete_batch(&storage, &csr_map, &batch_edges).await })
}
