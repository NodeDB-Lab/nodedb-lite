// SPDX-License-Identifier: Apache-2.0

use super::{GraphRagParams, fusion::search_results_to_ranked};
use crate::engine::graph::{index::Direction, traversal::DEFAULT_MAX_VISITED};
use crate::{
    nodedb::{LockExt, NodeDbLite},
    storage::engine::StorageEngine,
};
use nodedb_client::NodeDb;
use nodedb_types::{
    error::{NodeDbError, NodeDbResult},
    id::NodeId,
    result::SearchResult,
};
use std::collections::{HashMap, HashSet, VecDeque};

fn limit_error(collection: &str) -> NodeDbError {
    NodeDbError::program_limit_exceeded(format!(
        "graph retrieval exceeds {DEFAULT_MAX_VISITED} visited nodes in '{collection}': reduce seeds or graph_depth"
    ))
}

impl<S: StorageEngine> NodeDbLite<S> {
    /// Fuse vector retrieval with bounded, outgoing graph expansion.
    pub async fn graph_rag_search(
        &self,
        params: &GraphRagParams<'_>,
    ) -> NodeDbResult<Vec<SearchResult>> {
        if params.top_k == 0 || params.allowed_ids.is_some_and(HashSet::is_empty) {
            return Ok(Vec::new());
        }
        if !params.rrf_k.is_finite() || params.rrf_k < 0.0 {
            return Err(NodeDbError::bad_request(format!(
                "invalid rrf_k '{}': use a finite nonnegative value",
                params.rrf_k
            )));
        }
        let vector_results = if params.vector_k == 0 {
            Vec::new()
        } else {
            self.vector_search(
                params.collection,
                params.query,
                params.vector_k,
                params.filter,
                params.allowed_ids,
            )
            .await?
        };
        let mut unique_seeds = HashSet::new();
        let mut caller_seeds = HashSet::new();
        for (id, caller) in vector_results.iter().map(|r| (r.id.as_str(), false)).chain(
            params
                .seed_nodes
                .into_iter()
                .flatten()
                .map(|id| (id.as_str(), true)),
        ) {
            if params
                .allowed_ids
                .is_some_and(|allowed| !allowed.contains(id))
            {
                continue;
            }
            if !unique_seeds.contains(id) {
                if unique_seeds.len() == DEFAULT_MAX_VISITED {
                    return Err(limit_error(params.collection));
                }
                unique_seeds.insert(id);
            }
            if caller {
                caller_seeds.insert(id);
            }
        }
        let mut seeds: Vec<String> = unique_seeds.into_iter().map(str::to_owned).collect();
        seeds.sort_unstable();
        let graph_ranked = self
            .expand_rag_nodes(params, seeds)?
            .into_iter()
            .filter(|(id, depth)| *depth > 0 || caller_seeds.contains(id.as_str()))
            .enumerate()
            .map(|(rank, (id, depth))| nodedb_query::fusion::RankedResult {
                document_id: id,
                rank,
                score: depth as f32,
                source: "graph",
            })
            .collect();
        let fused = nodedb_query::fusion::reciprocal_rank_fusion_weighted(
            &[
                search_results_to_ranked(&vector_results, "vector"),
                graph_ranked,
            ],
            &[params.rrf_k, params.rrf_k],
            params.top_k,
        );
        let vector_map: HashMap<&str, &SearchResult> =
            vector_results.iter().map(|r| (r.id.as_str(), r)).collect();
        let crdt = self.crdt.lock_or_recover();
        Ok(fused
            .into_iter()
            .map(|f| {
                let (distance, metadata) = match vector_map.get(f.document_id.as_str()) {
                    Some(vr) => (vr.distance, vr.metadata.clone()),
                    None => {
                        let metadata = crdt
                            .read(params.collection, &f.document_id)
                            .map(|val| {
                                crate::nodedb::convert::loro_value_to_document(&f.document_id, &val)
                                    .fields
                            })
                            .unwrap_or_default();
                        (f.rrf_score as f32, metadata)
                    }
                };
                SearchResult {
                    node_id: Some(NodeId::from_validated(f.document_id.clone())),
                    id: f.document_id,
                    distance,
                    metadata,
                }
            })
            .collect())
    }

    fn expand_rag_nodes(
        &self,
        params: &GraphRagParams<'_>,
        seeds: Vec<String>,
    ) -> NodeDbResult<Vec<(String, u8)>> {
        let mut seen: HashSet<String> = seeds.iter().cloned().collect();
        let mut queue: VecDeque<(String, u8)> = seeds.into_iter().map(|id| (id, 0)).collect();
        let mut nodes = Vec::new();
        let csr_map = self.csr.lock_or_recover();
        let csr = csr_map.get(params.collection);
        while let Some((id, depth)) = queue.pop_front() {
            if depth < params.graph_depth
                && let Some(csr) = csr
            {
                let mut neighbors = csr.neighbors(&id, &[], Direction::Out);
                neighbors.sort_unstable_by(|a, b| a.1.cmp(&b.1));
                for (_, neighbor) in neighbors {
                    if params
                        .allowed_ids
                        .is_some_and(|allowed| !allowed.contains(&neighbor))
                        || seen.contains(&neighbor)
                    {
                        continue;
                    }
                    if seen.len() == DEFAULT_MAX_VISITED {
                        return Err(limit_error(params.collection));
                    }
                    seen.insert(neighbor.clone());
                    queue.push_back((neighbor, depth + 1));
                }
            }
            nodes.push((id, depth));
        }
        nodes.sort_unstable_by(|a, b| a.1.cmp(&b.1).then_with(|| a.0.cmp(&b.0)));
        Ok(nodes)
    }
}
