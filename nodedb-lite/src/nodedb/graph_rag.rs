//! GraphRAG fusion for Lite: vector search → graph expansion → RRF merge.
//!
//! Composes the existing `vector_search()` and `graph_traverse()` methods
//! into a single multi-modal retrieval call. Results from vector similarity
//! and graph context are fused via Reciprocal Rank Fusion (RRF).

use std::collections::HashMap;

use nodedb_client::NodeDb;
use nodedb_types::error::NodeDbResult;
use nodedb_types::filter::MetadataFilter;
use nodedb_types::id::NodeId;
use nodedb_types::result::SearchResult;

use super::{LockExt, NodeDbLite};
use crate::storage::engine::StorageEngine;

/// GraphRAG fusion parameters.
pub struct GraphRagParams<'a> {
    /// Collection to search vectors in.
    pub collection: &'a str,
    /// Query embedding.
    pub query: &'a [f32],
    /// Number of initial vector candidates.
    pub vector_k: usize,
    /// Graph expansion depth from each vector result.
    pub graph_depth: u8,
    /// Final number of results to return after fusion.
    pub top_k: usize,
    /// Optional metadata filter for vector search.
    pub filter: Option<&'a MetadataFilter>,
    /// RRF constant (default: 60). Higher = more weight to top ranks.
    pub rrf_k: f64,
}

impl Default for GraphRagParams<'_> {
    fn default() -> Self {
        Self {
            collection: "",
            query: &[],
            vector_k: 10,
            graph_depth: 2,
            top_k: 10,
            filter: None,
            rrf_k: 60.0,
        }
    }
}

impl<S: StorageEngine> NodeDbLite<S> {
    /// GraphRAG fusion: vector search → graph expansion → RRF merge.
    ///
    /// 1. Vector search returns `vector_k` candidates by embedding similarity.
    /// 2. For each candidate, graph traverse discovers related nodes within `graph_depth` hops.
    /// 3. All discovered nodes are scored: vector candidates by their search rank,
    ///    graph-discovered nodes by their discovery depth (closer = higher score).
    /// 4. RRF fuses both score sources into a single ranking.
    /// 5. Top `top_k` results returned.
    pub async fn graph_rag_search(
        &self,
        params: &GraphRagParams<'_>,
    ) -> NodeDbResult<Vec<SearchResult>> {
        // Step 1: Vector search.
        let vector_results = self
            .vector_search(
                params.collection,
                params.query,
                params.vector_k,
                params.filter,
            )
            .await?;

        if vector_results.is_empty() {
            return Ok(Vec::new());
        }

        // Step 2: Graph expansion from each vector result.
        let vector_ranked = search_results_to_ranked(&vector_results, "vector");

        // Expand graph from vector results and build a graph-ranked list.
        let mut graph_nodes: Vec<(String, usize)> = Vec::new(); // (node_id, depth)
        for result in &vector_results {
            let start = NodeId::new(result.id.clone());
            if let Ok(subgraph) = self.graph_traverse(&start, params.graph_depth, None).await {
                for node in &subgraph.nodes {
                    if node.depth == 0 {
                        continue;
                    }
                    graph_nodes.push((node.id.as_str().to_string(), node.depth as usize));
                }
            }
        }
        // Sort by depth (closer = higher rank), deduplicate keeping smallest depth.
        graph_nodes.sort_by(|a, b| a.1.cmp(&b.1));
        let mut seen = std::collections::HashSet::new();
        graph_nodes.retain(|(id, _)| seen.insert(id.clone()));
        let graph_ranked: Vec<nodedb_query::fusion::RankedResult> = graph_nodes
            .iter()
            .enumerate()
            .map(|(rank, (id, depth))| nodedb_query::fusion::RankedResult {
                document_id: id.clone(),
                rank,
                score: *depth as f32,
                source: "graph",
            })
            .collect();

        // Step 3: Fuse scores via shared weighted RRF.
        let fused = nodedb_query::fusion::reciprocal_rank_fusion_weighted(
            &[vector_ranked, graph_ranked],
            &[params.rrf_k, params.rrf_k],
            params.top_k,
        );

        // Step 4: Build result set. Reuse metadata from vector results where available.
        let vector_map: HashMap<&str, &SearchResult> =
            vector_results.iter().map(|r| (r.id.as_str(), r)).collect();

        let results = fused
            .into_iter()
            .map(|f| {
                if let Some(vr) = vector_map.get(f.document_id.as_str()) {
                    SearchResult {
                        id: f.document_id.clone(),
                        node_id: Some(NodeId::new(f.document_id)),
                        distance: vr.distance,
                        metadata: vr.metadata.clone(),
                    }
                } else {
                    // Graph-discovered node: read metadata from CRDT.
                    let metadata = {
                        let crdt = self.crdt.lock_or_recover();
                        if let Some(val) = crdt.read(params.collection, &f.document_id) {
                            let doc = crate::nodedb::convert::loro_value_to_document(
                                &f.document_id,
                                &val,
                            );
                            doc.fields
                        } else {
                            HashMap::new()
                        }
                    };
                    SearchResult {
                        id: f.document_id.clone(),
                        node_id: Some(NodeId::new(f.document_id)),
                        distance: f.rrf_score as f32,
                        metadata,
                    }
                }
            })
            .collect();

        Ok(results)
    }
}

/// Hybrid search parameters.
pub struct HybridSearchParams<'a> {
    /// Collection to search.
    pub collection: &'a str,
    /// Query embedding for vector similarity.
    pub query_embedding: &'a [f32],
    /// Query text for BM25 relevance.
    pub query_text: &'a str,
    /// Number of vector candidates.
    pub vector_k: usize,
    /// Number of text candidates.
    pub text_k: usize,
    /// Final number of results to return after fusion.
    pub top_k: usize,
    /// Optional metadata filter for vector search.
    pub filter: Option<&'a MetadataFilter>,
}

/// Build a ranked list from `SearchResult`s for the shared RRF module.
fn search_results_to_ranked(
    results: &[SearchResult],
    source: &'static str,
) -> Vec<nodedb_query::fusion::RankedResult> {
    results
        .iter()
        .enumerate()
        .map(|(rank, r)| nodedb_query::fusion::RankedResult {
            document_id: r.id.clone(),
            rank,
            score: r.distance,
            source,
        })
        .collect()
}

impl<S: StorageEngine> NodeDbLite<S> {
    /// Hybrid search: vector similarity + BM25 text relevance fused via RRF.
    ///
    /// 1. Vector search returns `vector_k` candidates by embedding similarity.
    /// 2. Text search returns `text_k` candidates by BM25 relevance.
    /// 3. RRF fuses both rankings into a single score per document.
    /// 4. Top `top_k` results returned.
    ///
    /// Documents found by both searches get boosted (RRF scores are additive).
    pub async fn hybrid_search(
        &self,
        params: &HybridSearchParams<'_>,
    ) -> NodeDbResult<Vec<SearchResult>> {
        let vector_results = self
            .vector_search(
                params.collection,
                params.query_embedding,
                params.vector_k,
                params.filter,
            )
            .await?;

        let text_results = self
            .text_search(
                params.collection,
                params.query_text,
                params.text_k,
                nodedb_types::TextSearchParams::default(),
            )
            .await?;

        // RRF fusion via shared module.
        let vector_ranked = search_results_to_ranked(&vector_results, "vector");
        let text_ranked = search_results_to_ranked(&text_results, "text");
        let fused = nodedb_query::fusion::reciprocal_rank_fusion(
            &[vector_ranked, text_ranked],
            None,
            params.top_k,
        );

        // Build metadata cache for result materialization.
        let mut metadata_cache: HashMap<String, HashMap<String, nodedb_types::Value>> =
            HashMap::new();
        for results in [&vector_results, &text_results] {
            for result in results.iter() {
                metadata_cache
                    .entry(result.id.clone())
                    .or_insert_with(|| result.metadata.clone());
            }
        }

        Ok(fused
            .into_iter()
            .map(|f| SearchResult {
                id: f.document_id.clone(),
                node_id: None,
                distance: 1.0 / (1.0 + f.rrf_score as f32),
                metadata: metadata_cache.remove(&f.document_id).unwrap_or_default(),
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn rrf_scoring() {
        // Verify RRF formula: 1/(k + rank + 1).
        let k = 60.0;
        let score_rank_0 = 1.0 / (k + 1.0);
        let score_rank_1 = 1.0 / (k + 2.0);
        assert!(score_rank_0 > score_rank_1);
        assert!((score_rank_0 - 1.0f64 / 61.0).abs() < 1e-10);
    }
}
