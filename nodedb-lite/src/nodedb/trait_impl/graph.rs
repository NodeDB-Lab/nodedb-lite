// SPDX-License-Identifier: Apache-2.0

//! Graph engine helpers for `NodeDbLite`.

use std::collections::{HashMap, HashSet};

use loro::LoroValue;

use nodedb_types::document::Document;
use nodedb_types::error::{NodeDbError, NodeDbResult};
use nodedb_types::filter::EdgeFilter;
use nodedb_types::graph::GraphStats;
use nodedb_types::id::{EdgeId, NodeId};
use nodedb_types::result::{SubGraph, SubGraphEdge, SubGraphNode};
use nodedb_types::value::Value;

use nodedb_graph::params::{AlgoParams, GraphAlgorithm};

use crate::engine::graph::history;
use crate::engine::graph::index::{CsrIndex, Direction};
use crate::engine::graph::traversal::DEFAULT_MAX_VISITED;
use crate::nodedb::LockExt;
use crate::nodedb::NodeDbLite;
use crate::nodedb::convert::{loro_value_to_document, value_to_loro};
use crate::query::graph_ops::algorithms;
use crate::runtime::now_millis_i64;
use crate::storage::engine::StorageEngine;

/// Returns the CRDT collection name for edges belonging to a graph collection.
fn edge_crdt_collection(collection: &str) -> String {
    format!("__edges__{collection}")
}

impl<S: StorageEngine> NodeDbLite<S> {
    /// Breadth-first traversal from `start` up to `depth` hops, returning a
    /// `SubGraph` with node properties and edges materialised from CRDT storage.
    ///
    /// Only edges belonging to `collection` are considered. Edges inserted into
    /// a different collection are invisible to this traversal.
    pub(super) async fn graph_traverse_impl(
        &self,
        collection: &str,
        start: &NodeId,
        depth: u8,
        edge_filter: Option<&EdgeFilter>,
    ) -> NodeDbResult<SubGraph> {
        let label_strs: Vec<&str> = edge_filter
            .map(|f| f.labels.iter().map(|s| s.as_str()).collect())
            .unwrap_or_default();

        // Collect BFS result and neighbors in a single lock scope.
        type BfsResult = (Vec<(String, u8)>, HashMap<String, Vec<(String, String)>>);
        let (result, neighbors_map): BfsResult = {
            let csr_map = self.csr.lock_or_recover();
            match csr_map.get(collection) {
                None => {
                    return Ok(SubGraph {
                        nodes: vec![],
                        edges: vec![],
                    });
                }
                Some(csr) => {
                    let bfs = csr.traverse_bfs_with_depth_multi(
                        &[start.as_str()],
                        &label_strs,
                        Direction::Out,
                        depth as usize,
                        DEFAULT_MAX_VISITED,
                    );
                    let mut nbrs: HashMap<String, Vec<(String, String)>> = HashMap::new();
                    for (node_name, _) in &bfs {
                        let n = csr.neighbors_multi(node_name, &label_strs, Direction::Out);
                        nbrs.insert(node_name.clone(), n);
                    }
                    (bfs, nbrs)
                }
            }
        };

        let edge_coll = edge_crdt_collection(collection);
        let crdt = self.crdt.lock_or_recover();
        let mut nodes = Vec::with_capacity(result.len());
        let mut edges = Vec::new();

        for (node_name, d) in &result {
            let properties = if let Some(loro_val) = crdt.read("__nodes", node_name) {
                let doc = loro_value_to_document(node_name, &loro_val);
                doc.fields
            } else {
                HashMap::new()
            };

            nodes.push(SubGraphNode {
                id: NodeId::from_validated(node_name.clone()),
                depth: *d,
                properties,
            });

            let empty = vec![];
            let neighbors = neighbors_map.get(node_name).unwrap_or(&empty);
            for (label, dst) in neighbors {
                if result.iter().any(|(n, _)| n == dst) {
                    let src_id = NodeId::from_validated(node_name.clone());
                    let dst_id = NodeId::from_validated(dst.clone());
                    let edge_id = EdgeId::try_first(src_id.clone(), dst_id.clone(), label.clone())
                        .map_err(|e| {
                            NodeDbError::storage(format!(
                                "edge_store: invalid edge label '{label}': {e}"
                            ))
                        })?;
                    let edge_key = format!("{edge_id}");
                    let edge_props = if let Some(loro_val) = crdt.read(&edge_coll, &edge_key) {
                        let doc = loro_value_to_document(&edge_key, &loro_val);
                        doc.fields
                            .into_iter()
                            .filter(|(k, _)| k != "src" && k != "dst" && k != "label")
                            .collect()
                    } else {
                        HashMap::new()
                    };

                    edges.push(SubGraphEdge {
                        id: edge_id,
                        from: src_id,
                        to: dst_id,
                        label: label.clone(),
                        properties: edge_props,
                    });
                }
            }
        }

        Ok(SubGraph { nodes, edges })
    }

    /// Insert an edge into the collection-scoped CSR adjacency index and persist
    /// a corresponding CRDT document holding `src`, `dst`, `label`, and any
    /// user-supplied properties. Edges are stored under a per-collection CRDT
    /// namespace so that collections are fully isolated from one another.
    pub(super) async fn graph_insert_edge_impl(
        &self,
        collection: &str,
        from: &NodeId,
        to: &NodeId,
        edge_type: &str,
        properties: Option<Document>,
    ) -> NodeDbResult<EdgeId> {
        {
            let mut csr_map = self.csr.lock_or_recover();
            let csr = csr_map
                .entry(collection.to_string())
                .or_insert_with(CsrIndex::new);
            let _ = csr.add_edge(from.as_str(), edge_type, to.as_str());
        }

        let edge_id = EdgeId::try_first(from.clone(), to.clone(), edge_type).map_err(|e| {
            NodeDbError::storage(format!("edge_store: invalid edge label '{edge_type}': {e}"))
        })?;
        let edge_key = format!("{edge_id}");
        let edge_coll = edge_crdt_collection(collection);

        {
            let mut crdt = self.crdt.lock_or_recover();
            let mut fields: Vec<(&str, LoroValue)> = vec![
                ("src", LoroValue::String(from.as_str().into())),
                ("dst", LoroValue::String(to.as_str().into())),
                ("label", LoroValue::String(edge_type.into())),
            ];

            if let Some(ref props) = properties {
                for (k, v) in &props.fields {
                    fields.push((k.as_str(), value_to_loro(v)));
                }
            }

            crdt.upsert(&edge_coll, &edge_key, &fields)
                .map_err(NodeDbError::storage)?;
        }

        // Record edge birth in the bitemporal history table if the collection
        // has bitemporal tracking enabled.
        let bitemporal = history::is_bitemporal(self.storage.as_ref(), collection)
            .await
            .unwrap_or(false);
        if bitemporal {
            let system_from_ms = now_millis_i64();
            let props_value = {
                let mut m = std::collections::HashMap::new();
                m.insert("src".to_string(), Value::String(from.as_str().to_string()));
                m.insert("dst".to_string(), Value::String(to.as_str().to_string()));
                m.insert("label".to_string(), Value::String(edge_type.to_string()));
                if let Some(ref props) = properties {
                    for (k, v) in &props.fields {
                        m.insert(k.clone(), v.clone());
                    }
                }
                Value::Object(m)
            };
            let _ = history::record_edge_insert(
                self.storage.as_ref(),
                collection,
                &edge_key,
                &props_value,
                system_from_ms,
            )
            .await;
        }

        self.update_memory_stats();
        Ok(edge_id)
    }

    /// Remove an edge from both the collection-scoped CSR index and the
    /// collection-scoped CRDT edge document store.
    pub(super) async fn graph_delete_edge_impl(
        &self,
        collection: &str,
        edge_id: &EdgeId,
    ) -> NodeDbResult<()> {
        let src = edge_id.src.as_str();
        let dst = edge_id.dst.as_str();
        let label = &edge_id.label;
        {
            let mut csr_map = self.csr.lock_or_recover();
            if let Some(csr) = csr_map.get_mut(collection) {
                csr.remove_edge(src, label, dst);
            }
        }

        let edge_key = format!("{edge_id}");
        let edge_coll = edge_crdt_collection(collection);
        {
            let mut crdt = self.crdt.lock_or_recover();
            crdt.delete(&edge_coll, &edge_key)
                .map_err(NodeDbError::storage)?;
        }

        // Finalize the history entry if the collection is bitemporal.
        let bitemporal = history::is_bitemporal(self.storage.as_ref(), collection)
            .await
            .unwrap_or(false);
        if bitemporal {
            let system_to_ms = now_millis_i64();
            let _ = history::record_edge_delete(
                self.storage.as_ref(),
                collection,
                &edge_key,
                system_to_ms,
            )
            .await;
        }

        Ok(())
    }

    /// Return edge statistics for `collection`. When `collection` is `Some(name)`,
    /// counts reflect only the edges in that collection. When `collection` is `None`,
    /// all known graph collections are aggregated and a single combined entry is
    /// returned under the key `"*"`.
    ///
    /// `as_of` is not supported on Lite: the backend has no bitemporal store.
    /// Passing `Some(_)` returns an error.
    pub(super) async fn graph_stats_impl(
        &self,
        collection: Option<&str>,
        as_of: Option<i64>,
    ) -> NodeDbResult<Vec<GraphStats>> {
        if as_of.is_some() {
            return Err(NodeDbError::storage(
                "AS OF SYSTEM TIME is not supported on the Lite backend",
            ));
        }

        let crdt = self.crdt.lock_or_recover();

        // Determine which CRDT collections to aggregate.
        let edge_colls: Vec<String> = match collection {
            Some(name) => vec![edge_crdt_collection(name)],
            None => crdt
                .collection_names()
                .into_iter()
                .filter(|c| c.starts_with("__edges__"))
                .collect(),
        };

        let mut node_ids: HashSet<String> = HashSet::new();
        let mut label_counts: HashMap<String, u64> = HashMap::new();
        let mut total_edges: u64 = 0;

        for ec in &edge_colls {
            let edge_ids = crdt.list_ids(ec);
            total_edges += edge_ids.len() as u64;
            for key in &edge_ids {
                if let Some(loro_val) = crdt.read(ec, key) {
                    let doc = loro_value_to_document(key, &loro_val);
                    if let Some(label) = doc.get_str("label") {
                        *label_counts.entry(label.to_string()).or_insert(0) += 1;
                    }
                    if let Some(src) = doc.get_str("src") {
                        node_ids.insert(src.to_string());
                    }
                    if let Some(dst) = doc.get_str("dst") {
                        node_ids.insert(dst.to_string());
                    }
                }
            }
        }

        let mut labels: Vec<(String, u64)> = label_counts.into_iter().collect();
        labels.sort_unstable_by(|(a, _), (b, _)| a.cmp(b));

        let coll_name = collection.unwrap_or("*").to_string();
        Ok(vec![GraphStats {
            collection: coll_name,
            node_count: node_ids.len() as u64,
            edge_count: total_edges,
            distinct_label_count: labels.len() as u64,
            labels,
        }])
    }

    /// Run PageRank (or Personalized PageRank) on the collection's CSR graph.
    ///
    /// Returns an empty `Vec` when the collection has no edges rather than an
    /// error — an empty graph simply has no ranks to report.
    pub(super) async fn graph_pagerank_impl(
        &self,
        collection: &str,
        personalization: Option<std::collections::HashMap<String, f64>>,
        damping: Option<f64>,
        max_iterations: Option<u32>,
    ) -> NodeDbResult<Vec<(String, f64)>> {
        // Fast path: if the collection isn't in the CSR map it has no edges.
        {
            let csr_map = self.csr.lock_or_recover();
            if !csr_map.contains_key(collection) {
                return Ok(Vec::new());
            }
        }

        let params = AlgoParams {
            collection: collection.to_string(),
            damping,
            max_iterations: max_iterations.map(|v| v as usize),
            personalization_vector: personalization,
            ..Default::default()
        };

        let result = algorithms::run_algo(&self.csr, GraphAlgorithm::PageRank, &params)
            .map_err(|e| NodeDbError::storage(format!("graph_pagerank: {e}")))?;

        // `result.columns` == ["node_id", "rank"]; extract and sort descending.
        let mut pairs: Vec<(String, f64)> = result
            .rows
            .into_iter()
            .filter_map(|mut row| {
                if row.len() < 2 {
                    return None;
                }
                let rank = match row.pop() {
                    Some(Value::Float(f)) => f,
                    _ => return None,
                };
                let node_id = match row.pop() {
                    Some(Value::String(s)) => s,
                    _ => return None,
                };
                Some((node_id, rank))
            })
            .collect();

        pairs.sort_unstable_by(|(_, a), (_, b)| {
            b.partial_cmp(a).unwrap_or(std::cmp::Ordering::Equal)
        });
        Ok(pairs)
    }

    /// Unweighted BFS shortest path from `from` to `to` within `collection`,
    /// bounded by `max_depth`. Returns `Ok(None)` when no path exists.
    pub(super) async fn graph_shortest_path_impl(
        &self,
        collection: &str,
        from: &NodeId,
        to: &NodeId,
        max_depth: u8,
        edge_filter: Option<&EdgeFilter>,
    ) -> NodeDbResult<Option<Vec<NodeId>>> {
        let label_filter = edge_filter
            .and_then(|f| f.labels.first())
            .map(|s| s.as_str());

        let csr_map = self.csr.lock_or_recover();
        let path = match csr_map.get(collection) {
            Some(csr) => csr.shortest_path(
                from.as_str(),
                to.as_str(),
                label_filter,
                max_depth as usize,
                DEFAULT_MAX_VISITED,
                None,
            ),
            None => None,
        };

        Ok(path.map(|p| p.into_iter().map(NodeId::from_validated).collect()))
    }
}
