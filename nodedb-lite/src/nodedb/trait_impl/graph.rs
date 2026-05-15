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

use crate::engine::graph::index::Direction;
use crate::engine::graph::traversal::DEFAULT_MAX_VISITED;
use crate::nodedb::LockExt;
use crate::nodedb::NodeDbLite;
use crate::nodedb::convert::{loro_value_to_document, value_to_loro};
use crate::storage::engine::{StorageEngine, StorageEngineSync};

impl<S: StorageEngine + StorageEngineSync> NodeDbLite<S> {
    /// Breadth-first traversal from `start` up to `depth` hops, returning a
    /// `SubGraph` with node properties and edges materialised from CRDT storage.
    ///
    /// `collection` is accepted for API parity with Origin but ignored on Lite:
    /// graph state is single-tenant. Edges are only included when both endpoints
    /// were reached within the BFS frontier.
    pub(super) async fn graph_traverse_impl(
        &self,
        _collection: &str,
        start: &NodeId,
        depth: u8,
        edge_filter: Option<&EdgeFilter>,
    ) -> NodeDbResult<SubGraph> {
        let csr = self.csr.lock_or_recover();

        let label_strs: Vec<&str> = edge_filter
            .map(|f| f.labels.iter().map(|s| s.as_str()).collect())
            .unwrap_or_default();

        let result = csr.traverse_bfs_with_depth_multi(
            &[start.as_str()],
            &label_strs,
            Direction::Out,
            depth as usize,
            DEFAULT_MAX_VISITED,
        );

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

            let neighbors = csr.neighbors_multi(node_name, &label_strs, Direction::Out);
            for (label, dst) in &neighbors {
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
                    let edge_props = if let Some(loro_val) = crdt.read("__edges", &edge_key) {
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

    /// Insert an edge into the CSR adjacency index and persist a corresponding
    /// `__edges` CRDT document holding `src`, `dst`, `label`, and user-supplied
    /// properties. The returned `EdgeId` is the first occurrence id for the
    /// `(from, to, label)` triple; duplicates re-use the same id slot.
    pub(super) async fn graph_insert_edge_impl(
        &self,
        _collection: &str,
        from: &NodeId,
        to: &NodeId,
        edge_type: &str,
        properties: Option<Document>,
    ) -> NodeDbResult<EdgeId> {
        {
            let mut csr = self.csr.lock_or_recover();
            let _ = csr.add_edge(from.as_str(), edge_type, to.as_str());
        }

        let edge_id = EdgeId::try_first(from.clone(), to.clone(), edge_type).map_err(|e| {
            NodeDbError::storage(format!("edge_store: invalid edge label '{edge_type}': {e}"))
        })?;
        let edge_key = format!("{edge_id}");

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

            crdt.upsert("__edges", &edge_key, &fields)
                .map_err(NodeDbError::storage)?;
        }

        self.update_memory_stats();
        Ok(edge_id)
    }

    /// Remove an edge from both the CSR adjacency index and the `__edges` CRDT
    /// document store. Missing edges in the CSR are silently ignored; the CRDT
    /// delete is authoritative for persistence.
    pub(super) async fn graph_delete_edge_impl(
        &self,
        _collection: &str,
        edge_id: &EdgeId,
    ) -> NodeDbResult<()> {
        let src = edge_id.src.as_str();
        let dst = edge_id.dst.as_str();
        let label = &edge_id.label;
        {
            let mut csr = self.csr.lock_or_recover();
            csr.remove_edge(src, label, dst);
        }

        let edge_key = format!("{edge_id}");
        {
            let mut crdt = self.crdt.lock_or_recover();
            crdt.delete("__edges", &edge_key)
                .map_err(NodeDbError::storage)?;
        }

        Ok(())
    }

    /// Aggregate edge statistics from the local CRDT edge store.
    ///
    /// Lite stores all edges in a single `__edges` CRDT document — there is no
    /// per-collection partitioning on the Lite backend. When `collection` is
    /// `Some(name)`, the returned `Vec` contains one entry with
    /// `collection: name`; when `collection` is `None`, the vec contains one
    /// entry keyed on `"__edges"`. In both cases the counts reflect the full
    /// local edge store, not a filtered subset.
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

        // Read all persisted edge keys from the CRDT edge store and aggregate.
        // Each document in "__edges" represents one edge; the src/dst/label fields
        // provide the node IDs and relationship types for counting.
        let crdt = self.crdt.lock_or_recover();
        let edge_ids = crdt.list_ids("__edges");

        let mut node_ids: HashSet<String> = HashSet::new();
        let mut label_counts: HashMap<String, u64> = HashMap::new();

        for key in &edge_ids {
            if let Some(loro_val) = crdt.read("__edges", key) {
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

        let mut labels: Vec<(String, u64)> = label_counts.into_iter().collect();
        labels.sort_unstable_by(|(a, _), (b, _)| a.cmp(b));

        let coll_name = collection.unwrap_or("__edges").to_string();
        Ok(vec![GraphStats {
            collection: coll_name,
            node_count: node_ids.len() as u64,
            edge_count: edge_ids.len() as u64,
            distinct_label_count: labels.len() as u64,
            labels,
        }])
    }

    /// Unweighted BFS shortest path from `from` to `to`, bounded by `max_depth`
    /// and optionally restricted to a single edge label (the first entry of
    /// `edge_filter.labels` if present). Returns `Ok(None)` when no path exists
    /// within the bound. `collection` is accepted for API parity but unused.
    pub(super) async fn graph_shortest_path_impl(
        &self,
        _collection: &str,
        from: &NodeId,
        to: &NodeId,
        max_depth: u8,
        edge_filter: Option<&EdgeFilter>,
    ) -> NodeDbResult<Option<Vec<NodeId>>> {
        let csr = self.csr.lock_or_recover();
        let label_filter = edge_filter
            .and_then(|f| f.labels.first())
            .map(|s| s.as_str());

        let path = csr.shortest_path(
            from.as_str(),
            to.as_str(),
            label_filter,
            max_depth as usize,
            DEFAULT_MAX_VISITED,
            None,
        );

        Ok(path.map(|p| p.into_iter().map(NodeId::from_validated).collect()))
    }
}
