// SPDX-License-Identifier: Apache-2.0

//! Graph-overlay methods on [`NodeDbLiteWasm`].

use wasm_bindgen::prelude::*;

use nodedb_client::NodeDb;
use nodedb_types::id::NodeId;

use crate::dispatch;
use crate::types::NodeDbLiteWasm;

#[wasm_bindgen]
impl NodeDbLiteWasm {
    /// Insert a directed graph edge into `collection`.
    ///
    /// Returns the generated edge ID as a string.
    #[wasm_bindgen(js_name = "graphInsertEdge")]
    pub async fn graph_insert_edge(
        &self,
        collection: &str,
        from: &str,
        to: &str,
        edge_type: &str,
    ) -> Result<String, JsError> {
        let from_id = NodeId::try_new(from).map_err(|e| JsError::new(&e.to_string()))?;
        let to_id = NodeId::try_new(to).map_err(|e| JsError::new(&e.to_string()))?;
        let edge_id = dispatch!(self, db, {
            db.graph_insert_edge(collection, &from_id, &to_id, edge_type, None)
                .await
                .map_err(|e| JsError::new(&e.to_string()))
        })?;
        Ok(edge_id.to_string())
    }

    /// Traverse the graph from a start node within `collection`. Returns JSON.
    #[wasm_bindgen(js_name = "graphTraverse")]
    pub async fn graph_traverse(
        &self,
        collection: &str,
        start: &str,
        depth: u8,
    ) -> Result<JsValue, JsError> {
        let start_id = NodeId::try_new(start).map_err(|e| JsError::new(&e.to_string()))?;
        let subgraph = dispatch!(self, db, {
            db.graph_traverse(collection, &start_id, depth, None)
                .await
                .map_err(|e| JsError::new(&e.to_string()))
        })?;

        let json = serde_json::json!({
            "nodes": subgraph.nodes.iter().map(|n| serde_json::json!({
                "id": n.id.as_str(),
                "depth": n.depth,
            })).collect::<Vec<_>>(),
            "edges": subgraph.edges.iter().map(|e| serde_json::json!({
                "from": e.from.as_str(),
                "to": e.to.as_str(),
                "label": e.label,
            })).collect::<Vec<_>>(),
        });

        serde_wasm_bindgen::to_value(&json).map_err(|e| JsError::new(&e.to_string()))
    }

    /// Delete a graph edge by ID from `collection`.
    #[wasm_bindgen(js_name = "graphDeleteEdge")]
    pub async fn graph_delete_edge(&self, collection: &str, edge_id: &str) -> Result<(), JsError> {
        let eid: nodedb_types::id::EdgeId = edge_id
            .parse()
            .map_err(|e: nodedb_types::id::EdgeIdParseError| JsError::new(&e.to_string()))?;
        dispatch!(self, db, {
            db.graph_delete_edge(collection, &eid)
                .await
                .map_err(|e| JsError::new(&e.to_string()))
        })
    }

    /// Return aggregate graph statistics for `collection`.
    #[wasm_bindgen(js_name = "graphStats")]
    pub async fn graph_stats(&self, collection: Option<String>) -> Result<JsValue, JsError> {
        let col = collection.as_deref();
        let stats = dispatch!(self, db, {
            db.graph_stats(col, None)
                .await
                .map_err(|e| JsError::new(&e.to_string()))
        })?;

        let json: Vec<serde_json::Value> = stats
            .iter()
            .map(|s| {
                serde_json::json!({
                    "collection": s.collection,
                    "node_count": s.node_count,
                    "edge_count": s.edge_count,
                    "distinct_label_count": s.distinct_label_count,
                    "labels": s.labels,
                })
            })
            .collect();

        serde_wasm_bindgen::to_value(&json).map_err(|e| JsError::new(&e.to_string()))
    }

    /// Find the shortest path between two nodes within `collection`. Returns JSON.
    #[wasm_bindgen(js_name = "graphShortestPath")]
    pub async fn graph_shortest_path(
        &self,
        collection: &str,
        from: &str,
        to: &str,
        max_depth: u8,
    ) -> Result<JsValue, JsError> {
        let from_id = NodeId::try_new(from).map_err(|e| JsError::new(&e.to_string()))?;
        let to_id = NodeId::try_new(to).map_err(|e| JsError::new(&e.to_string()))?;
        let path = dispatch!(self, db, {
            db.graph_shortest_path(collection, &from_id, &to_id, max_depth, None)
                .await
                .map_err(|e| JsError::new(&e.to_string()))
        })?;

        match path {
            Some(nodes) => {
                let ids: Vec<&str> = nodes.iter().map(|n| n.as_str()).collect();
                serde_wasm_bindgen::to_value(&ids).map_err(|e| JsError::new(&e.to_string()))
            }
            None => Ok(JsValue::NULL),
        }
    }
}
