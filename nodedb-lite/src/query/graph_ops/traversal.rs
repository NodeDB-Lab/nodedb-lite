// SPDX-License-Identifier: Apache-2.0

//! Hop, Neighbors, NeighborsMulti, Path, Subgraph handlers.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use nodedb_graph::traversal::DEFAULT_MAX_VISITED;
use nodedb_graph::{Direction, GraphTraversalOptions};
use nodedb_types::SurrogateBitmap;
use nodedb_types::result::QueryResult;
use nodedb_types::value::Value;

use crate::engine::graph::index::CsrIndex;
use crate::error::LiteError;

fn node_row(node: &str) -> Vec<Value> {
    vec![Value::String(node.to_string())]
}

fn node_cols() -> Vec<String> {
    vec!["node_id".to_string()]
}

/// Resolve max_visited from options, falling back to DEFAULT_MAX_VISITED.
fn max_visited(options: &GraphTraversalOptions) -> usize {
    if options.max_visited > 0 {
        options.max_visited
    } else {
        DEFAULT_MAX_VISITED
    }
}

/// Handle `GraphOp::Hop` — BFS traversal from start nodes.
#[allow(clippy::too_many_arguments)]
pub fn hop(
    csr_map: &Arc<Mutex<HashMap<String, CsrIndex>>>,
    collection: &str,
    start_nodes: &[String],
    edge_label: Option<&str>,
    direction: Direction,
    depth: usize,
    options: &GraphTraversalOptions,
    frontier_bitmap: Option<&SurrogateBitmap>,
) -> Result<QueryResult, LiteError> {
    let map = csr_map.lock().map_err(|_| LiteError::LockPoisoned)?;
    let Some(csr) = map.get(collection) else {
        return Ok(QueryResult::empty());
    };

    let starts: Vec<&str> = start_nodes.iter().map(String::as_str).collect();
    let mv = max_visited(options);
    let nodes = csr.traverse_bfs(&starts, edge_label, direction, depth, mv, frontier_bitmap);

    let rows = nodes.iter().map(|n| node_row(n)).collect();
    Ok(QueryResult {
        columns: node_cols(),
        rows,
        rows_affected: 0,
    })
}

/// Handle `GraphOp::Neighbors` — immediate 1-hop neighbors.
pub fn neighbors(
    csr_map: &Arc<Mutex<HashMap<String, CsrIndex>>>,
    collection: &str,
    node_id: &str,
    edge_label: Option<&str>,
    direction: Direction,
) -> Result<QueryResult, LiteError> {
    let map = csr_map.lock().map_err(|_| LiteError::LockPoisoned)?;
    let Some(csr) = map.get(collection) else {
        return Ok(QueryResult::empty());
    };

    let nbrs = csr.neighbors(node_id, edge_label, direction);
    let columns = vec!["label".to_string(), "neighbor".to_string()];
    let rows = nbrs
        .iter()
        .map(|(lbl, nb)| vec![Value::String(lbl.clone()), Value::String(nb.clone())])
        .collect();
    Ok(QueryResult {
        columns,
        rows,
        rows_affected: 0,
    })
}

/// Handle `GraphOp::NeighborsMulti` — batched 1-hop neighbors lookup.
pub fn neighbors_multi(
    csr_map: &Arc<Mutex<HashMap<String, CsrIndex>>>,
    collection: &str,
    node_ids: &[String],
    edge_label: Option<&str>,
    direction: Direction,
    max_results: u32,
) -> Result<QueryResult, LiteError> {
    let map = csr_map.lock().map_err(|_| LiteError::LockPoisoned)?;
    let Some(csr) = map.get(collection) else {
        return Ok(QueryResult::empty());
    };

    let label_slice: Vec<&str> = edge_label.into_iter().collect();
    let columns = vec![
        "src".to_string(),
        "label".to_string(),
        "neighbor".to_string(),
    ];
    let cap = if max_results == 0 {
        usize::MAX
    } else {
        max_results as usize
    };

    let mut rows: Vec<Vec<Value>> = Vec::new();
    for node in node_ids {
        if rows.len() >= cap {
            break;
        }
        let nbrs = csr.neighbors_multi(node, &label_slice, direction);
        for (lbl, nb) in nbrs {
            if rows.len() >= cap {
                break;
            }
            rows.push(vec![
                Value::String(node.clone()),
                Value::String(lbl),
                Value::String(nb),
            ]);
        }
    }

    Ok(QueryResult {
        columns,
        rows,
        rows_affected: 0,
    })
}

/// Handle `GraphOp::Path` — shortest path between two nodes.
#[allow(clippy::too_many_arguments)]
pub fn path(
    csr_map: &Arc<Mutex<HashMap<String, CsrIndex>>>,
    collection: &str,
    src: &str,
    dst: &str,
    edge_label: Option<&str>,
    max_depth: usize,
    options: &GraphTraversalOptions,
    frontier_bitmap: Option<&SurrogateBitmap>,
) -> Result<QueryResult, LiteError> {
    let map = csr_map.lock().map_err(|_| LiteError::LockPoisoned)?;
    let Some(csr) = map.get(collection) else {
        return Ok(QueryResult::empty());
    };

    let mv = max_visited(options);
    let maybe_path = csr.shortest_path(src, dst, edge_label, max_depth, mv, frontier_bitmap);

    let columns = vec!["path".to_string()];
    let rows = match maybe_path {
        None => Vec::new(),
        Some(p) => {
            let path_val = Value::Array(p.into_iter().map(Value::String).collect());
            vec![vec![path_val]]
        }
    };

    Ok(QueryResult {
        columns,
        rows,
        rows_affected: 0,
    })
}

/// Handle `GraphOp::Subgraph` — BFS edge materialization.
pub fn subgraph(
    csr_map: &Arc<Mutex<HashMap<String, CsrIndex>>>,
    collection: &str,
    start_nodes: &[String],
    edge_label: Option<&str>,
    depth: usize,
    options: &GraphTraversalOptions,
) -> Result<QueryResult, LiteError> {
    let map = csr_map.lock().map_err(|_| LiteError::LockPoisoned)?;
    let Some(csr) = map.get(collection) else {
        return Ok(QueryResult::empty());
    };

    let starts: Vec<&str> = start_nodes.iter().map(String::as_str).collect();
    let mv = max_visited(options);
    let edges = csr.subgraph(&starts, edge_label, depth, mv);

    let columns = vec!["src".to_string(), "label".to_string(), "dst".to_string()];
    let rows = edges
        .into_iter()
        .map(|(s, l, d)| vec![Value::String(s), Value::String(l), Value::String(d)])
        .collect();

    Ok(QueryResult {
        columns,
        rows,
        rows_affected: 0,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_csr_map_with_graph() -> Arc<Mutex<HashMap<String, CsrIndex>>> {
        let mut csr = CsrIndex::new();
        csr.add_edge("a", "KNOWS", "b").unwrap();
        csr.add_edge("b", "KNOWS", "c").unwrap();
        csr.add_edge("a", "WORKS", "d").unwrap();
        let mut map = HashMap::new();
        map.insert("social".to_string(), csr);
        Arc::new(Mutex::new(map))
    }

    #[test]
    fn test_neighbors() {
        let m = make_csr_map_with_graph();
        let r = neighbors(&m, "social", "a", Some("KNOWS"), Direction::Out).unwrap();
        assert_eq!(r.rows.len(), 1);
        assert_eq!(r.rows[0][1], Value::String("b".to_string()));
    }

    #[test]
    fn test_neighbors_multi() {
        let m = make_csr_map_with_graph();
        let node_ids = vec!["a".to_string(), "b".to_string()];
        let r = neighbors_multi(&m, "social", &node_ids, None, Direction::Out, 0).unwrap();
        // a has 2 out edges (KNOWS->b, WORKS->d), b has 1 (KNOWS->c)
        assert_eq!(r.rows.len(), 3);
    }

    #[test]
    fn test_hop_bfs() {
        let m = make_csr_map_with_graph();
        let opts = GraphTraversalOptions::default();
        let r = hop(
            &m,
            "social",
            &["a".to_string()],
            Some("KNOWS"),
            Direction::Out,
            2,
            &opts,
            None,
        )
        .unwrap();
        let nodes: Vec<&str> = r.rows.iter().filter_map(|row| row[0].as_str()).collect();
        assert!(nodes.contains(&"a"));
        assert!(nodes.contains(&"b"));
        assert!(nodes.contains(&"c"));
    }

    #[test]
    fn test_path() {
        let m = make_csr_map_with_graph();
        let opts = GraphTraversalOptions::default();
        let r = path(&m, "social", "a", "c", Some("KNOWS"), 5, &opts, None).unwrap();
        assert_eq!(r.rows.len(), 1);
        if let Value::Array(p) = &r.rows[0][0] {
            let names: Vec<&str> = p.iter().filter_map(|v| v.as_str()).collect();
            assert!(names.contains(&"a"));
            assert!(names.contains(&"c"));
        } else {
            panic!("expected list");
        }
    }

    #[test]
    fn test_subgraph() {
        let m = make_csr_map_with_graph();
        let opts = GraphTraversalOptions::default();
        let r = subgraph(&m, "social", &["a".to_string()], None, 1, &opts).unwrap();
        assert_eq!(r.rows.len(), 2); // a->b KNOWS, a->d WORKS
    }
}
