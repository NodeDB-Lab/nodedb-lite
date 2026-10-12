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

/// Parameters of a `GraphOp::Hop` BFS traversal.
pub struct HopSpec<'a> {
    pub start_nodes: &'a [String],
    pub edge_labels: &'a [&'a str],
    pub direction: Direction,
    pub depth: usize,
    pub options: &'a GraphTraversalOptions,
    pub frontier_bitmap: Option<&'a SurrogateBitmap>,
}

/// Parameters of a `GraphOp::Path` shortest-path search.
pub struct PathSpec<'a> {
    pub src: &'a str,
    pub dst: &'a str,
    pub edge_labels: &'a [&'a str],
    pub max_depth: usize,
    pub options: &'a GraphTraversalOptions,
    pub frontier_bitmap: Option<&'a SurrogateBitmap>,
}

/// Handle `GraphOp::Hop` — BFS traversal from start nodes.
pub fn hop(
    csr_map: &Arc<Mutex<HashMap<String, CsrIndex>>>,
    collection: &str,
    spec: HopSpec<'_>,
) -> Result<QueryResult, LiteError> {
    let HopSpec {
        start_nodes,
        edge_labels,
        direction,
        depth,
        options,
        frontier_bitmap,
    } = spec;
    let map = csr_map.lock().map_err(|_| LiteError::LockPoisoned)?;
    let Some(csr) = map.get(collection) else {
        return Ok(QueryResult::empty());
    };

    let starts: Vec<&str> = start_nodes.iter().map(String::as_str).collect();
    let mv = max_visited(options);
    let nodes = csr.traverse_bfs(
        nodedb_graph::BfsParams {
            start_nodes: &starts,
            label_filter: edge_labels,
            direction,
            max_depth: depth,
            max_visited: mv,
            frontier_bitmap,
        },
        None,
    );

    let rows = nodes.iter().map(|n| node_row(n)).collect();
    Ok(QueryResult {
        columns: node_cols(),
        rows,
        rows_affected: 0,
        command: None,
    })
}

/// Handle `GraphOp::Neighbors` — immediate 1-hop neighbors.
pub fn neighbors(
    csr_map: &Arc<Mutex<HashMap<String, CsrIndex>>>,
    collection: &str,
    node_id: &str,
    edge_labels: &[&str],
    direction: Direction,
) -> Result<QueryResult, LiteError> {
    let map = csr_map.lock().map_err(|_| LiteError::LockPoisoned)?;
    let Some(csr) = map.get(collection) else {
        return Ok(QueryResult::empty());
    };

    let nbrs = csr.neighbors(node_id, edge_labels, direction);
    let columns = vec!["label".to_string(), "neighbor".to_string()];
    let rows = nbrs
        .iter()
        .map(|(lbl, nb)| vec![Value::String(lbl.clone()), Value::String(nb.clone())])
        .collect();
    Ok(QueryResult {
        columns,
        rows,
        rows_affected: 0,
        command: None,
    })
}

/// Handle `GraphOp::NeighborsMulti` — batched 1-hop neighbors lookup.
pub fn neighbors_multi(
    csr_map: &Arc<Mutex<HashMap<String, CsrIndex>>>,
    collection: &str,
    node_ids: &[String],
    edge_labels: &[&str],
    direction: Direction,
    max_results: u32,
) -> Result<QueryResult, LiteError> {
    let map = csr_map.lock().map_err(|_| LiteError::LockPoisoned)?;
    let Some(csr) = map.get(collection) else {
        return Ok(QueryResult::empty());
    };

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
        let nbrs = csr.neighbors(node, edge_labels, direction);
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
        command: None,
    })
}

/// Handle `GraphOp::Path` — shortest path between two nodes.
pub fn path(
    csr_map: &Arc<Mutex<HashMap<String, CsrIndex>>>,
    collection: &str,
    spec: PathSpec<'_>,
) -> Result<QueryResult, LiteError> {
    let PathSpec {
        src,
        dst,
        edge_labels,
        max_depth,
        options,
        frontier_bitmap,
    } = spec;
    let map = csr_map.lock().map_err(|_| LiteError::LockPoisoned)?;
    let Some(csr) = map.get(collection) else {
        return Ok(QueryResult::empty());
    };

    let mv = max_visited(options);
    let maybe_path = csr.shortest_path(
        nodedb_graph::ShortestPathParams {
            src,
            dst,
            label_filter: edge_labels,
            max_depth,
            max_visited: mv,
            frontier_bitmap,
        },
        None,
    );

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
        command: None,
    })
}

/// Handle `GraphOp::Subgraph` — BFS edge materialization.
pub fn subgraph(
    csr_map: &Arc<Mutex<HashMap<String, CsrIndex>>>,
    collection: &str,
    start_nodes: &[String],
    edge_labels: &[&str],
    depth: usize,
    options: &GraphTraversalOptions,
) -> Result<QueryResult, LiteError> {
    let map = csr_map.lock().map_err(|_| LiteError::LockPoisoned)?;
    let Some(csr) = map.get(collection) else {
        return Ok(QueryResult::empty());
    };

    let starts: Vec<&str> = start_nodes.iter().map(String::as_str).collect();
    let mv = max_visited(options);
    let edges = csr.subgraph(&starts, edge_labels, Direction::Out, depth, mv, None);

    let columns = vec!["src".to_string(), "label".to_string(), "dst".to_string()];
    let rows = edges
        .into_iter()
        .map(|(s, l, d)| vec![Value::String(s), Value::String(l), Value::String(d)])
        .collect();

    Ok(QueryResult {
        columns,
        rows,
        rows_affected: 0,
        command: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_csr_map_with_graph() -> Arc<Mutex<HashMap<String, CsrIndex>>> {
        let mut csr = CsrIndex::new(crate::query::graph_ops::test_memory());
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
        let r = neighbors(&m, "social", "a", &["KNOWS"], Direction::Out).unwrap();
        assert_eq!(r.rows.len(), 1);
        assert_eq!(r.rows[0][1], Value::String("b".to_string()));
    }

    #[test]
    fn test_neighbors_multi() {
        let m = make_csr_map_with_graph();
        let node_ids = vec!["a".to_string(), "b".to_string()];
        let r = neighbors_multi(&m, "social", &node_ids, &[], Direction::Out, 0).unwrap();
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
            HopSpec {
                start_nodes: &["a".to_string()],
                edge_labels: &["KNOWS"],
                direction: Direction::Out,
                depth: 2,
                options: &opts,
                frontier_bitmap: None,
            },
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
        let r = path(
            &m,
            "social",
            PathSpec {
                src: "a",
                dst: "c",
                edge_labels: &["KNOWS"],
                max_depth: 5,
                options: &opts,
                frontier_bitmap: None,
            },
        )
        .unwrap();
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
        let r = subgraph(&m, "social", &["a".to_string()], &[], 1, &opts).unwrap();
        assert_eq!(r.rows.len(), 2); // a->b KNOWS, a->d WORKS
    }
}
