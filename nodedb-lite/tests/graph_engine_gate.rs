// SPDX-License-Identifier: Apache-2.0

//! Graph engine gate tests — correctness for NodeDB-Lite 0.1.0.
//!
//! Scope: collection-scoped traversal, insert/delete, shortest path, and stats.
//! Origin parity (distributed, bitemporal) is out of scope for this beta gate.

use nodedb_client::NodeDb;
use nodedb_lite::{NodeDbLite, RedbStorage};
use nodedb_types::id::{EdgeId, NodeId};

async fn open_db() -> NodeDbLite<RedbStorage> {
    let storage = RedbStorage::open_in_memory().expect("open in-memory storage");
    NodeDbLite::open(storage, 1).await.expect("open NodeDbLite")
}

// ---------------------------------------------------------------------------
// collection_isolation
// ---------------------------------------------------------------------------

/// Both collections use the SAME node names (`x` and `y`) but insert edges
/// with different labels. Traversing `coll_a` from `x` must return only the
/// `coll_a` edge; traversing `coll_b` from `x` must return only the `coll_b`
/// edge. Stats for each collection must reflect exactly one edge.
#[tokio::test]
async fn collection_isolation() {
    let db = open_db().await;

    let x = NodeId::try_new("x").expect("node id");
    let y = NodeId::try_new("y").expect("node id");

    // coll_a: x → y with label "LINK_A"
    db.graph_insert_edge("coll_a", &x, &y, "LINK_A", None)
        .await
        .expect("insert coll_a x→y");

    // coll_b: x → y with label "LINK_B"
    db.graph_insert_edge("coll_b", &x, &y, "LINK_B", None)
        .await
        .expect("insert coll_b x→y");

    // Traverse coll_a from x (depth 1): must see the LINK_A edge only.
    let sg_a = db
        .graph_traverse("coll_a", &x, 1, None)
        .await
        .expect("traverse coll_a");

    let a_labels: Vec<String> = sg_a.edges.iter().map(|e| e.label.clone()).collect();
    assert!(
        a_labels.iter().all(|l| l == "LINK_A"),
        "coll_a traversal must only contain LINK_A edges; got {a_labels:?}",
    );
    assert_eq!(
        sg_a.edges.len(),
        1,
        "coll_a traversal must contain exactly one edge; got {}",
        sg_a.edges.len(),
    );

    // Traverse coll_b from x (depth 1): must see the LINK_B edge only.
    let sg_b = db
        .graph_traverse("coll_b", &x, 1, None)
        .await
        .expect("traverse coll_b");

    let b_labels: Vec<String> = sg_b.edges.iter().map(|e| e.label.clone()).collect();
    assert!(
        b_labels.iter().all(|l| l == "LINK_B"),
        "coll_b traversal must only contain LINK_B edges; got {b_labels:?}",
    );
    assert_eq!(
        sg_b.edges.len(),
        1,
        "coll_b traversal must contain exactly one edge; got {}",
        sg_b.edges.len(),
    );

    // Stats: each collection must report exactly 1 edge.
    let stats_a = db
        .graph_stats(Some("coll_a"), None)
        .await
        .expect("stats coll_a");
    assert_eq!(stats_a.len(), 1);
    assert_eq!(
        stats_a[0].edge_count, 1,
        "coll_a must have 1 edge; got {}",
        stats_a[0].edge_count,
    );

    let stats_b = db
        .graph_stats(Some("coll_b"), None)
        .await
        .expect("stats coll_b");
    assert_eq!(stats_b.len(), 1);
    assert_eq!(
        stats_b[0].edge_count, 1,
        "coll_b must have 1 edge; got {}",
        stats_b[0].edge_count,
    );
}

// ---------------------------------------------------------------------------
// traversal_and_shortest_path
// ---------------------------------------------------------------------------

/// Builds chain A→B→C→D, verifies depth-3 traversal reaches D,
/// verifies shortest path A→D is the 3-edge path,
/// then deletes B→C and verifies the path is broken.
#[tokio::test]
async fn traversal_and_shortest_path() {
    let db = open_db().await;

    let na = NodeId::try_new("sp_a").expect("node id");
    let nb = NodeId::try_new("sp_b").expect("node id");
    let nc = NodeId::try_new("sp_c").expect("node id");
    let nd = NodeId::try_new("sp_d").expect("node id");

    // Build chain: A→B→C→D
    db.graph_insert_edge("chain", &na, &nb, "HOP", None)
        .await
        .expect("insert A→B");
    db.graph_insert_edge("chain", &nb, &nc, "HOP", None)
        .await
        .expect("insert B→C");
    db.graph_insert_edge("chain", &nc, &nd, "HOP", None)
        .await
        .expect("insert C→D");

    // Traversal from A with depth 3 must include D.
    let sg = db
        .graph_traverse("chain", &na, 3, None)
        .await
        .expect("traverse chain depth=3");

    let node_ids: Vec<String> = sg.nodes.iter().map(|n| n.id.as_str().to_string()).collect();
    assert!(
        node_ids.contains(&"sp_d".to_string()),
        "depth-3 traversal from sp_a must reach sp_d; got {node_ids:?}",
    );

    // Shortest path A→D must return exactly [A, B, C, D] (3 hops).
    let path = db
        .graph_shortest_path("chain", &na, &nd, 10, None)
        .await
        .expect("shortest_path A→D")
        .expect("path should exist before edge deletion");

    let path_strs: Vec<&str> = path.iter().map(|n| n.as_str()).collect();
    assert_eq!(
        path_strs,
        vec!["sp_a", "sp_b", "sp_c", "sp_d"],
        "shortest path A→D must be [A,B,C,D]; got {path_strs:?}",
    );

    // Delete edge B→C.
    let bc_id = EdgeId::try_first(nb.clone(), nc.clone(), "HOP").expect("edge id B→C");
    db.graph_delete_edge("chain", &bc_id)
        .await
        .expect("delete B→C");

    // After deletion: no path from A to D within max_depth=10.
    let path_after = db
        .graph_shortest_path("chain", &na, &nd, 10, None)
        .await
        .expect("shortest_path A→D after deletion");

    assert!(
        path_after.is_none(),
        "path A→D must not exist after B→C is deleted; got {path_after:?}",
    );
}

// ---------------------------------------------------------------------------
// graph_stats
// ---------------------------------------------------------------------------

/// Inserts N edges with distinct src/dst pairs, calls graph_stats, and
/// asserts edge_count and node_count match expectations.
#[tokio::test]
async fn graph_stats_counts_match_insertions() {
    let db = open_db().await;

    const N: usize = 8;
    // Insert N edges: s0→t0, s1→t1, …, s7→t7  (16 distinct nodes, N edges).
    for i in 0..N {
        let from = NodeId::try_new(format!("stats_s{i}")).expect("node id");
        let to = NodeId::try_new(format!("stats_t{i}")).expect("node id");
        db.graph_insert_edge("stats_col", &from, &to, "REL", None)
            .await
            .expect("insert edge");
    }

    let result = db
        .graph_stats(Some("stats_col"), None)
        .await
        .expect("graph_stats");

    assert_eq!(result.len(), 1, "expected exactly one stats entry");
    let stats = &result[0];
    assert_eq!(stats.collection, "stats_col");
    assert_eq!(
        stats.edge_count, N as u64,
        "edge_count must equal number of inserted edges",
    );
    // Each edge has a unique src and a unique dst → 2*N distinct nodes.
    assert_eq!(
        stats.node_count,
        (2 * N) as u64,
        "node_count must equal 2*N for disjoint src/dst pairs",
    );
    assert_eq!(
        stats.distinct_label_count, 1,
        "only one label 'REL' was used",
    );
}
