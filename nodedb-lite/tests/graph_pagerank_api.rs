// SPDX-License-Identifier: Apache-2.0

//! Integration tests for the `graph_pagerank` public API on `NodeDbLite`.
//!
//! Covers: empty-graph handling, uniform PageRank on a symmetric graph,
//! Personalized PageRank seed concentration, rank-sum invariant, and
//! descending sort of results.

use std::collections::HashMap;

use nodedb_client::NodeDb;
use nodedb_lite::{NodeDbLite, PagedbStorageMem};
use nodedb_types::id::NodeId;

async fn open_db() -> NodeDbLite<PagedbStorageMem> {
    let storage = PagedbStorageMem::open_in_memory().await.unwrap();
    NodeDbLite::open(storage, 1).await.unwrap()
}

/// Insert a directed triangle A→B, B→C, C→A into `collection`.
async fn insert_triangle(db: &NodeDbLite<PagedbStorageMem>, collection: &str) {
    let a = NodeId::from_validated("A".to_string());
    let b = NodeId::from_validated("B".to_string());
    let c = NodeId::from_validated("C".to_string());
    db.graph_insert_edge(collection, &a, &b, "E", None)
        .await
        .unwrap();
    db.graph_insert_edge(collection, &b, &c, "E", None)
        .await
        .unwrap();
    db.graph_insert_edge(collection, &c, &a, "E", None)
        .await
        .unwrap();
}

// ---------------------------------------------------------------------------
// Test 1 — pagerank_on_empty_graph_returns_empty
// ---------------------------------------------------------------------------

/// Calling `graph_pagerank` on a collection that has never had edges inserted
/// must return an empty `Vec`, not an error.
#[tokio::test]
async fn pagerank_on_empty_graph_returns_empty() {
    let db = open_db().await;
    let result = db
        .graph_pagerank("nonexistent_collection", None, None, None)
        .await
        .unwrap();
    assert!(
        result.is_empty(),
        "expected empty result for collection with no edges"
    );
}

// ---------------------------------------------------------------------------
// Test 2 — pagerank_uniform_returns_equal_ranks_on_symmetric_graph
// ---------------------------------------------------------------------------

/// A directed triangle is rotationally symmetric; all three nodes must
/// receive approximately the same PageRank (within 0.01).
#[tokio::test]
async fn pagerank_uniform_returns_equal_ranks_on_symmetric_graph() {
    let db = open_db().await;
    insert_triangle(&db, "tri").await;

    let result = db.graph_pagerank("tri", None, None, None).await.unwrap();

    assert_eq!(result.len(), 3, "triangle has three nodes");

    let ranks: Vec<f64> = result.iter().map(|(_, r)| *r).collect();
    let first = ranks[0];
    for r in &ranks {
        assert!(
            (r - first).abs() < 0.01,
            "expected equal ranks on symmetric triangle; got {ranks:?}"
        );
    }
}

// ---------------------------------------------------------------------------
// Test 3 — pagerank_personalized_concentrates_on_seed
// ---------------------------------------------------------------------------

/// Seeding node "A" with full weight must make "A" the highest-ranked node.
#[tokio::test]
async fn pagerank_personalized_concentrates_on_seed() {
    let db = open_db().await;
    insert_triangle(&db, "tri_ppr").await;

    let mut pv: HashMap<String, f64> = HashMap::new();
    pv.insert("A".to_string(), 1.0);

    let result = db
        .graph_pagerank("tri_ppr", Some(pv), None, None)
        .await
        .unwrap();

    assert_eq!(result.len(), 3);

    // Results are sorted descending, so the first entry must be "A".
    let (top_node, top_rank) = &result[0];
    assert_eq!(
        top_node, "A",
        "seeded node 'A' must have the highest rank; got top={top_node} rank={top_rank}"
    );

    // "A" must strictly outrank the other two.
    for (node, rank) in result.iter().skip(1) {
        assert!(
            top_rank > rank,
            "A ({top_rank}) must outrank {node} ({rank})"
        );
    }
}

// ---------------------------------------------------------------------------
// Test 4 — pagerank_ranks_sum_to_one
// ---------------------------------------------------------------------------

/// Regardless of personalization the rank vector must sum to ≈1.0.
#[tokio::test]
async fn pagerank_ranks_sum_to_one() {
    let db = open_db().await;
    insert_triangle(&db, "tri_sum").await;

    let result = db
        .graph_pagerank("tri_sum", None, None, None)
        .await
        .unwrap();

    let total: f64 = result.iter().map(|(_, r)| r).sum();
    assert!(
        (total - 1.0).abs() < 0.01,
        "ranks must sum to 1.0; got {total}"
    );
}

// ---------------------------------------------------------------------------
// Test 5 — pagerank_results_sorted_descending
// ---------------------------------------------------------------------------

/// Results must be in descending rank order (highest rank first).
#[tokio::test]
async fn pagerank_results_sorted_descending() {
    let db = open_db().await;

    // Build a star graph: A→B, A→C, A→D, A→E.
    // A has high out-degree so B/C/D/E receive most rank.
    // The exact ordering doesn't matter; what matters is the list is sorted.
    let a = NodeId::from_validated("A".to_string());
    for target in ["B", "C", "D", "E"] {
        let t = NodeId::from_validated(target.to_string());
        db.graph_insert_edge("star", &a, &t, "E", None)
            .await
            .unwrap();
    }

    let result = db.graph_pagerank("star", None, None, None).await.unwrap();

    for window in result.windows(2) {
        let (_, r1) = &window[0];
        let (_, r2) = &window[1];
        assert!(
            r1 >= r2,
            "results must be sorted descending; found {r1} before {r2}"
        );
    }
}
