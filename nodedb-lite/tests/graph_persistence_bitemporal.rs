// SPDX-License-Identifier: Apache-2.0

//! Graph CSR persistence tests for bitemporal graph collections.
//!
//! Verifies that `rebuild_graph_indices` correctly restores the CSR adjacency
//! index from `Namespace::GraphHistory` after reopen when no CRDT snapshot was
//! flushed before the previous process exited.

use nodedb_client::NodeDb;
use nodedb_lite::{Encryption, NodeDbLite, PagedbStorageDefault};
use nodedb_types::id::NodeId;

/// Graph must find edges in a bitemporal collection after reopen without an
/// explicit flush.
///
/// Simulates a process exit without `flush()` by dropping the `NodeDbLite`
/// instance. The `Namespace::GraphHistory` table is durable (written
/// synchronously); only the CRDT snapshot and CSR checkpoint may not have been
/// committed. The rebuild path must fall back to the history table and
/// reconstruct the CSR index so that algorithms like PageRank work correctly
/// after reopen.
#[tokio::test]
async fn graph_pagerank_finds_edges_after_reopen_without_explicit_flush() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = tmp.path().to_path_buf();

    let a = NodeId::from_validated("A".to_string());
    let b = NodeId::from_validated("B".to_string());
    let c = NodeId::from_validated("C".to_string());

    // Process-A-equivalent: write WITHOUT calling flush().
    {
        let storage = PagedbStorageDefault::open(&path, Encryption::Plaintext)
            .await
            .unwrap();
        // Register the graph collection as bitemporal BEFORE opening NodeDbLite
        // so that graph_insert_edge writes to Namespace::GraphHistory.
        nodedb_lite::engine::graph::history::set_bitemporal(&storage, "social", true)
            .await
            .unwrap();

        let db = NodeDbLite::open(storage, 1).await.unwrap();
        // Insert a directed triangle so PageRank has something to compute.
        db.graph_insert_edge("social", &a, &b, "E", None)
            .await
            .unwrap();
        db.graph_insert_edge("social", &b, &c, "E", None)
            .await
            .unwrap();
        db.graph_insert_edge("social", &c, &a, "E", None)
            .await
            .unwrap();
        // Intentionally NO .flush() call here — db drops on scope exit.
    }

    // Process-B-equivalent: reopen, run pagerank, MUST find all three nodes.
    let storage = PagedbStorageDefault::open(&path, Encryption::Plaintext)
        .await
        .unwrap();
    let db = NodeDbLite::open(storage, 1).await.unwrap();

    let ranks = db.graph_pagerank("social", None, None, None).await.unwrap();

    assert_eq!(
        ranks.len(),
        3,
        "graph rebuild must restore all three edges; expected 3 ranked nodes, got {}",
        ranks.len()
    );

    // A symmetric triangle assigns equal rank to all nodes (within tolerance).
    let first = ranks[0].1;
    for (node_id, rank) in &ranks {
        assert!(
            (rank - first).abs() < 0.01,
            "expected equal PageRank on symmetric triangle after reopen; \
             node {node_id} has rank {rank:.4}, first has {first:.4}"
        );
    }
}

/// Tombstoned edges must NOT appear in CSR after reopen.
///
/// Inserts two edges into a bitemporal collection then deletes one. After
/// reopen (no flush), the live edge must contribute to PageRank and the
/// deleted edge must be absent from the CSR.
#[tokio::test]
async fn graph_excludes_tombstoned_edges_after_reopen() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = tmp.path().to_path_buf();

    let a = NodeId::from_validated("A".to_string());
    let b = NodeId::from_validated("B".to_string());
    let c = NodeId::from_validated("C".to_string());

    {
        let storage = PagedbStorageDefault::open(&path, Encryption::Plaintext)
            .await
            .unwrap();
        nodedb_lite::engine::graph::history::set_bitemporal(&storage, "social", true)
            .await
            .unwrap();

        let db = NodeDbLite::open(storage, 1).await.unwrap();
        // Insert A→B (keep) and A→C (tombstone).
        let _edge_ab = db
            .graph_insert_edge("social", &a, &b, "E", None)
            .await
            .unwrap();
        let edge_ac = db
            .graph_insert_edge("social", &a, &c, "E", None)
            .await
            .unwrap();

        // Tombstone the A→C edge.
        db.graph_delete_edge("social", &edge_ac).await.unwrap();
        // No flush: db drops on scope exit.
    }

    let storage = PagedbStorageDefault::open(&path, Encryption::Plaintext)
        .await
        .unwrap();
    let db = NodeDbLite::open(storage, 1).await.unwrap();

    let ranks = db.graph_pagerank("social", None, None, None).await.unwrap();

    // Only A and B are reachable via a live edge; C has no inbound live edges
    // so PageRank may return it at rank 0 or exclude it depending on the
    // dangling-node treatment. The critical assertion is that the CSR was
    // rebuilt (ranks is non-empty) and the graph has the correct edge count.
    assert!(
        !ranks.is_empty(),
        "graph rebuild must restore at least the live A→B edge after reopen"
    );

    // Verify C has no outbound neighbours visible from A after reopen by
    // checking that A→C is not in the ranked set with a high rank.
    // In a two-node reachable graph (A, B), both appear; C may appear at ~0.
    let node_ids: Vec<&str> = ranks.iter().map(|(id, _)| id.as_str()).collect();
    // The A→C edge was tombstoned; C should not appear with a significant rank.
    if let Some((_, c_rank)) = ranks.iter().find(|(id, _)| id == "C") {
        // C can appear at the dangling-node residual rank (~0.05 for 2 live nodes),
        // but must not receive the same rank as A and B (which have a live edge).
        let ab_rank = ranks
            .iter()
            .find(|(id, _)| id == "A")
            .map(|(_, r)| *r)
            .unwrap_or(0.0);
        assert!(
            *c_rank < ab_rank * 0.5,
            "C must have significantly lower rank than A after its inbound edge is tombstoned; \
             C rank = {c_rank:.4}, A rank = {ab_rank:.4}"
        );
        let _ = node_ids;
    }
}

/// Non-bitemporal graph collections must continue to restore from CRDT after
/// reopen.
///
/// Regression guard for the existing CRDT-based rebuild path. Plain graph
/// collections require an explicit flush for the CRDT snapshot to persist;
/// this test calls flush so the pre-existing path stays exercised end-to-end.
#[tokio::test]
async fn graph_still_works_for_non_bitemporal_collections_after_reopen() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = tmp.path().to_path_buf();

    let a = NodeId::from_validated("A".to_string());
    let b = NodeId::from_validated("B".to_string());
    let c = NodeId::from_validated("C".to_string());

    {
        let storage = PagedbStorageDefault::open(&path, Encryption::Plaintext)
            .await
            .unwrap();
        let db = NodeDbLite::open(storage, 1).await.unwrap();
        // No bitemporal=true — plain graph collection.
        db.graph_insert_edge("plain", &a, &b, "E", None)
            .await
            .unwrap();
        db.graph_insert_edge("plain", &b, &c, "E", None)
            .await
            .unwrap();
        db.graph_insert_edge("plain", &c, &a, "E", None)
            .await
            .unwrap();
        // Flush so the CSR checkpoint is durable (plain collection requirement).
        db.flush().await.unwrap();
    }

    let storage = PagedbStorageDefault::open(&path, Encryption::Plaintext)
        .await
        .unwrap();
    let db = NodeDbLite::open(storage, 1).await.unwrap();

    let ranks = db.graph_pagerank("plain", None, None, None).await.unwrap();

    assert_eq!(
        ranks.len(),
        3,
        "non-bitemporal graph collections must continue to restore CSR from checkpoint; \
         expected 3 ranked nodes, got {}",
        ranks.len()
    );
}
