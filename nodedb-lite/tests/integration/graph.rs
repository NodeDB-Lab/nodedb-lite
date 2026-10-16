// SPDX-License-Identifier: Apache-2.0

use super::fixtures::open_test_db;
use nodedb_client::NodeDb;
use nodedb_lite::{Encryption, NodeDbLite, PagedbStorageDefault};
use nodedb_types::document::Document;
use nodedb_types::id::{EdgeId, NodeId};
use nodedb_types::value::Value;

// ─── 10K Graph Edges + Traverse ──────────────────────────────────────

/// Correctness-only counterpart of the 10k-edge graph workload.
/// Scale benchmark: `nodedb-bench/benches/micro/lite_graph.rs`.
#[tokio::test]
async fn graph_batch_and_traverse_correctness() {
    let db = open_test_db().await;

    // 50 nodes × 4 edges each = 200 edges. 2-hop BFS visits 10+ nodes.
    let mut edges: Vec<(String, String, &str)> = Vec::with_capacity(200);
    for i in 0..50 {
        for j in 1..=4 {
            let dst = (i * 4 + j) % 50;
            edges.push((format!("n{i}"), format!("n{dst}"), "LINK"));
        }
    }
    let refs: Vec<(NodeId, NodeId, &str, Option<Document>)> = edges
        .iter()
        .map(|(s, d, l)| {
            (
                NodeId::from_validated(s.clone()),
                NodeId::from_validated(d.clone()),
                *l,
                None,
            )
        })
        .collect();

    db.batch_graph_insert_edges("graph", &refs).await.unwrap();
    db.compact_graph().unwrap();

    let subgraph = db
        .graph_traverse(
            "graph",
            &NodeId::from_validated("n0".to_string()),
            2,
            nodedb_types::graph::Direction::Out,
            None,
        )
        .await
        .unwrap();

    assert!(subgraph.node_count() > 5);
    assert!(subgraph.edge_count() > 0);
}

// ─── Batch Graph Edge Insert: EdgeId Keying ───────────────────────────

/// A batch-inserted edge's returned `EdgeId` must be the same key
/// `graph_delete_edge` removes, and the deletion must survive a reopen
/// (regression guard: the pre-`EdgeId` batch path stored edges under
/// `"{src}--{label}-->{dst}"`, a key `graph_delete_edge` never matched, so
/// the edge resurrected on every CSR rebuild).
#[tokio::test]
async fn batch_inserted_edge_is_removed_by_delete_and_stays_removed_after_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("batch_delete.db");

    let edge_id = {
        let storage = PagedbStorageDefault::open(&path, Encryption::Plaintext)
            .await
            .unwrap();
        let db = NodeDbLite::open(storage).await.unwrap();

        let ids = db
            .batch_graph_insert_edges(
                "social",
                &[(
                    NodeId::from_validated("a".to_string()),
                    NodeId::from_validated("b".to_string()),
                    "KNOWS",
                    None,
                )],
            )
            .await
            .unwrap();
        assert_eq!(ids.len(), 1);
        let edge_id = ids[0].clone();

        db.graph_delete_edge("social", &edge_id).await.unwrap();
        db.flush().await.unwrap();
        db.shutdown().await;
        edge_id
    };

    let storage = PagedbStorageDefault::open(&path, Encryption::Plaintext)
        .await
        .unwrap();
    let db = NodeDbLite::open(storage).await.unwrap();

    let subgraph = db
        .graph_traverse(
            "social",
            &NodeId::from_validated("a".to_string()),
            1,
            nodedb_types::graph::Direction::Out,
            None,
        )
        .await
        .unwrap();

    assert!(
        subgraph.edges.iter().all(|e| e.id != edge_id),
        "a batch-inserted edge deleted before flush must stay deleted after reopen"
    );
}

/// Properties passed to a batch edge insert must be readable from traversal,
/// the same way a single `graph_insert_edge` call's properties are.
#[tokio::test]
async fn batch_inserted_edge_properties_are_visible_in_traversal() {
    let db = open_test_db().await;

    let mut props = Document::new("");
    props.set("weight", Value::Integer(7));

    db.batch_graph_insert_edges(
        "social",
        &[(
            NodeId::from_validated("a".to_string()),
            NodeId::from_validated("b".to_string()),
            "KNOWS",
            Some(props),
        )],
    )
    .await
    .unwrap();

    let subgraph = db
        .graph_traverse(
            "social",
            &NodeId::from_validated("a".to_string()),
            1,
            nodedb_types::graph::Direction::Out,
            None,
        )
        .await
        .unwrap();

    assert_eq!(subgraph.edges.len(), 1);
    assert_eq!(
        subgraph.edges[0].properties.get("weight"),
        Some(&Value::Integer(7))
    );
}

/// Each `EdgeId` a batch insert returns must identify exactly the edge it
/// was allocated for, so deleting one by its returned id leaves the rest.
#[tokio::test]
async fn batch_insert_returns_edge_ids_usable_for_delete() {
    let db = open_test_db().await;

    let ids = db
        .batch_graph_insert_edges(
            "social",
            &[
                (
                    NodeId::from_validated("a".to_string()),
                    NodeId::from_validated("b".to_string()),
                    "KNOWS",
                    None,
                ),
                (
                    NodeId::from_validated("a".to_string()),
                    NodeId::from_validated("c".to_string()),
                    "KNOWS",
                    None,
                ),
            ],
        )
        .await
        .unwrap();
    assert_eq!(ids.len(), 2);

    db.graph_delete_edge("social", &ids[0]).await.unwrap();

    let subgraph = db
        .graph_traverse(
            "social",
            &NodeId::from_validated("a".to_string()),
            1,
            nodedb_types::graph::Direction::Out,
            None,
        )
        .await
        .unwrap();

    assert_eq!(subgraph.edges.len(), 1);
    assert_eq!(subgraph.edges[0].id, ids[1]);
}

/// An edge written under the legacy `"{src}--{label}-->{dst}"` CRDT key
/// (the pre-`EdgeId` batch-insert format) must be rewritten to its `EdgeId`
/// key on open: its properties become visible to traversal, and deleting it
/// by its `EdgeId` is durable across a further reopen.
#[tokio::test]
async fn legacy_batch_edge_keys_are_migrated_on_open() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("legacy_migrate.db");

    {
        let storage = PagedbStorageDefault::open(&path, Encryption::Plaintext)
            .await
            .unwrap();
        let db = NodeDbLite::open(storage).await.unwrap();

        // Write a legacy-format edge document directly through the CRDT
        // document API, mirroring what the pre-EdgeId batch insert path
        // used to store: "{src}--{label}-->{dst}" as the CRDT doc id under
        // "__edges__{collection}", with src/dst/label as its fields.
        let mut legacy = Document::new("a--KNOWS-->b");
        legacy.set("src", Value::String("a".into()));
        legacy.set("dst", Value::String("b".into()));
        legacy.set("label", Value::String("KNOWS".into()));
        legacy.set("weight", Value::Integer(3));
        db.document_put("__edges__social", legacy).await.unwrap();

        db.flush().await.unwrap();
        db.shutdown().await;
    }

    // Reopen: migration must rewrite the legacy key to its EdgeId form.
    let storage = PagedbStorageDefault::open(&path, Encryption::Plaintext)
        .await
        .unwrap();
    let db = NodeDbLite::open(storage).await.unwrap();

    let subgraph = db
        .graph_traverse(
            "social",
            &NodeId::from_validated("a".to_string()),
            1,
            nodedb_types::graph::Direction::Out,
            None,
        )
        .await
        .unwrap();
    assert_eq!(
        subgraph.edges.len(),
        1,
        "migrated legacy edge must appear in traversal"
    );
    assert_eq!(
        subgraph.edges[0].properties.get("weight"),
        Some(&Value::Integer(3)),
        "migrated legacy edge properties must survive the key rewrite"
    );

    let edge_id = EdgeId::try_first(
        NodeId::from_validated("a".to_string()),
        NodeId::from_validated("b".to_string()),
        "KNOWS",
    )
    .unwrap();
    db.graph_delete_edge("social", &edge_id).await.unwrap();

    // Keep "social" non-empty across the next reopen so the assertion below
    // exercises a CSR-checkpoint restore, not a from-scratch rebuild.
    db.graph_insert_edge(
        "social",
        &NodeId::from_validated("a".to_string()),
        &NodeId::from_validated("c".to_string()),
        "KNOWS",
        None,
    )
    .await
    .unwrap();
    db.flush().await.unwrap();
    db.shutdown().await;
    drop(db);

    let storage = PagedbStorageDefault::open(&path, Encryption::Plaintext)
        .await
        .unwrap();
    let db = NodeDbLite::open(storage).await.unwrap();
    let subgraph = db
        .graph_traverse(
            "social",
            &NodeId::from_validated("a".to_string()),
            1,
            nodedb_types::graph::Direction::Out,
            None,
        )
        .await
        .unwrap();
    assert_eq!(
        subgraph.edges.len(),
        1,
        "the migrated edge deleted by its EdgeId must stay deleted after reopen"
    );
    assert_eq!(subgraph.edges[0].to.as_str(), "c");
}
