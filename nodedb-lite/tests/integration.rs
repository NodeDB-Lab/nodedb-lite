//! Integration tests for NodeDB-Lite.
//!
//! Tests the full stack: StorageEngine → Engines → NodeDbLite → NodeDb trait.
//! Performance/scale workloads live in `nodedb-bench/benches/`.

use std::sync::Arc;

use nodedb_client::NodeDb;
use nodedb_lite::{NodeDbLite, RedbStorage};
use nodedb_types::document::Document;
use nodedb_types::id::NodeId;
use nodedb_types::value::Value;

async fn open_test_db() -> NodeDbLite<RedbStorage> {
    let storage = RedbStorage::open_in_memory().unwrap();
    NodeDbLite::open(storage, 1).await.unwrap()
}

// ─── 1K Vector Insert + Search ───────────────────────────────────────

/// Correctness-only counterpart of the 1k×32d batch workload.
/// Scale benchmark: `nodedb-bench/benches/micro/lite_vector.rs`.
#[tokio::test]
async fn vector_batch_insert_and_search_correctness() {
    let db = open_test_db().await;
    let n = 50;
    let dim = 32;

    let vectors: Vec<(String, Vec<f32>)> = (0..n)
        .map(|i| {
            let emb: Vec<f32> = (0..dim).map(|d| ((i * dim + d) as f32) * 0.001).collect();
            (format!("v{i}"), emb)
        })
        .collect();

    let refs: Vec<(&str, &[f32])> = vectors
        .iter()
        .map(|(id, emb)| (id.as_str(), emb.as_slice()))
        .collect();

    db.batch_vector_insert("vecs", &refs).unwrap();

    let query: Vec<f32> = (0..dim).map(|d| ((25 * dim + d) as f32) * 0.001).collect();
    let results = db.vector_search("vecs", &query, 10, None).await.unwrap();

    assert_eq!(results.len(), 10);
    for w in results.windows(2) {
        assert!(
            w[0].distance <= w[1].distance,
            "results not sorted by distance"
        );
    }
    assert!(
        results[0].distance < 0.5,
        "top result distance {} is too large",
        results[0].distance
    );
}

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
    let refs: Vec<(&str, &str, &str)> = edges
        .iter()
        .map(|(s, d, l)| (s.as_str(), d.as_str(), *l))
        .collect();

    db.batch_graph_insert_edges(&refs).unwrap();
    db.compact_graph().unwrap();

    let subgraph = db
        .graph_traverse(&NodeId::new("n0"), 2, None)
        .await
        .unwrap();

    assert!(subgraph.node_count() > 5);
    assert!(subgraph.edge_count() > 0);
}

// ─── Document CRUD ───────────────────────────────────────────────────

#[tokio::test]
async fn document_crud_100() {
    let db = open_test_db().await;

    for i in 0..100 {
        let mut doc = Document::new(format!("doc-{i}"));
        doc.set("title", Value::String(format!("Document {i}")));
        doc.set("score", Value::Float(i as f64 * 0.1));
        db.document_put("notes", doc).await.unwrap();
    }

    let doc = db.document_get("notes", "doc-50").await.unwrap().unwrap();
    assert_eq!(doc.id, "doc-50");
    assert_eq!(doc.get_str("title"), Some("Document 50"));

    // Update.
    let mut updated = Document::new("doc-50");
    updated.set("title", Value::String("Updated 50".into()));
    db.document_put("notes", updated).await.unwrap();
    let doc = db.document_get("notes", "doc-50").await.unwrap().unwrap();
    assert_eq!(doc.get_str("title"), Some("Updated 50"));

    // Delete.
    db.document_delete("notes", "doc-50").await.unwrap();
    assert!(db.document_get("notes", "doc-50").await.unwrap().is_none());
    assert!(db.document_get("notes", "doc-49").await.unwrap().is_some());
}

// ─── Multi-Modal Query ───────────────────────────────────────────────

#[tokio::test]
async fn multi_modal_vector_graph_document() {
    let db = open_test_db().await;

    db.batch_vector_insert(
        "kb",
        &[
            ("concept-ai", &[1.0, 0.0, 0.0][..]),
            ("concept-ml", &[0.9, 0.1, 0.0]),
            ("concept-db", &[0.0, 0.0, 1.0]),
        ],
    )
    .unwrap();

    db.batch_graph_insert_edges(&[
        ("concept-ai", "concept-ml", "RELATES_TO"),
        ("concept-ml", "concept-db", "USES"),
    ])
    .unwrap();

    let mut doc = Document::new("note-1");
    doc.set("body", Value::String("AI and ML are related".into()));
    db.document_put("notes", doc).await.unwrap();

    // Vector search → graph traverse → document read.
    let results = db
        .vector_search("kb", &[1.0, 0.0, 0.0], 2, None)
        .await
        .unwrap();
    assert!(!results.is_empty());

    let start = NodeId::new(results[0].id.clone());
    let subgraph = db.graph_traverse(&start, 2, None).await.unwrap();
    assert!(subgraph.node_count() >= 1);

    let note = db.document_get("notes", "note-1").await.unwrap().unwrap();
    assert!(note.get_str("body").unwrap().contains("AI"));
}

// ─── Persistence: Flush and Reopen ───────────────────────────────────

#[tokio::test]
async fn flush_and_reopen_persists_all() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("persist.db");

    {
        let storage = RedbStorage::open(&path).unwrap();
        let db = NodeDbLite::open(storage, 1).await.unwrap();

        db.batch_vector_insert("vecs", &[("v1", &[1.0, 2.0, 3.0][..])])
            .unwrap();
        db.batch_graph_insert_edges(&[("a", "b", "KNOWS")]).unwrap();
        let mut doc = Document::new("d1");
        doc.set("key", Value::String("persistent".into()));
        db.document_put("docs", doc).await.unwrap();

        db.flush().await.unwrap();
    }

    {
        let storage = RedbStorage::open(&path).unwrap();
        let db = NodeDbLite::open(storage, 1).await.unwrap();

        let doc = db.document_get("docs", "d1").await.unwrap();
        assert!(doc.is_some(), "document should persist across restart");

        let results = db
            .vector_search("vecs", &[1.0, 2.0, 3.0], 1, None)
            .await
            .unwrap();
        assert!(!results.is_empty(), "vector should persist across restart");

        let sg = db.graph_traverse(&NodeId::new("a"), 1, None).await.unwrap();
        assert!(sg.node_count() >= 2, "graph should persist across restart");
    }
}

// ─── CRDT Deltas ─────────────────────────────────────────────────────

#[tokio::test]
async fn all_operations_generate_deltas() {
    let db = open_test_db().await;

    db.vector_insert("v", "v1", &[1.0], None).await.unwrap();
    db.graph_insert_edge(&NodeId::new("a"), &NodeId::new("b"), "L", None)
        .await
        .unwrap();
    db.document_put("d", Document::new("d1")).await.unwrap();
    db.document_delete("d", "d1").await.unwrap();

    let deltas = db.pending_crdt_deltas().unwrap();
    assert!(
        deltas.len() >= 4,
        "expected >= 4 deltas, got {}",
        deltas.len()
    );
}

// ─── Arc<dyn NodeDb> Pattern ─────────────────────────────────────────

#[tokio::test]
async fn arc_dyn_nodedb_pattern() {
    let storage = RedbStorage::open_in_memory().unwrap();
    let db: Arc<dyn NodeDb> = Arc::new(NodeDbLite::open(storage, 1).await.unwrap());

    db.vector_insert("coll", "v1", &[1.0, 0.0], None)
        .await
        .unwrap();
    let results = db
        .vector_search("coll", &[1.0, 0.0], 1, None)
        .await
        .unwrap();
    assert_eq!(results.len(), 1);

    db.document_put("docs", Document::new("d1")).await.unwrap();
    assert!(db.document_get("docs", "d1").await.unwrap().is_some());
}

// `benchmark_vector_search_1k` and `benchmark_graph_bfs_10k_edges` were
// migrated to fluxbench benchmarks — they asserted only wall-clock budgets
// and don't belong in the test suite. See:
//   nodedb-bench/benches/micro/lite_vector.rs (lite_vector_search_1k_32d)
//   nodedb-bench/benches/micro/lite_graph.rs  (lite_graph_bfs_2hop_10k)
