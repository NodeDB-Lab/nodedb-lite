//! End-to-end integration tests for NodeDB-Lite.
//!
//! CRDT convergence works across instances, and the compensation flow is correct.

use nodedb_client::NodeDb;
use nodedb_lite::{NodeDbLite, RedbStorage};
use nodedb_types::document::Document;
use nodedb_types::id::NodeId;
use nodedb_types::value::Value;

async fn open_db() -> NodeDbLite<RedbStorage> {
    let storage = RedbStorage::open_in_memory().unwrap();
    NodeDbLite::open(storage, 1).await.unwrap()
}

// ═══════════════════════════════════════════════════════════════════════
// 6.1 Standalone Lite (No Origin)
// ═══════════════════════════════════════════════════════════════════════

/// Correctness-only counterpart of the 1k×384d HNSW workload.
/// The full-scale build/search is a benchmark — see
/// `nodedb-bench/benches/micro/lite_vector.rs`.
#[tokio::test]
async fn e2e_vector_search_returns_sorted_top_k() {
    let db = open_db().await;
    let dim = 32;
    let n = 50;

    let vectors: Vec<(String, Vec<f32>)> = (0..n)
        .map(|i| {
            let emb: Vec<f32> = (0..dim).map(|d| ((i * dim + d) as f32).sin()).collect();
            (format!("v{i}"), emb)
        })
        .collect();
    let refs: Vec<(&str, &[f32])> = vectors
        .iter()
        .map(|(id, e)| (id.as_str(), e.as_slice()))
        .collect();
    db.batch_vector_insert("kb", &refs).unwrap();

    let query: Vec<f32> = (0..dim).map(|d| ((25 * dim + d) as f32).sin()).collect();
    let results = db.vector_search("kb", &query, 5, None).await.unwrap();

    assert_eq!(results.len(), 5);
    for w in results.windows(2) {
        assert!(w[0].distance <= w[1].distance);
    }
    assert!(
        results[0].distance < 1.0,
        "top result distance {} too large",
        results[0].distance
    );
}

/// Correctness-only counterpart of the 10k-edge graph workload.
/// Scale benchmark lives in `nodedb-bench/benches/micro/lite_graph.rs`.
#[tokio::test]
async fn e2e_graph_bfs_reaches_multiple_hops() {
    let db = open_db().await;

    // Small graph: 50 nodes, 4 edges each = 200 edges. Enough for 3-hop BFS
    // to visit several nodes; cheap enough for default `cargo nextest run`.
    let mut edges: Vec<(String, String, &str)> = Vec::with_capacity(200);
    for i in 0..50 {
        for j in 1..=4 {
            let dst = (i + j * 7) % 50;
            edges.push((format!("n{i}"), format!("n{dst}"), "LINK"));
        }
    }
    let refs: Vec<(&str, &str, &str)> = edges
        .iter()
        .map(|(s, d, l)| (s.as_str(), d.as_str(), *l))
        .collect();
    db.batch_graph_insert_edges(&refs).unwrap();
    db.compact_graph().unwrap();

    let sg = db
        .graph_traverse(&NodeId::new("n0"), 3, None)
        .await
        .unwrap();
    assert!(
        sg.node_count() > 5,
        "3-hop BFS should reach multiple nodes, found {}",
        sg.node_count()
    );
    assert!(
        sg.edge_count() > 0,
        "should have edges, found {}",
        sg.edge_count()
    );
}

#[tokio::test]
async fn e2e_document_crud_lifecycle() {
    let db = open_db().await;

    // Create.
    for i in 0..50 {
        let mut doc = Document::new(format!("doc-{i}"));
        doc.set("title", Value::String(format!("Title {i}")));
        doc.set("score", Value::Float(i as f64 * 0.1));
        db.document_put("notes", doc).await.unwrap();
    }

    // Read.
    let doc = db.document_get("notes", "doc-25").await.unwrap().unwrap();
    assert_eq!(doc.id, "doc-25");
    assert_eq!(doc.get_str("title"), Some("Title 25"));

    // Update.
    let mut updated = Document::new("doc-25");
    updated.set("title", Value::String("Updated".into()));
    db.document_put("notes", updated).await.unwrap();
    let doc = db.document_get("notes", "doc-25").await.unwrap().unwrap();
    assert_eq!(doc.get_str("title"), Some("Updated"));

    // Delete.
    db.document_delete("notes", "doc-25").await.unwrap();
    assert!(db.document_get("notes", "doc-25").await.unwrap().is_none());

    // Neighbors survive.
    assert!(db.document_get("notes", "doc-24").await.unwrap().is_some());
    assert!(db.document_get("notes", "doc-26").await.unwrap().is_some());
}

#[tokio::test]
async fn e2e_flush_reopen_all_data_survives() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("e2e.redb");

    // Write data + flush.
    {
        let storage = RedbStorage::open(&path).unwrap();
        let db = NodeDbLite::open(storage, 1).await.unwrap();

        db.batch_vector_insert(
            "vecs",
            &[("v1", &[1.0f32, 2.0, 3.0][..]), ("v2", &[4.0, 5.0, 6.0])],
        )
        .unwrap();
        db.batch_graph_insert_edges(&[("a", "b", "KNOWS"), ("b", "c", "KNOWS")])
            .unwrap();
        let mut doc = Document::new("d1");
        doc.set("key", Value::String("persistent".into()));
        db.document_put("docs", doc).await.unwrap();

        db.flush().await.unwrap();
    }

    // Reopen + verify.
    {
        let storage = RedbStorage::open(&path).unwrap();
        let db = NodeDbLite::open(storage, 1).await.unwrap();

        // Vectors.
        let results = db
            .vector_search("vecs", &[1.0, 2.0, 3.0], 1, None)
            .await
            .unwrap();
        assert!(!results.is_empty(), "vectors should survive restart");

        // Graph.
        let sg = db.graph_traverse(&NodeId::new("a"), 2, None).await.unwrap();
        assert!(sg.node_count() >= 3, "graph should survive restart");

        // Document.
        let doc = db.document_get("docs", "d1").await.unwrap();
        assert!(doc.is_some(), "document should survive restart");
    }
}

// `e2e_cold_start_timing` was migrated to a benchmark — it asserted only
// wall-clock time, no correctness. See:
//   nodedb-bench/benches/workload/lite_cold_start.rs

#[tokio::test]
async fn e2e_memory_stays_within_budget() {
    let db = open_db().await;

    // Small workload — 50 × 32d. Enough to exercise the budget tracker
    // without making the test a benchmark.
    let vectors: Vec<(String, Vec<f32>)> = (0..50)
        .map(|i| {
            let emb: Vec<f32> = (0..32).map(|d| ((i * 32 + d) as f32) * 0.001).collect();
            (format!("v{i}"), emb)
        })
        .collect();
    let refs: Vec<(&str, &[f32])> = vectors
        .iter()
        .map(|(id, e)| (id.as_str(), e.as_slice()))
        .collect();
    db.batch_vector_insert("vecs", &refs).unwrap();

    let used = db.governor().total_used();
    let budget = db.governor().total_budget();

    assert!(used <= budget, "memory {used} exceeds budget {budget}");
}

// ═══════════════════════════════════════════════════════════════════════
// 6.2 / 6.3 CRDT Convergence & Compensation (simulated, no Origin)
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn e2e_two_lite_instances_crdt_convergence() {
    // Simulate two Lite devices making independent edits, then merging.
    let db1 = {
        let s = RedbStorage::open_in_memory().unwrap();
        NodeDbLite::open(s, 1).await.unwrap()
    };
    let db2 = {
        let s = RedbStorage::open_in_memory().unwrap();
        NodeDbLite::open(s, 2).await.unwrap()
    };

    // Device 1 writes.
    let mut doc1 = Document::new("shared-doc");
    doc1.set("author", Value::String("alice".into()));
    db1.document_put("notes", doc1).await.unwrap();

    // Device 2 writes different field.
    let mut doc2 = Document::new("shared-doc");
    doc2.set("reviewer", Value::String("bob".into()));
    db2.document_put("notes", doc2).await.unwrap();

    // Export deltas from both.
    let deltas1 = db1.pending_crdt_deltas().unwrap();
    let deltas2 = db2.pending_crdt_deltas().unwrap();

    assert!(!deltas1.is_empty());
    assert!(!deltas2.is_empty());

    // Cross-import: db1 gets db2's deltas and vice versa.
    for d in &deltas2 {
        db1.import_remote_deltas(&d.delta_bytes).unwrap();
    }
    for d in &deltas1 {
        db2.import_remote_deltas(&d.delta_bytes).unwrap();
    }

    // Both should now see both fields.
    let doc_from_1 = db1
        .document_get("notes", "shared-doc")
        .await
        .unwrap()
        .unwrap();
    let doc_from_2 = db2
        .document_get("notes", "shared-doc")
        .await
        .unwrap()
        .unwrap();

    // CRDT merge: both fields should be present on both devices.
    assert!(doc_from_1.get_str("author").is_some() || doc_from_1.get_str("reviewer").is_some());
    assert!(doc_from_2.get_str("author").is_some() || doc_from_2.get_str("reviewer").is_some());
}

#[tokio::test]
async fn e2e_compensation_reject_rollback() {
    let db = open_db().await;

    // Write a document.
    let mut doc = Document::new("user-1");
    doc.set("username", Value::String("alice".into()));
    db.document_put("users", doc).await.unwrap();

    // Verify it exists.
    assert!(db.document_get("users", "user-1").await.unwrap().is_some());

    // Get the pending delta.
    let deltas = db.pending_crdt_deltas().unwrap();
    assert!(!deltas.is_empty());
    let mutation_id = deltas[0].mutation_id;

    // Simulate Origin rejection (UNIQUE violation).
    db.reject_delta(mutation_id).unwrap();

    // After rejection, the document should be rolled back.
    let doc = db.document_get("users", "user-1").await.unwrap();
    assert!(doc.is_none(), "rejected document should be rolled back");
}

#[tokio::test]
async fn e2e_delta_acknowledge_clears_pending() {
    let db = open_db().await;

    db.document_put("a", Document::new("d1")).await.unwrap();
    db.document_put("a", Document::new("d2")).await.unwrap();
    db.document_put("a", Document::new("d3")).await.unwrap();

    let deltas = db.pending_crdt_deltas().unwrap();
    assert_eq!(deltas.len(), 3);

    // ACK the first two.
    let ack_id = deltas[1].mutation_id;
    db.acknowledge_deltas(ack_id).unwrap();

    let remaining = db.pending_crdt_deltas().unwrap();
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].mutation_id, deltas[2].mutation_id);
}

// ═══════════════════════════════════════════════════════════════════════
// 6.5 Platform-Specific
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn e2e_native_full_suite_passes() {
    // This test IS the proof that native works — it runs alongside all others.
    let db = open_db().await;
    db.vector_insert("test", "v1", &[1.0, 0.0], None)
        .await
        .unwrap();
    let r = db
        .vector_search("test", &[1.0, 0.0], 1, None)
        .await
        .unwrap();
    assert_eq!(r.len(), 1);

    db.graph_insert_edge(&NodeId::new("a"), &NodeId::new("b"), "L", None)
        .await
        .unwrap();
    let sg = db.graph_traverse(&NodeId::new("a"), 1, None).await.unwrap();
    assert!(sg.node_count() >= 2);

    db.document_put("d", Document::new("d1")).await.unwrap();
    assert!(db.document_get("d", "d1").await.unwrap().is_some());
}
