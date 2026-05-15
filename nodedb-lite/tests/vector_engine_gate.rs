// SPDX-License-Identifier: Apache-2.0

//! §16 Vector engine gate tests.
//!
//! Covers HNSW + FP32 local-correctness for NodeDB-Lite 0.1.0 beta.
//! Quantization / IVF-PQ / hybrid / distributed are out of scope.

use nodedb_client::NodeDb;
use nodedb_lite::{NodeDbLite, RedbStorage};

async fn open_db() -> NodeDbLite<RedbStorage> {
    let storage = RedbStorage::open_in_memory().expect("open in-memory storage");
    NodeDbLite::open(storage, 1).await.expect("open NodeDbLite")
}

/// Inserts 100 FP32 vectors (dim=8, deterministic values), searches top-k=5,
/// and asserts 5 results are returned in non-decreasing distance order.
#[tokio::test]
async fn vector_insert_and_search_top_k_sorted() {
    let db = open_db().await;

    // Insert 100 deterministic vectors: v[i][d] = (i * 8 + d) as f32 * 0.01
    for i in 0u32..100 {
        let embedding: Vec<f32> = (0..8).map(|d| ((i * 8 + d) as f32) * 0.01).collect();
        db.vector_insert("gate_vecs", &format!("v{i}"), &embedding, None)
            .await
            .expect("vector_insert");
    }

    // Query near vector 42: same construction as the inserted vector.
    let query: Vec<f32> = (0..8).map(|d| ((42u32 * 8 + d) as f32) * 0.01).collect();
    let results = db
        .vector_search("gate_vecs", &query, 5, None)
        .await
        .expect("vector_search");

    assert_eq!(
        results.len(),
        5,
        "expected exactly 5 results, got {}",
        results.len()
    );

    // Results must be sorted by ascending distance.
    for window in results.windows(2) {
        assert!(
            window[0].distance <= window[1].distance,
            "results not sorted by ascending distance: {} > {}",
            window[0].distance,
            window[1].distance
        );
    }
}

/// Inserts a vector, deletes it, then re-searches and asserts it does not appear.
#[tokio::test]
async fn vector_delete_removes_from_search() {
    let db = open_db().await;

    // Insert a handful of background vectors so the index has neighbours.
    for i in 0u32..10 {
        let embedding: Vec<f32> = (0..8).map(|d| ((i * 8 + d) as f32) * 0.1).collect();
        db.vector_insert("del_vecs", &format!("bg{i}"), &embedding, None)
            .await
            .expect("vector_insert background");
    }

    // Insert the target vector close to the query we will use.
    let target: Vec<f32> = vec![1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0];
    db.vector_insert("del_vecs", "target", &target, None)
        .await
        .expect("vector_insert target");

    // Confirm it appears before deletion.
    let before = db
        .vector_search("del_vecs", &target, 5, None)
        .await
        .expect("vector_search before delete");
    assert!(
        before.iter().any(|r| r.id == "target"),
        "target should appear in search results before deletion"
    );

    // Delete and re-search.
    db.vector_delete("del_vecs", "target")
        .await
        .expect("vector_delete");

    let after = db
        .vector_search("del_vecs", &target, 5, None)
        .await
        .expect("vector_search after delete");
    assert!(
        !after.iter().any(|r| r.id == "target"),
        "target must not appear in search results after deletion"
    );
}
