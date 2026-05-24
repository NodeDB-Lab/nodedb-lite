// SPDX-License-Identifier: Apache-2.0

//! Integration tests: `vector_id_map` survives flush → close → reopen.
//!
//! Before this fix, the `vector_id_map` (which maps HNSW integer IDs back to
//! user-supplied doc_ids) was never persisted. After any restart, vector_search
//! would fall back to returning HNSW integer strings ("0", "1", ...) instead of
//! real doc_ids. These tests verify that the fix holds.
//!
//! Vectors require an explicit `flush()` to persist (HNSW is a checkpoint-only
//! index with no per-insert durability path). The id_map follows the same contract.

use nodedb_client::NodeDb;
use nodedb_lite::{NodeDbLite, PagedbStorageDefault};

fn make_embedding(seed: f32, dim: usize) -> Vec<f32> {
    (0..dim).map(|i| seed + i as f32 * 0.001).collect()
}

#[tokio::test]
async fn vector_search_returns_real_doc_id_after_flush_and_reopen() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().to_path_buf();

    // ── Write + flush ──────────────────────────────────────────────────────────
    {
        let storage = PagedbStorageDefault::open(&path).await.unwrap();
        let db = NodeDbLite::open(storage, 1).await.unwrap();

        let embedding = make_embedding(0.1, 384);
        db.vector_insert("embeds", "my-real-doc-id", &embedding, None)
            .await
            .unwrap();

        // Explicit flush: HNSW checkpoint + id_map land on disk.
        db.flush().await.unwrap();
    }

    // ── Reopen + search ────────────────────────────────────────────────────────
    let storage = PagedbStorageDefault::open(&path).await.unwrap();
    let db = NodeDbLite::open(storage, 1).await.unwrap();

    let query = make_embedding(0.1, 384);
    let results = db.vector_search("embeds", &query, 5, None).await.unwrap();

    assert!(
        !results.is_empty(),
        "vector_search must return the indexed embedding after reopen"
    );
    assert_eq!(
        results[0].id, "my-real-doc-id",
        "vector_search must return the REAL doc_id after reopen, \
         not an HNSW integer like \"0\" — got {:?}",
        results[0].id
    );
}

#[tokio::test]
async fn vector_search_multiple_collections_preserve_ids_after_reopen() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().to_path_buf();

    // ── Write two collections with two docs each, flush ────────────────────────
    {
        let storage = PagedbStorageDefault::open(&path).await.unwrap();
        let db = NodeDbLite::open(storage, 1).await.unwrap();

        // alpha: doc-a0 and doc-a1
        for (i, id) in ["doc-a0", "doc-a1"].iter().enumerate() {
            let emb = make_embedding(1.0 + i as f32 * 10.0, 64);
            db.vector_insert("alpha", id, &emb, None).await.unwrap();
        }

        // beta: doc-b0 and doc-b1
        for (i, id) in ["doc-b0", "doc-b1"].iter().enumerate() {
            let emb = make_embedding(100.0 + i as f32 * 10.0, 64);
            db.vector_insert("beta", id, &emb, None).await.unwrap();
        }

        db.flush().await.unwrap();
    }

    // ── Reopen + verify each collection independently ──────────────────────────
    let storage = PagedbStorageDefault::open(&path).await.unwrap();
    let db = NodeDbLite::open(storage, 1).await.unwrap();

    // Query close to doc-a0's embedding.
    let query_a = make_embedding(1.0, 64);
    let results_a = db.vector_search("alpha", &query_a, 2, None).await.unwrap();
    assert!(
        !results_a.is_empty(),
        "alpha search must return results after reopen"
    );
    let ids_a: Vec<&str> = results_a.iter().map(|r| r.id.as_str()).collect();
    for id in &ids_a {
        assert!(
            id.starts_with("doc-a"),
            "alpha results must have doc-a* ids, not HNSW integers or beta ids — got {id}"
        );
    }

    // Query close to doc-b0's embedding.
    let query_b = make_embedding(100.0, 64);
    let results_b = db.vector_search("beta", &query_b, 2, None).await.unwrap();
    assert!(
        !results_b.is_empty(),
        "beta search must return results after reopen"
    );
    let ids_b: Vec<&str> = results_b.iter().map(|r| r.id.as_str()).collect();
    for id in &ids_b {
        assert!(
            id.starts_with("doc-b"),
            "beta results must have doc-b* ids, not HNSW integers or alpha ids — got {id}"
        );
    }
}
