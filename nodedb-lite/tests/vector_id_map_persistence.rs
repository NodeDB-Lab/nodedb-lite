// SPDX-License-Identifier: Apache-2.0

//! Integration tests: `vector_id_map` survives flush → close → reopen.
//!
//! The `vector_id_map` maps HNSW integer ids back to user-supplied doc_ids.
//! After a reopen, vector_search returns the real doc_ids, never HNSW integer
//! strings such as "0" or "1".
//!
//! Vectors require an explicit `flush()` to persist (HNSW is a checkpoint-only
//! index with no per-insert durability path). The id_map follows the same contract.

use nodedb_client::NodeDb;
use nodedb_lite::{Encryption, NodeDbLite, PagedbStorageDefault};

fn make_embedding(seed: f32, dim: usize) -> Vec<f32> {
    (0..dim).map(|i| seed + i as f32 * 0.001).collect()
}

#[tokio::test]
async fn vector_search_returns_real_doc_id_after_flush_and_reopen() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().to_path_buf();

    // ── Write + flush ──────────────────────────────────────────────────────────
    {
        let storage = PagedbStorageDefault::open(&path, Encryption::Plaintext)
            .await
            .unwrap();
        let db = NodeDbLite::open(storage).await.unwrap();

        let embedding = make_embedding(0.1, 384);
        db.vector_insert("embeds", "my-real-doc-id", &embedding, None)
            .await
            .unwrap();

        // Explicit flush: HNSW checkpoint + id_map land on disk.
        db.flush().await.unwrap();
    }

    // ── Reopen + search ────────────────────────────────────────────────────────
    let storage = PagedbStorageDefault::open(&path, Encryption::Plaintext)
        .await
        .unwrap();
    let db = NodeDbLite::open(storage).await.unwrap();

    let query = make_embedding(0.1, 384);
    let results = db
        .vector_search("embeds", &query, 5, None, None)
        .await
        .unwrap();

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
        let storage = PagedbStorageDefault::open(&path, Encryption::Plaintext)
            .await
            .unwrap();
        let db = NodeDbLite::open(storage).await.unwrap();

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
    let storage = PagedbStorageDefault::open(&path, Encryption::Plaintext)
        .await
        .unwrap();
    let db = NodeDbLite::open(storage).await.unwrap();

    // Query close to doc-a0's embedding.
    let query_a = make_embedding(1.0, 64);
    let results_a = db
        .vector_search("alpha", &query_a, 2, None, None)
        .await
        .unwrap();
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
    let results_b = db
        .vector_search("beta", &query_b, 2, None, None)
        .await
        .unwrap();
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

/// A re-inserted id comes back from a reopen as one node, scored against
/// its latest vector.
#[tokio::test]
async fn reinserted_id_appears_once_after_reopen() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().to_path_buf();

    {
        let storage = PagedbStorageDefault::open(&path, Encryption::Plaintext)
            .await
            .unwrap();
        let db = NodeDbLite::open(storage).await.unwrap();
        db.vector_insert("embeds", "bg", &[0.0, 0.0, 1.0], None)
            .await
            .unwrap();
        db.vector_insert("embeds", "x", &[1.0, 0.0, 0.0], None)
            .await
            .unwrap();
        db.vector_insert("embeds", "x", &[0.0, 1.0, 0.0], None)
            .await
            .unwrap();
        db.flush().await.unwrap();
    }

    let storage = PagedbStorageDefault::open(&path, Encryption::Plaintext)
        .await
        .unwrap();
    let db = NodeDbLite::open(storage).await.unwrap();
    let results = db
        .vector_search("embeds", &[0.0, 1.0, 0.0], 10, None, None)
        .await
        .unwrap();
    let ids: Vec<&str> = results.iter().map(|r| r.id.as_str()).collect();
    assert_eq!(ids.len(), 2, "one result per id, got {ids:?}");
    assert_eq!(ids[0], "x", "the latest vector of x is the nearest");
    assert!(results[0].distance.abs() < 1e-5);
}

/// Collections whose names share a prefix keep separate bindings across a
/// reopen: an `allowed_ids` search never resolves another collection's id.
#[tokio::test]
async fn prefix_colliding_collections_stay_separate_after_reopen() {
    use std::collections::HashSet;

    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().to_path_buf();

    {
        let storage = PagedbStorageDefault::open(&path, Encryption::Plaintext)
            .await
            .unwrap();
        let db = NodeDbLite::open(storage).await.unwrap();
        db.vector_insert("chat", "a", &[1.0, 0.0], None)
            .await
            .unwrap();
        db.vector_insert("chat2", "b", &[1.0, 0.0], None)
            .await
            .unwrap();
        db.flush().await.unwrap();
    }

    let storage = PagedbStorageDefault::open(&path, Encryption::Plaintext)
        .await
        .unwrap();
    let db = NodeDbLite::open(storage).await.unwrap();

    let allowed: HashSet<String> = std::iter::once("b".to_owned()).collect();
    let restricted = db
        .vector_search("chat", &[1.0, 0.0], 10, None, Some(&allowed))
        .await
        .unwrap();
    assert!(restricted.is_empty(), "got {restricted:?}");

    let all = db
        .vector_search("chat", &[1.0, 0.0], 10, None, None)
        .await
        .unwrap();
    let ids: Vec<&str> = all.iter().map(|r| r.id.as_str()).collect();
    assert_eq!(ids, vec!["a"]);
}

/// A segment backs its index positionally, so ids inserted out of key
/// order must each come back from a reopen scored against their own vector.
#[tokio::test]
async fn ids_inserted_out_of_key_order_keep_their_vectors_after_reopen() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().to_path_buf();
    let rows: [(&str, [f32; 3]); 3] = [
        ("c", [1.0, 0.0, 0.0]),
        ("a", [0.0, 1.0, 0.0]),
        ("b", [0.0, 0.0, 1.0]),
    ];

    {
        let storage = PagedbStorageDefault::open(&path, Encryption::Plaintext)
            .await
            .unwrap();
        let db = NodeDbLite::open(storage).await.unwrap();
        for (id, v) in &rows {
            db.vector_insert("ordered", id, v, None).await.unwrap();
        }
        db.flush().await.unwrap();
    }

    let storage = PagedbStorageDefault::open(&path, Encryption::Plaintext)
        .await
        .unwrap();
    let db = NodeDbLite::open(storage).await.unwrap();
    for (id, v) in &rows {
        let hits = db.vector_search("ordered", v, 1, None, None).await.unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].id, *id, "exact query for {id} returns {id}");
        assert!(
            hits[0].distance.abs() < 1e-5,
            "{id} is scored against its own vector, got {}",
            hits[0].distance
        );
    }
}

/// A base index rebuilt from durable rows on reopen holds only base
/// vectors; the named index is rebuilt from its own rows.
#[tokio::test]
async fn base_rebuild_after_reopen_excludes_named_field_vectors() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().to_path_buf();

    {
        let storage = PagedbStorageDefault::open(&path, Encryption::Plaintext)
            .await
            .unwrap();
        let db = NodeDbLite::open(storage).await.unwrap();
        db.vector_insert("chat", "a", &[1.0, 0.0], None)
            .await
            .unwrap();
        db.vector_insert_field("chat", "emb", "b", &[1.0, 0.0], None)
            .await
            .unwrap();
        // No flush: the reopen rebuilds both indexes from durable rows.
    }

    let storage = PagedbStorageDefault::open(&path, Encryption::Plaintext)
        .await
        .unwrap();
    let db = NodeDbLite::open(storage).await.unwrap();
    let base = db
        .vector_search("chat", &[1.0, 0.0], 10, None, None)
        .await
        .unwrap();
    let ids: Vec<&str> = base.iter().map(|r| r.id.as_str()).collect();
    assert_eq!(ids, vec!["a"]);
    let named = db
        .vector_search_field("chat", "emb", &[1.0, 0.0], 10, None)
        .await
        .unwrap();
    let ids: Vec<&str> = named.iter().map(|r| r.id.as_str()).collect();
    assert_eq!(ids, vec!["b"]);
}

async fn open_db(path: &std::path::Path) -> std::sync::Arc<NodeDbLite<PagedbStorageDefault>> {
    let storage = PagedbStorageDefault::open(path, Encryption::Plaintext)
        .await
        .unwrap();
    NodeDbLite::open(storage).await.unwrap()
}

/// Asserts that an exact query for `vector` returns `id` at distance ~0.
async fn assert_found(
    db: &NodeDbLite<PagedbStorageDefault>,
    collection: &str,
    id: &str,
    vector: &[f32],
    tolerance: f32,
) {
    let hits = db
        .vector_search(collection, vector, 1, None, None)
        .await
        .unwrap();
    assert_eq!(hits.len(), 1, "query for {id} returns one hit");
    assert_eq!(
        hits[0].id, id,
        "exact query for {id} returns {id} (top hit distance {})",
        hits[0].distance
    );
    assert!(
        hits[0].distance.abs() < tolerance,
        "{id} is scored against its own vector, got {}",
        hits[0].distance
    );
}

/// Ids inserted after a reopen join the restored index and survive a
/// second reopen.
#[tokio::test]
async fn vectors_inserted_after_reopen_are_found_and_survive_a_second_reopen() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().to_path_buf();
    let rows: [(&str, [f32; 5]); 5] = [
        ("c", [1.0, 0.0, 0.0, 0.0, 0.0]),
        ("a", [0.0, 1.0, 0.0, 0.0, 0.0]),
        ("b", [0.0, 0.0, 1.0, 0.0, 0.0]),
        ("d", [0.0, 0.0, 0.0, 1.0, 0.0]),
        ("e", [0.0, 0.0, 0.0, 0.0, 1.0]),
    ];

    {
        let db = open_db(&path).await;
        for (id, v) in &rows[..3] {
            db.vector_insert("grow", id, v, None).await.unwrap();
        }
        db.flush().await.unwrap();
    }

    {
        let db = open_db(&path).await;
        for (id, v) in &rows[3..] {
            db.vector_insert("grow", id, v, None).await.unwrap();
        }
        for (id, v) in &rows {
            assert_found(&db, "grow", id, v, 1e-5).await;
        }
        db.flush().await.unwrap();
    }

    let db = open_db(&path).await;
    for (id, v) in &rows {
        assert_found(&db, "grow", id, v, 1e-5).await;
    }
}

const BULK_COUNT: usize = 200;
const BULK_DIM: usize = 64;

fn splitmix64(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// `BULK_COUNT` distinct unit vectors paired with ids `id-0`..`id-199`,
/// ordered lexicographically by id.
fn bulk_rows() -> Vec<(String, Vec<f32>)> {
    let mut state = 0x5EED_u64;
    let mut rows: Vec<(String, Vec<f32>)> = (0..BULK_COUNT)
        .map(|i| {
            let raw: Vec<f32> = (0..BULK_DIM)
                .map(|_| (splitmix64(&mut state) >> 40) as f32 / (1u64 << 24) as f32 - 0.5)
                .collect();
            let norm = raw.iter().map(|x| x * x).sum::<f32>().sqrt();
            (format!("id-{i}"), raw.iter().map(|x| x / norm).collect())
        })
        .collect();
    rows.sort_by(|a, b| a.0.cmp(&b.0));
    rows
}

/// Inserts the bulk rows in lexicographic id order, deletes the row at
/// `delete_position`, flushes, reopens, and checks that every remaining id
/// still pairs with its own vector.
async fn delete_then_reopen_keeps_ids_paired(collection: &str, delete_position: usize) {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().to_path_buf();
    let rows = bulk_rows();
    let deleted = rows[delete_position].0.clone();

    {
        let db = open_db(&path).await;
        for (id, v) in &rows {
            db.vector_insert(collection, id, v, None).await.unwrap();
        }
        db.vector_delete(collection, &deleted).await.unwrap();
        db.flush().await.unwrap();
    }

    let db = open_db(&path).await;
    for (id, v) in &rows {
        if *id == deleted {
            let hits = db
                .vector_search(collection, v, BULK_COUNT, None, None)
                .await
                .unwrap();
            assert!(
                hits.iter().all(|h| h.id != deleted),
                "deleted id {deleted} is absent after reopen"
            );
        } else {
            assert_found(&db, collection, id, v, 1e-4).await;
        }
    }

    let extra = extra_vector();
    db.vector_insert(collection, "extra", &extra, None)
        .await
        .unwrap();
    assert_found(&db, collection, "extra", &extra, 1e-4).await;
}

/// A tombstone before the flush leaves the remaining ids paired with their
/// own vectors after a reopen.
#[tokio::test]
async fn delete_before_flush_then_reopen_keeps_remaining_ids_paired() {
    delete_then_reopen_keeps_ids_paired("bulk_early", 10).await;
}

/// A tombstone in the middle of the insertion order leaves the ids on both
/// sides paired with their own vectors after a reopen.
#[tokio::test]
async fn tombstoned_node_in_the_middle_keeps_remaining_ids_paired_after_reopen() {
    delete_then_reopen_keeps_ids_paired("bulk_middle", BULK_COUNT / 2).await;
}

/// The unit vector along the first axis, distinct from every bulk row.
fn extra_vector() -> Vec<f32> {
    (0..BULK_DIM)
        .map(|i| if i == 0 { 1.0 } else { 0.0 })
        .collect()
}

/// A vector inserted into a restored index with no tombstones is reachable,
/// and it and every bulk id survive a second reopen.
#[tokio::test]
async fn vectors_inserted_after_reopen_are_found_without_a_delete() {
    const COLLECTION: &str = "bulk_no_delete";
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().to_path_buf();
    let rows = bulk_rows();
    let extra = extra_vector();

    {
        let db = open_db(&path).await;
        for (id, v) in &rows {
            db.vector_insert(COLLECTION, id, v, None).await.unwrap();
        }
        db.flush().await.unwrap();
    }

    {
        let db = open_db(&path).await;
        db.vector_insert(COLLECTION, "extra", &extra, None)
            .await
            .unwrap();
        assert_found(&db, COLLECTION, "extra", &extra, 1e-4).await;
        for (id, v) in &rows {
            assert_found(&db, COLLECTION, id, v, 1e-4).await;
        }
        db.flush().await.unwrap();
    }

    let db = open_db(&path).await;
    assert_found(&db, COLLECTION, "extra", &extra, 1e-4).await;
    for (id, v) in &rows {
        assert_found(&db, COLLECTION, id, v, 1e-4).await;
    }
}
