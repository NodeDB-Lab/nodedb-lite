// SPDX-License-Identifier: Apache-2.0

//! Sparse-vector search integration tests.
//!
//! Exercises the public API only: documents carrying a sparse-vector literal
//! field are indexed automatically on write, and `sparse_search` scores them by
//! dot product over the inverted index.

use nodedb_client::NodeDb;
use nodedb_lite::{Encryption, NodeDbLite, PagedbStorageDefault, PagedbStorageMem};
use nodedb_types::document::Document;
use nodedb_types::value::Value;

const COLLECTION: &str = "embeddings";
const FIELD: &str = "terms";

async fn open_mem() -> NodeDbLite<PagedbStorageMem> {
    let storage = PagedbStorageMem::open_in_memory()
        .await
        .expect("open in-memory storage");
    NodeDbLite::open(storage, 7).await.expect("open NodeDbLite")
}

/// A document whose `terms` field holds a sparse-vector literal.
fn sparse_doc(id: &str, literal: &str) -> Document {
    let mut doc = Document::new(id);
    doc.set(FIELD, Value::String(literal.to_string()));
    doc.set("label", Value::String(format!("document {id}")));
    doc
}

fn ids(hits: &[(String, f32)]) -> Vec<&str> {
    hits.iter().map(|(id, _)| id.as_str()).collect()
}

#[tokio::test]
async fn search_returns_top_k_ordered_by_dot_product() {
    let db = open_mem().await;

    // Query is `{1: 1.0}`, so scores are exactly each document's weight on
    // dimension 1: high=4.0, mid=2.0, low=0.5.
    db.document_put(COLLECTION, sparse_doc("high", "{1: 4.0, 9: 0.1}"))
        .await
        .expect("put high");
    db.document_put(COLLECTION, sparse_doc("mid", "{1: 2.0}"))
        .await
        .expect("put mid");
    db.document_put(COLLECTION, sparse_doc("low", "{1: 0.5, 4: 3.0}"))
        .await
        .expect("put low");

    let hits = db
        .sparse_search(COLLECTION, FIELD, &[(1, 1.0)], 10)
        .expect("sparse_search");

    assert_eq!(ids(&hits), vec!["high", "mid", "low"]);
    assert!((hits[0].1 - 4.0).abs() < 1e-6, "score was {}", hits[0].1);
    assert!((hits[2].1 - 0.5).abs() < 1e-6, "score was {}", hits[2].1);

    // top_k truncates from the head of the ranking.
    let top_two = db
        .sparse_search(COLLECTION, FIELD, &[(1, 1.0)], 2)
        .expect("sparse_search top 2");
    assert_eq!(ids(&top_two), vec!["high", "mid"]);
}

#[tokio::test]
async fn document_sharing_no_dimension_is_excluded() {
    let db = open_mem().await;

    db.document_put(COLLECTION, sparse_doc("overlapping", "{1: 1.0}"))
        .await
        .expect("put overlapping");
    db.document_put(COLLECTION, sparse_doc("disjoint", "{777: 9.0}"))
        .await
        .expect("put disjoint");

    let hits = db
        .sparse_search(COLLECTION, FIELD, &[(1, 1.0)], 10)
        .expect("sparse_search");

    assert_eq!(
        ids(&hits),
        vec!["overlapping"],
        "a document with no shared dimension scores 0 and must not be returned"
    );
}

#[tokio::test]
async fn upsert_replaces_postings_instead_of_duplicating() {
    let db = open_mem().await;

    db.document_put(COLLECTION, sparse_doc("d1", "{1: 1.0, 2: 1.0}"))
        .await
        .expect("first put");
    db.document_put(COLLECTION, sparse_doc("d1", "{1: 5.0}"))
        .await
        .expect("second put");

    let hits = db
        .sparse_search(COLLECTION, FIELD, &[(1, 1.0)], 10)
        .expect("sparse_search dim 1");
    assert_eq!(hits.len(), 1, "re-indexing must not duplicate the document");
    assert!((hits[0].1 - 5.0).abs() < 1e-6, "score was {}", hits[0].1);

    let dropped = db
        .sparse_search(COLLECTION, FIELD, &[(2, 1.0)], 10)
        .expect("sparse_search dim 2");
    assert!(
        dropped.is_empty(),
        "a dimension dropped by the new vector must leave no stale posting"
    );
}

#[tokio::test]
async fn delete_removes_document_from_results() {
    let db = open_mem().await;

    db.document_put(COLLECTION, sparse_doc("keep", "{1: 1.0}"))
        .await
        .expect("put keep");
    db.document_put(COLLECTION, sparse_doc("drop", "{1: 2.0}"))
        .await
        .expect("put drop");

    db.document_delete(COLLECTION, "drop")
        .await
        .expect("document_delete");

    let hits = db
        .sparse_search(COLLECTION, FIELD, &[(1, 1.0)], 10)
        .expect("sparse_search");
    assert_eq!(ids(&hits), vec!["keep"]);
}

#[tokio::test]
async fn explicit_insert_and_delete_api_round_trip() {
    let db = open_mem().await;

    db.sparse_insert("manual", "vec", "a", &[(3, 1.0), (8, 0.5)])
        .expect("sparse_insert a");
    db.sparse_insert("manual", "vec", "b", &[(3, 0.25)])
        .expect("sparse_insert b");

    let hits = db
        .sparse_search("manual", "vec", &[(3, 1.0)], 10)
        .expect("sparse_search");
    assert_eq!(ids(&hits), vec!["a", "b"]);

    assert!(db.sparse_delete("manual", "vec", "a"));
    assert!(
        !db.sparse_delete("manual", "vec", "a"),
        "deleting an absent document reports false"
    );

    let hits = db
        .sparse_search("manual", "vec", &[(3, 1.0)], 10)
        .expect("sparse_search after delete");
    assert_eq!(ids(&hits), vec!["b"]);
}

#[tokio::test]
async fn search_on_unknown_collection_is_empty_not_an_error() {
    let db = open_mem().await;
    let hits = db
        .sparse_search("never_written", FIELD, &[(1, 1.0)], 10)
        .expect("sparse_search must not error on an unindexed collection");
    assert!(hits.is_empty());
}

#[tokio::test]
async fn non_finite_query_weight_is_rejected() {
    let db = open_mem().await;
    assert!(
        db.sparse_search(COLLECTION, FIELD, &[(1, f32::NAN)], 10)
            .is_err(),
        "a non-finite query weight must be a typed error, not a silent empty result"
    );
}

#[tokio::test]
async fn index_survives_flush_close_and_reopen() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("sparse_test.db");

    let pre_restart: Vec<(String, f32)>;

    // ── First open: write, search, flush, drop ───────────────────────────────
    {
        let storage = PagedbStorageDefault::open(&path, Encryption::Plaintext)
            .await
            .expect("open storage");
        let db = NodeDbLite::open(storage, 42)
            .await
            .expect("open NodeDbLite");

        for (id, literal) in [
            ("a", "{1: 3.0, 2: 1.0}"),
            ("b", "{1: 2.0}"),
            ("c", "{5: 9.0}"),
        ] {
            db.document_put(COLLECTION, sparse_doc(id, literal))
                .await
                .expect("document_put");
        }

        pre_restart = db
            .sparse_search(COLLECTION, FIELD, &[(1, 1.0), (2, 1.0)], 10)
            .expect("sparse_search before flush");
        assert_eq!(ids(&pre_restart), vec!["a", "b"]);

        db.flush().await.expect("flush");
    }

    // ── Second open: identical results, loaded from the checkpoint ───────────
    {
        let storage = PagedbStorageDefault::open(&path, Encryption::Plaintext)
            .await
            .expect("reopen storage");
        let db = NodeDbLite::open(storage, 42)
            .await
            .expect("reopen NodeDbLite");

        let post_restart = db
            .sparse_search(COLLECTION, FIELD, &[(1, 1.0), (2, 1.0)], 10)
            .expect("sparse_search after restart");

        assert_eq!(
            ids(&post_restart),
            ids(&pre_restart),
            "reopen must return the same ranking without a rebuild"
        );
        for (before, after) in pre_restart.iter().zip(post_restart.iter()) {
            assert!(
                (before.1 - after.1).abs() < 1e-6,
                "score drifted across restart: {} vs {}",
                before.1,
                after.1
            );
        }

        // Writes after the restart still land in the restored index.
        db.document_put(COLLECTION, sparse_doc("d", "{1: 100.0}"))
            .await
            .expect("document_put after restart");
        let hits = db
            .sparse_search(COLLECTION, FIELD, &[(1, 1.0)], 10)
            .expect("sparse_search after post-restart write");
        assert_eq!(ids(&hits), vec!["d", "a", "b"]);
    }
}
