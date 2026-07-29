// SPDX-License-Identifier: Apache-2.0
//! Vectors must be as durable as the documents they belong to.
//!
//! Regression coverage for a defect where a vector lived ONLY in the in-memory
//! HNSW until some later flush wrote the `vec/hnsw/<collection>` segment. An
//! acknowledged write could therefore lose its vector on an unclean exit, and
//! because the segment was the only copy, an unreadable segment was
//! unrecoverable — the CRDT holds `embedding_dim`, never the floats.

use nodedb_client::NodeDb;
use nodedb_lite::NodeDbLite;
use nodedb_types::document::Document;
use nodedb_types::value::Value;

const COLLECTION: &str = "docs";

fn doc(id: &str) -> Document {
    let mut d = Document::new(COLLECTION);
    d.id = id.to_string();
    d.set("body", Value::String("hello".into()));
    d
}

/// The core contract: a vector written with its document is durable
/// IMMEDIATELY, without any flush. Reopening the database must still find it.
#[tokio::test]
async fn vector_is_durable_without_an_explicit_flush() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path();
    let vector: Vec<f32> = (0..8).map(|i| i as f32).collect();

    {
        let db = NodeDbLite::open_at_path(path, 1, nodedb_lite::Encryption::Plaintext)
            .await
            .unwrap();
        db.document_put_with_vector(COLLECTION, doc("a"), COLLECTION, "a", &vector)
            .await
            .unwrap();
        // Deliberately NO flush: this models a process that dies between the
        // acknowledged write and the next flush tick.
        drop(db);
    }

    let db = NodeDbLite::open_at_path(path, 1, nodedb_lite::Encryption::Plaintext)
        .await
        .unwrap();
    let hits = db
        .vector_search(COLLECTION, &vector, 5, None, None)
        .await
        .unwrap();

    assert!(
        hits.iter().any(|h| h.id == "a"),
        "a vector written with its document must survive reopen WITHOUT a flush; \
         got {hits:?}. If this fails the vector is once again reachable only \
         through the in-memory HNSW."
    );
}

/// A flushed database must of course also round-trip — this is the path that
/// goes through the pagedb vector segment rather than the rebuild.
#[tokio::test]
async fn vector_survives_reopen_after_flush() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path();
    let vector: Vec<f32> = (0..8).map(|i| (8 - i) as f32).collect();

    {
        let db = NodeDbLite::open_at_path(path, 1, nodedb_lite::Encryption::Plaintext)
            .await
            .unwrap();
        db.document_put_with_vector(COLLECTION, doc("b"), COLLECTION, "b", &vector)
            .await
            .unwrap();
        db.flush().await.unwrap();
        drop(db);
    }

    let db = NodeDbLite::open_at_path(path, 1, nodedb_lite::Encryption::Plaintext)
        .await
        .unwrap();
    let hits = db
        .vector_search(COLLECTION, &vector, 5, None, None)
        .await
        .unwrap();

    assert!(
        hits.iter().any(|h| h.id == "b"),
        "a flushed vector must survive reopen; got {hits:?}"
    );
}

/// Search must return DOCUMENT ids after a reopen, not HNSW integer ids. A
/// rebuild reassigns internal ids, so a stale persisted id map would silently
/// map hits to the wrong documents.
#[tokio::test]
async fn reopened_search_returns_document_ids() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path();

    {
        let db = NodeDbLite::open_at_path(path, 1, nodedb_lite::Encryption::Plaintext)
            .await
            .unwrap();
        for (i, id) in ["x", "y", "z"].iter().enumerate() {
            let v: Vec<f32> = (0..8).map(|k| (k + i) as f32).collect();
            db.document_put_with_vector(COLLECTION, doc(id), COLLECTION, id, &v)
                .await
                .unwrap();
        }
        drop(db);
    }

    let db = NodeDbLite::open_at_path(path, 1, nodedb_lite::Encryption::Plaintext)
        .await
        .unwrap();
    let probe: Vec<f32> = (0..8).map(|k| k as f32).collect();
    let hits = db
        .vector_search(COLLECTION, &probe, 3, None, None)
        .await
        .unwrap();

    assert!(!hits.is_empty(), "expected hits after reopen");
    for h in &hits {
        assert!(
            ["x", "y", "z"].contains(&h.id.as_str()),
            "hit id {:?} is not a document id — the id map is stale or missing",
            h.id
        );
    }
}

/// A deleted vector must NOT come back when the index is rebuilt. The in-memory
/// HNSW tombstone does not survive a reopen, so if the durable row outlived the
/// delete the rebuild would resurrect it.
#[tokio::test]
async fn deleted_vector_does_not_resurrect_on_rebuild() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path();
    let vector: Vec<f32> = (0..8).map(|i| i as f32).collect();

    {
        let db = NodeDbLite::open_at_path(path, 1, nodedb_lite::Encryption::Plaintext)
            .await
            .unwrap();
        db.document_put_with_vector(COLLECTION, doc("gone"), COLLECTION, "gone", &vector)
            .await
            .unwrap();
        db.vector_delete(COLLECTION, "gone").await.unwrap();
        drop(db);
    }

    let db = NodeDbLite::open_at_path(path, 1, nodedb_lite::Encryption::Plaintext)
        .await
        .unwrap();
    let hits = db
        .vector_search(COLLECTION, &vector, 5, None, None)
        .await
        .unwrap();

    assert!(
        !hits.iter().any(|h| h.id == "gone"),
        "a deleted vector must not be resurrected by the rebuild; got {hits:?}"
    );
}
