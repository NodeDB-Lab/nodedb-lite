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
        let db = NodeDbLite::open_at_path(path, nodedb_lite::Encryption::Plaintext)
            .await
            .unwrap();
        db.document_put_with_vector(COLLECTION, doc("a"), COLLECTION, "a", &vector)
            .await
            .unwrap();
        // Deliberately NO flush: this models a process that dies between the
        // acknowledged write and the next flush tick.
        drop(db);
    }

    let db = NodeDbLite::open_at_path(path, nodedb_lite::Encryption::Plaintext)
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
        let db = NodeDbLite::open_at_path(path, nodedb_lite::Encryption::Plaintext)
            .await
            .unwrap();
        db.document_put_with_vector(COLLECTION, doc("b"), COLLECTION, "b", &vector)
            .await
            .unwrap();
        db.flush().await.unwrap();
        drop(db);
    }

    let db = NodeDbLite::open_at_path(path, nodedb_lite::Encryption::Plaintext)
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
        let db = NodeDbLite::open_at_path(path, nodedb_lite::Encryption::Plaintext)
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

    let db = NodeDbLite::open_at_path(path, nodedb_lite::Encryption::Plaintext)
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

/// The segment-poisoning reproducer: flush TWICE across a reopen.
///
/// After the first flush the index is persisted as a graph-only checkpoint whose
/// per-node vector storage is empty — the floats live in the pagedb segment. On
/// reopen the index is restored from that checkpoint, so the second flush must
/// source its segment payload from the durable vector rows. Reading it from the
/// restored index instead serializes empty vectors under a header that declares
/// the real count and dimension, which overwrites the good segment with a
/// corrupt one and loses every vector in the collection.
#[tokio::test]
async fn second_flush_after_graph_only_restore_preserves_vectors() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path();
    let vectors: Vec<Vec<f32>> = (0..3)
        .map(|i| (0..8).map(|k| (k + i * 8) as f32).collect())
        .collect();
    let ids = ["p", "q", "r"];

    // Generation 1: write and flush. The segment now holds the real vectors.
    {
        let db = NodeDbLite::open_at_path(path, nodedb_lite::Encryption::Plaintext)
            .await
            .unwrap();
        for (id, v) in ids.iter().zip(&vectors) {
            db.document_put_with_vector(COLLECTION, doc(id), COLLECTION, id, v)
                .await
                .unwrap();
        }
        db.flush().await.unwrap();
        drop(db);
    }

    // Generation 2: reopen (graph-only restore) and flush again WITHOUT writing
    // anything. This is the flush that used to poison the segment.
    {
        let db = NodeDbLite::open_at_path(path, nodedb_lite::Encryption::Plaintext)
            .await
            .unwrap();
        db.flush().await.unwrap();
        drop(db);
    }

    // Generation 3: every vector must still be findable.
    let db = NodeDbLite::open_at_path(path, nodedb_lite::Encryption::Plaintext)
        .await
        .unwrap();
    for (id, v) in ids.iter().zip(&vectors) {
        let hits = db
            .vector_search(COLLECTION, v, 5, None, None)
            .await
            .unwrap();
        assert!(
            hits.iter().any(|h| h.id == *id),
            "{id:?} was lost by the second flush — the segment was rewritten from \
             the restored index's empty vector slots instead of the durable rows; \
             got {hits:?}"
        );
    }
}

/// Eviction persists a collection before dropping it from memory, and takes the
/// same graph-only-checkpoint-plus-segment path as flush. It must therefore also
/// source its segment payload from the durable rows, not from the in-memory index.
#[tokio::test]
async fn eviction_after_graph_only_restore_preserves_vectors() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path();
    let vector: Vec<f32> = (0..8).map(|i| (i * 3) as f32).collect();

    {
        let db = NodeDbLite::open_at_path(path, nodedb_lite::Encryption::Plaintext)
            .await
            .unwrap();
        db.document_put_with_vector(COLLECTION, doc("e"), COLLECTION, "e", &vector)
            .await
            .unwrap();
        db.flush().await.unwrap();
        drop(db);
    }

    {
        // Reopened index has empty per-node storage; evict it in that state.
        let db = NodeDbLite::open_at_path(path, nodedb_lite::Encryption::Plaintext)
            .await
            .unwrap();
        db.vector_search(COLLECTION, &vector, 1, None, None)
            .await
            .unwrap();
        db.evict_collections(1).await.unwrap();
        drop(db);
    }

    let db = NodeDbLite::open_at_path(path, nodedb_lite::Encryption::Plaintext)
        .await
        .unwrap();
    let hits = db
        .vector_search(COLLECTION, &vector, 5, None, None)
        .await
        .unwrap();
    assert!(
        hits.iter().any(|h| h.id == "e"),
        "eviction rewrote the segment from the restored index's empty vector \
         slots instead of the durable rows; got {hits:?}"
    );
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
        let db = NodeDbLite::open_at_path(path, nodedb_lite::Encryption::Plaintext)
            .await
            .unwrap();
        db.document_put_with_vector(COLLECTION, doc("gone"), COLLECTION, "gone", &vector)
            .await
            .unwrap();
        db.vector_delete(COLLECTION, "gone").await.unwrap();
        drop(db);
    }

    let db = NodeDbLite::open_at_path(path, nodedb_lite::Encryption::Plaintext)
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
