// SPDX-License-Identifier: Apache-2.0

//! Batch document ingest tests.
//!
//! Verifies that `document_put_with_vector_batch_impl` correctly writes all
//! documents (queryable via `document_get`), indexes their vectors (queryable
//! via vector search), and advances the CRDT version vector by producing
//! exactly one pending delta for the entire batch.

use nodedb_client::NodeDb;
use nodedb_lite::storage::pagedb_storage::PagedbStorageMem;
use nodedb_lite::{BatchItem, NodeDbLite};
use nodedb_types::document::Document;

async fn open_db() -> NodeDbLite<PagedbStorageMem> {
    let storage = PagedbStorageMem::open_in_memory()
        .await
        .expect("open in-memory storage");
    NodeDbLite::open(storage, 1).await.expect("open NodeDbLite")
}

fn make_doc(id: &str, content: &str) -> Document {
    let mut doc = Document::new(id);
    doc.set(
        "content",
        nodedb_types::value::Value::String(content.to_owned()),
    );
    doc
}

fn make_embedding(dim: usize, seed: f32) -> Vec<f32> {
    (0..dim).map(|i| seed + i as f32 * 0.01).collect()
}

#[tokio::test]
async fn batch_100_docs_all_queryable() {
    let db = open_db().await;

    let docs: Vec<Document> = (0..100)
        .map(|i| make_doc(&format!("doc{i:03}"), &format!("content {i}")))
        .collect();
    let embeddings: Vec<Vec<f32>> = (0..100).map(|i| make_embedding(8, i as f32)).collect();

    let items: Vec<BatchItem<'_>> = docs
        .iter()
        .zip(embeddings.iter())
        .map(|(doc, emb)| BatchItem {
            doc_collection: "docs",
            doc: doc.clone(),
            vector_collection: "vecs",
            id: doc.id.as_str(),
            embedding: Some(emb.as_slice()),
        })
        .collect();

    let ids = db
        .document_put_with_vector_batch_impl(&items)
        .await
        .expect("batch put");

    assert_eq!(ids.len(), 100, "should return one ID per item");

    // All documents must be readable.
    for i in 0..100usize {
        let id = format!("doc{i:03}");
        let doc = db
            .document_get("docs", &id)
            .await
            .expect("document_get")
            .unwrap_or_else(|| panic!("doc {id} not found after batch insert"));
        assert_eq!(doc.id, id);
    }
}

#[tokio::test]
async fn batch_produces_one_crdt_delta() {
    let db = open_db().await;

    let docs: Vec<Document> = (0..50)
        .map(|i| make_doc(&format!("d{i}"), &format!("text {i}")))
        .collect();
    let embeddings: Vec<Vec<f32>> = (0..50).map(|i| make_embedding(4, i as f32)).collect();

    let items: Vec<BatchItem<'_>> = docs
        .iter()
        .zip(embeddings.iter())
        .map(|(doc, emb)| BatchItem {
            doc_collection: "col",
            doc: doc.clone(),
            vector_collection: "col_vec",
            id: doc.id.as_str(),
            embedding: Some(emb.as_slice()),
        })
        .collect();

    let deltas_before = db
        .pending_crdt_deltas()
        .expect("pending_crdt_deltas before")
        .len();

    db.document_put_with_vector_batch_impl(&items)
        .await
        .expect("batch put");

    // Exactly one new pending delta should have been added for the entire batch.
    let deltas_after = db
        .pending_crdt_deltas()
        .expect("pending_crdt_deltas after")
        .len();

    assert_eq!(
        deltas_after,
        deltas_before + 1,
        "batch of 50 docs should produce exactly one CRDT delta, \
         got {deltas_after} (was {deltas_before})"
    );
}

#[tokio::test]
async fn batch_vectors_searchable() {
    let db = open_db().await;

    let docs: Vec<Document> = (0..10)
        .map(|i| make_doc(&format!("e{i}"), &format!("entry {i}")))
        .collect();

    // Make the first embedding a unit vector so it ranks first.
    let mut embeddings: Vec<Vec<f32>> = (0..10).map(|i| make_embedding(4, i as f32)).collect();
    embeddings[0] = vec![1.0, 0.0, 0.0, 0.0];

    let items: Vec<BatchItem<'_>> = docs
        .iter()
        .zip(embeddings.iter())
        .map(|(doc, emb)| BatchItem {
            doc_collection: "vec_entries",
            doc: doc.clone(),
            vector_collection: "vec_entries",
            id: doc.id.as_str(),
            embedding: Some(emb.as_slice()),
        })
        .collect();

    db.document_put_with_vector_batch_impl(&items)
        .await
        .expect("batch put");

    let query = vec![1.0f32, 0.0, 0.0, 0.0];
    let results = db
        .vector_search("vec_entries", &query, 3, None, None)
        .await
        .expect("vector_search");

    assert!(
        !results.is_empty(),
        "vector search should return results after batch insert"
    );
    assert_eq!(
        results[0].id, "e0",
        "closest vector should be e0 (exact match)"
    );
}

#[tokio::test]
async fn batch_without_embeddings() {
    let db = open_db().await;

    let docs: Vec<Document> = (0..20)
        .map(|i| make_doc(&format!("p{i}"), &format!("plain {i}")))
        .collect();

    let items: Vec<BatchItem<'_>> = docs
        .iter()
        .map(|doc| BatchItem {
            doc_collection: "plain",
            doc: doc.clone(),
            vector_collection: "plain",
            id: doc.id.as_str(),
            embedding: None,
        })
        .collect();

    let ids = db
        .document_put_with_vector_batch_impl(&items)
        .await
        .expect("batch put without embeddings");

    assert_eq!(ids.len(), 20);

    for i in 0..20usize {
        let id = format!("p{i}");
        assert!(
            db.document_get("plain", &id).await.expect("get").is_some(),
            "doc {id} should exist"
        );
    }
}
