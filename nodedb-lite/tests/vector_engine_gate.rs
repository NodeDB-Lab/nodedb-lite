// SPDX-License-Identifier: Apache-2.0

//! §16 Vector engine gate tests.
//!
//! Covers HNSW + FP32 local-correctness for NodeDB-Lite 0.1.0 beta.
//! Quantization / IVF-PQ / hybrid / distributed are out of scope.

use nodedb_client::NodeDb;
use nodedb_lite::{NodeDbLite, PagedbStorageMem};

async fn open_db() -> std::sync::Arc<NodeDbLite<PagedbStorageMem>> {
    let storage = PagedbStorageMem::open_in_memory()
        .await
        .expect("open in-memory storage");
    NodeDbLite::open(storage).await.expect("open NodeDbLite")
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
        .vector_search("gate_vecs", &query, 5, None, None)
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
        .vector_search("del_vecs", &target, 5, None, None)
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
        .vector_search("del_vecs", &target, 5, None, None)
        .await
        .expect("vector_search after delete");
    assert!(
        !after.iter().any(|r| r.id == "target"),
        "target must not appear in search results after deletion"
    );
}

/// When `allowed_ids` is `Some`, vector_search must return only documents
/// whose IDs are in the set regardless of pure vector similarity ranking.
#[tokio::test]
async fn vector_search_allowed_ids_filters_to_set() {
    use std::collections::HashSet;

    let db = open_db().await;

    // Insert two vectors with nearly identical embeddings.
    let emb: Vec<f32> = vec![1.0, 0.0, 0.0, 0.0];
    db.vector_insert("filter_vecs", "in-set", &emb, None)
        .await
        .expect("insert in-set");
    db.vector_insert("filter_vecs", "out-of-set", &emb, None)
        .await
        .expect("insert out-of-set");

    let allowed: HashSet<String> = std::iter::once("in-set".to_string()).collect();
    let results = db
        .vector_search("filter_vecs", &emb, 10, None, Some(&allowed))
        .await
        .expect("vector_search with allowed_ids");

    let ids: Vec<&str> = results.iter().map(|r| r.id.as_str()).collect();
    assert!(
        ids.contains(&"in-set"),
        "in-set must appear in results, got: {ids:?}"
    );
    assert!(
        !ids.contains(&"out-of-set"),
        "out-of-set must be excluded by allowed_ids filter, got: {ids:?}"
    );
}

// ── A vector is an attachment to the row with the same collection and id ──

fn text_doc(id: &str, field: &str, text: &str) -> nodedb_types::document::Document {
    let mut doc = nodedb_types::document::Document::new(id);
    doc.set(field, nodedb_types::value::Value::String(text.to_owned()));
    doc
}

fn text(value: &str) -> nodedb_types::value::Value {
    nodedb_types::value::Value::String(value.to_owned())
}

/// Inserting a vector for an id that already has a document merges the
/// vector's fields into that row; the document's fields stay.
#[tokio::test]
async fn vector_insert_on_existing_document_keeps_document_fields() {
    let db = open_db().await;
    db.document_put("chat", text_doc("m1", "title", "hello"))
        .await
        .expect("document_put");
    db.vector_insert("chat", "m1", &[1.0, 0.0, 0.0], None)
        .await
        .expect("vector_insert");

    let row = db
        .document_get("chat", "m1")
        .await
        .expect("document_get")
        .expect("row exists");
    assert_eq!(row.fields.get("title"), Some(&text("hello")));

    let hits = db
        .vector_search("chat", &[1.0, 0.0, 0.0], 1, None, None)
        .await
        .expect("vector_search");
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].id, "m1");
    assert_eq!(hits[0].metadata.get("title"), Some(&text("hello")));
    assert!(
        !hits[0].metadata.contains_key("embedding_dim"),
        "vector-internal fields stay out of result metadata"
    );
}

/// A document and its vector written together into the same collection
/// keep every document field.
#[tokio::test]
async fn document_put_with_vector_same_collection_keeps_document_fields() {
    let db = open_db().await;
    db.document_put_with_vector(
        "chat",
        text_doc("m1", "title", "hello"),
        "chat",
        "m1",
        &[0.0, 1.0, 0.0],
    )
    .await
    .expect("document_put_with_vector");

    let row = db
        .document_get("chat", "m1")
        .await
        .expect("document_get")
        .expect("row exists");
    assert_eq!(row.fields.get("title"), Some(&text("hello")));

    let hits = db
        .vector_search("chat", &[0.0, 1.0, 0.0], 1, None, None)
        .await
        .expect("vector_search");
    assert_eq!(hits.first().map(|h| h.id.as_str()), Some("m1"));
}

/// Deleting a vector detaches it: the row loses the vector fields, keeps
/// the document fields, and leaves search.
#[tokio::test]
async fn vector_delete_detaches_vector_and_keeps_document() {
    let db = open_db().await;
    db.document_put("chat", text_doc("m1", "title", "hello"))
        .await
        .expect("document_put");
    db.vector_insert("chat", "m1", &[1.0, 0.0, 0.0], None)
        .await
        .expect("vector_insert");
    db.vector_delete("chat", "m1").await.expect("vector_delete");

    let row = db
        .document_get("chat", "m1")
        .await
        .expect("document_get")
        .expect("document survives the vector delete");
    assert_eq!(row.fields.get("title"), Some(&text("hello")));
    assert!(!row.fields.contains_key("embedding_dim"));

    let hits = db
        .vector_search("chat", &[1.0, 0.0, 0.0], 5, None, None)
        .await
        .expect("vector_search");
    assert!(hits.iter().all(|h| h.id != "m1"));
}

/// A row that holds nothing but the vector goes with the vector.
#[tokio::test]
async fn vector_delete_on_vector_only_row_removes_row() {
    let db = open_db().await;
    db.vector_insert("vecs", "v1", &[1.0, 0.0, 0.0], None)
        .await
        .expect("vector_insert");
    db.vector_delete("vecs", "v1").await.expect("vector_delete");
    assert!(
        db.document_get("vecs", "v1")
            .await
            .expect("document_get")
            .is_none()
    );
}

/// Re-inserting an id merges the new metadata into the row.
#[tokio::test]
async fn repeated_vector_insert_merges_metadata() {
    let db = open_db().await;
    db.vector_insert("vecs", "v1", &[1.0, 0.0], Some(text_doc("v1", "a", "one")))
        .await
        .expect("first insert");
    db.vector_insert("vecs", "v1", &[0.0, 1.0], Some(text_doc("v1", "b", "two")))
        .await
        .expect("second insert");

    let row = db
        .document_get("vecs", "v1")
        .await
        .expect("document_get")
        .expect("row exists");
    assert_eq!(row.fields.get("a"), Some(&text("one")));
    assert_eq!(row.fields.get("b"), Some(&text("two")));
}

// ── One id holds one node ─────────────────────────────────────────────────

/// A re-inserted id appears once in search, scored against its latest
/// vector.
#[tokio::test]
async fn reinserted_id_appears_once_scored_against_latest_vector() {
    let db = open_db().await;
    db.vector_insert("vecs", "bg", &[0.0, 0.0, 1.0], None)
        .await
        .expect("background");
    db.vector_insert("vecs", "x", &[1.0, 0.0, 0.0], None)
        .await
        .expect("first x");
    db.vector_insert("vecs", "x", &[0.0, 1.0, 0.0], None)
        .await
        .expect("second x");

    let hits = db
        .vector_search("vecs", &[0.0, 1.0, 0.0], 10, None, None)
        .await
        .expect("vector_search");
    let xs: Vec<_> = hits.iter().filter(|h| h.id == "x").collect();
    assert_eq!(xs.len(), 1, "one result per id, got {hits:?}");
    assert!(
        xs[0].distance.abs() < 1e-5,
        "scored against the latest vector"
    );
    assert_eq!(hits.len(), 2, "two ids, two results");
}

/// Deleting a re-inserted id removes every vector it ever had from search.
#[tokio::test]
async fn deleting_reinserted_id_removes_it_from_search() {
    let db = open_db().await;
    db.vector_insert("vecs", "bg", &[0.0, 0.0, 1.0], None)
        .await
        .expect("background");
    db.vector_insert("vecs", "x", &[1.0, 0.0, 0.0], None)
        .await
        .expect("first x");
    db.vector_insert("vecs", "x", &[0.0, 1.0, 0.0], None)
        .await
        .expect("second x");
    db.vector_delete("vecs", "x").await.expect("vector_delete");

    for query in [[1.0, 0.0, 0.0], [0.0, 1.0, 0.0]] {
        let hits = db
            .vector_search("vecs", &query, 10, None, None)
            .await
            .expect("vector_search");
        assert!(hits.iter().all(|h| h.id != "x"), "got {hits:?}");
    }
}

// ── Index keys that share a prefix stay separate ──────────────────────────

/// `allowed_ids` resolves against the searched collection only, never a
/// collection whose name extends it.
#[tokio::test]
async fn search_with_allowed_ids_excludes_prefix_colliding_collection() {
    use std::collections::HashSet;

    let db = open_db().await;
    db.vector_insert("chat", "a", &[1.0, 0.0], None)
        .await
        .expect("chat insert");
    db.vector_insert("chat2", "b", &[1.0, 0.0], None)
        .await
        .expect("chat2 insert");

    let allowed: HashSet<String> = std::iter::once("b".to_owned()).collect();
    let hits = db
        .vector_search("chat", &[1.0, 0.0], 10, None, Some(&allowed))
        .await
        .expect("vector_search");
    assert!(hits.is_empty(), "no id of `chat` is allowed, got {hits:?}");
}

/// A metadata-filtered search returns rows of the searched collection only.
#[tokio::test]
async fn filtered_search_excludes_prefix_colliding_collection() {
    use nodedb_types::filter::MetadataFilter;

    let db = open_db().await;
    let tagged = |id: &str, tag: &str| Some(text_doc(id, "tag", tag));
    db.vector_insert("chat", "a", &[1.0, 0.0], tagged("a", "keep"))
        .await
        .expect("chat a");
    db.vector_insert("chat", "c", &[0.0, 1.0], tagged("c", "drop"))
        .await
        .expect("chat c");
    db.vector_insert("chat2", "c", &[0.0, 1.0], tagged("c", "keep"))
        .await
        .expect("chat2 c");

    let filter = MetadataFilter::Eq {
        field: "tag".into(),
        value: text("keep"),
    };
    let hits = db
        .vector_search("chat", &[0.0, 1.0], 10, Some(&filter), None)
        .await
        .expect("vector_search");
    let ids: Vec<&str> = hits.iter().map(|h| h.id.as_str()).collect();
    assert_eq!(ids, vec!["a"]);
}

/// A base-collection search never reaches the collection's named-vector
/// indexes, and a named-vector search never reaches the base index.
#[tokio::test]
async fn base_collection_search_excludes_named_field_vectors() {
    use std::collections::HashSet;

    let db = open_db().await;
    db.vector_insert("chat", "a", &[1.0, 0.0], None)
        .await
        .expect("base insert");
    db.vector_insert_field("chat", "emb", "b", &[1.0, 0.0], None)
        .await
        .expect("named insert");

    let base = db
        .vector_search("chat", &[1.0, 0.0], 10, None, None)
        .await
        .expect("base search");
    let ids: Vec<&str> = base.iter().map(|h| h.id.as_str()).collect();
    assert_eq!(ids, vec!["a"]);

    let allowed: HashSet<String> = std::iter::once("b".to_owned()).collect();
    let restricted = db
        .vector_search("chat", &[1.0, 0.0], 10, None, Some(&allowed))
        .await
        .expect("base search with allowed_ids");
    assert!(restricted.is_empty(), "got {restricted:?}");

    let named = db
        .vector_search_field("chat", "emb", &[1.0, 0.0], 10, None)
        .await
        .expect("named search");
    let ids: Vec<&str> = named.iter().map(|h| h.id.as_str()).collect();
    assert_eq!(ids, vec!["b"]);
}

// ── A detach removes only what the deleted vector owns ────────────────────

/// Deleting the base vector keeps a named vector on the same id attached,
/// with its field tag and dimension.
#[tokio::test]
async fn base_vector_delete_keeps_named_vector_fields() {
    let db = open_db().await;
    db.vector_insert("chat", "m", &[1.0, 0.0], None)
        .await
        .expect("base insert");
    db.vector_insert_field("chat", "emb", "m", &[0.0, 1.0], None)
        .await
        .expect("named insert");
    db.vector_delete("chat", "m").await.expect("base delete");

    let row = db
        .document_get("chat", "m")
        .await
        .expect("document_get")
        .expect("row keeps the named vector");
    assert_eq!(row.fields.get("__field"), Some(&text("emb")));
    assert!(row.fields.contains_key("embedding_dim"));
    let named = db
        .vector_search_field("chat", "emb", &[0.0, 1.0], 5, None)
        .await
        .expect("named search");
    assert_eq!(named.first().map(|h| h.id.as_str()), Some("m"));
}

/// Deleting a named vector keeps the base vector on the same id attached,
/// with its dimension; only the field tag goes.
#[tokio::test]
async fn named_vector_delete_keeps_base_vector_fields() {
    let db = open_db().await;
    db.vector_insert("chat", "m", &[1.0, 0.0], None)
        .await
        .expect("base insert");
    db.vector_insert_field("chat", "emb", "m", &[0.0, 1.0], None)
        .await
        .expect("named insert");
    db.vector_delete_field("chat", "emb", "m")
        .await
        .expect("named delete");

    let row = db
        .document_get("chat", "m")
        .await
        .expect("document_get")
        .expect("row keeps the base vector");
    assert!(row.fields.contains_key("embedding_dim"));
    assert!(!row.fields.contains_key("__field"));
    let named = db
        .vector_search_field("chat", "emb", &[0.0, 1.0], 5, None)
        .await
        .expect("named search");
    assert!(named.is_empty(), "got {named:?}");
    let base = db
        .vector_search("chat", &[1.0, 0.0], 5, None, None)
        .await
        .expect("base search");
    assert_eq!(base.first().map(|h| h.id.as_str()), Some("m"));
}

/// A collection whose last vector is deleted keeps serving vectors inserted
/// afterwards, including a re-insert of the deleted id.
#[tokio::test]
async fn vector_insert_after_last_vector_deleted_is_searchable() {
    let db = open_db().await;
    let v = |a: usize| -> Vec<f32> {
        let x: Vec<f32> = (0..8).map(|i| if i == a { 1.0 } else { 0.1 }).collect();
        let n = x.iter().map(|y| y * y).sum::<f32>().sqrt();
        x.into_iter().map(|y| y / n).collect()
    };
    db.vector_insert("empty_vecs", "K", &v(0), None)
        .await
        .expect("insert K");
    db.vector_delete("empty_vecs", "K").await.expect("delete K");
    for (id, a) in [("M", 2), ("P", 5), ("K", 7)] {
        db.vector_insert("empty_vecs", id, &v(a), None)
            .await
            .expect("insert after delete");
    }
    for (id, a) in [("M", 2), ("P", 5), ("K", 7)] {
        let top = db
            .vector_search("empty_vecs", &v(a), 1, None, None)
            .await
            .expect("vector_search");
        assert_eq!(
            top.first().map(|h| h.id.as_str()),
            Some(id),
            "{id} unreachable"
        );
    }
}
