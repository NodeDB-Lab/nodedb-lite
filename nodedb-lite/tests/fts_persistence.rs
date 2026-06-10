//! Round-trip test: FTS index survives flush → close → reopen.
//!
//! Inserts N documents with text content, runs `text_search`, flushes, drops
//! the handle, reopens with the same on-disk path, and asserts that the
//! identical top-k results are returned — confirming the index loaded from
//! storage without re-tokenizing source documents.

use nodedb_client::NodeDb;
use nodedb_lite::storage::engine::StorageEngine;
use nodedb_lite::{NodeDbLite, PagedbStorageDefault};
use nodedb_types::document::Document;
use nodedb_types::text_search::TextSearchParams;
use nodedb_types::value::Value;

const COLLECTION: &str = "articles";
const DOC_COUNT: usize = 10;

/// Build test documents: each document has a unique id and a `body` field
/// that contains the search term "rustsearch" plus a per-doc identifier.
fn make_doc(i: usize) -> (String, Document) {
    let id = format!("doc{i}");
    let mut doc = Document::new(&id);
    doc.set(
        "body",
        Value::String(format!(
            "rustsearch document number {i} about embedded databases"
        )),
    );
    doc.set("idx", Value::Integer(i as i64));
    (id, doc)
}

#[tokio::test]
async fn fts_index_persists_across_restart() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("fts_test.db");

    let pre_restart_results: Vec<(String, f32)>; // (id, distance)

    // ── First open: insert documents, search, flush, drop ────────────────────
    {
        let storage = PagedbStorageDefault::open(&path)
            .await
            .expect("open storage");
        let db = NodeDbLite::open(storage, 42)
            .await
            .expect("open NodeDbLite");

        for i in 0..DOC_COUNT {
            let (_id, doc) = make_doc(i);
            db.document_put(COLLECTION, doc)
                .await
                .expect("document_put");
        }

        let results = db
            .text_search(
                COLLECTION,
                "body",
                "rustsearch",
                DOC_COUNT,
                TextSearchParams::default(),
                None,
            )
            .await
            .expect("text_search before flush");

        assert!(
            !results.is_empty(),
            "expected at least one result before flush"
        );

        pre_restart_results = results.iter().map(|r| (r.id.clone(), r.distance)).collect();

        db.flush().await.expect("flush");
        // db is dropped here, releasing the file lock.
    }

    // ── Second open: reopen, search, assert byte-identical results ────────────
    {
        // Sanity check: Fts namespace must have entries after flush.
        {
            use nodedb_types::Namespace;
            let storage = PagedbStorageDefault::open(&path)
                .await
                .expect("storage for fts count check");
            let fts_count = storage.count(Namespace::Fts).await.expect("fts count");
            assert!(
                fts_count > 0,
                "Namespace::Fts should have entries after flush, got 0"
            );
        }

        let storage = PagedbStorageDefault::open(&path)
            .await
            .expect("reopen storage");
        let db = NodeDbLite::open(storage, 42)
            .await
            .expect("reopen NodeDbLite");

        let results = db
            .text_search(
                COLLECTION,
                "body",
                "rustsearch",
                DOC_COUNT,
                TextSearchParams::default(),
                None,
            )
            .await
            .expect("text_search after restart");

        assert!(
            !results.is_empty(),
            "expected at least one result after restart"
        );

        let post_restart_results: Vec<(String, f32)> =
            results.iter().map(|r| (r.id.clone(), r.distance)).collect();

        assert_eq!(
            pre_restart_results.len(),
            post_restart_results.len(),
            "result count mismatch after restart"
        );

        // Doc IDs must match (order may vary by score — sort both by doc_id
        // for stable comparison).
        let mut pre_sorted = pre_restart_results.clone();
        let mut post_sorted = post_restart_results.clone();
        pre_sorted.sort_by(|a, b| a.0.cmp(&b.0));
        post_sorted.sort_by(|a, b| a.0.cmp(&b.0));

        for (pre, post) in pre_sorted.iter().zip(post_sorted.iter()) {
            assert_eq!(
                pre.0, post.0,
                "doc_id mismatch after restart: pre={}, post={}",
                pre.0, post.0
            );
            // Scores must be identical (same index state, same BM25 parameters).
            assert!(
                (pre.1 - post.1).abs() < f32::EPSILON,
                "score mismatch for {}: pre={}, post={}",
                pre.0,
                pre.1,
                post.1
            );
        }
    }
}
