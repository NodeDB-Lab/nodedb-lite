// SPDX-License-Identifier: Apache-2.0

//! Full-text search is scoped to the field the caller names.
//!
//! - A schemaless document is indexed per top-level string field, and whole.
//! - `text_search` with a field searches that field's index only. An empty
//!   field searches every string field.
//! - A field no document holds, in a collection with text, is an error. An
//!   unknown collection is not found.
//! - A field literally named `_doc` is its own index.
//! - Overwrites and deletes retract the terms the old document held.
//! - Per-field indexes survive flush and reopen.
//! - SQL `text_match(field, q)` searches the named field.
//!
//! Run with:
//!   cargo nextest run -p nodedb-lite --test fts_field_scoping

use std::sync::Arc;

use nodedb_client::NodeDb;
use nodedb_lite::{Encryption, NodeDbLite, PagedbStorageDefault, PagedbStorageMem};
use nodedb_types::document::Document;
use nodedb_types::text_search::TextSearchParams;
use nodedb_types::value::Value;

const COLLECTION: &str = "articles";

async fn open_test_db() -> Arc<NodeDbLite<PagedbStorageMem>> {
    let storage = PagedbStorageMem::open_in_memory()
        .await
        .expect("open_in_memory");
    NodeDbLite::open(storage).await.expect("NodeDbLite::open")
}

/// A document with the given string fields.
fn make_doc(id: &str, fields: &[(&str, &str)]) -> Document {
    let mut doc = Document::new(id);
    for (name, text) in fields {
        doc.set(*name, Value::String((*text).to_owned()));
    }
    doc
}

/// Ids `text_search` returns for `query` on `field`, sorted.
async fn search_ids<D: NodeDb>(db: &D, field: &str, query: &str) -> Vec<String> {
    let mut ids: Vec<String> = db
        .text_search(
            COLLECTION,
            field,
            query,
            10,
            TextSearchParams::default(),
            None,
        )
        .await
        .unwrap_or_else(|e| panic!("text_search on field {field:?} for {query:?}: {e}"))
        .into_iter()
        .map(|r| r.id)
        .collect();
    ids.sort();
    ids
}

/// Two documents whose fields hold each other's words: `d1` has "rust" in
/// `title`, `d2` has "rust" in `body`.
async fn put_crossed_docs<D: NodeDb>(db: &D) {
    db.document_put(
        COLLECTION,
        make_doc("d1", &[("title", "rust guide"), ("body", "python tips")]),
    )
    .await
    .expect("put d1");
    db.document_put(
        COLLECTION,
        make_doc(
            "d2",
            &[("title", "python cookbook"), ("body", "rust notes")],
        ),
    )
    .await
    .expect("put d2");
}

#[tokio::test]
async fn text_search_on_field_matches_only_that_field() {
    let db = open_test_db().await;
    put_crossed_docs(&*db).await;

    assert_eq!(search_ids(&*db, "title", "rust").await, vec!["d1"]);
    assert_eq!(search_ids(&*db, "body", "rust").await, vec!["d2"]);
    assert_eq!(search_ids(&*db, "title", "python").await, vec!["d2"]);
}

#[tokio::test]
async fn text_search_with_empty_field_searches_all_fields() {
    let db = open_test_db().await;
    put_crossed_docs(&*db).await;

    assert_eq!(search_ids(&*db, "", "rust").await, vec!["d1", "d2"]);
    assert_eq!(search_ids(&*db, "", "cookbook").await, vec!["d2"]);
}

#[tokio::test]
async fn text_search_on_field_without_index_returns_error() {
    let db = open_test_db().await;
    put_crossed_docs(&*db).await;

    let err = db
        .text_search(
            COLLECTION,
            "summary",
            "rust",
            10,
            TextSearchParams::default(),
            None,
        )
        .await
        .expect_err("a field no document holds as a string must be refused");
    let message = err.to_string();
    assert!(
        message.contains("summary") && message.contains(COLLECTION),
        "error must name the collection and field: {message}"
    );

    // A collection no engine or catalog knows is not found.
    let err = db
        .text_search(
            "no_such_collection",
            "",
            "rust",
            10,
            TextSearchParams::default(),
            None,
        )
        .await
        .expect_err("an unknown collection must be refused");
    assert!(
        err.to_string().contains("no_such_collection") && err.to_string().contains("not found"),
        "error must say the collection is not found: {err}"
    );
}

#[tokio::test]
async fn a_field_named_doc_does_not_collide_with_the_whole_document() {
    let db = open_test_db().await;
    db.document_put(
        COLLECTION,
        make_doc("d1", &[("_doc", "alpha"), ("title", "beta")]),
    )
    .await
    .expect("put d1");

    assert_eq!(search_ids(&*db, "_doc", "alpha").await, vec!["d1"]);
    assert!(
        search_ids(&*db, "_doc", "beta").await.is_empty(),
        "the `_doc` field index holds only that field"
    );
    assert_eq!(search_ids(&*db, "", "beta").await, vec!["d1"]);
    assert_eq!(search_ids(&*db, "", "alpha").await, vec!["d1"]);
}

#[tokio::test]
async fn overwriting_document_removes_old_field_terms() {
    let db = open_test_db().await;
    db.document_put(
        COLLECTION,
        make_doc("d1", &[("title", "alpha"), ("body", "beta")]),
    )
    .await
    .expect("first put");
    db.document_put(COLLECTION, make_doc("d1", &[("title", "gamma")]))
        .await
        .expect("overwrite");

    assert!(search_ids(&*db, "title", "alpha").await.is_empty());
    assert!(
        search_ids(&*db, "body", "beta").await.is_empty(),
        "a field the new document dropped must stop matching"
    );
    assert!(search_ids(&*db, "", "beta").await.is_empty());
    assert_eq!(search_ids(&*db, "title", "gamma").await, vec!["d1"]);
}

#[tokio::test]
async fn deleting_document_removes_field_terms() {
    let db = open_test_db().await;
    put_crossed_docs(&*db).await;
    db.document_delete(COLLECTION, "d1").await.expect("delete");

    assert!(search_ids(&*db, "title", "rust").await.is_empty());
    assert!(search_ids(&*db, "body", "python").await.is_empty());
    assert_eq!(search_ids(&*db, "", "rust").await, vec!["d2"]);
}

#[tokio::test]
async fn field_text_index_survives_reopen() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("fts_field_scoping.db");

    {
        let storage = PagedbStorageDefault::open(&path, Encryption::Plaintext)
            .await
            .expect("open storage");
        let db = NodeDbLite::open(storage).await.expect("open NodeDbLite");
        put_crossed_docs(&*db).await;
        db.document_put(COLLECTION, make_doc("d3", &[("title", "stale words")]))
            .await
            .expect("put d3");
        db.flush().await.expect("first flush");

        // Retract terms after the first flush: the second flush must not
        // leave them behind for the reopen to bring back.
        db.document_put(COLLECTION, make_doc("d3", &[("title", "fresh words")]))
            .await
            .expect("overwrite d3");
        db.flush().await.expect("second flush");
    }

    let storage = PagedbStorageDefault::open(&path, Encryption::Plaintext)
        .await
        .expect("reopen storage");
    let db = NodeDbLite::open(storage).await.expect("reopen NodeDbLite");

    assert_eq!(search_ids(&*db, "title", "rust").await, vec!["d1"]);
    assert_eq!(search_ids(&*db, "body", "rust").await, vec!["d2"]);
    assert_eq!(search_ids(&*db, "", "rust").await, vec!["d1", "d2"]);
    assert!(search_ids(&*db, "title", "stale").await.is_empty());
    assert_eq!(search_ids(&*db, "title", "fresh").await, vec!["d3"]);
}

#[tokio::test]
async fn sql_text_match_is_scoped_to_named_field() {
    let db = open_test_db().await;
    db.execute_sql(&format!("CREATE COLLECTION {COLLECTION}"), &[])
        .await
        .expect("create collection");
    put_crossed_docs(&*db).await;

    let result = db
        .execute_sql(
            &format!("SELECT id FROM {COLLECTION} WHERE text_match(title, 'rust')"),
            &[],
        )
        .await
        .expect("text_match query");
    let id_col = result
        .columns
        .iter()
        .position(|c| c == "id")
        .unwrap_or_else(|| panic!("no id column in {:?}", result.columns));
    let mut ids: Vec<String> = result
        .rows
        .iter()
        .map(|row| match row.get(id_col) {
            Some(Value::String(id)) => id.clone(),
            other => panic!("id column holds {other:?}"),
        })
        .collect();
    ids.sort();

    assert_eq!(ids, vec!["d1"], "only the document with 'rust' in title");
}

async fn open_file_db(path: &std::path::Path) -> Arc<NodeDbLite<PagedbStorageDefault>> {
    let storage = PagedbStorageDefault::open(path, Encryption::Plaintext)
        .await
        .expect("open storage");
    NodeDbLite::open(storage).await.expect("open NodeDbLite")
}

#[tokio::test]
async fn deleting_the_only_document_after_a_flush_stays_deleted_after_reopen() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("fts_delete_only.db");

    {
        let db = open_file_db(&path).await;
        db.document_put(COLLECTION, make_doc("X", &[("text", "alpha zebrafish")]))
            .await
            .expect("put X");
        db.flush().await.expect("first flush");
        db.document_delete(COLLECTION, "X").await.expect("delete X");
        db.flush().await.expect("second flush");
        db.shutdown().await;
    }

    let db = open_file_db(&path).await;
    assert!(search_ids(&*db, "text", "zebrafish").await.is_empty());
}

#[tokio::test]
async fn deleting_the_only_document_then_compacting_stays_deleted_after_reopen() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("fts_delete_compact.db");

    {
        let db = open_file_db(&path).await;
        db.document_put(COLLECTION, make_doc("X", &[("text", "alpha zebrafish")]))
            .await
            .expect("put X");
        db.flush().await.expect("first flush");
        db.document_delete(COLLECTION, "X").await.expect("delete X");
        db.compact().await.expect("compact");
        db.flush().await.expect("second flush");
        db.shutdown().await;
    }

    let db = open_file_db(&path).await;
    assert!(search_ids(&*db, "text", "zebrafish").await.is_empty());
}

#[tokio::test]
async fn deleting_one_of_two_documents_keeps_the_other_after_reopen() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("fts_delete_one_of_two.db");

    {
        let db = open_file_db(&path).await;
        db.document_put(COLLECTION, make_doc("X", &[("text", "alpha zebrafish")]))
            .await
            .expect("put X");
        db.document_put(COLLECTION, make_doc("Y", &[("text", "beta wombat")]))
            .await
            .expect("put Y");
        db.flush().await.expect("first flush");
        db.document_delete(COLLECTION, "X").await.expect("delete X");
        db.flush().await.expect("second flush");
        db.shutdown().await;
    }

    let db = open_file_db(&path).await;
    assert!(search_ids(&*db, "text", "zebrafish").await.is_empty());
    assert_eq!(search_ids(&*db, "text", "wombat").await, vec!["Y"]);
}
