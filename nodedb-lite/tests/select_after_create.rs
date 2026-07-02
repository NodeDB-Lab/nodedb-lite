// SPDX-License-Identifier: Apache-2.0

//! Regression tests confirming that `SELECT` on a bitemporal collection works
//! immediately after `CREATE COLLECTION … WITH (bitemporal=true)`, without
//! requiring a flush, and also works after the database is closed and reopened.

use nodedb_client::NodeDb;
use nodedb_lite::{NodeDbLite, PagedbStorageMem};
use nodedb_types::document::Document;
use nodedb_types::value::Value;

#[cfg(not(target_arch = "wasm32"))]
use nodedb_lite::{Encryption, PagedbStorageDefault};

// ── In-memory: SELECT works in the same session as CREATE ────────────────────

/// Creating a bitemporal collection and writing a document via `document_put`,
/// then querying via SELECT, must return a valid non-empty result.  Previously
/// this failed with "table not found" because the collection was not yet
/// registered in the in-memory SQL catalog.
#[tokio::test]
async fn select_after_create_works_without_flush() {
    let storage = PagedbStorageMem::open_in_memory().await.unwrap();
    let db = NodeDbLite::open(storage, 1).await.unwrap();

    db.execute_sql("CREATE COLLECTION foo WITH (bitemporal=true)", &[])
        .await
        .unwrap();

    let mut doc = Document::new("a");
    doc.set("content", Value::String("x".into()));
    db.document_put("foo", doc).await.unwrap();

    let result = db
        .execute_sql("SELECT id FROM foo", &[])
        .await
        .expect("SELECT on bitemporal collection must not fail");

    assert!(
        !result.rows.is_empty(),
        "expected at least one row; got zero"
    );
}

/// SELECT on an empty bitemporal collection (no documents written yet) must
/// succeed and return zero rows, not an error.
#[tokio::test]
async fn select_on_empty_bitemporal_collection_succeeds() {
    let storage = PagedbStorageMem::open_in_memory().await.unwrap();
    let db = NodeDbLite::open(storage, 1).await.unwrap();

    db.execute_sql("CREATE COLLECTION bar WITH (bitemporal=true)", &[])
        .await
        .unwrap();

    let result = db
        .execute_sql("SELECT id FROM bar", &[])
        .await
        .expect("SELECT on empty bitemporal collection must not fail");

    assert!(
        result.rows.is_empty(),
        "expected zero rows on empty collection"
    );
}

// ── On-disk: SELECT works after close + reopen without flush ─────────────────

/// Documents written via `document_put` are durably stored in the history
/// table (committed to disk on every write).  After reopening without an
/// explicit flush the CRDT Loro snapshot is absent, but the history table
/// still has the data.  `SELECT` must read from the history table and return
/// the documents.
#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn select_after_reopen_works() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("reopen.db");

    {
        let storage = PagedbStorageDefault::open(&path, Encryption::Plaintext)
            .await
            .unwrap();
        let db = NodeDbLite::open(storage, 1).await.unwrap();

        db.execute_sql("CREATE COLLECTION baz WITH (bitemporal=true)", &[])
            .await
            .unwrap();

        let mut doc = Document::new("doc1");
        doc.set("content", Value::String("hello".into()));
        db.document_put("baz", doc).await.unwrap();

        // Intentionally drop WITHOUT calling flush — the CRDT snapshot is
        // not saved, but the DocumentHistory write is already durable.
    }

    {
        let storage = PagedbStorageDefault::open(&path, Encryption::Plaintext)
            .await
            .unwrap();
        let db = NodeDbLite::open(storage, 1).await.unwrap();

        let result = db
            .execute_sql("SELECT id FROM baz", &[])
            .await
            .expect("SELECT after reopen must not fail");

        assert!(
            !result.rows.is_empty(),
            "expected at least one row after reopen; got zero"
        );
    }
}
