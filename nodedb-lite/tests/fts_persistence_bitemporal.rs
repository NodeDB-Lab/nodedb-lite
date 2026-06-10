// SPDX-License-Identifier: Apache-2.0

//! FTS persistence tests for bitemporal document collections.
//!
//! Verifies that `rebuild_text_indices` correctly restores the FTS index from
//! `Namespace::DocumentHistory` after reopen when no CRDT snapshot was flushed
//! before the previous process exited.

use nodedb_client::NodeDb;
use nodedb_lite::{NodeDbLite, PagedbStorageDefault};
use nodedb_types::document::Document;
use nodedb_types::text_search::TextSearchParams;
use nodedb_types::value::Value;

/// FTS must find a bitemporal document after reopen without an explicit flush.
///
/// Simulates a process exit without `flush()` by dropping the `NodeDbLite`
/// instance. The history table is durable (written synchronously); only the
/// CRDT snapshot may not have been committed. The FTS rebuild path must fall
/// back to the history table and reconstruct the index.
#[tokio::test]
async fn fts_returns_bitemporal_documents_after_reopen_without_explicit_flush() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = tmp.path().to_path_buf();

    // Process-A-equivalent: write WITHOUT calling flush().
    {
        let storage = PagedbStorageDefault::open(&path).await.unwrap();
        let db = NodeDbLite::open(storage, 1).await.unwrap();
        db.execute_sql("CREATE COLLECTION entries WITH (bitemporal=true)", &[])
            .await
            .unwrap();
        let mut doc = Document::new("e1");
        doc.set("content", Value::String("hello world".into()));
        db.document_put("entries", doc).await.unwrap();
        // Intentionally NO .flush() call here — db drops on scope exit.
    }

    // Process-B-equivalent: reopen, search, MUST find the document.
    let storage = PagedbStorageDefault::open(&path).await.unwrap();
    let db = NodeDbLite::open(storage, 1).await.unwrap();
    let results = db
        .text_search(
            "entries",
            "",
            "hello",
            10,
            TextSearchParams::default(),
            None,
        )
        .await
        .unwrap();

    assert!(
        !results.is_empty(),
        "FTS should return the bitemporal document after reopen without explicit flush; got 0 results"
    );
    assert_eq!(results[0].id, "e1");
}

/// Tombstoned documents must NOT appear in FTS results after reopen.
///
/// Writes two documents into a bitemporal collection, then tombstones one of
/// them. After reopen (no flush), the live document must be found and the
/// tombstoned document must be absent.
#[tokio::test]
async fn fts_returns_only_live_versions_after_reopen() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = tmp.path().to_path_buf();

    {
        let storage = PagedbStorageDefault::open(&path).await.unwrap();
        let db = NodeDbLite::open(storage, 1).await.unwrap();
        db.execute_sql("CREATE COLLECTION entries WITH (bitemporal=true)", &[])
            .await
            .unwrap();

        let mut a = Document::new("live");
        a.set("content", Value::String("present forever".into()));
        db.document_put("entries", a).await.unwrap();

        let mut b = Document::new("ghost");
        b.set("content", Value::String("about to be tombstoned".into()));
        db.document_put("entries", b).await.unwrap();

        // Tombstone "ghost" — delete on a bitemporal collection appends Tombstone.
        db.document_delete("entries", "ghost").await.unwrap();
        // No flush: db drops on scope exit.
    }

    let storage = PagedbStorageDefault::open(&path).await.unwrap();
    let db = NodeDbLite::open(storage, 1).await.unwrap();

    let r_live = db
        .text_search(
            "entries",
            "",
            "present",
            10,
            TextSearchParams::default(),
            None,
        )
        .await
        .unwrap();
    let r_ghost = db
        .text_search(
            "entries",
            "",
            "tombstoned",
            10,
            TextSearchParams::default(),
            None,
        )
        .await
        .unwrap();

    assert_eq!(
        r_live.len(),
        1,
        "live document must appear in FTS after reopen"
    );
    assert_eq!(r_live[0].id, "live");
    assert!(
        r_ghost.is_empty(),
        "tombstoned documents must NOT appear in FTS after reopen; got {} results",
        r_ghost.len()
    );
}

/// Non-bitemporal collections must continue to restore from CRDT after reopen.
///
/// Regression guard for the existing CRDT-based rebuild path. Plain collections
/// require an explicit flush for the CRDT snapshot to persist; this test calls
/// flush so the pre-existing path stays exercised end-to-end.
#[tokio::test]
async fn fts_still_works_for_non_bitemporal_collections_after_reopen() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = tmp.path().to_path_buf();

    {
        let storage = PagedbStorageDefault::open(&path).await.unwrap();
        let db = NodeDbLite::open(storage, 1).await.unwrap();
        // No WITH (bitemporal=true) — plain schemaless collection (no DDL needed).
        let mut doc = Document::new("p1");
        doc.set("content", Value::String("plain text".into()));
        db.document_put("plain", doc).await.unwrap();
        // Flush so the CRDT snapshot is durable (plain collection requirement).
        db.flush().await.unwrap();
    }

    let storage = PagedbStorageDefault::open(&path).await.unwrap();
    let db = NodeDbLite::open(storage, 1).await.unwrap();
    let results = db
        .text_search("plain", "", "plain", 10, TextSearchParams::default(), None)
        .await
        .unwrap();

    assert!(
        !results.is_empty(),
        "non-bitemporal collections must continue to restore FTS from CRDT; got 0 results"
    );
}
