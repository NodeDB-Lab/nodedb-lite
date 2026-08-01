// SPDX-License-Identifier: Apache-2.0

//! Flush must do snapshot work in proportion to what changed.
//!
//! A Loro snapshot export is O(document), and `flush()` runs on a timer, so an
//! export that ignores whether the document moved makes an idle store rewrite
//! its entire state once per `auto_flush_ms` — file growth with no writes
//! behind it, and once the document is large enough to outlast the interval, a
//! flush duty cycle that leaves no room for readers.
//!
//! These assert on the export counter rather than on elapsed time, so they fail
//! for the reason they name on any machine.

use std::sync::Arc;

use nodedb_client::NodeDb;
use nodedb_lite::{Encryption, LiteConfig, NodeDbLite, PagedbStorageDefault};
use nodedb_types::document::Document;
use nodedb_types::value::Value;

/// Open with auto-flush disabled so every flush in the test is one we asked for.
async fn open_manual_flush(path: &std::path::Path) -> Arc<NodeDbLite<PagedbStorageDefault>> {
    let storage = PagedbStorageDefault::open(path, Encryption::Plaintext)
        .await
        .expect("open storage");
    let config = LiteConfig {
        auto_flush_ms: 0,
        ..LiteConfig::default()
    };
    NodeDbLite::open_with_config(storage, config)
        .await
        .expect("open db")
}

async fn put_note(db: &NodeDbLite<PagedbStorageDefault>, id: &str, body: &str) {
    let mut doc = Document::new(id.to_string());
    doc.set("body", Value::String(body.to_string()));
    db.document_put("bt_notes", doc)
        .await
        .expect("document_put");
}

// ---------------------------------------------------------------------------
// idle_flush_exports_no_snapshots
// ---------------------------------------------------------------------------

/// An idle store performs zero snapshot exports, however many times it flushes.
#[tokio::test]
async fn idle_flush_exports_no_snapshots() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = open_manual_flush(&dir.path().join("idle_flush.pagedb")).await;

    db.execute_sql("CREATE COLLECTION bt_notes WITH (bitemporal=true)", &[])
        .await
        .expect("create bitemporal collection");
    put_note(&db, "note0", "first").await;
    db.flush().await.expect("flush the write");

    let after_write = db.crdt_snapshot_export_count();
    assert!(
        after_write > 0,
        "the flush that persists a new write must export its snapshot"
    );

    for _ in 0..8 {
        db.flush().await.expect("idle flush");
    }

    assert_eq!(
        db.crdt_snapshot_export_count(),
        after_write,
        "an idle store must export nothing: {} flushes with no writes in between exported {} \
         additional snapshots, each one a full rewrite of the document",
        8,
        db.crdt_snapshot_export_count() - after_write
    );
}

// ---------------------------------------------------------------------------
// flush_after_write_exports_again
// ---------------------------------------------------------------------------

/// Dirty tracking must not turn into never-export: a write after a flush is
/// exported by the next one, and reads back after a reopen.
#[tokio::test]
async fn flush_after_write_exports_again() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("dirty_after_write.pagedb");

    {
        let db = open_manual_flush(&path).await;
        db.execute_sql("CREATE COLLECTION bt_notes WITH (bitemporal=true)", &[])
            .await
            .expect("create bitemporal collection");

        put_note(&db, "note0", "first").await;
        db.flush().await.expect("flush first write");
        let after_first = db.crdt_snapshot_export_count();

        put_note(&db, "note1", "second").await;
        db.flush().await.expect("flush second write");
        assert!(
            db.crdt_snapshot_export_count() > after_first,
            "a collection written since its last flush must be exported again"
        );
    }

    let db = open_manual_flush(&path).await;
    let fetched = db
        .document_get("bt_notes", "note1")
        .await
        .expect("document_get after reopen");
    assert!(
        fetched.is_some(),
        "the row written after the first flush must be durable"
    );

    let dump = db.diagnostic_dump().await;
    assert!(
        dump.storage_counts.loro_state > 0,
        "the collection's CRDT snapshot must be on disk, not just its row: {:?}",
        dump.storage_counts
    );
}
