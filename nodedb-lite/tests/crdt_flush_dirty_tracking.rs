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
// write_after_flush_is_persisted
// ---------------------------------------------------------------------------

/// Skipping unchanged collections must not turn into skipping changed ones: a
/// write made after a flush is persisted by the next one and reads back after a
/// reopen.
#[tokio::test]
async fn write_after_flush_is_persisted() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("dirty_after_write.pagedb");

    {
        let db = open_manual_flush(&path).await;
        db.execute_sql("CREATE COLLECTION bt_notes WITH (bitemporal=true)", &[])
            .await
            .expect("create bitemporal collection");

        put_note(&db, "note0", "first").await;
        db.flush().await.expect("flush first write");

        put_note(&db, "note1", "second").await;
        db.flush().await.expect("flush second write");
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

// ---------------------------------------------------------------------------
// sustained_writes_do_not_export_a_snapshot_per_flush
// ---------------------------------------------------------------------------

/// Under sustained writes — the workload dirty-tracking alone does not help —
/// flush writes updates, not a fresh snapshot per tick.
#[tokio::test]
async fn sustained_writes_do_not_export_a_snapshot_per_flush() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = open_manual_flush(&dir.path().join("sustained.pagedb")).await;

    db.execute_sql("CREATE COLLECTION bt_notes WITH (bitemporal=true)", &[])
        .await
        .expect("create bitemporal collection");
    put_note(&db, "note0", "first").await;
    db.flush().await.expect("flush the first write");
    let after_first = db.crdt_snapshot_export_count();

    for i in 1..64 {
        put_note(&db, &format!("note{i}"), &format!("body {i}")).await;
        db.flush().await.expect("flush");
    }

    assert_eq!(
        db.crdt_snapshot_export_count(),
        after_first,
        "63 flushes, each with one small write behind it, exported {} full snapshots; a write of \
         a few hundred bytes must cost a few hundred bytes of update, not a rewrite of the \
         collection",
        db.crdt_snapshot_export_count() - after_first
    );
}

// ---------------------------------------------------------------------------
// updates_written_between_checkpoints_survive_reopen
// ---------------------------------------------------------------------------

/// The writes that reach disk as updates rather than as part of a snapshot must
/// come back on open. Losing them is invisible until a reopen, so this asserts
/// the replay directly.
#[tokio::test]
async fn updates_written_between_checkpoints_survive_reopen() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("update_replay.pagedb");

    {
        let db = open_manual_flush(&path).await;
        db.execute_sql("CREATE COLLECTION bt_notes WITH (bitemporal=true)", &[])
            .await
            .expect("create bitemporal collection");

        put_note(&db, "note0", "checkpointed").await;
        db.flush().await.expect("flush the checkpoint");
        let exports = db.crdt_snapshot_export_count();

        for i in 1..8 {
            put_note(&db, &format!("note{i}"), &format!("update {i}")).await;
            db.flush().await.expect("flush an update");
        }
        assert_eq!(
            db.crdt_snapshot_export_count(),
            exports,
            "these writes must have gone to disk as updates for this test to mean anything"
        );
    }

    let db = open_manual_flush(&path).await;
    for i in 1..8 {
        assert!(
            db.document_get("bt_notes", &format!("note{i}"))
                .await
                .expect("document_get after reopen")
                .is_some(),
            "note{i} was written as an update after the last checkpoint and must be replayed on \
             open, not rolled back to the checkpoint"
        );
    }
}

// ---------------------------------------------------------------------------
// idle_flush_does_not_rewrite_the_sync_queue
// ---------------------------------------------------------------------------

/// The unsent-delta queue is append-only and each entry owns its key, so an
/// idle flush must write none of it.
///
/// A replica with no Origin never has a delta acknowledged, so the queue only
/// grows — and rewriting it in full per tick is the same unbounded cost the
/// snapshot rewrite had, one layer over.
#[tokio::test]
async fn idle_flush_does_not_rewrite_the_sync_queue() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("idle_queue.pagedb");
    let db = open_manual_flush(&path).await;

    db.execute_sql("CREATE COLLECTION bt_notes WITH (bitemporal=true)", &[])
        .await
        .expect("create bitemporal collection");
    for i in 0..32 {
        put_note(&db, &format!("note{i}"), &format!("body {i}")).await;
    }
    db.flush().await.expect("flush the writes");

    let after_writes = db.crdt_delta_write_count();
    assert!(
        after_writes >= 32,
        "the flush that persists the queue must write its entries"
    );

    for _ in 0..8 {
        db.flush().await.expect("idle flush");
    }

    assert_eq!(
        db.crdt_delta_write_count(),
        after_writes,
        "an idle store must write none of the queue: 8 flushes with nothing queued in between \
         rewrote {} entries, and a replica with no Origin never retires any of them",
        db.crdt_delta_write_count() - after_writes
    );
}

// ---------------------------------------------------------------------------
// queued_deltas_survive_reopen
// ---------------------------------------------------------------------------

/// Writing only the changed entries must still leave the whole queue on disk:
/// the deltas are what a replica has yet to send, so losing them loses writes
/// that no Origin has seen.
#[tokio::test]
async fn queued_deltas_survive_reopen() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("queue_reopen.pagedb");

    {
        let db = open_manual_flush(&path).await;
        db.execute_sql("CREATE COLLECTION bt_notes WITH (bitemporal=true)", &[])
            .await
            .expect("create bitemporal collection");

        // Two batches with a flush in between, so the second flush writes only
        // the second batch and the first must already be durable.
        for i in 0..4 {
            put_note(&db, &format!("first{i}"), "a").await;
        }
        db.flush().await.expect("flush first batch");
        for i in 0..4 {
            put_note(&db, &format!("second{i}"), "b").await;
        }
        db.flush().await.expect("flush second batch");
    }

    let db = open_manual_flush(&path).await;
    let dump = db.diagnostic_dump().await;
    assert!(
        dump.storage_counts.crdt >= 8,
        "every queued delta must be readable after reopen, from both batches; \
         storage_counts: {:?}",
        dump.storage_counts
    );
}
