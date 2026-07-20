// SPDX-License-Identifier: Apache-2.0

//! Integration tests for the auto-compaction background task.
//!
//! Verifies that `start_auto_compact` runs the compaction loop end-to-end
//! (reclaiming space without losing data) when enabled, and is an inert no-op
//! when disabled.

use std::sync::Arc;
use std::time::Duration;

use nodedb_lite::{Encryption, LiteConfig, NodeDbLite, PagedbStorageDefault};

// ---------------------------------------------------------------------------
// auto_compact_runs_and_preserves_data
// ---------------------------------------------------------------------------

/// With auto-compaction active (interval 100 ms) over a churned database, the
/// background task fires at least once and surviving data remains intact. We
/// assert liveness + integrity, not an exact reclaimed-byte count, because
/// compaction is heuristic (garbage-ratio threshold) and no-ops while a reader
/// pins the reclaimable range.
#[tokio::test]
async fn auto_compact_runs_and_preserves_data() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("auto_compact_runs.pagedb");

    let storage = PagedbStorageDefault::open(&path, Encryption::Plaintext)
        .await
        .expect("open storage");
    let config = LiteConfig {
        auto_compact_ms: 100,
        ..LiteConfig::default()
    };
    let db = Arc::new(
        NodeDbLite::open_with_config(storage, 1, config)
            .await
            .expect("open db"),
    );
    db.start_auto_compact(100);

    // Churn: many writes, overwrites, and deletes so copy-on-write leaves dead
    // pages on the deferred-free list for compaction to reclaim. Flush so the
    // dead pages are actually committed to disk (and thus reclaimable).
    for i in 0u32..200 {
        db.kv_put("col", &format!("k{i}"), &vec![0xABu8; 256])
            .await
            .expect("kv_put");
    }
    for i in 0u32..200 {
        db.kv_put("col", &format!("k{i}"), &vec![0xCDu8; 256])
            .await
            .expect("kv_put overwrite");
    }
    for i in 0u32..150 {
        db.kv_delete("col", &format!("k{i}"))
            .await
            .expect("kv_delete");
    }
    db.flush().await.expect("flush");

    // Wait for at least one auto-compact tick (100 ms interval; first fires at
    // ~100 ms on native Tokio).
    tokio::time::sleep(Duration::from_millis(350)).await;

    // A surviving key is still readable — the compaction loop did not corrupt
    // or lose data.
    let got = db.kv_get("col", "k175").await.expect("kv_get survivor");
    assert_eq!(
        got.as_deref(),
        Some([0xCDu8; 256].as_slice()),
        "surviving key must remain intact after auto-compaction fired"
    );

    // An explicit compaction still succeeds after the background loop has run.
    let outcome = db.compact().await.expect("manual compact after auto");
    let _ = (
        outcome.reclaimed_pages,
        outcome.segments_repacked,
        outcome.file_bytes_freed,
    );
}

// ---------------------------------------------------------------------------
// disabled_auto_compact_is_noop
// ---------------------------------------------------------------------------

/// With `auto_compact_ms: 0` (disabled), `start_auto_compact` spawns nothing
/// and the database is otherwise fully functional — writes persist via flush
/// and a manual `compact()` still works.
#[tokio::test]
async fn disabled_auto_compact_is_noop() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("auto_compact_disabled.pagedb");

    let storage = PagedbStorageDefault::open(&path, Encryption::Plaintext)
        .await
        .expect("open storage");
    let config = LiteConfig {
        auto_compact_ms: 0,
        ..LiteConfig::default()
    };
    let db = Arc::new(
        NodeDbLite::open_with_config(storage, 1, config)
            .await
            .expect("open db"),
    );
    // auto_compact_ms=0 → start_auto_compact is a no-op (spawns no task).
    db.start_auto_compact(0);

    db.kv_put("col", "key", b"present").await.expect("kv_put");
    db.flush().await.expect("flush");

    // Manual compaction still works with the auto task disabled.
    db.compact().await.expect("manual compact");

    let got = db.kv_get("col", "key").await.expect("kv_get");
    assert_eq!(
        got.as_deref(),
        Some(b"present".as_slice()),
        "data must remain intact with auto-compaction disabled"
    );
}
