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

// ---------------------------------------------------------------------------
// open_with_config_honors_auto_compact_ms
// ---------------------------------------------------------------------------

/// Churn a database enough that compaction has real work to do, then verify it
/// gets done in the background when `auto_compact_ms` is set through
/// `LiteConfig` alone.
///
/// The control database runs the identical workload with compaction left
/// manual; its `compact()` must reclaim something, which is what makes the
/// second half of the test meaningful rather than trivially satisfied. On the
/// configured database the background task should already have reclaimed that
/// space, leaving a subsequent manual `compact()` with nothing to do.
#[tokio::test]
async fn open_with_config_honors_auto_compact_ms() {
    async fn churn<S: nodedb_lite::StorageEngine>(db: &NodeDbLite<S>) {
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
    }

    let dir = tempfile::tempdir().expect("tempdir");

    // Control: compaction left fully manual, so the reclaimable space is still
    // there when we ask for it.
    let control_path = dir.path().join("config_auto_compact_control.pagedb");
    let control_storage = PagedbStorageDefault::open(&control_path, Encryption::Plaintext)
        .await
        .expect("open control storage");
    let control = NodeDbLite::open_with_config(
        control_storage,
        1,
        LiteConfig {
            auto_compact_ms: 0,
            ..LiteConfig::default()
        },
    )
    .await
    .expect("open control db");
    churn(&control).await;
    tokio::time::sleep(Duration::from_millis(350)).await;
    let control_outcome = control.compact().await.expect("control manual compact");
    assert!(
        control_outcome.reclaimed_pages > 0,
        "this workload must leave reclaimable pages for the comparison below to mean anything; \
         control reclaimed nothing: {control_outcome:?}"
    );

    // Subject: same workload, compaction interval supplied through LiteConfig
    // and nothing else.
    let path = dir.path().join("config_auto_compact.pagedb");
    let storage = PagedbStorageDefault::open(&path, Encryption::Plaintext)
        .await
        .expect("open storage");
    let db = NodeDbLite::open_with_config(
        storage,
        1,
        LiteConfig {
            auto_compact_ms: 100,
            ..LiteConfig::default()
        },
    )
    .await
    .expect("open db");
    churn(&db).await;

    // Several ticks of the configured interval.
    tokio::time::sleep(Duration::from_millis(350)).await;

    let outcome = db.compact().await.expect("manual compact after auto");
    assert_eq!(
        outcome.reclaimed_pages, 0,
        "auto_compact_ms supplied via LiteConfig must run the background compaction: the \
         control reclaimed {} pages from the same workload, so anything left here means the \
         configured interval was never wired; outcome: {outcome:?}",
        control_outcome.reclaimed_pages
    );

    // Compaction ran without eating live data.
    let got = db.kv_get("col", "k175").await.expect("kv_get survivor");
    assert_eq!(
        got.as_deref(),
        Some([0xCDu8; 256].as_slice()),
        "surviving key must remain intact after background compaction"
    );
}
