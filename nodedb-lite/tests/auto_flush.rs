// SPDX-License-Identifier: Apache-2.0

//! Integration tests for the auto-flush background task.
//!
//! Verifies the bounded-durability contract: writes are durable within
//! `auto_flush_ms` milliseconds even without an explicit `flush()` call.

use std::sync::Arc;
use std::time::Duration;

use nodedb_lite::{Encryption, LiteConfig, NodeDbLite, PagedbStorageDefault};

// ---------------------------------------------------------------------------
// auto_flush_persists_without_explicit_flush
// ---------------------------------------------------------------------------

/// A key written while auto-flush is active (interval 200 ms) survives a
/// drop + reopen without any explicit `flush()` call, provided we wait long
/// enough for at least one tick to fire.
#[tokio::test]
async fn auto_flush_persists_without_explicit_flush() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("auto_flush_persist.pagedb");

    {
        let storage = PagedbStorageDefault::open(&path, Encryption::Plaintext)
            .await
            .expect("open storage");
        let config = LiteConfig {
            auto_flush_ms: 200,
            ..LiteConfig::default()
        };
        let db = Arc::new(
            NodeDbLite::open_with_config(storage, 1, config)
                .await
                .expect("open db"),
        );
        db.start_auto_flush(200);

        db.kv_put("col", "key", b"auto_flushed")
            .await
            .expect("kv_put");

        // Wait long enough for at least one auto-flush tick (200 ms interval,
        // first tick is immediate on native Tokio; second tick fires at ~200 ms).
        tokio::time::sleep(Duration::from_millis(450)).await;

        // Drop without explicit flush — the auto-flush task already ran.
    }

    {
        let storage = PagedbStorageDefault::open(&path, Encryption::Plaintext)
            .await
            .expect("reopen storage");
        let db = NodeDbLite::open(storage, 1).await.expect("reopen db");
        let got = db.kv_get("col", "key").await.expect("kv_get after reopen");
        assert_eq!(
            got.as_deref(),
            Some(b"auto_flushed".as_slice()),
            "key must survive reopen when auto-flush fired before drop"
        );
    }
}

// ---------------------------------------------------------------------------
// disabled_auto_flush_does_not_persist
// ---------------------------------------------------------------------------

/// With `auto_flush_ms: 0` (disabled) and no explicit `flush()`, a write is
/// NOT durable — a drop + immediate reopen finds nothing. This documents the
/// bounded-window contract: callers must either enable auto-flush or call
/// `flush()` explicitly.
#[tokio::test]
async fn disabled_auto_flush_does_not_persist() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("auto_flush_disabled.pagedb");

    {
        let storage = PagedbStorageDefault::open(&path, Encryption::Plaintext)
            .await
            .expect("open storage");
        let config = LiteConfig {
            auto_flush_ms: 0,
            ..LiteConfig::default()
        };
        let db = Arc::new(
            NodeDbLite::open_with_config(storage, 1, config)
                .await
                .expect("open db"),
        );
        // auto_flush_ms=0 → start_auto_flush is a no-op.
        db.start_auto_flush(0);

        db.kv_put("col", "key", b"unflushed").await.expect("kv_put");

        // Drop immediately without flush — no auto-flush task was started.
    }

    {
        let storage = PagedbStorageDefault::open(&path, Encryption::Plaintext)
            .await
            .expect("reopen storage");
        let db = NodeDbLite::open(storage, 1).await.expect("reopen db");
        let got = db.kv_get("col", "key").await.expect("kv_get after reopen");
        assert!(
            got.is_none(),
            "key must NOT survive reopen when auto-flush is disabled and flush() was not called; \
             got: {got:?}"
        );
    }
}
