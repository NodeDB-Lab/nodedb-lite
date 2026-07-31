// SPDX-License-Identifier: Apache-2.0

//! Integration tests for the auto-flush background task.
//!
//! Verifies the bounded-durability contract: writes are durable within
//! `auto_flush_ms` milliseconds even without an explicit `flush()` call.
//!
//! Two surfaces are covered. `start_auto_flush` tests exercise the task
//! directly. The `LiteConfig`-driven tests exercise the contract the config
//! field itself states — that opening with `auto_flush_ms` set bounds the
//! data-loss window, with no further call required by the embedder.

use std::sync::Arc;
use std::time::Duration;

use nodedb_client::NodeDb;
use nodedb_lite::{Encryption, LiteConfig, NodeDbLite, PagedbStorageDefault};
use nodedb_types::document::Document;
use nodedb_types::value::Value;

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
            NodeDbLite::open_with_config(storage, config)
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
        let db = NodeDbLite::open(storage).await.expect("reopen db");
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
            NodeDbLite::open_with_config(storage, config)
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
        let db = NodeDbLite::open(storage).await.expect("reopen db");
        let got = db.kv_get("col", "key").await.expect("kv_get after reopen");
        assert!(
            got.is_none(),
            "key must NOT survive reopen when auto-flush is disabled and flush() was not called; \
             got: {got:?}"
        );
    }
}

// ---------------------------------------------------------------------------
// open_with_config_honors_auto_flush_ms
// ---------------------------------------------------------------------------

/// Opening with `auto_flush_ms` set bounds durability on its own: the embedder
/// sets the field, writes, and loses nothing on an unclean exit. No separate
/// opt-in call is part of the documented contract.
#[tokio::test]
async fn open_with_config_honors_auto_flush_ms() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("config_auto_flush.pagedb");

    {
        let storage = PagedbStorageDefault::open(&path, Encryption::Plaintext)
            .await
            .expect("open storage");
        let config = LiteConfig {
            auto_flush_ms: 200,
            ..LiteConfig::default()
        };
        let db = NodeDbLite::open_with_config(storage, config)
            .await
            .expect("open db");

        db.kv_put("col", "key", b"config_flushed")
            .await
            .expect("kv_put");

        // The documented bound elapses several times over.
        tokio::time::sleep(Duration::from_millis(450)).await;

        // Drop without an explicit flush — this stands in for process death.
    }

    {
        let storage = PagedbStorageDefault::open(&path, Encryption::Plaintext)
            .await
            .expect("reopen storage");
        let db = NodeDbLite::open(storage).await.expect("reopen db");
        let got = db.kv_get("col", "key").await.expect("kv_get after reopen");
        assert_eq!(
            got.as_deref(),
            Some(b"config_flushed".as_slice()),
            "a write must survive reopen once auto_flush_ms has elapsed, with the interval \
             supplied through LiteConfig alone"
        );
    }
}

// ---------------------------------------------------------------------------
// open_with_config_bounds_crdt_state_durability
// ---------------------------------------------------------------------------

/// The CRDT layer is covered by the same bound as the row data.
///
/// Bitemporal document rows commit through their own history path on every
/// write, so they read back after an unclean exit whether or not a flush ever
/// ran. Loro snapshot/delta state is written only by `flush()`. Losing it is
/// therefore invisible locally and surfaces much later as rows that can no
/// longer sync to an Origin — so this asserts the CRDT side explicitly rather
/// than trusting a successful read.
#[tokio::test]
async fn open_with_config_bounds_crdt_state_durability() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("config_auto_flush_crdt.pagedb");

    {
        let storage = PagedbStorageDefault::open(&path, Encryption::Plaintext)
            .await
            .expect("open storage");
        let config = LiteConfig {
            auto_flush_ms: 200,
            ..LiteConfig::default()
        };
        let db = NodeDbLite::open_with_config(storage, config)
            .await
            .expect("open db");

        db.execute_sql("CREATE COLLECTION bt_notes WITH (bitemporal=true)", &[])
            .await
            .expect("create bitemporal collection");

        for i in 0u32..8 {
            let mut doc = Document::new(format!("note{i}"));
            doc.set("body", Value::String(format!("entry {i}")));
            db.document_put("bt_notes", doc)
                .await
                .expect("document_put");
        }

        tokio::time::sleep(Duration::from_millis(450)).await;

        // Drop without an explicit flush.
    }

    {
        let storage = PagedbStorageDefault::open(&path, Encryption::Plaintext)
            .await
            .expect("reopen storage");
        let db = NodeDbLite::open(storage).await.expect("reopen db");

        let fetched = db
            .document_get("bt_notes", "note3")
            .await
            .expect("document_get after reopen");
        assert!(
            fetched.is_some(),
            "bitemporal rows must survive reopen (they commit per write)"
        );

        let dump = db.diagnostic_dump().await;
        assert!(
            dump.storage_counts.loro_state > 0,
            "CRDT state must be persisted within auto_flush_ms as well — rows reading back \
             correctly while loro_state is empty is the silent-divergence failure: the replica \
             can no longer sync those rows to an Origin; storage_counts: {:?}",
            dump.storage_counts
        );
    }
}

// ---------------------------------------------------------------------------
// open_honors_default_auto_flush_ms
// ---------------------------------------------------------------------------

/// The default path — `NodeDbLite::open`, as shown in the crate-level quick
/// start — resolves its config from the environment and so carries the default
/// `auto_flush_ms` of one second. Writing and waiting past that bound must be
/// enough to survive an unclean exit.
#[tokio::test]
async fn open_honors_default_auto_flush_ms() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("default_auto_flush.pagedb");

    {
        let storage = PagedbStorageDefault::open(&path, Encryption::Plaintext)
            .await
            .expect("open storage");
        let db = NodeDbLite::open(storage).await.expect("open db");

        db.kv_put("col", "key", b"default_flushed")
            .await
            .expect("kv_put");

        // Default interval is 1000 ms.
        tokio::time::sleep(Duration::from_millis(1_500)).await;
    }

    {
        let storage = PagedbStorageDefault::open(&path, Encryption::Plaintext)
            .await
            .expect("reopen storage");
        let db = NodeDbLite::open(storage).await.expect("reopen db");
        let got = db.kv_get("col", "key").await.expect("kv_get after reopen");
        assert_eq!(
            got.as_deref(),
            Some(b"default_flushed".as_slice()),
            "the default auto_flush_ms must bound durability for a plain open()"
        );
    }
}

// ---------------------------------------------------------------------------
// open_with_budget_honors_default_auto_flush_ms
// ---------------------------------------------------------------------------

/// `open_with_budget` overrides only the memory budget and takes the remaining
/// configuration from `LiteConfig::default()`, so the default flush interval
/// applies to it too.
#[tokio::test]
async fn open_with_budget_honors_default_auto_flush_ms() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("budget_auto_flush.pagedb");

    {
        let storage = PagedbStorageDefault::open(&path, Encryption::Plaintext)
            .await
            .expect("open storage");
        let db = NodeDbLite::open_with_budget(storage, 64 * 1024 * 1024)
            .await
            .expect("open db");

        db.kv_put("col", "key", b"budget_flushed")
            .await
            .expect("kv_put");

        tokio::time::sleep(Duration::from_millis(1_500)).await;
    }

    {
        let storage = PagedbStorageDefault::open(&path, Encryption::Plaintext)
            .await
            .expect("reopen storage");
        let db = NodeDbLite::open(storage).await.expect("reopen db");
        let got = db.kv_get("col", "key").await.expect("kv_get after reopen");
        assert_eq!(
            got.as_deref(),
            Some(b"budget_flushed".as_slice()),
            "open_with_budget must honor the default auto_flush_ms it inherits from LiteConfig"
        );
    }
}

// ---------------------------------------------------------------------------
// open_at_path_with_config_honors_auto_flush_ms
// ---------------------------------------------------------------------------

/// The path-opening constructor delegates to `open_with_config` and must carry
/// the same durability bound.
#[tokio::test]
async fn open_at_path_with_config_honors_auto_flush_ms() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("at_path_auto_flush.pagedb");

    {
        let config = LiteConfig {
            auto_flush_ms: 200,
            ..LiteConfig::default()
        };
        let db = NodeDbLite::open_at_path_with_config(&path, Encryption::Plaintext, config)
            .await
            .expect("open db at path");

        db.kv_put("col", "key", b"at_path_flushed")
            .await
            .expect("kv_put");

        tokio::time::sleep(Duration::from_millis(450)).await;
    }

    {
        let db = NodeDbLite::open_at_path(&path, Encryption::Plaintext)
            .await
            .expect("reopen db at path");
        let got = db.kv_get("col", "key").await.expect("kv_get after reopen");
        assert_eq!(
            got.as_deref(),
            Some(b"at_path_flushed".as_slice()),
            "open_at_path_with_config must honor auto_flush_ms from the supplied config"
        );
    }
}

// ---------------------------------------------------------------------------
// open_with_config_auto_flush_ms_zero_leaves_writes_unflushed
// ---------------------------------------------------------------------------

/// The disable path of the same contract: `auto_flush_ms: 0` means no
/// background task, so an unflushed write is genuinely lost. This pins the
/// opt-out so a wiring that always spawns — or that treats 0 as "use the
/// default" — is caught.
#[tokio::test]
async fn open_with_config_auto_flush_ms_zero_leaves_writes_unflushed() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("config_auto_flush_zero.pagedb");

    {
        let storage = PagedbStorageDefault::open(&path, Encryption::Plaintext)
            .await
            .expect("open storage");
        let config = LiteConfig {
            auto_flush_ms: 0,
            ..LiteConfig::default()
        };
        let db = NodeDbLite::open_with_config(storage, config)
            .await
            .expect("open db");

        db.kv_put("col", "key", b"unflushed").await.expect("kv_put");

        // Long enough that any spawned task would have fired repeatedly.
        tokio::time::sleep(Duration::from_millis(450)).await;
    }

    {
        let storage = PagedbStorageDefault::open(&path, Encryption::Plaintext)
            .await
            .expect("reopen storage");
        let db = NodeDbLite::open(storage).await.expect("reopen db");
        let got = db.kv_get("col", "key").await.expect("kv_get after reopen");
        assert!(
            got.is_none(),
            "auto_flush_ms = 0 must disable the background task entirely; got: {got:?}"
        );
    }
}
