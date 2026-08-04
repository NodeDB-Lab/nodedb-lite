// SPDX-License-Identifier: Apache-2.0

//! Opening a damaged store must never destroy or discard data on its own.
//!
//! An embedded database is the only copy of its data unless the embedder has
//! wired a sync origin, and the embedder is the only party that knows whether
//! one exists. So every corruption-class fault reached during open belongs to
//! the caller: it is reported, and the bytes behind it are left exactly where
//! they were. Discarding a store, a collection snapshot, or a queued local
//! mutation is a decision the library does not get to make silently — an open
//! that returns `Ok` must mean the data behind it survived.

use std::path::Path;
use std::sync::Arc;

use nodedb_client::NodeDb;
use nodedb_lite::engine::crdt::CrdtEngine;
use nodedb_lite::{
    CorruptionPolicy, Encryption, LiteConfig, NodeDbLite, PagedbStorageMem, StorageEngine,
};
use nodedb_types::Namespace;
use nodedb_types::document::Document;
use nodedb_types::error::ErrorDetails;
use nodedb_types::value::Value;

const PAGE_SIZE: u64 = 4096;
/// Pages 0-3 are reserved by the pager; page 4 is the first data page.
const FIRST_DATA_PAGE: u64 = 4;
/// Trailing AEAD tag of each page — flipping it fails page verification.
const AEAD_TAG_LEN: usize = 16;

// ─── Helpers ─────────────────────────────────────────────────────────────────

/// Write a document into a store at `path` and close it, so the bytes on disk
/// are a complete store with real user data in it.
async fn seed_store(path: &Path) {
    let config = LiteConfig {
        auto_flush_ms: 0,
        ..LiteConfig::default()
    };
    let db = NodeDbLite::open_at_path_with_config(path, Encryption::Plaintext, config)
        .await
        .expect("seed open");
    // Written without DDL: a collection created by writing to it is a supported
    // path, and it keeps the seed independent of which SQL features are on.
    let mut doc = Document::new("note0".to_string());
    doc.set("body", Value::String("the only copy".to_string()));
    db.document_put("notes", doc).await.expect("document_put");
    db.flush().await.expect("flush");
    drop(db);
}

/// Damage every data page of the main file the way a bad sector or a failing
/// controller does: the page contents are intact but no longer verify.
fn corrupt_data_pages(path: &Path) {
    use std::io::{Read, Seek, SeekFrom, Write};

    let main = path.join("main.db");
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&main)
        .expect("open main.db");
    let len = file.metadata().expect("stat main.db").len();
    assert!(
        len > FIRST_DATA_PAGE * PAGE_SIZE,
        "the seed writes must have allocated at least one data page; main.db is {len} bytes"
    );

    let mut page = FIRST_DATA_PAGE;
    while (page + 1) * PAGE_SIZE <= len {
        let offset = (page + 1) * PAGE_SIZE - AEAD_TAG_LEN as u64;
        let mut tag = [0u8; AEAD_TAG_LEN];
        file.seek(SeekFrom::Start(offset)).expect("seek");
        file.read_exact(&mut tag).expect("read tag");
        for b in &mut tag {
            *b ^= 0xFF;
        }
        file.seek(SeekFrom::Start(offset)).expect("seek back");
        file.write_all(&tag).expect("write tag");
        page += 1;
    }
    file.sync_all().expect("sync main.db");
}

/// Sibling entries created next to `path` — a store renamed aside shows up here.
fn siblings(path: &Path) -> Vec<String> {
    let parent = path.parent().expect("store path has a parent");
    std::fs::read_dir(parent)
        .expect("read store parent dir")
        .map(|e| {
            e.expect("dir entry")
                .file_name()
                .to_string_lossy()
                .into_owned()
        })
        .filter(|name| Path::new(name) != path.file_name().map(Path::new).unwrap_or(Path::new("")))
        .collect()
}

/// Seed a store at a fresh temp path and corrupt it. Returns the tempdir guard
/// (kept alive by the caller) and the store path.
async fn corrupt_store(name: &str) -> (tempfile::TempDir, std::path::PathBuf) {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join(format!("{name}.pagedb"));
    seed_store(&path).await;
    corrupt_data_pages(&path);
    (dir, path)
}

/// Seed an in-memory store with one collection's CRDT state and hand back the
/// storage handle, so a test can damage a single persisted blob inside it.
async fn seed_mem_storage() -> PagedbStorageMem {
    let storage = PagedbStorageMem::open_in_memory()
        .await
        .expect("in-memory storage");
    let config = LiteConfig {
        auto_flush_ms: 0,
        ..LiteConfig::default()
    };
    let db: Arc<NodeDbLite<PagedbStorageMem>> =
        NodeDbLite::open_with_config(storage.clone(), config)
            .await
            .expect("seed open");
    let mut doc = Document::new("note0".to_string());
    doc.set("body", Value::String("the only copy".to_string()));
    db.document_put("notes", doc).await.expect("document_put");
    db.flush().await.expect("flush");
    drop(db);
    storage
}

// ─── Store-level corruption at open ──────────────────────────────────────────

/// An unreadable page is a fault the embedder has to decide about. Open reports
/// it; it does not hand back a working handle onto a store it emptied first.
#[tokio::test]
async fn open_at_path_fails_on_a_corrupt_store() {
    let (_dir, path) = corrupt_store("fail_closed").await;

    // `NodeDbLite` is not `Debug`, so match rather than `expect_err`.
    match NodeDbLite::open_at_path(&path, Encryption::Plaintext).await {
        Ok(_) => panic!("a store with unreadable pages must not open successfully"),
        Err(e) => {
            let msg = e.to_string();
            assert!(
                msg.to_lowercase().contains("corrupt"),
                "the error must name corruption so the embedder can tell it from a \
                 missing file or a bad passphrase, got: {msg}"
            );
        }
    }
}

/// The error has to be matchable, not just readable: an embedder that wants to
/// re-sync rather than refuse to start has to branch on the corruption class
/// without string-matching the message.
#[tokio::test]
async fn corrupt_store_open_error_is_typed_as_corruption() {
    let (_dir, path) = corrupt_store("typed_error").await;

    match NodeDbLite::open_at_path(&path, Encryption::Plaintext).await {
        Ok(_) => panic!("a store with unreadable pages must not open successfully"),
        Err(e) => assert!(
            matches!(e.details(), ErrorDetails::SegmentCorrupted { .. }),
            "corruption must be typed so callers can match on it, got: {:?}",
            e.details()
        ),
    }
}

/// A failed open leaves the bytes untouched. Renaming the store aside is a
/// destructive act whose only saving grace is an implementation detail of where
/// the bytes land — the caller cannot rely on it, so the store stays put.
#[tokio::test]
async fn failed_open_leaves_the_corrupt_store_in_place() {
    let (_dir, path) = corrupt_store("left_in_place").await;
    let before = std::fs::metadata(path.join("main.db"))
        .expect("main.db exists before open")
        .len();

    let _ = NodeDbLite::open_at_path(&path, Encryption::Plaintext).await;

    assert!(
        path.exists(),
        "the store directory must still be at its own path after a failed open"
    );
    let after = std::fs::metadata(path.join("main.db"))
        .expect("main.db must still exist after a failed open")
        .len();
    assert_eq!(
        before, after,
        "a failed open must not rewrite the store it could not read"
    );
    let moved: Vec<_> = siblings(&path)
        .into_iter()
        .filter(|name| name.contains(".corrupt."))
        .collect();
    assert!(
        moved.is_empty(),
        "the store must not be renamed aside behind the caller's back, found: {moved:?}"
    );
}

/// A failed open creates nothing. The recovery path's replacement store is what
/// makes the loss permanent: the service starts, accepts writes into the empty
/// store, and the set-aside copy diverges from it.
#[tokio::test]
async fn failed_open_creates_no_replacement_store() {
    let (_dir, path) = corrupt_store("no_replacement").await;

    let _ = NodeDbLite::open_at_path(&path, Encryption::Plaintext).await;

    let db = NodeDbLite::open_at_path(&path, Encryption::Plaintext).await;
    assert!(
        db.is_err(),
        "reopening after a failed open must report the same fault, not succeed \
         against a fresh store put there by the first attempt"
    );
}

/// The config-carrying entry point is the one an embedder with tuned budgets
/// uses, and it opens the same store through the same recovery path.
#[tokio::test]
async fn open_at_path_with_config_fails_on_a_corrupt_store() {
    let (_dir, path) = corrupt_store("with_config").await;

    match NodeDbLite::open_at_path_with_config(&path, Encryption::Plaintext, LiteConfig::default())
        .await
    {
        Ok(_) => panic!("a store with unreadable pages must not open successfully"),
        Err(e) => assert!(
            matches!(e.details(), ErrorDetails::SegmentCorrupted { .. }),
            "corruption must be typed so callers can match on it, got: {:?}",
            e.details()
        ),
    }
}

// ─── The opt-in ──────────────────────────────────────────────────────────────

/// A caller who has an Origin to refill from can ask for the old behaviour by
/// name, and gets a working, empty database.
#[tokio::test]
async fn opting_in_recovers_by_discarding_the_store() {
    let (_dir, path) = corrupt_store("opt_in").await;
    let config = LiteConfig {
        corruption_policy: CorruptionPolicy::DiscardStoreAndRecreate,
        ..LiteConfig::default()
    };

    let db = NodeDbLite::open_at_path_with_config(&path, Encryption::Plaintext, config)
        .await
        .expect("opting into discarding the store must open a fresh database");

    let rows = db
        .document_get("notes", "note0")
        .await
        .expect("read from the fresh store");
    assert!(
        rows.is_none(),
        "the recovered database is a new empty one, so the old document must be gone"
    );
}

/// Even under the opt-in the old bytes are preserved — that is the only thing
/// that makes the choice survivable, so it is part of the contract, not an
/// implementation detail of `std::fs::rename`.
#[tokio::test]
async fn opting_in_preserves_the_discarded_store() {
    let (_dir, path) = corrupt_store("opt_in_preserved").await;
    let config = LiteConfig {
        corruption_policy: CorruptionPolicy::DiscardStoreAndRecreate,
        ..LiteConfig::default()
    };

    let _db = NodeDbLite::open_at_path_with_config(&path, Encryption::Plaintext, config)
        .await
        .expect("opting into discarding the store must open a fresh database");

    let set_aside: Vec<_> = siblings(&path)
        .into_iter()
        .filter(|name| name.contains(".corrupt."))
        .collect();
    assert_eq!(
        set_aside.len(),
        1,
        "the discarded store must be kept alongside the new one, found: {set_aside:?}"
    );
}

// ─── Blob-level corruption during restore ────────────────────────────────────

/// One collection's snapshot failing its checksum is the same decision at a
/// smaller scale: the collection's history is gone if it is dropped, and only
/// the embedder knows whether anything can replay it.
#[tokio::test]
async fn corrupt_crdt_snapshot_fails_open() {
    let storage = seed_mem_storage().await;
    let key = CrdtEngine::snapshot_key_for("notes");
    let envelope = storage
        .get(Namespace::LoroState, &key)
        .await
        .expect("read snapshot")
        .expect("the seeded flush must have written a snapshot");
    let mut damaged = envelope.clone();
    damaged[0] ^= 0xFF;
    storage
        .put(Namespace::LoroState, &key, &damaged)
        .await
        .expect("write damaged snapshot");

    match NodeDbLite::open(storage).await {
        Ok(_) => panic!("a collection whose snapshot fails its checksum must not open silently"),
        Err(e) => assert!(
            matches!(e.details(), ErrorDetails::SegmentCorrupted { .. }),
            "a failed snapshot checksum must be typed as corruption, got: {:?}",
            e.details()
        ),
    }
}

/// Deleting the damaged snapshot destroys the forensic copy and makes the loss
/// irreversible on the very next open. Whatever the caller decides, the bytes
/// have to still be there when they decide it.
#[tokio::test]
async fn corrupt_crdt_snapshot_is_not_deleted_by_open() {
    let storage = seed_mem_storage().await;
    let key = CrdtEngine::snapshot_key_for("notes");
    let envelope = storage
        .get(Namespace::LoroState, &key)
        .await
        .expect("read snapshot")
        .expect("the seeded flush must have written a snapshot");
    let mut damaged = envelope.clone();
    damaged[0] ^= 0xFF;
    storage
        .put(Namespace::LoroState, &key, &damaged)
        .await
        .expect("write damaged snapshot");

    let _ = NodeDbLite::open(storage.clone()).await;

    let after = storage
        .get(Namespace::LoroState, &key)
        .await
        .expect("read snapshot after open");
    assert_eq!(
        after.as_deref(),
        Some(damaged.as_slice()),
        "open must leave the damaged snapshot where it found it"
    );
}

/// A queued delta is a local write that no Origin has acknowledged yet, so
/// dropping an undecodable one loses data that exists nowhere else.
#[tokio::test]
async fn corrupt_pending_delta_fails_open() {
    let storage = seed_mem_storage().await;
    storage
        .put(
            Namespace::Crdt,
            b"delta:0000000000000001",
            b"\x6enot-a-delta",
        )
        .await
        .expect("write damaged pending delta");

    match NodeDbLite::open(storage).await {
        Ok(_) => panic!("an undecodable queued mutation must not be dropped behind the caller"),
        Err(e) => assert!(
            matches!(e.details(), ErrorDetails::SegmentCorrupted { .. }),
            "an undecodable queued mutation must be typed as corruption, got: {:?}",
            e.details()
        ),
    }
}

/// The queue is not cleared behind the caller either — a dropped entry is only
/// recoverable while its bytes are still in the store.
#[tokio::test]
async fn corrupt_pending_delta_is_not_deleted_by_open() {
    let storage = seed_mem_storage().await;
    let damaged: &[u8] = b"\x6enot-a-delta";
    storage
        .put(Namespace::Crdt, b"delta:0000000000000001", damaged)
        .await
        .expect("write damaged pending delta");

    let _ = NodeDbLite::open(storage.clone()).await;

    let after = storage
        .get(Namespace::Crdt, b"delta:0000000000000001")
        .await
        .expect("read pending delta after open");
    assert_eq!(
        after.as_deref(),
        Some(damaged),
        "open must leave the undecodable queued mutation where it found it"
    );
}
