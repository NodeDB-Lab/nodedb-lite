//! Loro peer identity is a property of the local store, not of the caller.
//!
//! A Loro peer id names the producer of every operation the replica authors.
//! Two live replicas writing under one peer id have their operations merged as
//! replays of each other, so the identity must be unique per store and must
//! survive a reopen — an id that resets on restart cannot be rotated away from
//! a collision, and an id supplied by the caller collides the moment two
//! installs are handed the same constant.

use nodedb_client::NodeDb;
use nodedb_lite::{Encryption, NodeDbLite, PagedbStorageDefault, PagedbStorageMem};
use nodedb_types::document::Document;
use nodedb_types::value::Value;

#[tokio::test]
async fn peer_id_persists_across_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("peer_identity.db");

    let first_open;
    {
        let storage = PagedbStorageDefault::open(&path, Encryption::Plaintext)
            .await
            .expect("open storage");
        let db = NodeDbLite::open(storage).await.expect("open NodeDbLite");

        let mut doc = Document::new("user-alice");
        doc.set("username", Value::String("alice".into()));
        db.document_put("users", doc).await.unwrap();

        first_open = db.peer_id();
        db.flush().await.expect("flush");
    }

    {
        // Reopened with a different caller-supplied value: the store's own
        // identity is authoritative, otherwise a rotation performed to escape a
        // collision is forgotten on the next restart and the collision returns.
        let storage = PagedbStorageDefault::open(&path, Encryption::Plaintext)
            .await
            .expect("reopen storage");
        let db = NodeDbLite::open(storage).await.expect("reopen NodeDbLite");

        assert_eq!(
            db.peer_id(),
            first_open,
            "the persisted peer id must survive a reopen"
        );
        assert_ne!(
            db.peer_id(),
            99,
            "the caller's value must not displace the store's own peer identity"
        );
    }
}

#[tokio::test]
async fn separate_stores_do_not_share_a_peer_id() {
    // Every install of an application passes the same constant here — a
    // hardcoded id, a build-time constant, a value restored from a template.
    // Deriving the peer identity from it hands two live replicas one producer
    // identity, and the CRDT merge then discards one replica's writes.
    let a = NodeDbLite::open(PagedbStorageMem::open_in_memory().await.unwrap())
        .await
        .unwrap();
    let b = NodeDbLite::open(PagedbStorageMem::open_in_memory().await.unwrap())
        .await
        .unwrap();

    assert_ne!(
        a.peer_id(),
        b.peer_id(),
        "two independent stores must mint independent peer identities"
    );
    assert_ne!(a.peer_id(), 0, "a peer id of 0 reads as unset to Loro");
    assert_ne!(b.peer_id(), 0, "a peer id of 0 reads as unset to Loro");
}
