// SPDX-License-Identifier: Apache-2.0

//! §24 KV engine gate tests — BETA narrow subset for NodeDB-Lite 0.1.0.
//!
//! Scope: put / get / delete only.
//! TTL and sorted-index are EXPERIMENTAL and are NOT exercised here.
//!
//! The KV implementation lives in `nodedb/collection/kv.rs` and is not a
//! standalone engine module.  Both sync modes (direct-redb and CRDT-backed)
//! share the same public `kv_put` / `kv_get` / `kv_delete` surface tested
//! below.

use nodedb_lite::{NodeDbLite, RedbStorage};

async fn open_db() -> NodeDbLite<RedbStorage> {
    let storage = RedbStorage::open_in_memory().expect("open in-memory storage");
    NodeDbLite::open(storage, 1).await.expect("open NodeDbLite")
}

// ---------------------------------------------------------------------------
// kv_put_get_delete_roundtrip
// ---------------------------------------------------------------------------

/// Put 5 keys, get each back asserting value matches, delete 2, re-get those
/// asserting absent, re-get remaining 3 asserting still present.
#[tokio::test]
async fn kv_put_get_delete_roundtrip() {
    let db = open_db().await;
    let col = "gate_roundtrip";

    let pairs: &[(&str, &[u8])] = &[
        ("key1", b"value_one"),
        ("key2", b"value_two"),
        ("key3", b"value_three"),
        ("key4", b"value_four"),
        ("key5", b"value_five"),
    ];

    // Insert all 5 keys.
    for (k, v) in pairs {
        db.kv_put(col, k, v).expect("kv_put");
    }

    // Flush to redb to ensure persistence layer is exercised.
    db.kv_flush().expect("kv_flush");

    // Get each key back and assert value matches.
    for (k, expected) in pairs {
        let got = db.kv_get(col, k).expect("kv_get");
        assert_eq!(
            got.as_deref(),
            Some(*expected),
            "value mismatch for key {k}"
        );
    }

    // Delete key2 and key4.
    db.kv_delete(col, "key2").expect("kv_delete key2");
    db.kv_delete(col, "key4").expect("kv_delete key4");
    db.kv_flush().expect("kv_flush after deletes");

    // Deleted keys must be absent.
    let gone2 = db.kv_get(col, "key2").expect("kv_get key2 after delete");
    assert!(gone2.is_none(), "key2 should be absent after delete");

    let gone4 = db.kv_get(col, "key4").expect("kv_get key4 after delete");
    assert!(gone4.is_none(), "key4 should be absent after delete");

    // Remaining keys must still be present.
    for k in ["key1", "key3", "key5"] {
        let expected = pairs
            .iter()
            .find(|(pk, _)| *pk == k)
            .map(|(_, v)| *v)
            .expect("pair exists");
        let got = db.kv_get(col, k).expect("kv_get remaining");
        assert_eq!(
            got.as_deref(),
            Some(expected),
            "value mismatch for surviving key {k}"
        );
    }
}

// ---------------------------------------------------------------------------
// kv_get_missing_returns_none_or_error
// ---------------------------------------------------------------------------

/// Get a never-inserted key and assert the API returns `None` (the overlay
/// path) and then `None` from redb as well (after a flush with no writes).
#[tokio::test]
async fn kv_get_missing_returns_none_or_error() {
    let db = open_db().await;
    let col = "gate_missing";

    // Query a key that was never inserted.
    let result = db
        .kv_get(col, "never_inserted_key")
        .expect("kv_get should not error for missing key");

    assert!(
        result.is_none(),
        "expected None for a missing key, got {result:?}"
    );

    // Also verify that a flush + re-get still returns None (not an error).
    db.kv_flush().expect("kv_flush");
    let result2 = db
        .kv_get(col, "never_inserted_key")
        .expect("kv_get after flush should not error");

    assert!(
        result2.is_none(),
        "expected None after flush for a missing key, got {result2:?}"
    );
}
