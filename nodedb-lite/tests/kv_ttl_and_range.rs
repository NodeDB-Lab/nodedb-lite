// SPDX-License-Identifier: Apache-2.0

//! KV TTL and range-scan gate tests — BETA gate for NodeDB-Lite.
//!
//! Exercises: kv_put_with_ttl, kv_get (expiry), kv_range_scan.
//! See docs/lite-support-matrix.md §Key-value.

use nodedb_lite::{NodeDbLite, RedbStorage};

async fn open_memory_db() -> NodeDbLite<RedbStorage> {
    let storage = RedbStorage::open_in_memory().expect("open in-memory storage");
    NodeDbLite::open(storage, 1).await.expect("open NodeDbLite")
}

// ---------------------------------------------------------------------------
// ttl_expires_on_read
// ---------------------------------------------------------------------------

/// A key written with ttl_ms=50 is visible immediately but returns None
/// after 75ms.
#[tokio::test]
async fn ttl_expires_on_read() {
    let db = open_memory_db().await;
    let col = "ttl_test_expire";

    db.kv_put_with_ttl(col, "k", b"hello", 50)
        .expect("kv_put_with_ttl");

    // Immediately readable.
    let got = db.kv_get(col, "k").expect("kv_get immediate");
    assert_eq!(
        got.as_deref(),
        Some(b"hello".as_slice()),
        "should be visible before TTL"
    );

    // Wait for TTL to elapse.
    std::thread::sleep(std::time::Duration::from_millis(75));

    let got = db.kv_get(col, "k").expect("kv_get after expiry");
    assert!(got.is_none(), "expired key should return None");
}

// ---------------------------------------------------------------------------
// ttl_survives_reopen
// ---------------------------------------------------------------------------

/// A key with a very long TTL (1 000 000 ms) persists across a database
/// reopen with the same on-disk path.
#[tokio::test]
async fn ttl_survives_reopen() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("ttl_survive.redb");

    {
        let storage = RedbStorage::open(&path).expect("open storage");
        let db = NodeDbLite::open(storage, 1).await.expect("open db");
        db.kv_put_with_ttl("col", "key", b"persistent", 1_000_000)
            .expect("kv_put_with_ttl");
        db.kv_flush().expect("kv_flush");
    }

    {
        let storage = RedbStorage::open(&path).expect("reopen storage");
        let db = NodeDbLite::open(storage, 1).await.expect("reopen db");
        let got = db.kv_get("col", "key").expect("kv_get after reopen");
        assert_eq!(
            got.as_deref(),
            Some(b"persistent".as_slice()),
            "value should survive reopen when TTL not yet elapsed"
        );
    }
}

// ---------------------------------------------------------------------------
// ttl_expired_after_reopen
// ---------------------------------------------------------------------------

/// A key with ttl_ms=50 is written, the database is flushed and dropped,
/// then after 75ms we reopen — get must return None.
#[tokio::test]
async fn ttl_expired_after_reopen() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("ttl_expired_reopen.redb");

    {
        let storage = RedbStorage::open(&path).expect("open storage");
        let db = NodeDbLite::open(storage, 1).await.expect("open db");
        db.kv_put_with_ttl("col", "key", b"transient", 50)
            .expect("kv_put_with_ttl");
        db.kv_flush().expect("kv_flush");
    }

    // Wait for TTL to elapse before reopening.
    std::thread::sleep(std::time::Duration::from_millis(75));

    {
        let storage = RedbStorage::open(&path).expect("reopen storage");
        let db = NodeDbLite::open(storage, 1).await.expect("reopen db");
        let got = db.kv_get("col", "key").expect("kv_get after reopen");
        assert!(got.is_none(), "expired key should return None after reopen");
    }
}

// ---------------------------------------------------------------------------
// range_scan_lex_order
// ---------------------------------------------------------------------------

/// Keys inserted out of order are returned in lexicographic order by
/// kv_range_scan(None, None).
#[tokio::test]
async fn range_scan_lex_order() {
    let db = open_memory_db().await;
    let col = "range_lex";

    db.kv_put(col, "a", b"va").expect("put a");
    db.kv_put(col, "c", b"vc").expect("put c");
    db.kv_put(col, "b", b"vb").expect("put b");
    db.kv_flush().expect("flush");

    let results = db.kv_range_scan(col, None, None, None).expect("range_scan");

    let keys: Vec<Vec<u8>> = results.iter().map(|(k, _)| k.clone()).collect();
    assert_eq!(
        keys,
        vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()],
        "keys must be in lex order"
    );
}

// ---------------------------------------------------------------------------
// range_scan_bounds
// ---------------------------------------------------------------------------

/// range_scan(Some(b"b"), Some(b"d")) on keys a..=e returns [b, c].
#[tokio::test]
async fn range_scan_bounds() {
    let db = open_memory_db().await;
    let col = "range_bounds";

    for ch in b'a'..=b'e' {
        let key = std::str::from_utf8(&[ch]).unwrap().to_string();
        let val = format!("v{key}");
        db.kv_put(col, &key, val.as_bytes()).expect("put");
    }
    db.kv_flush().expect("flush");

    let results = db
        .kv_range_scan(col, Some(b"b"), Some(b"d"), None)
        .expect("range_scan");

    let keys: Vec<Vec<u8>> = results.iter().map(|(k, _)| k.clone()).collect();
    assert_eq!(
        keys,
        vec![b"b".to_vec(), b"c".to_vec()],
        "range [b, d) should return b and c"
    );
}

// ---------------------------------------------------------------------------
// range_scan_skips_expired
// ---------------------------------------------------------------------------

/// An expired key is invisible to range_scan.
#[tokio::test]
async fn range_scan_skips_expired() {
    let db = open_memory_db().await;
    let col = "range_expire";

    db.kv_put_with_ttl(col, "a", b"va", 50)
        .expect("put a with ttl");
    db.kv_put_with_ttl(col, "b", b"vb", 1_000_000)
        .expect("put b long ttl");
    db.kv_flush().expect("flush");

    std::thread::sleep(std::time::Duration::from_millis(75));

    let results = db.kv_range_scan(col, None, None, None).expect("range_scan");

    let keys: Vec<Vec<u8>> = results.iter().map(|(k, _)| k.clone()).collect();
    assert_eq!(
        keys,
        vec![b"b".to_vec()],
        "expired key a must be absent from range scan"
    );
}
