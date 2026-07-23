// SPDX-License-Identifier: Apache-2.0

//! Integration tests for the bitemporal document history storage layer.
//!
//! These tests exercise Stage A only: the storage primitives in
//! `engine::document::history`. Public `NodeDb` trait wiring is Stage B.

use nodedb_lite::engine::document::history::ops::{
    is_bitemporal, set_bitemporal, versioned_get_as_of, versioned_get_current, versioned_put,
    versioned_tombstone,
};
use nodedb_lite::storage::pagedb_storage::PagedbStorageMem;

async fn open_storage() -> PagedbStorageMem {
    PagedbStorageMem::open_in_memory()
        .await
        .expect("open in-memory storage")
}

// ---------------------------------------------------------------------------
// Test 1 — set_then_is_bitemporal
// ---------------------------------------------------------------------------

/// `set_bitemporal` followed by `is_bitemporal` returns the value that was set.
#[tokio::test]
async fn set_then_is_bitemporal() {
    let s = open_storage().await;

    // Default is false.
    assert!(!is_bitemporal(&s, "docs").await.unwrap());

    // Set to true and read back.
    set_bitemporal(&s, "docs", true).await.unwrap();
    assert!(is_bitemporal(&s, "docs").await.unwrap());

    // Set back to false and read back.
    set_bitemporal(&s, "docs", false).await.unwrap();
    assert!(!is_bitemporal(&s, "docs").await.unwrap());
}

// ---------------------------------------------------------------------------
// Test 2 — is_bitemporal_default_false
// ---------------------------------------------------------------------------

/// A collection that has never been explicitly flagged returns `false`.
#[tokio::test]
async fn is_bitemporal_default_false() {
    let s = open_storage().await;
    assert!(!is_bitemporal(&s, "never_seen_collection").await.unwrap());
}

// ---------------------------------------------------------------------------
// Test 3 — put_then_get_current_round_trips_body
// ---------------------------------------------------------------------------

/// Writing a document version and reading it back yields the original body.
#[tokio::test]
async fn put_then_get_current_round_trips_body() {
    let s = open_storage().await;
    let body = b"msgpack_encoded_document";
    versioned_put(&s, "articles", "doc-1", body, 1_000, None, None)
        .await
        .unwrap();

    let version = versioned_get_current(&s, "articles", "doc-1")
        .await
        .unwrap()
        .expect("should have a live version");

    assert_eq!(version.body, body);
    assert!(version.is_live());
    // valid_from defaults to system_from when not specified.
    assert_eq!(version.valid_from_ms, 1_000);
    assert_eq!(version.valid_until_ms, i64::MAX);
}

// ---------------------------------------------------------------------------
// Test 4 — put_then_tombstone_then_get_current_returns_none
// ---------------------------------------------------------------------------

/// Writing a tombstone after a live version makes `versioned_get_current`
/// return `None`.
#[tokio::test]
async fn put_then_tombstone_then_get_current_returns_none() {
    let s = open_storage().await;
    versioned_put(&s, "docs", "d1", b"body", 100, None, None)
        .await
        .unwrap();
    versioned_tombstone(&s, "docs", "d1", 200, None)
        .await
        .unwrap();

    let result = versioned_get_current(&s, "docs", "d1").await.unwrap();
    assert!(result.is_none(), "tombstoned document must not be returned");
}

// ---------------------------------------------------------------------------
// Test 5 — as_of_returns_version_visible_at_that_time
// ---------------------------------------------------------------------------

/// Two versions written at t=100 and t=200. `as_of` queries return the
/// correct version at each point in system time.
#[tokio::test]
async fn as_of_returns_version_visible_at_that_time() {
    let s = open_storage().await;

    // v1 written at system_from = 100.
    versioned_put(&s, "docs", "d1", b"v1_body", 100, None, None)
        .await
        .unwrap();
    // v2 written at system_from = 200.
    versioned_put(&s, "docs", "d1", b"v2_body", 200, None, None)
        .await
        .unwrap();

    // as_of(150) → v1 visible (100 <= 150, 200 > 150).
    let v = versioned_get_as_of(&s, "docs", "d1", 150, None)
        .await
        .unwrap()
        .expect("v1 must be visible at t=150");
    assert_eq!(v.body, b"v1_body");

    // as_of(250) → v2 visible (most recent, 200 <= 250).
    let v = versioned_get_as_of(&s, "docs", "d1", 250, None)
        .await
        .unwrap()
        .expect("v2 must be visible at t=250");
    assert_eq!(v.body, b"v2_body");

    // as_of(50) → nothing (100 > 50).
    let v = versioned_get_as_of(&s, "docs", "d1", 50, None)
        .await
        .unwrap();
    assert!(v.is_none(), "no version should exist before t=100");
}

// ---------------------------------------------------------------------------
// Test 6 — as_of_with_valid_time_filter
// ---------------------------------------------------------------------------

/// A document written with explicit valid_from=300, valid_until=500.
///
/// - `valid_time_ms = 400` → within range, returns the document.
/// - `valid_time_ms = 200` → before valid_from, returns None.
/// - `valid_time_ms = 600` → at or after valid_until, returns None.
#[tokio::test]
async fn as_of_with_valid_time_filter() {
    let s = open_storage().await;

    // System time 1000, valid window [300, 500).
    versioned_put(
        &s,
        "events",
        "e1",
        b"event_body",
        1_000,
        Some(300),
        Some(500),
    )
    .await
    .unwrap();

    // valid_time 400 is within [300, 500).
    let v = versioned_get_as_of(&s, "events", "e1", 2_000, Some(400))
        .await
        .unwrap()
        .expect("event valid at valid_time=400");
    assert_eq!(v.body, b"event_body");

    // valid_time 200 is before valid_from=300.
    let v = versioned_get_as_of(&s, "events", "e1", 2_000, Some(200))
        .await
        .unwrap();
    assert!(v.is_none(), "valid_time 200 is before valid_from 300");

    // valid_time 600 is at or after valid_until=500.
    let v = versioned_get_as_of(&s, "events", "e1", 2_000, Some(600))
        .await
        .unwrap();
    assert!(v.is_none(), "valid_time 600 is at or after valid_until 500");

    // Boundary: valid_time 500 is exactly at valid_until (exclusive).
    let v = versioned_get_as_of(&s, "events", "e1", 2_000, Some(500))
        .await
        .unwrap();
    assert!(v.is_none(), "valid_time 500 == valid_until is excluded");
}
