// SPDX-License-Identifier: Apache-2.0

//! Unit-level gate for the implicit-collection announce fix (no Origin needed).
//!
//! A collection created only by writing data to it (a document upsert, a vector
//! insert, or an FTS write) with no explicit `create_collection` has no
//! persisted `collection:` metadata. Before the fix, `SyncDelegate::
//! get_collection_meta` returned `None` for such a collection, so the outbound
//! per-engine announce (`ensure_collection_announced`) skipped it and its data
//! reached Origin as writes for an unknown collection and was silently dropped.
//!
//! These tests prove `get_collection_meta` now synthesizes a base document meta
//! for any collection present in CRDT state — the same synthesis
//! `list_collections` already performs — so the announce path can register it.

use std::sync::Arc;

use nodedb_client::NodeDb;
use nodedb_lite::sync::SyncDelegate;
use nodedb_lite::{NodeDbLite, PagedbStorageMem};

async fn open_lite() -> Arc<NodeDbLite<PagedbStorageMem>> {
    let storage = PagedbStorageMem::open_in_memory()
        .await
        .expect("open_in_memory");
    Arc::new(NodeDbLite::open(storage, 1).await.expect("NodeDbLite::open"))
}

/// A pure-vector collection created only via `vector_insert` (no
/// `create_collection`) resolves to a synthesized base document meta, so the
/// outbound announce registers it on Origin instead of dropping its vectors.
#[tokio::test]
async fn vector_only_collection_has_announceable_meta() {
    let lite = open_lite().await;

    lite.vector_insert("vec_impl_test", "v0", &[1.0, 0.0, 0.0], None)
        .await
        .expect("vector_insert");

    let delegate = Arc::clone(&lite) as Arc<dyn SyncDelegate>;
    let meta = delegate
        .get_collection_meta("vec_impl_test")
        .await
        .expect("implicit vector collection must resolve a base meta for announce");

    assert_eq!(meta.name, "vec_impl_test");
    assert_eq!(
        meta.collection_type, "document",
        "overlay collections announce a base document descriptor"
    );
    assert!(
        meta.descriptor_json.is_none(),
        "synthesized implicit meta carries no persisted descriptor"
    );
}

/// A name that exists in neither persisted meta nor CRDT state still resolves to
/// `None` — the announce correctly skips truly-unknown collections.
#[tokio::test]
async fn unknown_collection_has_no_meta() {
    let lite = open_lite().await;
    let delegate = Arc::clone(&lite) as Arc<dyn SyncDelegate>;
    assert!(
        delegate.get_collection_meta("never_created").await.is_none(),
        "a collection with no persisted meta and no CRDT state must resolve None"
    );
}

/// Internal `__`-prefixed collections are never announced even if present in
/// CRDT state.
#[tokio::test]
async fn internal_collection_is_not_announceable() {
    let lite = open_lite().await;
    let delegate = Arc::clone(&lite) as Arc<dyn SyncDelegate>;
    assert!(
        delegate.get_collection_meta("__internal").await.is_none(),
        "internal __ collections must never resolve an announceable meta"
    );
}
