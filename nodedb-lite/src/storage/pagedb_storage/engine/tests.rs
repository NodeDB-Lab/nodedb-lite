// SPDX-License-Identifier: Apache-2.0

//! `StorageEngine` behaviour over the pagedb backing.

use pagedb::vfs::memory::MemVfs;

use nodedb_types::Namespace;

use crate::storage::engine::{StorageEngine, WriteOp};
use crate::storage::pagedb_storage::types::PagedbStorage;

async fn make_storage() -> PagedbStorage<MemVfs> {
    PagedbStorage::open_in_memory().await.unwrap()
}

#[tokio::test]
async fn put_get_roundtrip() {
    let s = make_storage().await;
    s.put(Namespace::Vector, b"v1", b"hello").await.unwrap();
    let val = s.get(Namespace::Vector, b"v1").await.unwrap();
    assert_eq!(val.as_deref(), Some(b"hello".as_slice()));
}

#[tokio::test]
async fn get_missing_returns_none() {
    let s = make_storage().await;
    let val = s.get(Namespace::Vector, b"nope").await.unwrap();
    assert!(val.is_none());
}

#[tokio::test]
async fn put_overwrites() {
    let s = make_storage().await;
    s.put(Namespace::Graph, b"k", b"first").await.unwrap();
    s.put(Namespace::Graph, b"k", b"second").await.unwrap();
    let val = s.get(Namespace::Graph, b"k").await.unwrap();
    assert_eq!(val.as_deref(), Some(b"second".as_slice()));
}

#[tokio::test]
async fn delete_removes_key() {
    let s = make_storage().await;
    s.put(Namespace::Crdt, b"k", b"val").await.unwrap();
    s.delete(Namespace::Crdt, b"k").await.unwrap();
    assert!(s.get(Namespace::Crdt, b"k").await.unwrap().is_none());
}

#[tokio::test]
async fn delete_nonexistent_is_noop() {
    let s = make_storage().await;
    s.delete(Namespace::Meta, b"ghost").await.unwrap();
}

#[tokio::test]
async fn namespaces_are_isolated() {
    let s = make_storage().await;
    s.put(Namespace::Vector, b"k", b"vec").await.unwrap();
    s.put(Namespace::Graph, b"k", b"graph").await.unwrap();

    assert_eq!(
        s.get(Namespace::Vector, b"k").await.unwrap().as_deref(),
        Some(b"vec".as_slice())
    );
    assert_eq!(
        s.get(Namespace::Graph, b"k").await.unwrap().as_deref(),
        Some(b"graph".as_slice())
    );
}

#[tokio::test]
async fn scan_prefix_basic() {
    let s = make_storage().await;
    s.put(Namespace::Vector, b"vec:001", b"a").await.unwrap();
    s.put(Namespace::Vector, b"vec:002", b"b").await.unwrap();
    s.put(Namespace::Vector, b"vec:003", b"c").await.unwrap();
    s.put(Namespace::Vector, b"other:001", b"d").await.unwrap();

    let results = s.scan_prefix(Namespace::Vector, b"vec:").await.unwrap();
    assert_eq!(results.len(), 3);
    assert_eq!(results[0].0, b"vec:001");
    assert_eq!(results[1].0, b"vec:002");
    assert_eq!(results[2].0, b"vec:003");
}

#[tokio::test]
async fn scan_prefix_empty_returns_all() {
    let s = make_storage().await;
    s.put(Namespace::Meta, b"a", b"1").await.unwrap();
    s.put(Namespace::Meta, b"b", b"2").await.unwrap();
    s.put(Namespace::Vector, b"c", b"3").await.unwrap();

    let results = s.scan_prefix(Namespace::Meta, b"").await.unwrap();
    assert_eq!(results.len(), 2);
}

#[tokio::test]
async fn scan_prefix_no_match() {
    let s = make_storage().await;
    s.put(Namespace::Graph, b"edge:1", b"data").await.unwrap();
    let results = s.scan_prefix(Namespace::Graph, b"node:").await.unwrap();
    assert!(results.is_empty());
}

#[tokio::test]
async fn batch_write_atomic() {
    let s = make_storage().await;
    s.put(Namespace::Crdt, b"to_delete", b"old").await.unwrap();

    s.batch_write(&[
        WriteOp::Put {
            ns: Namespace::Crdt,
            key: b"new1".to_vec(),
            value: b"val1".to_vec(),
        },
        WriteOp::Put {
            ns: Namespace::Crdt,
            key: b"new2".to_vec(),
            value: b"val2".to_vec(),
        },
        WriteOp::Delete {
            ns: Namespace::Crdt,
            key: b"to_delete".to_vec(),
        },
    ])
    .await
    .unwrap();

    assert!(s.get(Namespace::Crdt, b"new1").await.unwrap().is_some());
    assert!(s.get(Namespace::Crdt, b"new2").await.unwrap().is_some());
    assert!(
        s.get(Namespace::Crdt, b"to_delete")
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn batch_write_unique_puts_accepts_unsorted_keys() {
    let s = make_storage().await;
    s.batch_write(&[
        WriteOp::Put {
            ns: Namespace::Graph,
            key: b"z".to_vec(),
            value: b"last-key".to_vec(),
        },
        WriteOp::Put {
            ns: Namespace::Graph,
            key: b"a".to_vec(),
            value: b"first-key".to_vec(),
        },
    ])
    .await
    .unwrap();

    assert_eq!(
        s.get(Namespace::Graph, b"a").await.unwrap().as_deref(),
        Some(b"first-key".as_slice())
    );
    assert_eq!(
        s.get(Namespace::Graph, b"z").await.unwrap().as_deref(),
        Some(b"last-key".as_slice())
    );
}

#[tokio::test]
async fn batch_write_duplicate_puts_preserves_last_write() {
    let s = make_storage().await;
    s.batch_write(&[
        WriteOp::Put {
            ns: Namespace::Graph,
            key: b"same".to_vec(),
            value: b"first".to_vec(),
        },
        WriteOp::Put {
            ns: Namespace::Graph,
            key: b"same".to_vec(),
            value: b"second".to_vec(),
        },
    ])
    .await
    .unwrap();

    assert_eq!(
        s.get(Namespace::Graph, b"same")
            .await
            .unwrap()
            .as_deref(),
        Some(b"second".as_slice())
    );
}

/// Same-key put-then-delete in a batch: the delete must win.
#[tokio::test]
async fn batch_write_same_key_put_then_delete() {
    let s = make_storage().await;
    s.batch_write(&[
        WriteOp::Put {
            ns: Namespace::Meta,
            key: b"clash".to_vec(),
            value: b"written".to_vec(),
        },
        WriteOp::Delete {
            ns: Namespace::Meta,
            key: b"clash".to_vec(),
        },
    ])
    .await
    .unwrap();
    // Delete came after Put in the ops slice, so the key must be absent.
    assert!(s.get(Namespace::Meta, b"clash").await.unwrap().is_none());
}

/// Same-key delete-then-put in a batch: the put must win.
#[tokio::test]
async fn batch_write_same_key_delete_then_put() {
    let s = make_storage().await;
    s.put(Namespace::Meta, b"exists", b"old").await.unwrap();
    s.batch_write(&[
        WriteOp::Delete {
            ns: Namespace::Meta,
            key: b"exists".to_vec(),
        },
        WriteOp::Put {
            ns: Namespace::Meta,
            key: b"exists".to_vec(),
            value: b"new".to_vec(),
        },
    ])
    .await
    .unwrap();
    // Put came after Delete, so the key must be present with the new value.
    assert_eq!(
        s.get(Namespace::Meta, b"exists").await.unwrap().as_deref(),
        Some(b"new".as_slice())
    );
}

#[tokio::test]
async fn batch_write_empty_is_noop() {
    let s = make_storage().await;
    s.batch_write(&[]).await.unwrap();
}

#[tokio::test]
async fn count_entries() {
    let s = make_storage().await;
    assert_eq!(s.count(Namespace::Vector).await.unwrap(), 0);

    s.put(Namespace::Vector, b"v1", b"a").await.unwrap();
    s.put(Namespace::Vector, b"v2", b"b").await.unwrap();
    s.put(Namespace::Graph, b"g1", b"c").await.unwrap();

    assert_eq!(s.count(Namespace::Vector).await.unwrap(), 2);
    assert_eq!(s.count(Namespace::Graph).await.unwrap(), 1);
    assert_eq!(s.count(Namespace::Crdt).await.unwrap(), 0);
}

#[tokio::test]
async fn large_value_roundtrip() {
    let s = make_storage().await;
    let large = vec![0xABu8; 1_000_000];
    s.put(Namespace::Vector, b"hnsw:layer0", &large)
        .await
        .unwrap();
    let val = s.get(Namespace::Vector, b"hnsw:layer0").await.unwrap();
    assert_eq!(val.unwrap().len(), 1_000_000);
}

#[tokio::test]
async fn scan_range_with_limit() {
    let s = make_storage().await;
    for i in 0u8..10 {
        s.put(Namespace::Vector, &[i], &[i * 2]).await.unwrap();
    }
    let results = s.scan_range(Namespace::Vector, &[0], 3).await.unwrap();
    assert_eq!(results.len(), 3);
    assert_eq!(results[0].0, &[0u8]);
    assert_eq!(results[1].0, &[1u8]);
    assert_eq!(results[2].0, &[2u8]);
}

#[tokio::test]
async fn scan_range_bounded_with_start_and_end() {
    let s = make_storage().await;
    for i in 0u8..10 {
        s.put(Namespace::Graph, &[i], &[i]).await.unwrap();
    }
    // Keys [2, 3, 4] — start inclusive, end exclusive.
    let results = s
        .scan_range_bounded(Namespace::Graph, Some(&[2]), Some(&[5]), None)
        .await
        .unwrap();
    assert_eq!(results.len(), 3);
    assert_eq!(results[0].0, &[2u8]);
    assert_eq!(results[1].0, &[3u8]);
    assert_eq!(results[2].0, &[4u8]);
}

/// In-memory engine: `compact()` is a successful no-op (nothing to reclaim).
#[tokio::test]
async fn compact_mem_is_ok_noop() {
    let s = make_storage().await;
    s.put(Namespace::Vector, b"v1", b"hello").await.unwrap();
    s.put(Namespace::Graph, b"g1", b"world").await.unwrap();
    let outcome = s.compact().await.unwrap();
    // Data still readable after compaction.
    assert_eq!(
        s.get(Namespace::Vector, b"v1").await.unwrap().as_deref(),
        Some(b"hello".as_slice())
    );
    // MemVfs has no file truncation, but the call must succeed regardless.
    let _ = outcome.reclaimed_pages;
}

/// Disk-backed engine on a tempdir: write rows (including churn that leaves
/// dead pages), then `compact()` must succeed and report a non-negative
/// outcome. Data must remain intact afterward.
#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn compact_default_disk_is_ok() {
    use pagedb::vfs::DefaultVfs;

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("compact-test.db");
    let s =
        PagedbStorage::<DefaultVfs>::open(&path, crate::storage::encryption::Encryption::Plaintext)
            .await
            .unwrap();

    // Churn: write then overwrite/delete a batch of keys so the
    // deferred-free list has pages to reclaim.
    for i in 0u32..200 {
        let key = i.to_be_bytes();
        s.put(Namespace::Meta, &key, &vec![0xCDu8; 512])
            .await
            .unwrap();
    }
    for i in 0u32..150 {
        let key = i.to_be_bytes();
        s.delete(Namespace::Meta, &key).await.unwrap();
    }

    let outcome = s.compact().await.unwrap();

    // Surviving keys still readable.
    let survivor = 175u32.to_be_bytes();
    assert!(s.get(Namespace::Meta, &survivor).await.unwrap().is_some());

    // Outcome fields are well-formed (u64/u32 — always >= 0); just touch
    // them so the assertion documents the reported shape.
    let _ = (
        outcome.reclaimed_pages,
        outcome.segments_repacked,
        outcome.file_bytes_freed,
    );
}

/// Keys in namespace N must not appear in a scan of namespace N+1, and
/// vice versa. Verifies the single-byte prefix boundary.
#[tokio::test]
async fn scan_range_bounded_namespace_isolation() {
    let s = make_storage().await;

    // Write keys into two consecutive namespaces.
    for i in 0u8..5 {
        s.put(Namespace::Vector, &[i], b"vec").await.unwrap();
    }
    for i in 0u8..5 {
        s.put(Namespace::Graph, &[i], b"graph").await.unwrap();
    }

    // Full unbounded scan of Vector must return only Vector entries.
    let vec_results = s
        .scan_range_bounded(Namespace::Vector, None, None, None)
        .await
        .unwrap();
    assert_eq!(
        vec_results.len(),
        5,
        "Vector scan leaked into another namespace"
    );
    assert!(vec_results.iter().all(|(_, v)| v == b"vec"));

    // Full unbounded scan of Graph must return only Graph entries.
    let graph_results = s
        .scan_range_bounded(Namespace::Graph, None, None, None)
        .await
        .unwrap();
    assert_eq!(
        graph_results.len(),
        5,
        "Graph scan leaked into another namespace"
    );
    assert!(graph_results.iter().all(|(_, v)| v == b"graph"));
}
