// SPDX-License-Identifier: Apache-2.0

//! Removal of index entries in the layouts used before index definitions
//! were persisted, both in `Namespace::Meta` holding a MessagePack
//! `Vec<String>` of ids: document entries `{collection}:{path}:{value}` and
//! key-value postings `kv:{collection}:{field}:{value}`.
//!
//! No definition names those entries, so nothing reads them and nothing can
//! rebuild them. They are removed once, at open, and a marker records that
//! the store holds none. A key is removed only when it is provably such an
//! entry: it starts with a known collection (after `kv:` for a posting) that
//! no other Meta key family starts with, it has a field and a value segment,
//! and its value decodes as a non-empty list of non-empty ids. Every other
//! key is kept.

use std::collections::BTreeSet;

use nodedb_types::Namespace;

use crate::error::LiteError;
use crate::storage::engine::{StorageEngine, WriteOp};

use super::key;

/// First segments of the other Meta key families spelled `{prefix}:...`. A
/// collection with one of these names cannot prove a key is its old entry.
const RESERVED_FIRST_SEGMENTS: &[&str] = &[
    "kv",
    "kv_sorted",
    "meta",
    "crdt",
    "collection",
    "graph",
    "fts",
    "catalog",
    "manifest",
    "strict_schema",
    "columnar_schema",
    "document_bitemporal",
    "graph_bitemporal",
    "vr",
    "v",
];

/// Whether `key` is `{prefix}{field}:{value}` holding an id list.
fn is_legacy_entry(key: &[u8], value: &[u8], prefix: &str) -> bool {
    let Ok(key) = std::str::from_utf8(key) else {
        return false;
    };
    let Some(rest) = key.strip_prefix(prefix) else {
        return false;
    };
    let Some((path, _value)) = rest.split_once(':') else {
        return false;
    };
    if path.is_empty() {
        return false;
    }
    match zerompk::from_msgpack::<Vec<String>>(value) {
        Ok(ids) => !ids.is_empty() && ids.iter().all(|id| !id.is_empty()),
        Err(_) => false,
    }
}

/// Remove the old-layout entries of `collections` unless the marker says a
/// previous open already did. Returns the number of keys removed.
pub(crate) async fn clear_legacy_entries<S: StorageEngine>(
    storage: &S,
    collections: &BTreeSet<String>,
) -> Result<u64, LiteError> {
    let marker = key::legacy_cleared_key();
    if storage.get(Namespace::Meta, &marker).await?.is_some() {
        return Ok(0);
    }
    let mut ops = Vec::new();
    for collection in collections {
        let mut prefixes = vec![format!("kv:{collection}:")];
        if !RESERVED_FIRST_SEGMENTS.contains(&collection.as_str()) {
            prefixes.push(format!("{collection}:"));
        }
        for prefix in prefixes {
            for (k, v) in storage
                .scan_prefix(Namespace::Meta, prefix.as_bytes())
                .await?
            {
                if is_legacy_entry(&k, &v, &prefix) {
                    ops.push(WriteOp::Delete {
                        ns: Namespace::Meta,
                        key: k,
                    });
                }
            }
        }
    }
    let removed = ops.len() as u64;
    ops.push(WriteOp::Put {
        ns: Namespace::Meta,
        key: marker,
        value: Vec::new(),
    });
    storage.batch_write(&ops).await?;
    Ok(removed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::pagedb_storage::PagedbStorageMem;

    fn ids(list: &[&str]) -> Vec<u8> {
        let owned: Vec<String> = list.iter().map(|s| (*s).to_string()).collect();
        zerompk::to_msgpack_vec(&owned).expect("encode")
    }

    #[tokio::test]
    async fn only_provable_old_entries_are_removed_once() {
        let storage = PagedbStorageMem::open_in_memory().await.expect("storage");
        let put = |k: &'static str, v: Vec<u8>| {
            let storage = &storage;
            async move {
                storage
                    .put(Namespace::Meta, k.as_bytes(), &v)
                    .await
                    .expect("put")
            }
        };
        put("users:email:a@x", ids(&["u1", "u2"])).await;
        put("users:$.email:b:c", ids(&["u3"])).await;
        // Not provable: no value segment, a value that is not an id list,
        // an unknown collection, and a KV posting.
        put("users:email", ids(&["u1"])).await;
        put("users:note:x", b"{\"a\":1}".to_vec()).await;
        put("ghost:email:a@x", ids(&["g1"])).await;
        put("kv:ghost:email:a@x", ids(&["k1"])).await;
        // An old key-value posting of a known collection.
        put("kv:users:email:a@x", ids(&["k1"])).await;

        let known: BTreeSet<String> = ["users", "kv"].iter().map(|s| (*s).to_string()).collect();
        assert_eq!(
            clear_legacy_entries(&storage, &known).await.expect("clear"),
            3
        );

        let get = |k: &'static str| {
            let storage = &storage;
            async move {
                storage
                    .get(Namespace::Meta, k.as_bytes())
                    .await
                    .expect("get")
            }
        };
        assert!(get("users:email:a@x").await.is_none());
        assert!(get("users:$.email:b:c").await.is_none());
        assert!(get("kv:users:email:a@x").await.is_none());
        for kept in [
            "users:email",
            "users:note:x",
            "ghost:email:a@x",
            "kv:ghost:email:a@x",
        ] {
            assert!(get(kept).await.is_some(), "{kept} is kept");
        }

        // The marker makes the pass run once.
        put("users:email:late", ids(&["u9"])).await;
        assert_eq!(
            clear_legacy_entries(&storage, &known).await.expect("clear"),
            0
        );
        assert!(get("users:email:late").await.is_some());
    }
}
