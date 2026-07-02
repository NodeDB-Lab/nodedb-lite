// SPDX-License-Identifier: Apache-2.0

//! Rebuild the `LatestVersion` index from existing `DocumentHistory` rows for
//! databases written before the index was introduced.

use nodedb_types::Namespace;

use crate::error::LiteError;
use crate::storage::engine::{StorageEngine, WriteOp};

use std::collections::HashMap;

use super::super::key::{coll_prefix, format_sys_from, latest_version_key, parse_sys_from};
use super::super::value::{VersionTag, decode_value};

/// Populate the `LatestVersion` index for `collection` from existing history rows.
///
/// Call this once per collection at open time.  If the index already has
/// entries for the collection (i.e. this database was written with the current
/// code), the function scans history, computes the correct pointers, and
/// overwrites any that are missing or stale — safe to call repeatedly.
///
/// A log line at `INFO` level reports the number of pointer rows written.
pub async fn backfill_latest_version<S: StorageEngine>(
    storage: &S,
    collection: &str,
) -> Result<(), LiteError> {
    let prefix = coll_prefix(collection);
    let entries = storage
        .scan_prefix(Namespace::DocumentHistory, &prefix)
        .await?;

    if entries.is_empty() {
        return Ok(());
    }

    // Walk all history rows and track, per doc_id, the highest system_from_ms
    // seen alongside its VersionTag.  The last row (highest timestamp) is the
    // current state of each document.
    let mut latest: HashMap<String, (i64, VersionTag)> = HashMap::new();

    for (key, value) in &entries {
        let after_prefix = match key.get(prefix.len()..) {
            Some(s) => s,
            None => continue,
        };
        let nul = match after_prefix.iter().position(|&b| b == 0) {
            Some(p) => p,
            None => continue,
        };
        let doc_id = match std::str::from_utf8(&after_prefix[..nul]) {
            Ok(s) => s.to_owned(),
            Err(_) => continue,
        };
        let decoded = match decode_value(value) {
            Ok(d) => d,
            Err(_) => continue,
        };
        let sys_from = match parse_sys_from(key) {
            Some(t) => t,
            None => continue,
        };

        // Higher timestamp = more recent; last-writer wins.
        let entry = latest.entry(doc_id).or_insert((sys_from, decoded.tag));
        if sys_from >= entry.0 {
            *entry = (sys_from, decoded.tag);
        }
    }

    // Build one batch: set pointer for live docs, delete pointer for tombstoned/erased.
    let mut ops: Vec<WriteOp> = Vec::with_capacity(latest.len());
    let mut written = 0usize;

    for (doc_id, (sys_from, tag)) in latest {
        let pointer_key = latest_version_key(collection, &doc_id);
        if tag == VersionTag::Live {
            let pointer_value = format_sys_from(sys_from).into_bytes();
            // Only write if pointer is absent or stale.
            let existing = storage.get(Namespace::LatestVersion, &pointer_key).await?;
            let expected = pointer_value.clone();
            if existing.as_deref() != Some(&expected) {
                ops.push(WriteOp::Put {
                    ns: Namespace::LatestVersion,
                    key: pointer_key,
                    value: pointer_value,
                });
                written += 1;
            }
        } else {
            // Non-live: remove stale pointer if present.
            if storage
                .get(Namespace::LatestVersion, &pointer_key)
                .await?
                .is_some()
            {
                ops.push(WriteOp::Delete {
                    ns: Namespace::LatestVersion,
                    key: pointer_key,
                });
                written += 1;
            }
        }
    }

    if !ops.is_empty() {
        storage.batch_write(&ops).await?;
    }

    if written > 0 {
        tracing::info!(
            collection,
            written,
            "backfilled LatestVersion index from DocumentHistory"
        );
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::storage::engine::WriteOp;
    use crate::storage::pagedb_storage::PagedbStorageMem;

    use super::super::super::key::versioned_doc_key;
    use super::super::super::value::encode_value;
    use super::super::read::versioned_get_current;
    use super::super::write::versioned_put;
    use super::*;

    async fn mem_storage() -> PagedbStorageMem {
        PagedbStorageMem::open_in_memory()
            .await
            .expect("open in-memory storage")
    }

    /// Simulate a database written before the LatestVersion index existed:
    /// write DocumentHistory rows directly (bypassing versioned_put to skip the
    /// pointer write), then call backfill and verify get_current works.
    #[tokio::test]
    async fn backfill_builds_index_from_history() {
        let s = mem_storage().await;

        // Write a history row without the LatestVersion pointer (pre-index state).
        let history_key = versioned_doc_key("c", "d1", 100).unwrap();
        let history_value = encode_value(VersionTag::Live, 100, i64::MAX, b"legacy_body");
        s.batch_write(&[WriteOp::Put {
            ns: Namespace::DocumentHistory,
            key: history_key,
            value: history_value,
        }])
        .await
        .unwrap();

        // No pointer yet.
        let ptr_key = latest_version_key("c", "d1");
        assert!(
            s.get(Namespace::LatestVersion, &ptr_key)
                .await
                .unwrap()
                .is_none(),
            "pointer must be absent before backfill"
        );

        // Run backfill.
        backfill_latest_version(&s, "c").await.unwrap();

        // Pointer now present.
        let ptr = s
            .get(Namespace::LatestVersion, &ptr_key)
            .await
            .unwrap()
            .expect("pointer must exist after backfill");
        assert_eq!(ptr, format_sys_from(100).into_bytes());

        // get_current works via the new pointer.
        let v = versioned_get_current(&s, "c", "d1").await.unwrap().unwrap();
        assert_eq!(v.body, b"legacy_body");
    }

    /// Backfill on a tombstoned doc removes any stale pointer.
    #[tokio::test]
    async fn backfill_removes_stale_pointer_for_tombstoned_doc() {
        let s = mem_storage().await;

        // Write a LIVE history row at t=100 and a TOMBSTONE at t=200 directly,
        // but manually insert a stale LatestVersion pointer pointing at t=100.
        let live_key = versioned_doc_key("c", "d1", 100).unwrap();
        let live_value = encode_value(VersionTag::Live, 100, i64::MAX, b"body");
        let tomb_key = versioned_doc_key("c", "d1", 200).unwrap();
        let tomb_value = encode_value(VersionTag::Tombstone, 200, i64::MAX, &[]);
        let ptr_key = latest_version_key("c", "d1");

        s.batch_write(&[
            WriteOp::Put {
                ns: Namespace::DocumentHistory,
                key: live_key,
                value: live_value,
            },
            WriteOp::Put {
                ns: Namespace::DocumentHistory,
                key: tomb_key,
                value: tomb_value,
            },
            // Stale pointer pointing at the old live row.
            WriteOp::Put {
                ns: Namespace::LatestVersion,
                key: ptr_key.clone(),
                value: format_sys_from(100).into_bytes(),
            },
        ])
        .await
        .unwrap();

        // Backfill corrects the pointer.
        backfill_latest_version(&s, "c").await.unwrap();

        // Pointer must be gone (tombstone is the latest row).
        assert!(
            s.get(Namespace::LatestVersion, &ptr_key)
                .await
                .unwrap()
                .is_none(),
            "stale pointer must be removed after backfill"
        );

        // get_current returns None.
        assert!(
            versioned_get_current(&s, "c", "d1")
                .await
                .unwrap()
                .is_none()
        );
    }

    /// Backfill is idempotent: calling it twice on an up-to-date index is a no-op.
    #[tokio::test]
    async fn backfill_idempotent() {
        let s = mem_storage().await;
        versioned_put(&s, "c", "d1", b"body", 100, None, None)
            .await
            .unwrap();

        // First call (pointer already correct from versioned_put).
        backfill_latest_version(&s, "c").await.unwrap();
        // Second call — must not corrupt anything.
        backfill_latest_version(&s, "c").await.unwrap();

        let v = versioned_get_current(&s, "c", "d1").await.unwrap().unwrap();
        assert_eq!(v.body, b"body");
    }

    /// Backfill on an empty collection is a no-op and does not error.
    #[tokio::test]
    async fn backfill_empty_collection_noop() {
        let s = mem_storage().await;
        backfill_latest_version(&s, "never_written").await.unwrap();
    }
}
