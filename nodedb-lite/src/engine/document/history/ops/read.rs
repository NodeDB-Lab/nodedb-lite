// SPDX-License-Identifier: Apache-2.0

//! Read primitives: current-version resolution, point-in-time lookups, and
//! live-document scans.

use nodedb_types::Namespace;

use crate::error::LiteError;
use crate::storage::engine::StorageEngine;

use std::collections::HashMap;

use super::super::key::{
    coll_prefix, doc_prefix, latest_version_key, parse_sys_from, versioned_doc_key,
};
use super::super::value::{DecodedVersion, VersionTag, decode_value};

/// Read the most recent `LIVE` version for `(collection, doc_id)`.
///
/// Uses the `LatestVersion` index for an O(1) pointer lookup followed by a
/// single `DocumentHistory` fetch.  Returns `None` when the pointer is absent
/// (document never written, tombstoned, or GDPR-erased).
///
/// Call [`backfill_latest_version`](super::backfill::backfill_latest_version)
/// on collection open to populate the index for databases written before this
/// index was introduced.
pub async fn versioned_get_current<S: StorageEngine>(
    storage: &S,
    collection: &str,
    doc_id: &str,
) -> Result<Option<DecodedVersion>, LiteError> {
    let pointer_key = latest_version_key(collection, doc_id);
    let Some(pointer_bytes) = storage.get(Namespace::LatestVersion, &pointer_key).await? else {
        return Ok(None);
    };

    let sys_from_str =
        std::str::from_utf8(&pointer_bytes).map_err(|_| LiteError::Serialization {
            detail: "LatestVersion pointer is not valid UTF-8".into(),
        })?;
    let sys_from_ms: i64 = sys_from_str
        .trim()
        .parse()
        .map_err(|_| LiteError::Serialization {
            detail: format!("LatestVersion pointer is not a valid i64 decimal: {sys_from_str:?}"),
        })?;

    let history_key = versioned_doc_key(collection, doc_id, sys_from_ms)?;
    let Some(history_bytes) = storage
        .get(Namespace::DocumentHistory, &history_key)
        .await?
    else {
        // Pointer refers to a missing history row — storage inconsistency.
        return Err(LiteError::Serialization {
            detail: format!(
                "LatestVersion pointer for {collection}/{doc_id} points to \
                 system_from_ms={sys_from_ms} but no DocumentHistory row exists"
            ),
        });
    };

    let decoded = decode_value(&history_bytes)?;
    if decoded.is_live() {
        Ok(Some(decoded))
    } else {
        // Pointer left stale (e.g. GdprErased row that wiped the live tag).
        Ok(None)
    }
}

/// Read the version that was current at `system_as_of_ms`.
///
/// Scans all history rows for the document in ascending key order and finds
/// the last version where `system_from_ms <= system_as_of_ms`. If that version
/// is not `Live`, returns `None`.
///
/// When `valid_time_ms` is `Some(vt)`, the returned version must additionally
/// satisfy `valid_from_ms <= vt < valid_until_ms`. Returns `None` if the
/// version visible at `system_as_of_ms` does not cover `valid_time_ms`.
pub async fn versioned_get_as_of<S: StorageEngine>(
    storage: &S,
    collection: &str,
    doc_id: &str,
    system_as_of_ms: i64,
    valid_time_ms: Option<i64>,
) -> Result<Option<DecodedVersion>, LiteError> {
    let prefix = doc_prefix(collection, doc_id);
    let entries = storage
        .scan_prefix(Namespace::DocumentHistory, &prefix)
        .await?;

    // Walk entries in reverse (most-recent first). The first entry where
    // system_from_ms <= system_as_of_ms is the version visible at that point
    // in system time.
    for (_key, value) in entries.iter().rev() {
        let decoded = decode_value(value)?;
        let sys_from = parse_sys_from(_key).ok_or_else(|| LiteError::Serialization {
            detail: "document history key missing NUL separator".into(),
        })?;

        if sys_from > system_as_of_ms {
            // This version was written after the requested point — skip.
            continue;
        }

        // This is the version visible at system_as_of_ms.
        if decoded.tag != VersionTag::Live {
            return Ok(None);
        }

        // Apply valid-time filter if requested.
        if let Some(vt) = valid_time_ms
            && (vt < decoded.valid_from_ms || vt >= decoded.valid_until_ms)
        {
            return Ok(None);
        }

        return Ok(Some(decoded));
    }

    Ok(None)
}

/// Scan all live documents in `collection` from the history table.
///
/// Scans every history row under the collection prefix, groups them by
/// `doc_id`, and retains only documents whose most-recent row (highest
/// `system_from_ms`) is tagged `Live`.  Tombstoned and GDPR-erased documents
/// are excluded.
///
/// Returns `(doc_id, body_bytes)` pairs where `body_bytes` is the raw
/// MessagePack body of the current live version (empty `Vec` if the live
/// entry has an empty body).
///
/// This is the authoritative source for bitemporal collection contents because
/// the CRDT Loro snapshot may lag storage (it is only saved on explicit flush).
pub async fn scan_live_documents<S: StorageEngine>(
    storage: &S,
    collection: &str,
) -> Result<Vec<(String, Vec<u8>)>, LiteError> {
    let prefix = coll_prefix(collection);
    let entries = storage
        .scan_prefix(Namespace::DocumentHistory, &prefix)
        .await?;

    // Group rows by doc_id, keeping only the latest (highest system_from_ms).
    // Key layout: `{coll}:{doc_id}\x00{system_from_ms:020}` — rows for the
    // same doc_id are adjacent and sorted ascending by key, so the last row
    // per doc_id is the current version.
    let mut latest: HashMap<String, (VersionTag, Vec<u8>)> = HashMap::new();

    for (key, value) in &entries {
        // Extract doc_id from the key by splitting at the NUL separator.
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

        // Later keys overwrite earlier ones (ascending sort = ascending
        // system_from_ms), so the final entry per doc_id is the current
        // version.
        latest.insert(doc_id, (decoded.tag, decoded.body));
    }

    Ok(latest
        .into_iter()
        .filter(|(_, (tag, _))| *tag == VersionTag::Live)
        .map(|(id, (_, body))| (id, body))
        .collect())
}

#[cfg(test)]
mod tests {
    use crate::storage::engine::WriteOp;
    use crate::storage::pagedb_storage::PagedbStorageMem;

    use super::super::super::key::{format_sys_from, latest_version_key, versioned_doc_key};
    use super::super::super::value::encode_value;
    use super::super::write::{versioned_put, versioned_tombstone};
    use super::*;

    async fn mem_storage() -> PagedbStorageMem {
        PagedbStorageMem::open_in_memory()
            .await
            .expect("open in-memory storage")
    }

    /// Insert a document and verify `versioned_get_current` returns it via the
    /// O(1) LatestVersion pointer, and the pointer is present in storage.
    #[tokio::test]
    async fn latest_version_insert_pointer_present() {
        let s = mem_storage().await;
        versioned_put(&s, "c", "d1", b"hello", 100, None, None)
            .await
            .unwrap();

        // Pointer must be present.
        let ptr_key = latest_version_key("c", "d1");
        let ptr = s
            .get(Namespace::LatestVersion, &ptr_key)
            .await
            .unwrap()
            .expect("LatestVersion pointer must exist after insert");
        assert_eq!(ptr, format_sys_from(100).into_bytes());

        // get_current returns the live row.
        let v = versioned_get_current(&s, "c", "d1").await.unwrap().unwrap();
        assert_eq!(v.body, b"hello");
        assert!(v.is_live());
    }

    /// Update a document (two successive puts): pointer tracks the new version.
    #[tokio::test]
    async fn latest_version_update_pointer_tracks_new() {
        let s = mem_storage().await;
        versioned_put(&s, "c", "d1", b"v1", 100, None, None)
            .await
            .unwrap();
        versioned_put(&s, "c", "d1", b"v2", 200, None, None)
            .await
            .unwrap();

        // Pointer points to v2.
        let ptr_key = latest_version_key("c", "d1");
        let ptr = s
            .get(Namespace::LatestVersion, &ptr_key)
            .await
            .unwrap()
            .expect("pointer must exist");
        assert_eq!(ptr, format_sys_from(200).into_bytes());

        // get_current returns v2.
        let v = versioned_get_current(&s, "c", "d1").await.unwrap().unwrap();
        assert_eq!(v.body, b"v2");

        // Old version still accessible via as_of.
        let v1 = versioned_get_as_of(&s, "c", "d1", 150, None)
            .await
            .unwrap()
            .expect("v1 visible at t=150");
        assert_eq!(v1.body, b"v1");
    }

    /// Tombstone removes the pointer; get_current returns None.
    #[tokio::test]
    async fn latest_version_tombstone_removes_pointer() {
        let s = mem_storage().await;
        versioned_put(&s, "c", "d1", b"hello", 100, None, None)
            .await
            .unwrap();
        versioned_tombstone(&s, "c", "d1", 200, None).await.unwrap();

        // Pointer must be absent after tombstone.
        let ptr_key = latest_version_key("c", "d1");
        let ptr = s.get(Namespace::LatestVersion, &ptr_key).await.unwrap();
        assert!(ptr.is_none(), "pointer must be deleted after tombstone");

        // get_current returns None.
        assert!(
            versioned_get_current(&s, "c", "d1")
                .await
                .unwrap()
                .is_none()
        );
    }

    /// Multiple updates followed by a tombstone: pointer gone, history preserved.
    #[tokio::test]
    async fn latest_version_multi_update_then_tombstone() {
        let s = mem_storage().await;
        versioned_put(&s, "c", "d1", b"v1", 100, None, None)
            .await
            .unwrap();
        versioned_put(&s, "c", "d1", b"v2", 200, None, None)
            .await
            .unwrap();
        versioned_put(&s, "c", "d1", b"v3", 300, None, None)
            .await
            .unwrap();
        versioned_tombstone(&s, "c", "d1", 400, None).await.unwrap();

        // No current version.
        assert!(
            versioned_get_current(&s, "c", "d1")
                .await
                .unwrap()
                .is_none()
        );

        // All historical versions still accessible.
        for (t, body) in [(150, b"v1"), (250, b"v2"), (350, b"v3")] {
            let v = versioned_get_as_of(&s, "c", "d1", t, None)
                .await
                .unwrap()
                .unwrap_or_else(|| panic!("version at t={t} must be present"));
            assert_eq!(v.body.as_slice(), body as &[u8]);
        }
    }

    /// Original put-then-get test (kept for regression coverage).
    #[tokio::test]
    async fn put_get_current_roundtrip() {
        let s = mem_storage().await;
        versioned_put(&s, "c", "d1", b"hello", 100, None, None)
            .await
            .unwrap();
        let v = versioned_get_current(&s, "c", "d1").await.unwrap().unwrap();
        assert_eq!(v.body, b"hello");
        assert!(v.is_live());
    }

    /// Original tombstone-hides-live test (kept for regression coverage).
    #[tokio::test]
    async fn tombstone_hides_live() {
        let s = mem_storage().await;
        versioned_put(&s, "c", "d1", b"hello", 100, None, None)
            .await
            .unwrap();
        versioned_tombstone(&s, "c", "d1", 200, None).await.unwrap();
        assert!(
            versioned_get_current(&s, "c", "d1")
                .await
                .unwrap()
                .is_none()
        );
    }

    /// Live scan returns only the current live version per doc, skipping
    /// tombstoned docs.
    #[tokio::test]
    async fn scan_live_documents_skips_tombstoned() {
        let s = mem_storage().await;
        versioned_put(&s, "c", "alive", b"body", 100, None, None)
            .await
            .unwrap();
        versioned_put(&s, "c", "dead", b"body", 100, None, None)
            .await
            .unwrap();
        versioned_tombstone(&s, "c", "dead", 200, None)
            .await
            .unwrap();

        let mut docs = scan_live_documents(&s, "c").await.unwrap();
        docs.sort();
        assert_eq!(docs, vec![("alive".to_owned(), b"body".to_vec())]);
    }

    // Directly-written history rows exercise the value/key codecs without going
    // through versioned_put (used by the backfill tests, mirrored here for the
    // read path).
    #[tokio::test]
    async fn get_current_reads_pointerless_row_after_manual_pointer() {
        let s = mem_storage().await;
        let history_key = versioned_doc_key("c", "d1", 100).unwrap();
        let history_value = encode_value(VersionTag::Live, 100, i64::MAX, b"body");
        let ptr_key = latest_version_key("c", "d1");
        s.batch_write(&[
            WriteOp::Put {
                ns: Namespace::DocumentHistory,
                key: history_key,
                value: history_value,
            },
            WriteOp::Put {
                ns: Namespace::LatestVersion,
                key: ptr_key,
                value: format_sys_from(100).into_bytes(),
            },
        ])
        .await
        .unwrap();

        let v = versioned_get_current(&s, "c", "d1").await.unwrap().unwrap();
        assert_eq!(v.body, b"body");
    }
}
