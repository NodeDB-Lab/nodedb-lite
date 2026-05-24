// SPDX-License-Identifier: Apache-2.0

//! Async storage operations for versioned document history.
//!
//! These functions are the write and read primitives for bitemporal document
//! collections. They do not wire into the public `NodeDb` trait — that is
//! Stage B. Stage A delivers the storage foundation only.

use nodedb_types::Namespace;

use crate::error::LiteError;
use crate::storage::engine::StorageEngine;

use super::key::{doc_prefix, versioned_doc_key};
use super::value::{DecodedVersion, VersionTag, decode_value, encode_value};

/// Meta key prefix for the document bitemporal flag.
const META_DOCUMENT_BITEMPORAL_PREFIX: &str = "document_bitemporal:";

// ---------------------------------------------------------------------------
// Flag helpers
// ---------------------------------------------------------------------------

/// Query whether a document collection has bitemporal tracking enabled.
///
/// Returns `false` for any collection that has not had the flag explicitly set.
pub async fn is_bitemporal<S: StorageEngine>(
    storage: &S,
    collection: &str,
) -> Result<bool, LiteError> {
    let key = format!("{META_DOCUMENT_BITEMPORAL_PREFIX}{collection}");
    Ok(storage
        .get(Namespace::Meta, key.as_bytes())
        .await?
        .map(|v| v.first().copied() == Some(1))
        .unwrap_or(false))
}

/// Mark a document collection as bitemporal (or non-bitemporal). Idempotent.
pub async fn set_bitemporal<S: StorageEngine>(
    storage: &S,
    collection: &str,
    bitemporal: bool,
) -> Result<(), LiteError> {
    let key = format!("{META_DOCUMENT_BITEMPORAL_PREFIX}{collection}");
    storage
        .put(Namespace::Meta, key.as_bytes(), &[bitemporal as u8])
        .await
}

// ---------------------------------------------------------------------------
// Write operations
// ---------------------------------------------------------------------------

/// Append a new `LIVE` version for `(collection, doc_id)` at `system_from_ms`.
///
/// - `valid_from_ms` defaults to `system_from_ms` when `None`.
/// - `valid_until_ms` defaults to `i64::MAX` (open / still-current) when `None`.
pub async fn versioned_put<S: StorageEngine>(
    storage: &S,
    collection: &str,
    doc_id: &str,
    body: &[u8],
    system_from_ms: i64,
    valid_from_ms: Option<i64>,
    valid_until_ms: Option<i64>,
) -> Result<(), LiteError> {
    let key = versioned_doc_key(collection, doc_id, system_from_ms)?;
    let vf = valid_from_ms.unwrap_or(system_from_ms);
    let vu = valid_until_ms.unwrap_or(i64::MAX);
    let value = encode_value(VersionTag::Live, vf, vu, body);
    storage.put(Namespace::DocumentHistory, &key, &value).await
}

/// Append a `TOMBSTONE` version at `system_from_ms`, closing the open version.
///
/// Writing a tombstone marks the document as deleted in system time from
/// `system_from_ms` onward. The body is left empty.
pub async fn versioned_tombstone<S: StorageEngine>(
    storage: &S,
    collection: &str,
    doc_id: &str,
    system_from_ms: i64,
) -> Result<(), LiteError> {
    let key = versioned_doc_key(collection, doc_id, system_from_ms)?;
    let value = encode_value(VersionTag::Tombstone, system_from_ms, i64::MAX, &[]);
    storage.put(Namespace::DocumentHistory, &key, &value).await
}

// ---------------------------------------------------------------------------
// Read operations
// ---------------------------------------------------------------------------

/// Read the most recent `LIVE` version for `(collection, doc_id)`.
///
/// Scans all history rows for the document in ascending key order (ascending
/// `system_from_ms`) and returns the last entry that has `tag == Live`.
/// Returns `None` if no live version exists (document was never written or
/// was subsequently tombstoned).
pub async fn versioned_get_current<S: StorageEngine>(
    storage: &S,
    collection: &str,
    doc_id: &str,
) -> Result<Option<DecodedVersion>, LiteError> {
    let prefix = doc_prefix(collection, doc_id);
    let entries = storage
        .scan_prefix(Namespace::DocumentHistory, &prefix)
        .await?;

    // Entries are ordered by key (ascending system_from_ms). The last LIVE
    // entry is the current version. We walk in reverse to find it quickly.
    for (_key, value) in entries.iter().rev() {
        let decoded = decode_value(value)?;
        if decoded.is_live() {
            return Ok(Some(decoded));
        }
        // A tombstone as the most recent entry means the document is deleted.
        if decoded.tag == VersionTag::Tombstone {
            return Ok(None);
        }
        // GdprErased — keep scanning backward to find the live predecessor
        // (unusual; normally erased rows have no live predecessor remaining,
        // but we scan to be thorough rather than returning None prematurely).
    }
    Ok(None)
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
        let sys_from =
            super::key::parse_sys_from(_key).ok_or_else(|| LiteError::Serialization {
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
        if let Some(vt) = valid_time_ms {
            if vt < decoded.valid_from_ms || vt >= decoded.valid_until_ms {
                return Ok(None);
            }
        }

        return Ok(Some(decoded));
    }

    Ok(None)
}

#[cfg(test)]
mod tests {
    use crate::storage::pagedb_storage::PagedbStorageMem;

    use super::*;

    async fn mem_storage() -> PagedbStorageMem {
        PagedbStorageMem::open_in_memory()
            .await
            .expect("open in-memory storage")
    }

    #[tokio::test]
    async fn flag_default_false() {
        let s = mem_storage().await;
        assert!(!is_bitemporal(&s, "coll").await.unwrap());
    }

    #[tokio::test]
    async fn flag_roundtrip() {
        let s = mem_storage().await;
        set_bitemporal(&s, "coll", true).await.unwrap();
        assert!(is_bitemporal(&s, "coll").await.unwrap());
        set_bitemporal(&s, "coll", false).await.unwrap();
        assert!(!is_bitemporal(&s, "coll").await.unwrap());
    }

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

    #[tokio::test]
    async fn tombstone_hides_live() {
        let s = mem_storage().await;
        versioned_put(&s, "c", "d1", b"hello", 100, None, None)
            .await
            .unwrap();
        versioned_tombstone(&s, "c", "d1", 200).await.unwrap();
        assert!(
            versioned_get_current(&s, "c", "d1")
                .await
                .unwrap()
                .is_none()
        );
    }
}
