// SPDX-License-Identifier: Apache-2.0

//! Key layout for versioned document history.
//!
//! Key format: `{collection}:{doc_id}\x00{system_from_ms:020}`
//!
//! The 20-digit zero-padded decimal gives lexicographic ordering that matches
//! temporal ordering. The most-recent version of a document is the last key
//! under its doc prefix — a forward scan followed by taking the last element
//! gives the current version.
//!
//! NUL (`\x00`) is the reserved version separator. Callers must reject
//! doc_ids that contain a NUL byte.

use crate::error::LiteError;

/// Format `system_from_ms` as a 20-digit zero-padded decimal string.
///
/// This gives lexicographic ordering equal to numeric ordering for i64 values
/// that fit in 20 decimal digits (all i64 values do).
pub fn format_sys_from(system_from_ms: i64) -> String {
    format!("{system_from_ms:020}")
}

/// Build a versioned document key.
///
/// Returns an error if `doc_id` contains a NUL byte — NUL is the version
/// separator and must not appear in document identifiers.
pub fn versioned_doc_key(
    collection: &str,
    doc_id: &str,
    system_from_ms: i64,
) -> Result<Vec<u8>, LiteError> {
    if doc_id.as_bytes().contains(&0) {
        return Err(LiteError::BadRequest {
            detail: "document id may not contain NUL byte".into(),
        });
    }
    let s = format!(
        "{collection}:{doc_id}\x00{}",
        format_sys_from(system_from_ms)
    );
    Ok(s.into_bytes())
}

/// Byte prefix matching every version of one `doc_id` in `collection`.
///
/// Used for prefix scans to retrieve all history rows for a document.
/// The prefix ends with `\x00` — the separator that precedes the timestamp
/// suffix — so it matches only this doc_id's rows and not any adjacent ones.
pub fn doc_prefix(collection: &str, doc_id: &str) -> Vec<u8> {
    format!("{collection}:{doc_id}\x00").into_bytes()
}

/// Exclusive upper bound for [`doc_prefix`].
///
/// Because `\x00` is the minimum byte, `\x01` is the next-greater separator
/// and cleanly bounds all version suffixes for this `doc_id`.
pub fn doc_prefix_end(collection: &str, doc_id: &str) -> Vec<u8> {
    format!("{collection}:{doc_id}\x01").into_bytes()
}

/// Byte prefix matching every version of every doc_id in `collection`.
pub fn coll_prefix(collection: &str) -> Vec<u8> {
    format!("{collection}:").into_bytes()
}

/// Exclusive upper bound for [`coll_prefix`].
///
/// `;` is one ASCII code point above `:`, so `{collection};` is the
/// smallest string that sorts after all keys starting with `{collection}:`.
pub fn coll_prefix_end(collection: &str) -> Vec<u8> {
    format!("{collection};").into_bytes()
}

/// Extract `system_from_ms` from a versioned key byte slice.
///
/// Returns `None` if the key has no NUL separator or if the timestamp
/// suffix cannot be parsed. Defensive — well-formed keys produced by
/// [`versioned_doc_key`] always succeed.
pub fn parse_sys_from(key: &[u8]) -> Option<i64> {
    let nul_pos = key.iter().rposition(|&b| b == 0)?;
    let suffix = &key[nul_pos + 1..];
    let s = std::str::from_utf8(suffix).ok()?;
    s.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_ordering() {
        let k1 = versioned_doc_key("coll", "doc1", 1_000).unwrap();
        let k2 = versioned_doc_key("coll", "doc1", 2_000).unwrap();
        assert!(k1 < k2, "earlier timestamp must sort before later");
    }

    #[test]
    fn key_rejects_nul_doc_id() {
        assert!(versioned_doc_key("coll", "bad\x00id", 1_000).is_err());
    }

    #[test]
    fn parse_sys_from_roundtrip() {
        let ms = 1_700_000_000_000_i64;
        let key = versioned_doc_key("coll", "doc", ms).unwrap();
        assert_eq!(parse_sys_from(&key), Some(ms));
    }

    #[test]
    fn doc_prefix_bounds_doc_id() {
        let pfx = doc_prefix("c", "d");
        let pfx_end = doc_prefix_end("c", "d");
        let k = versioned_doc_key("c", "d", 0).unwrap();
        assert!(k >= pfx && k < pfx_end);
    }

    #[test]
    fn coll_prefix_bounds_collection() {
        let pfx = coll_prefix("c");
        let pfx_end = coll_prefix_end("c");
        let k1 = versioned_doc_key("c", "a", 0).unwrap();
        let k2 = versioned_doc_key("c", "z", i64::MAX).unwrap();
        assert!(k1 >= pfx && k1 < pfx_end);
        assert!(k2 >= pfx && k2 < pfx_end);
    }
}
