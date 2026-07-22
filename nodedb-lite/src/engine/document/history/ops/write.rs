// SPDX-License-Identifier: Apache-2.0

//! Write primitives: appending live versions and tombstones.

use nodedb_types::Namespace;

use crate::error::LiteError;
use crate::storage::engine::{StorageEngine, WriteOp};

use super::super::key::{format_sys_from, latest_version_key, versioned_doc_key};
use super::super::value::{VersionTag, encode_value};

/// Append a new `LIVE` version for `(collection, doc_id)` at `system_from_ms`.
///
/// - `valid_from_ms` defaults to `system_from_ms` when `None`.
/// - `valid_until_ms` defaults to `i64::MAX` (open / still-current) when `None`.
///
/// Atomically updates the `LatestVersion` pointer alongside the history row so
/// that [`versioned_get_current`](super::read::versioned_get_current) can
/// resolve the live version in O(1).
pub async fn versioned_put<S: StorageEngine>(
    storage: &S,
    collection: &str,
    doc_id: &str,
    body: &[u8],
    system_from_ms: i64,
    valid_from_ms: Option<i64>,
    valid_until_ms: Option<i64>,
) -> Result<(), LiteError> {
    let history_key = versioned_doc_key(collection, doc_id, system_from_ms)?;
    let vf = valid_from_ms.unwrap_or(system_from_ms);
    let vu = valid_until_ms.unwrap_or(i64::MAX);
    let history_value = encode_value(VersionTag::Live, vf, vu, body);

    let pointer_key = latest_version_key(collection, doc_id);
    let pointer_value = format_sys_from(system_from_ms).into_bytes();

    storage
        .batch_write(&[
            WriteOp::Put {
                ns: Namespace::DocumentHistory,
                key: history_key,
                value: history_value,
            },
            WriteOp::Put {
                ns: Namespace::LatestVersion,
                key: pointer_key,
                value: pointer_value,
            },
        ])
        .await
}

/// Append a `TOMBSTONE` version at `system_from_ms`, closing the open version.
///
/// Writing a tombstone marks the document as deleted in system time from
/// `system_from_ms` onward. The body is left empty.
///
/// `valid_from_ms` defaults to `system_from_ms` when `None`. Callers that pass a
/// monotonic (uniqueness-guaranteed) `system_from_ms` for the version key should
/// pass the true wall-clock time as `valid_from_ms` so a "valid as-of now" query
/// sees the deletion immediately (the monotonic system time can sit a few ms
/// ahead of wall-clock under burst).
///
/// Atomically removes the `LatestVersion` pointer so `versioned_get_current`
/// returns `None` without scanning history.
pub async fn versioned_tombstone<S: StorageEngine>(
    storage: &S,
    collection: &str,
    doc_id: &str,
    system_from_ms: i64,
    valid_from_ms: Option<i64>,
) -> Result<(), LiteError> {
    let history_key = versioned_doc_key(collection, doc_id, system_from_ms)?;
    let vf = valid_from_ms.unwrap_or(system_from_ms);
    let history_value = encode_value(VersionTag::Tombstone, vf, i64::MAX, &[]);

    let pointer_key = latest_version_key(collection, doc_id);

    storage
        .batch_write(&[
            WriteOp::Put {
                ns: Namespace::DocumentHistory,
                key: history_key,
                value: history_value,
            },
            WriteOp::Delete {
                ns: Namespace::LatestVersion,
                key: pointer_key,
            },
        ])
        .await
}
