//! Bitemporal history tracking for graph edge collections.
//!
//! When a graph collection is created with `bitemporal=true`, every edge
//! mutation (insert, delete) writes a versioned record to
//! `Namespace::GraphHistory`.
//!
//! History key layout:
//!   `{collection}:{edge_id_str}:{system_from_ms_8be}`
//!
//! History value layout:
//!   `{edge_props_msgpack}{system_to_ms_8be}`
//!
//! `system_to_ms = SYSTEM_TO_CURRENT` (= `i64::MAX as u64`) encodes
//! "still current" — the row has not been deleted yet.  Using `i64::MAX as
//! u64` (rather than `u64::MAX`) keeps every timestamp representable as an
//! `i64` while remaining distinguishable from any real deletion timestamp.
//!
//! The collection-level bitemporal flag is persisted in `Namespace::Meta`
//! under key `graph_bitemporal:{collection}`.

use nodedb_types::Namespace;
use nodedb_types::value::Value;

use crate::error::LiteError;
use crate::storage::engine::{StorageEngine, WriteOp};

/// Meta key prefix for the graph bitemporal flag.
const META_GRAPH_BITEMPORAL_PREFIX: &str = "graph_bitemporal:";

/// Trailer size appended to every history value: 8-byte big-endian system_to_ms.
const HISTORY_TRAILER_LEN: usize = 8;

/// Sentinel value for `system_to_ms` that marks an edge as "still current"
/// (not yet deleted).  Equal to `i64::MAX as u64` so it remains within the
/// positive `i64` range while being larger than any realistic wall-clock ms.
pub(crate) const SYSTEM_TO_CURRENT: u64 = i64::MAX as u64;

/// Query whether a graph collection has bitemporal tracking enabled.
pub async fn is_bitemporal<S: StorageEngine>(
    storage: &S,
    collection: &str,
) -> Result<bool, LiteError> {
    let key = format!("{META_GRAPH_BITEMPORAL_PREFIX}{collection}");
    Ok(storage
        .get(Namespace::Meta, key.as_bytes())
        .await?
        .map(|v| v.first().copied() == Some(1))
        .unwrap_or(false))
}

/// Mark a graph collection as bitemporal. Idempotent.
pub async fn set_bitemporal<S: StorageEngine>(
    storage: &S,
    collection: &str,
    enabled: bool,
) -> Result<(), LiteError> {
    let key = format!("{META_GRAPH_BITEMPORAL_PREFIX}{collection}");
    storage
        .put(Namespace::Meta, key.as_bytes(), &[enabled as u8])
        .await
}

/// Record the insertion of an edge into the history table.
///
/// `edge_key` is the string representation of the `EdgeId`.
/// `props_msgpack` is the MessagePack-encoded edge properties (including
/// `src`, `dst`, `label`).
/// `system_from_ms` is the insertion timestamp.
pub async fn record_edge_insert<S: StorageEngine>(
    storage: &S,
    collection: &str,
    edge_key: &str,
    props_value: &Value,
    system_from_ms: i64,
) -> Result<(), LiteError> {
    let hist_key = history_key(collection, edge_key, system_from_ms);
    let props_bytes =
        zerompk::to_msgpack_vec(props_value).map_err(|e| LiteError::Serialization {
            detail: e.to_string(),
        })?;
    // system_to = u64::MAX → still current
    let hist_value = append_system_to(props_bytes, i64::MAX);
    storage
        .put(Namespace::GraphHistory, &hist_key, &hist_value)
        .await
}

/// Finalize an edge's history entry when it is deleted.
///
/// Scans history rows for `{collection}:{edge_key}:` and updates the most
/// recent one (largest `system_from_ms`) that still has `system_to = u64::MAX`
/// to set `system_to = system_to_ms`.
pub async fn record_edge_delete<S: StorageEngine>(
    storage: &S,
    collection: &str,
    edge_key: &str,
    system_to_ms: i64,
) -> Result<(), LiteError> {
    let prefix = edge_history_prefix(collection, edge_key);
    let entries = storage
        .scan_prefix(Namespace::GraphHistory, &prefix)
        .await?;

    // Find the most recent entry with system_to == SYSTEM_TO_CURRENT (still-current row).
    let mut ops: Vec<WriteOp> = Vec::new();
    for (key, value) in &entries {
        if let Some(current_system_to) = extract_system_to(value)
            && current_system_to == SYSTEM_TO_CURRENT
        {
            // Replace system_to trailer with the deletion timestamp.
            let payload_end = value.len() - HISTORY_TRAILER_LEN;
            let mut new_value = value[..payload_end].to_vec();
            new_value.extend_from_slice(&(system_to_ms as u64).to_be_bytes());
            ops.push(WriteOp::Put {
                ns: Namespace::GraphHistory,
                key: key.clone(),
                value: new_value,
            });
        }
    }

    if !ops.is_empty() {
        storage.batch_write(&ops).await?;
    }
    Ok(())
}

/// Purge history rows for `collection` where `system_to_ms < cutoff_ms`.
///
/// Returns the number of history entries deleted.
pub async fn purge_edge_history_before<S: StorageEngine>(
    storage: &S,
    collection: &str,
    cutoff_ms: i64,
) -> Result<u64, LiteError> {
    let prefix = collection_history_prefix(collection);
    let entries = storage
        .scan_prefix(Namespace::GraphHistory, &prefix)
        .await?;

    let mut to_delete: Vec<Vec<u8>> = Vec::new();
    for (key, value) in &entries {
        if let Some(system_to) = extract_system_to(value)
            && system_to < SYSTEM_TO_CURRENT
            && (system_to as i64) < cutoff_ms
        {
            to_delete.push(key.clone());
        }
    }

    let count = to_delete.len() as u64;
    let ops: Vec<WriteOp> = to_delete
        .into_iter()
        .map(|key| WriteOp::Delete {
            ns: Namespace::GraphHistory,
            key,
        })
        .collect();

    if !ops.is_empty() {
        storage.batch_write(&ops).await?;
    }
    Ok(count)
}

/// Compose history key: `{collection}:{edge_key}:{system_from_ms_8be}`.
fn history_key(collection: &str, edge_key: &str, system_from_ms: i64) -> Vec<u8> {
    let mut key = collection.as_bytes().to_vec();
    key.push(b':');
    key.extend_from_slice(edge_key.as_bytes());
    key.push(b':');
    key.extend_from_slice(&(system_from_ms as u64).to_be_bytes());
    key
}

/// Prefix for scanning all history rows of one edge: `{collection}:{edge_key}:`.
fn edge_history_prefix(collection: &str, edge_key: &str) -> Vec<u8> {
    let mut prefix = collection.as_bytes().to_vec();
    prefix.push(b':');
    prefix.extend_from_slice(edge_key.as_bytes());
    prefix.push(b':');
    prefix
}

/// Prefix for scanning all history rows of a collection: `{collection}:`.
fn collection_history_prefix(collection: &str) -> Vec<u8> {
    let mut prefix = collection.as_bytes().to_vec();
    prefix.push(b':');
    prefix
}

/// Append a big-endian system_to_ms trailer to a payload vec.
fn append_system_to(mut payload: Vec<u8>, system_to_ms: i64) -> Vec<u8> {
    payload.extend_from_slice(&(system_to_ms as u64).to_be_bytes());
    payload
}

/// Extract the `system_to_ms` trailer from a history value.
fn extract_system_to(value: &[u8]) -> Option<u64> {
    if value.len() < HISTORY_TRAILER_LEN {
        return None;
    }
    let start = value.len() - HISTORY_TRAILER_LEN;
    let bytes: [u8; 8] = value[start..].try_into().ok()?;
    Some(u64::from_be_bytes(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn history_key_ordering() {
        // Earlier system_from_ms should sort before later one under the same edge_key.
        let k1 = history_key("social", "a->b:follows", 1_000);
        let k2 = history_key("social", "a->b:follows", 2_000);
        assert!(k1 < k2);
    }

    #[test]
    fn system_to_extraction() {
        let payload = vec![1u8, 2, 3];
        let val = append_system_to(payload, 12345_i64);
        assert_eq!(extract_system_to(&val), Some(12345u64));
    }

    #[test]
    fn system_to_extraction_too_short() {
        assert_eq!(extract_system_to(&[1, 2]), None);
    }
}
