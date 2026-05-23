// SPDX-License-Identifier: Apache-2.0
//! CRDT version-history handlers: time-travel reads, delta export, restore, compaction.

use std::collections::HashMap;

use loro::VersionVector;
use nodedb_types::result::QueryResult;
use nodedb_types::value::Value;

use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::storage::engine::StorageEngine;

/// Parse a JSON `{"<peer_hex>": counter}` object into a Loro `VersionVector`.
///
/// Peer IDs are encoded as 16-char lowercase hex strings. Counters are i64
/// on the wire (JSON number) and narrowed to i32 (Loro's `Counter` type).
fn parse_version_vector(json: &str) -> Result<VersionVector, LiteError> {
    let map: HashMap<String, i64> =
        sonic_rs::from_str(json).map_err(|e| LiteError::BadRequest {
            detail: format!("invalid version vector JSON: {e}"),
        })?;

    let pairs: Vec<(u64, i32)> = map
        .into_iter()
        .map(|(hex, counter)| {
            let peer = u64::from_str_radix(&hex, 16).map_err(|e| LiteError::BadRequest {
                detail: format!("peer ID '{hex}' is not valid hex: {e}"),
            })?;
            let counter_i32 = i32::try_from(counter).map_err(|_| LiteError::BadRequest {
                detail: format!("peer '{hex}' counter {counter} exceeds Loro's i32 Counter range"),
            })?;
            Ok((peer, counter_i32))
        })
        .collect::<Result<Vec<_>, LiteError>>()?;

    Ok(pairs.into_iter().collect())
}

/// Read a document's state at a historical version.
pub async fn handle_read_at_version<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    collection: &str,
    document_id: &str,
    version_vector_json: &str,
) -> Result<QueryResult, LiteError> {
    let vv = parse_version_vector(version_vector_json)?;

    let crdt = engine.crdt.lock().map_err(|_| LiteError::LockPoisoned)?;
    let state = crdt.state();

    let value = state
        .read_at_version(collection, document_id, &vv)
        .map_err(|e| LiteError::Storage {
            detail: format!("read_at_version: {e}"),
        })?;

    let row = match value {
        Some(v) => {
            let json = sonic_rs::to_string(&v).map_err(|e| LiteError::Serialization {
                detail: format!("ReadAtVersion serialize: {e}"),
            })?;
            vec![vec![Value::String(json)]]
        }
        None => vec![],
    };

    Ok(QueryResult {
        columns: vec!["document".to_string()],
        rows: row,
        rows_affected: 0,
    })
}

/// Return the current oplog version vector as a JSON string.
pub async fn handle_get_version_vector<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
) -> Result<QueryResult, LiteError> {
    let crdt = engine.crdt.lock().map_err(|_| LiteError::LockPoisoned)?;
    let clock = crdt.export_vector_clock();
    let json = sonic_rs::to_string(&clock).map_err(|e| LiteError::Serialization {
        detail: format!("GetVersionVector serialize: {e}"),
    })?;
    Ok(QueryResult {
        columns: vec!["version_vector_json".to_string()],
        rows: vec![vec![Value::String(json)]],
        rows_affected: 0,
    })
}

/// Export the oplog delta from a version to current state.
pub async fn handle_export_delta<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    from_version_json: &str,
) -> Result<QueryResult, LiteError> {
    let vv = parse_version_vector(from_version_json)?;

    let crdt = engine.crdt.lock().map_err(|_| LiteError::LockPoisoned)?;
    let delta_bytes = crdt
        .export_delta_from(&vv)
        .map_err(|e| LiteError::Storage {
            detail: format!("ExportDelta: {e}"),
        })?;

    Ok(QueryResult {
        columns: vec!["delta_bytes".to_string()],
        rows: vec![vec![Value::Bytes(delta_bytes)]],
        rows_affected: 0,
    })
}

/// Restore a document to a historical version via a forward mutation.
///
/// Returns the delta bytes for the restore operation.
pub async fn handle_restore_to_version<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    collection: &str,
    document_id: &str,
    target_version_json: &str,
) -> Result<QueryResult, LiteError> {
    let vv = parse_version_vector(target_version_json)?;

    let crdt = engine.crdt.lock().map_err(|_| LiteError::LockPoisoned)?;
    let delta_bytes = crdt
        .state()
        .restore_to_version(collection, document_id, &vv)
        .map_err(|e| LiteError::Storage {
            detail: format!("RestoreToVersion: {e}"),
        })?;

    Ok(QueryResult {
        columns: vec!["delta_bytes".to_string()],
        rows: vec![vec![Value::Bytes(delta_bytes)]],
        rows_affected: 1,
    })
}

/// Compact the CRDT oplog at a specific version, discarding history before it.
pub async fn handle_compact_at_version<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    target_version_json: &str,
) -> Result<QueryResult, LiteError> {
    let vv = parse_version_vector(target_version_json)?;

    let mut crdt = engine.crdt.lock().map_err(|_| LiteError::LockPoisoned)?;
    crdt.compact_at_version(&vv)
        .map_err(|e| LiteError::Storage {
            detail: format!("CompactAtVersion: {e}"),
        })?;

    Ok(QueryResult {
        columns: vec![],
        rows: vec![],
        rows_affected: 1,
    })
}
