// SPDX-License-Identifier: Apache-2.0
//! Window metadata storage and lazy purge for time-windowed sorted indexes.
//!
//! Window definition is persisted in the Meta namespace under a well-known key
//! so that purge logic survives restarts without re-registering the index.
//!
//! Key layout:
//!   `kv_sorted:{index_name}:window` → msgpack-encoded WindowDef
//!
//! Each score entry in a windowed index carries an 8-byte timestamp appended
//! after the pk separator:
//!   `kv_sorted:{index_name}:score:{score_bytes}:{pk_hex}:{ts_bytes}` → empty
//!
//! The timestamp bytes are 8-byte little-endian u64 (ms since epoch).
//!
//! Purge semantics:
//!   tumbling / custom : delete entries whose stored_ts ∉ [window_start_ms, window_end_ms]
//!   sliding           : delete entries whose stored_ts < (now_ms - window_size_ms)
//!   session           : treated as sliding with window_size_ms = (window_end_ms - window_start_ms)

use std::collections::HashMap;

use nodedb_types::Namespace;
use nodedb_types::value::Value;

use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::storage::engine::{StorageEngine, StorageEngineSync, WriteOp};

use super::keys::{SCORE_TS_SEPARATOR, score_prefix};

// ─── Window definition ───────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub(super) struct WindowDef {
    /// "tumbling", "sliding", "session", or "none".
    pub window_type: String,
    /// Column from which the timestamp is read (stored for documentation; Lite
    /// uses the timestamp embedded in the score entry key itself).
    pub window_timestamp_column: String,
    /// Lower bound ms (tumbling/custom) or window size ms (sliding/session).
    pub window_start_ms: u64,
    /// Upper bound ms (tumbling/custom); 0 for sliding/session.
    pub window_end_ms: u64,
}

fn window_meta_key(index_name: &str) -> Vec<u8> {
    format!("kv_sorted:{index_name}:window").into_bytes()
}

/// Persist the window definition for `index_name`.
pub(super) fn store_window_def<S: StorageEngine + StorageEngineSync>(
    engine: &LiteQueryEngine<S>,
    index_name: &str,
    def: &WindowDef,
) -> Result<(), LiteError> {
    let mut map: HashMap<String, Value> = HashMap::with_capacity(4);
    map.insert("window_type".into(), Value::String(def.window_type.clone()));
    map.insert(
        "window_timestamp_column".into(),
        Value::String(def.window_timestamp_column.clone()),
    );
    map.insert(
        "window_start_ms".into(),
        Value::Integer(def.window_start_ms as i64),
    );
    map.insert(
        "window_end_ms".into(),
        Value::Integer(def.window_end_ms as i64),
    );
    let bytes =
        zerompk::to_msgpack_vec(&Value::Object(map)).map_err(|e| LiteError::Serialization {
            detail: format!("store_window_def serialize: {e}"),
        })?;
    engine
        .storage
        .put_sync(Namespace::Meta, &window_meta_key(index_name), &bytes)
        .map_err(|e| LiteError::Storage {
            detail: e.to_string(),
        })
}

/// Load the window definition for `index_name`, returning `None` if not found
/// (non-windowed index).
pub(super) fn load_window_def<S: StorageEngine + StorageEngineSync>(
    engine: &LiteQueryEngine<S>,
    index_name: &str,
) -> Result<Option<WindowDef>, LiteError> {
    let raw = engine
        .storage
        .get_sync(Namespace::Meta, &window_meta_key(index_name))
        .map_err(|e| LiteError::Storage {
            detail: e.to_string(),
        })?;
    match raw {
        None => Ok(None),
        Some(bytes) => {
            let val: Value =
                zerompk::from_msgpack(&bytes).map_err(|e| LiteError::Serialization {
                    detail: format!("load_window_def deserialize: {e}"),
                })?;
            let map = match val {
                Value::Object(m) => m,
                _ => {
                    return Err(LiteError::Storage {
                        detail: "load_window_def: expected object".into(),
                    });
                }
            };
            let get_str = |key: &str| -> Result<String, LiteError> {
                match map.get(key) {
                    Some(Value::String(s)) => Ok(s.clone()),
                    _ => Err(LiteError::Storage {
                        detail: format!("load_window_def: missing or invalid field '{key}'"),
                    }),
                }
            };
            let get_u64 = |key: &str| -> Result<u64, LiteError> {
                match map.get(key) {
                    Some(Value::Integer(n)) => Ok(*n as u64),
                    _ => Err(LiteError::Storage {
                        detail: format!("load_window_def: missing or invalid field '{key}'"),
                    }),
                }
            };
            Ok(Some(WindowDef {
                window_type: get_str("window_type")?,
                window_timestamp_column: get_str("window_timestamp_column")?,
                window_start_ms: get_u64("window_start_ms")?,
                window_end_ms: get_u64("window_end_ms")?,
            }))
        }
    }
}

/// Remove the window definition for `index_name` (called from DropSortedIndex).
pub(super) fn delete_window_def<S: StorageEngine + StorageEngineSync>(
    engine: &LiteQueryEngine<S>,
    index_name: &str,
) -> Result<(), LiteError> {
    engine
        .storage
        .delete_sync(Namespace::Meta, &window_meta_key(index_name))
        .map_err(|e| LiteError::Storage {
            detail: e.to_string(),
        })
}

// ─── Lazy purge ──────────────────────────────────────────────────────────────

/// Call before every sorted-index read. Scans the score entries, identifies
/// those whose embedded timestamp falls outside the active window, and removes
/// them together with their reverse pk entries.
///
/// For non-windowed indexes (window_def is None) this is a no-op.
pub(super) fn purge_outside_window<S: StorageEngine + StorageEngineSync>(
    engine: &LiteQueryEngine<S>,
    index_name: &str,
    now_ms: u64,
) -> Result<(), LiteError> {
    let def = match load_window_def(engine, index_name)? {
        None => return Ok(()),
        Some(d) => d,
    };

    if def.window_type == "none" {
        return Ok(());
    }

    // Compute [keep_from, keep_to) based on window type.
    let (keep_from, keep_to) = window_bounds(&def, now_ms);

    let score_pfx = score_prefix(index_name);
    let all = engine
        .storage
        .scan_range_bounded_sync(Namespace::Meta, Some(score_pfx.as_bytes()), None, None)
        .map_err(|e| LiteError::Storage {
            detail: e.to_string(),
        })?;

    let prefix_bytes = score_pfx.as_bytes();
    let mut ops: Vec<WriteOp> = Vec::new();

    for (key, _) in &all {
        if !key.starts_with(prefix_bytes) {
            break;
        }
        // Key layout after prefix: {score_bytes:8}{SCORE_TS_SEPARATOR}{pk}{SCORE_TS_SEPARATOR}{ts_bytes:8}
        let rest = &key[prefix_bytes.len()..];
        if rest.len() < 8 {
            continue;
        }
        let ts_opt = extract_timestamp(rest);
        let ts = match ts_opt {
            Some(t) => t,
            // No timestamp embedded → non-windowed entry, skip
            None => continue,
        };

        let in_window = ts >= keep_from && ts < keep_to;
        if !in_window {
            ops.push(WriteOp::Delete {
                ns: Namespace::Meta,
                key: key.clone(),
            });
            // Also remove the pk reverse entry.
            let pk = extract_pk(rest);
            if let Some(pk_bytes) = pk {
                let pk_key = super::keys::pk_entry_key(index_name, pk_bytes);
                ops.push(WriteOp::Delete {
                    ns: Namespace::Meta,
                    key: pk_key,
                });
            }
        }
    }

    if !ops.is_empty() {
        engine
            .storage
            .batch_write_sync(&ops)
            .map_err(|e| LiteError::Storage {
                detail: e.to_string(),
            })?;
    }
    Ok(())
}

fn window_bounds(def: &WindowDef, now_ms: u64) -> (u64, u64) {
    match def.window_type.as_str() {
        "tumbling" | "custom" => (def.window_start_ms, def.window_end_ms),
        "sliding" | "session" => {
            // window_start_ms holds the window size in ms.
            let size = if def.window_start_ms > 0 {
                def.window_start_ms
            } else {
                // Fallback: derive size from start/end if both nonzero.
                def.window_end_ms.saturating_sub(def.window_start_ms)
            };
            let from = now_ms.saturating_sub(size);
            (from, now_ms + 1)
        }
        // Unknown window type — keep everything.
        _ => (0, u64::MAX),
    }
}

/// Extract the trailing 8-byte timestamp from a score key's rest segment
/// (everything after the score prefix). Returns `None` if no SCORE_TS_SEPARATOR
/// is found (i.e. a non-windowed entry).
fn extract_timestamp(rest: &[u8]) -> Option<u64> {
    // Rest = {score:8}{SEP}{pk}{SEP}{ts:8}
    // Find the last occurrence of SCORE_TS_SEPARATOR.
    let sep = SCORE_TS_SEPARATOR;
    let pos = rest.iter().rposition(|&b| b == sep)?;
    let ts_slice = &rest[pos + 1..];
    if ts_slice.len() != 8 {
        return None;
    }
    Some(u64::from_le_bytes(ts_slice.try_into().ok()?))
}

/// Extract the pk bytes from rest. pk sits between the first sep (after score)
/// and the last sep (before ts). Returns None if layout doesn't match.
fn extract_pk(rest: &[u8]) -> Option<&[u8]> {
    if rest.len() < 8 {
        return None;
    }
    let sep = SCORE_TS_SEPARATOR;
    // First sep is after the 8-byte score.
    let first_sep = rest[8..].iter().position(|&b| b == sep)? + 8;
    // Last sep: find rposition from full slice.
    let last_sep = rest.iter().rposition(|&b| b == sep)?;
    if last_sep <= first_sep {
        return None;
    }
    Some(&rest[first_sep + 1..last_sep])
}

// ─── Window-aware score key builder ──────────────────────────────────────────

/// Build the score entry key for a windowed index.
/// Layout: `{score_prefix}{score_bytes:8}{SEP}{pk}{SEP}{ts_bytes:8}`
#[allow(dead_code)]
pub(super) fn windowed_score_key(
    index_name: &str,
    score_bytes: &[u8; 8],
    pk: &[u8],
    ts_ms: u64,
) -> Vec<u8> {
    let pfx = score_prefix(index_name);
    let mut k = pfx.into_bytes();
    k.extend_from_slice(score_bytes);
    k.push(SCORE_TS_SEPARATOR);
    k.extend_from_slice(pk);
    k.push(SCORE_TS_SEPARATOR);
    k.extend_from_slice(&ts_ms.to_le_bytes());
    k
}

// ─── Value helper: extract timestamp from a KV value map ─────────────────────

/// Try to read the `window_timestamp_column` field from a msgpack-encoded
/// KV value as a u64 millisecond timestamp.
///
/// Returns `None` if the column is absent or the value is not numeric.
#[allow(dead_code)]
pub(super) fn extract_ts_from_value(value_bytes: &[u8], column: &str) -> Option<u64> {
    let map: std::collections::HashMap<String, Value> = zerompk::from_msgpack(value_bytes).ok()?;
    match map.get(column)? {
        Value::Integer(n) => {
            if *n >= 0 {
                Some(*n as u64)
            } else {
                None
            }
        }
        Value::Float(f) => {
            if *f >= 0.0 {
                Some(*f as u64)
            } else {
                None
            }
        }
        _ => None,
    }
}
