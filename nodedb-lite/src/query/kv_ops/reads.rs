// SPDX-License-Identifier: Apache-2.0
//! Read operations for the KV engine physical visitor.

use nodedb_types::Namespace;
use nodedb_types::result::QueryResult;
use nodedb_types::value::Value;

use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::query::msgpack_helpers::{write_array_header, write_bin};
use crate::query::value_utils::now_ms_u64;
use crate::storage::engine::StorageEngine;

// ─── Encoding helpers ────────────────────────────────────────────────────────

const DEADLINE_PREFIX_LEN: usize = 8;

pub(super) fn kv_key(collection: &str, key: &[u8]) -> Vec<u8> {
    let mut k = Vec::with_capacity(collection.len() + 1 + key.len());
    k.extend_from_slice(collection.as_bytes());
    k.push(0);
    k.extend_from_slice(key);
    k
}

pub(super) fn encode_value(deadline_ms: u64, value: &[u8]) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(DEADLINE_PREFIX_LEN + value.len());
    encoded.extend_from_slice(&deadline_ms.to_le_bytes());
    encoded.extend_from_slice(value);
    encoded
}

pub(super) fn decode_value(stored: &[u8]) -> Option<(u64, &[u8])> {
    if stored.len() < DEADLINE_PREFIX_LEN {
        return None;
    }
    let deadline = u64::from_le_bytes(stored[..DEADLINE_PREFIX_LEN].try_into().ok()?);
    Some((deadline, &stored[DEADLINE_PREFIX_LEN..]))
}

pub(super) fn now_ms() -> u64 {
    now_ms_u64()
}

pub(super) fn is_expired(deadline_ms: u64) -> bool {
    deadline_ms != 0 && now_ms() >= deadline_ms
}

pub(super) fn split_kv_key(composite: &[u8]) -> Option<(&str, &[u8])> {
    let sep = composite.iter().position(|&b| b == 0)?;
    let coll = std::str::from_utf8(&composite[..sep]).ok()?;
    let key = &composite[sep + 1..];
    Some((coll, key))
}

// ─── Read operations ─────────────────────────────────────────────────────────

/// Get: point lookup by primary key.
///
/// `surrogate_ceiling` is accepted for plan-shape compatibility with Origin
/// but unused: Lite is single-node and has no clone-resolver delegation that
/// would attach a surrogate to KV values.
pub async fn kv_get<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    collection: &str,
    key: &[u8],
    _surrogate_ceiling: Option<u32>,
) -> Result<QueryResult, LiteError> {
    let rkey = kv_key(collection, key);
    let stored = engine
        .storage
        .get(Namespace::Kv, &rkey)
        .await
        .map_err(|e| LiteError::Storage {
            detail: e.to_string(),
        })?;

    match stored {
        None => Ok(QueryResult::empty()),
        Some(raw) => match decode_value(&raw) {
            None => Ok(QueryResult::empty()),
            Some((deadline, user_bytes)) => {
                if is_expired(deadline) {
                    return Ok(QueryResult::empty());
                }
                Ok(QueryResult {
                    columns: vec!["key".into(), "value".into()],
                    rows: vec![vec![
                        Value::Bytes(key.to_vec()),
                        Value::Bytes(user_bytes.to_vec()),
                    ]],
                    rows_affected: 0,
                })
            }
        },
    }
}

/// GetTtl: return remaining TTL in milliseconds.
///
/// Returns JSON `{"ttl_ms": N}` where:
/// - `-2` = key does not exist
/// - `-1` = key exists but has no TTL
/// - `>= 0` = remaining ms
pub async fn kv_get_ttl<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    collection: &str,
    key: &[u8],
) -> Result<QueryResult, LiteError> {
    let rkey = kv_key(collection, key);
    let stored = engine
        .storage
        .get(Namespace::Kv, &rkey)
        .await
        .map_err(|e| LiteError::Storage {
            detail: e.to_string(),
        })?;

    let ttl_ms: i64 = match stored {
        None => -2,
        Some(raw) => match decode_value(&raw) {
            None => -2,
            Some((0, _)) => -1,
            Some((deadline, _)) => {
                let now = now_ms();
                if now >= deadline {
                    -2 // expired
                } else {
                    (deadline - now) as i64
                }
            }
        },
    };

    Ok(QueryResult {
        columns: vec!["ttl_ms".into()],
        rows: vec![vec![Value::Integer(ttl_ms)]],
        rows_affected: 0,
    })
}

/// BatchGet: fetch multiple keys in one pass.
pub async fn kv_batch_get<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    collection: &str,
    keys: &[Vec<u8>],
) -> Result<QueryResult, LiteError> {
    let mut rows: Vec<Vec<Value>> = Vec::with_capacity(keys.len());
    for key in keys {
        let rkey = kv_key(collection, key);
        let stored = engine
            .storage
            .get(Namespace::Kv, &rkey)
            .await
            .map_err(|e| LiteError::Storage {
                detail: e.to_string(),
            })?;
        let val = match stored {
            None => Value::Null,
            Some(raw) => match decode_value(&raw) {
                None => Value::Null,
                Some((deadline, user_bytes)) => {
                    if is_expired(deadline) {
                        Value::Null
                    } else {
                        Value::Bytes(user_bytes.to_vec())
                    }
                }
            },
        };
        rows.push(vec![Value::Bytes(key.clone()), val]);
    }
    Ok(QueryResult {
        columns: vec!["key".into(), "value".into()],
        rows,
        rows_affected: 0,
    })
}

/// FieldGet: extract named fields from a MessagePack-encoded value.
pub async fn kv_field_get<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    collection: &str,
    key: &[u8],
    fields: &[String],
) -> Result<QueryResult, LiteError> {
    let rkey = kv_key(collection, key);
    let stored = engine
        .storage
        .get(Namespace::Kv, &rkey)
        .await
        .map_err(|e| LiteError::Storage {
            detail: e.to_string(),
        })?;

    let raw = match stored {
        None => return Ok(QueryResult::empty()),
        Some(r) => r,
    };
    let (deadline, user_bytes) = decode_value(&raw).ok_or_else(|| LiteError::Storage {
        detail: "corrupt KV entry: too short".into(),
    })?;
    if is_expired(deadline) {
        return Ok(QueryResult::empty());
    }

    let map: std::collections::HashMap<String, nodedb_types::value::Value> =
        zerompk::from_msgpack(user_bytes).map_err(|e| LiteError::Serialization {
            detail: format!("FieldGet decode: {e}"),
        })?;

    let row: Vec<Value> = fields
        .iter()
        .map(|f| map.get(f).cloned().unwrap_or(Value::Null))
        .collect();

    Ok(QueryResult {
        columns: fields.to_vec(),
        rows: vec![row],
        rows_affected: 0,
    })
}

/// Scan: cursor-based scan of a KV collection.
///
/// `surrogate_ceiling` is accepted for plan-shape compatibility with Origin
/// but unused — see [`kv_get`] for the rationale.
pub async fn kv_scan<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    collection: &str,
    cursor: &[u8],
    count: usize,
    match_pattern: Option<&str>,
    _surrogate_ceiling: Option<u32>,
) -> Result<QueryResult, LiteError> {
    let start = kv_key(collection, cursor);
    let entries = engine
        .storage
        .scan_range(Namespace::Kv, &start, count + 1)
        .await
        .map_err(|e| LiteError::Storage {
            detail: e.to_string(),
        })?;

    let mut rows: Vec<Vec<Value>> = Vec::with_capacity(count.min(entries.len()));
    for (composite_key, raw_value) in entries.iter().take(count) {
        let Some((coll, user_key_bytes)) = split_kv_key(composite_key) else {
            continue;
        };
        if coll != collection {
            break;
        }
        let Some((deadline, user_bytes)) = decode_value(raw_value) else {
            continue;
        };
        if is_expired(deadline) {
            continue;
        }
        if let Some(pattern) = match_pattern {
            let key_str = String::from_utf8_lossy(user_key_bytes);
            if !glob_matches(pattern, &key_str) {
                continue;
            }
        }
        rows.push(vec![
            Value::Bytes(user_key_bytes.to_vec()),
            Value::Bytes(user_bytes.to_vec()),
        ]);
    }

    Ok(QueryResult {
        columns: vec!["key".into(), "value".into()],
        rows,
        rows_affected: 0,
    })
}

/// MaterializeScan: cursor-paginated raw KV scan for the clone materializer.
///
/// Lite is single-node — no distributed cursor executor is needed. The scan
/// iterates the KV table for `collection`, resuming from `cursor` if
/// provided, returning at most `count` live (non-expired) entries per call.
///
/// Response payload is msgpack-encoded as a 2-element array:
///   `[ next_cursor: bytes, entries: [[key: bytes, value: bytes], ...] ]`
/// `next_cursor` is empty when the scan is complete.
///
/// `surrogate_ceiling` is accepted for plan-shape compatibility with Origin
/// but unused — see [`kv_get`] for the rationale.
pub async fn kv_materialize_scan<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    collection: &str,
    cursor: &[u8],
    count: usize,
    _surrogate_ceiling: Option<u32>,
) -> Result<QueryResult, LiteError> {
    let start = kv_key(collection, cursor);
    let raw_entries = engine
        .storage
        .scan_range(Namespace::Kv, &start, count + 1)
        .await
        .map_err(|e| LiteError::Storage {
            detail: e.to_string(),
        })?;

    let mut pairs: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(count.min(raw_entries.len()));
    for (composite_key, raw_value) in &raw_entries {
        if pairs.len() >= count {
            break;
        }
        let Some((coll, user_key_bytes)) = split_kv_key(composite_key) else {
            continue;
        };
        if coll != collection {
            break;
        }
        let Some((deadline, user_bytes)) = decode_value(raw_value) else {
            continue;
        };
        if is_expired(deadline) {
            continue;
        }
        pairs.push((user_key_bytes.to_vec(), user_bytes.to_vec()));
    }

    let next_cursor: Vec<u8> = if pairs.len() < count {
        Vec::new()
    } else {
        pairs.last().map(|(k, _)| k.clone()).unwrap_or_default()
    };

    let payload = encode_materialize_payload(&next_cursor, &pairs);

    Ok(QueryResult {
        columns: vec!["payload".into()],
        rows: vec![vec![Value::Bytes(payload)]],
        rows_affected: 0,
    })
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

fn encode_materialize_payload(next_cursor: &[u8], pairs: &[(Vec<u8>, Vec<u8>)]) -> Vec<u8> {
    let mut out = Vec::new();
    write_array_header(&mut out, 2);
    write_bin(&mut out, next_cursor);
    write_array_header(&mut out, pairs.len());
    for (key, value) in pairs {
        write_array_header(&mut out, 2);
        write_bin(&mut out, key);
        write_bin(&mut out, value);
    }
    out
}

fn glob_matches(pattern: &str, input: &str) -> bool {
    let pat = pattern.as_bytes();
    let inp = input.as_bytes();
    let mut pi = 0;
    let mut ii = 0;
    let mut star_pi = usize::MAX;
    let mut star_ii = 0;

    while ii < inp.len() {
        if pi < pat.len() && (pat[pi] == b'?' || pat[pi] == inp[ii]) {
            pi += 1;
            ii += 1;
        } else if pi < pat.len() && pat[pi] == b'*' {
            star_pi = pi;
            star_ii = ii;
            pi += 1;
        } else if star_pi != usize::MAX {
            pi = star_pi + 1;
            star_ii += 1;
            ii = star_ii;
        } else {
            return false;
        }
    }
    while pi < pat.len() && pat[pi] == b'*' {
        pi += 1;
    }
    pi == pat.len()
}
