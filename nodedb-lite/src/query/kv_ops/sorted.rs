// SPDX-License-Identifier: Apache-2.0
//! Sorted index (leaderboard) operations for the KV engine physical visitor.
//!
//! Sorted indexes are backed by entries in the Meta namespace with keys of the form:
//! `kv_sorted:{index_name}:score:{score_bytes}:{pk_hex}` → empty value
//! `kv_sorted:{index_name}:pk:{pk_hex}` → score_bytes (for ZSCORE / rank lookup)
//!
//! Score is stored as 8-byte big-endian f64 so lexicographic ordering matches
//! ascending numeric ordering. For descending indexes the score bytes are bitwise-NOT.
//!
//! Window-typed sorted indexes (daily/weekly/monthly/custom) require time-windowed
//! compaction infrastructure that does not exist in single-node Lite. Registering
//! a windowed index returns `BadRequest` with an explanatory message. Non-windowed
//! (`window_type = "none"`) indexes are fully implemented.

use nodedb_types::Namespace;
use nodedb_types::result::QueryResult;
use nodedb_types::value::Value;

use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::storage::engine::{StorageEngine, StorageEngineSync, WriteOp};

// ─── Key helpers ─────────────────────────────────────────────────────────────

fn score_prefix(index_name: &str) -> String {
    format!("kv_sorted:{index_name}:score:")
}

fn pk_entry_key(index_name: &str, pk: &[u8]) -> Vec<u8> {
    let mut k = format!("kv_sorted:{index_name}:pk:").into_bytes();
    k.extend_from_slice(pk);
    k
}

fn sort_bytes_to_f64(bytes: &[u8; 8]) -> f64 {
    let bits = u64::from_be_bytes(*bytes);
    let original = if bits >> 63 != 0 {
        bits ^ (1u64 << 63)
    } else {
        !bits
    };
    f64::from_bits(original)
}

// ─── DDL ─────────────────────────────────────────────────────────────────────

/// RegisterSortedIndex: register a sorted index on a KV collection.
///
/// Window-typed indexes (daily/weekly/monthly/custom) are rejected — Lite lacks
/// the time-windowed compaction infrastructure they require.
pub fn kv_register_sorted_index<S: StorageEngine + StorageEngineSync>(
    _engine: &LiteQueryEngine<S>,
    _index_name: &str,
    window_type: &str,
) -> Result<QueryResult, LiteError> {
    if window_type != "none" {
        return Err(LiteError::BadRequest {
            detail: format!(
                "RegisterSortedIndex: window_type='{window_type}' requires time-windowed \
                 compaction; Lite supports only window_type='none'. Use Origin for \
                 time-windowed leaderboards."
            ),
        });
    }
    // For window_type="none" the index is implicitly ready — entries are
    // written at score-update time (via the data-plane KV writes that will
    // call the index maintenance path). No persistent DDL record is required
    // in this redb-backed implementation.
    Ok(QueryResult {
        columns: vec![],
        rows: vec![],
        rows_affected: 0,
    })
}

/// DropSortedIndex: remove all entries for a sorted index.
pub fn kv_drop_sorted_index<S: StorageEngine + StorageEngineSync>(
    engine: &LiteQueryEngine<S>,
    index_name: &str,
) -> Result<QueryResult, LiteError> {
    // Delete score-entries.
    let score_pfx = score_prefix(index_name);
    let score_entries = engine
        .storage
        .scan_range_bounded_sync(Namespace::Meta, Some(score_pfx.as_bytes()), None, None)
        .map_err(|e| LiteError::Storage {
            detail: e.to_string(),
        })?;

    // Delete pk-entries.
    let pk_pfx = format!("kv_sorted:{index_name}:pk:");
    let pk_entries = engine
        .storage
        .scan_range_bounded_sync(Namespace::Meta, Some(pk_pfx.as_bytes()), None, None)
        .map_err(|e| LiteError::Storage {
            detail: e.to_string(),
        })?;

    let mut ops: Vec<WriteOp> = Vec::with_capacity(score_entries.len() + pk_entries.len());
    for (key, _) in &score_entries {
        if key.starts_with(score_pfx.as_bytes()) {
            ops.push(WriteOp::Delete {
                ns: Namespace::Meta,
                key: key.clone(),
            });
        }
    }
    for (key, _) in &pk_entries {
        if key.starts_with(pk_pfx.as_bytes()) {
            ops.push(WriteOp::Delete {
                ns: Namespace::Meta,
                key: key.clone(),
            });
        }
    }
    let count = ops.len() as u64;
    if !ops.is_empty() {
        engine
            .storage
            .batch_write_sync(&ops)
            .map_err(|e| LiteError::Storage {
                detail: e.to_string(),
            })?;
    }
    Ok(QueryResult {
        columns: vec![],
        rows: vec![],
        rows_affected: count,
    })
}

// ─── Queries ─────────────────────────────────────────────────────────────────

/// SortedIndexScore: return the score for a given primary key (ZSCORE).
pub fn kv_sorted_index_score<S: StorageEngine + StorageEngineSync>(
    engine: &LiteQueryEngine<S>,
    index_name: &str,
    primary_key: &[u8],
) -> Result<QueryResult, LiteError> {
    let pk_key = pk_entry_key(index_name, primary_key);
    let stored = engine
        .storage
        .get_sync(Namespace::Meta, &pk_key)
        .map_err(|e| LiteError::Storage {
            detail: e.to_string(),
        })?;

    match stored {
        None => Ok(QueryResult {
            columns: vec!["score".into()],
            rows: vec![vec![Value::Null]],
            rows_affected: 0,
        }),
        Some(bytes) => {
            if bytes.len() < 8 {
                return Err(LiteError::Storage {
                    detail: "corrupt sorted index: score bytes too short".into(),
                });
            }
            let score =
                sort_bytes_to_f64(bytes[..8].try_into().map_err(|_| LiteError::Storage {
                    detail: "corrupt sorted index: score bytes malformed".into(),
                })?);
            Ok(QueryResult {
                columns: vec!["score".into()],
                rows: vec![vec![Value::Float(score)]],
                rows_affected: 0,
            })
        }
    }
}

/// SortedIndexRank: 1-based rank of a primary key in ascending score order.
pub fn kv_sorted_index_rank<S: StorageEngine + StorageEngineSync>(
    engine: &LiteQueryEngine<S>,
    index_name: &str,
    primary_key: &[u8],
) -> Result<QueryResult, LiteError> {
    // Fetch the score for this key first.
    let pk_key = pk_entry_key(index_name, primary_key);
    let stored = engine
        .storage
        .get_sync(Namespace::Meta, &pk_key)
        .map_err(|e| LiteError::Storage {
            detail: e.to_string(),
        })?;

    let target_score_bytes: [u8; 8] = match stored {
        None => {
            return Ok(QueryResult {
                columns: vec!["rank".into()],
                rows: vec![vec![Value::Null]],
                rows_affected: 0,
            });
        }
        Some(ref bytes) if bytes.len() >= 8 => {
            bytes[..8].try_into().map_err(|_| LiteError::Storage {
                detail: "corrupt sorted index score".into(),
            })?
        }
        Some(_) => {
            return Err(LiteError::Storage {
                detail: "corrupt sorted index: score bytes too short".into(),
            });
        }
    };

    // Count how many score entries are strictly less than this score.
    let score_pfx = score_prefix(index_name);
    let all = engine
        .storage
        .scan_range_bounded_sync(Namespace::Meta, Some(score_pfx.as_bytes()), None, None)
        .map_err(|e| LiteError::Storage {
            detail: e.to_string(),
        })?;

    let prefix_bytes = score_pfx.as_bytes();
    let mut rank: u64 = 1;
    for (key, _) in &all {
        if !key.starts_with(prefix_bytes) {
            break;
        }
        let score_offset = prefix_bytes.len();
        if key.len() < score_offset + 8 {
            continue;
        }
        let entry_score: [u8; 8] =
            key[score_offset..score_offset + 8]
                .try_into()
                .map_err(|_| LiteError::Storage {
                    detail: "corrupt score key".into(),
                })?;
        if entry_score < target_score_bytes {
            rank += 1;
        }
    }

    Ok(QueryResult {
        columns: vec!["rank".into()],
        rows: vec![vec![Value::Integer(rank as i64)]],
        rows_affected: 0,
    })
}

/// SortedIndexTopK: return top K entries in ascending score order.
pub fn kv_sorted_index_top_k<S: StorageEngine + StorageEngineSync>(
    engine: &LiteQueryEngine<S>,
    index_name: &str,
    k: u32,
) -> Result<QueryResult, LiteError> {
    let score_pfx = score_prefix(index_name);
    let all = engine
        .storage
        .scan_range_bounded_sync(Namespace::Meta, Some(score_pfx.as_bytes()), None, None)
        .map_err(|e| LiteError::Storage {
            detail: e.to_string(),
        })?;

    let prefix_bytes = score_pfx.as_bytes();
    let mut rows: Vec<Vec<Value>> = Vec::new();
    for (key, _) in all.iter().take(k as usize) {
        if !key.starts_with(prefix_bytes) {
            break;
        }
        let score_offset = prefix_bytes.len();
        if key.len() < score_offset + 9 {
            continue;
        }
        let score_bytes: [u8; 8] =
            key[score_offset..score_offset + 8]
                .try_into()
                .map_err(|_| LiteError::Storage {
                    detail: "corrupt score key bytes".into(),
                })?;
        let score = sort_bytes_to_f64(&score_bytes);
        let pk = &key[score_offset + 9..]; // skip the ':' separator
        rows.push(vec![Value::Bytes(pk.to_vec()), Value::Float(score)]);
    }

    Ok(QueryResult {
        columns: vec!["primary_key".into(), "score".into()],
        rows,
        rows_affected: 0,
    })
}

/// SortedIndexRange: return entries with score in [score_min, score_max].
pub fn kv_sorted_index_range<S: StorageEngine + StorageEngineSync>(
    engine: &LiteQueryEngine<S>,
    index_name: &str,
    score_min: Option<&[u8]>,
    score_max: Option<&[u8]>,
) -> Result<QueryResult, LiteError> {
    let score_pfx = score_prefix(index_name);
    let prefix_bytes = score_pfx.as_bytes();

    // Build start/end keys for the redb range scan.
    let start_key: Vec<u8> = match score_min {
        None => prefix_bytes.to_vec(),
        Some(min_bytes) if min_bytes.len() >= 8 => {
            let score_bytes: [u8; 8] =
                min_bytes[..8]
                    .try_into()
                    .map_err(|_| LiteError::BadRequest {
                        detail: "SortedIndexRange: score_min bytes malformed".into(),
                    })?;
            let mut k = prefix_bytes.to_vec();
            k.extend_from_slice(&score_bytes);
            k
        }
        Some(_) => {
            return Err(LiteError::BadRequest {
                detail: "SortedIndexRange: score_min must be 8 bytes (f64 encoded)".into(),
            });
        }
    };

    let all = engine
        .storage
        .scan_range_bounded_sync(Namespace::Meta, Some(&start_key), None, None)
        .map_err(|e| LiteError::Storage {
            detail: e.to_string(),
        })?;

    let max_score_bytes: Option<[u8; 8]> = match score_max {
        None => None,
        Some(max_bytes) if max_bytes.len() >= 8 => Some(max_bytes[..8].try_into().map_err(
            |_| LiteError::BadRequest {
                detail: "SortedIndexRange: score_max bytes malformed".into(),
            },
        )?),
        Some(_) => {
            return Err(LiteError::BadRequest {
                detail: "SortedIndexRange: score_max must be 8 bytes (f64 encoded)".into(),
            });
        }
    };

    let mut rows: Vec<Vec<Value>> = Vec::new();
    for (key, _) in &all {
        if !key.starts_with(prefix_bytes) {
            break;
        }
        let score_offset = prefix_bytes.len();
        if key.len() < score_offset + 9 {
            continue;
        }
        let entry_score: [u8; 8] =
            key[score_offset..score_offset + 8]
                .try_into()
                .map_err(|_| LiteError::Storage {
                    detail: "corrupt score key bytes".into(),
                })?;
        if let Some(max_bytes) = max_score_bytes
            && entry_score > max_bytes
        {
            break;
        }
        let score = sort_bytes_to_f64(&entry_score);
        let pk = &key[score_offset + 9..];
        rows.push(vec![Value::Bytes(pk.to_vec()), Value::Float(score)]);
    }

    Ok(QueryResult {
        columns: vec!["primary_key".into(), "score".into()],
        rows,
        rows_affected: 0,
    })
}

/// SortedIndexCount: total count of entries in a sorted index.
pub fn kv_sorted_index_count<S: StorageEngine + StorageEngineSync>(
    engine: &LiteQueryEngine<S>,
    index_name: &str,
) -> Result<QueryResult, LiteError> {
    let score_pfx = score_prefix(index_name);
    let all = engine
        .storage
        .scan_range_bounded_sync(Namespace::Meta, Some(score_pfx.as_bytes()), None, None)
        .map_err(|e| LiteError::Storage {
            detail: e.to_string(),
        })?;

    let prefix_bytes = score_pfx.as_bytes();
    let count = all
        .iter()
        .take_while(|(key, _)| key.starts_with(prefix_bytes))
        .count() as i64;

    Ok(QueryResult {
        columns: vec!["count".into()],
        rows: vec![vec![Value::Integer(count)]],
        rows_affected: 0,
    })
}
