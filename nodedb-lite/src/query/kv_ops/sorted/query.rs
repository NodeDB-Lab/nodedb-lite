// SPDX-License-Identifier: Apache-2.0
//! Query operations for sorted indexes: rank, top-k, range, count, score.
//!
//! Every read operation calls `purge_outside_window` first so that expired
//! entries are invisible to the caller without requiring a background task.

use nodedb_types::Namespace;
use nodedb_types::result::QueryResult;
use nodedb_types::value::Value;

use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::storage::engine::StorageEngine;

use super::keys::{pk_entry_key, score_prefix, sort_bytes_to_f64};
use super::window::purge_outside_window;

/// SortedIndexScore: return the score for a given primary key (ZSCORE).
pub async fn kv_sorted_index_score<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    index_name: &str,
    primary_key: &[u8],
) -> Result<QueryResult, LiteError> {
    purge_outside_window(engine, index_name, crate::runtime::now_millis()).await?;

    let pk_key = pk_entry_key(index_name, primary_key);
    let stored = engine
        .storage
        .get(Namespace::Meta, &pk_key)
        .await
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
pub async fn kv_sorted_index_rank<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    index_name: &str,
    primary_key: &[u8],
) -> Result<QueryResult, LiteError> {
    purge_outside_window(engine, index_name, crate::runtime::now_millis()).await?;

    let pk_key = pk_entry_key(index_name, primary_key);
    let stored = engine
        .storage
        .get(Namespace::Meta, &pk_key)
        .await
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

    let score_pfx = score_prefix(index_name);
    let all = engine
        .storage
        .scan_range_bounded(Namespace::Meta, Some(score_pfx.as_bytes()), None, None)
        .await
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
pub async fn kv_sorted_index_top_k<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    index_name: &str,
    k: u32,
) -> Result<QueryResult, LiteError> {
    purge_outside_window(engine, index_name, crate::runtime::now_millis()).await?;

    let score_pfx = score_prefix(index_name);
    let all = engine
        .storage
        .scan_range_bounded(Namespace::Meta, Some(score_pfx.as_bytes()), None, None)
        .await
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
        // pk follows the score bytes and separator byte (0x1F or ':')
        let pk = &key[score_offset + 9..];
        // For windowed entries there's a trailing {SEP}{ts:8}; strip it.
        let pk = strip_windowed_suffix(pk);
        rows.push(vec![Value::Bytes(pk.to_vec()), Value::Float(score)]);
    }

    Ok(QueryResult {
        columns: vec!["primary_key".into(), "score".into()],
        rows,
        rows_affected: 0,
    })
}

/// SortedIndexRange: return entries with score in [score_min, score_max].
pub async fn kv_sorted_index_range<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    index_name: &str,
    score_min: Option<&[u8]>,
    score_max: Option<&[u8]>,
) -> Result<QueryResult, LiteError> {
    purge_outside_window(engine, index_name, crate::runtime::now_millis()).await?;

    let score_pfx = score_prefix(index_name);
    let prefix_bytes = score_pfx.as_bytes();

    let start_key: Vec<u8> = match score_min {
        None => prefix_bytes.to_vec(),
        Some(min_bytes) if min_bytes.len() >= 8 => {
            let score_bytes: [u8; 8] =
                min_bytes[..8].try_into().map_err(|_| LiteError::Storage {
                    detail: "SortedIndexRange: score_min bytes malformed".into(),
                })?;
            let mut k = prefix_bytes.to_vec();
            k.extend_from_slice(&score_bytes);
            k
        }
        Some(_) => {
            return Err(LiteError::Storage {
                detail: "SortedIndexRange: score_min must be 8 bytes (f64 encoded)".into(),
            });
        }
    };

    let all = engine
        .storage
        .scan_range_bounded(Namespace::Meta, Some(&start_key), None, None)
        .await
        .map_err(|e| LiteError::Storage {
            detail: e.to_string(),
        })?;

    let max_score_bytes: Option<[u8; 8]> = match score_max {
        None => None,
        Some(max_bytes) if max_bytes.len() >= 8 => {
            Some(max_bytes[..8].try_into().map_err(|_| LiteError::Storage {
                detail: "SortedIndexRange: score_max bytes malformed".into(),
            })?)
        }
        Some(_) => {
            return Err(LiteError::Storage {
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
        let pk = strip_windowed_suffix(pk);
        rows.push(vec![Value::Bytes(pk.to_vec()), Value::Float(score)]);
    }

    Ok(QueryResult {
        columns: vec!["primary_key".into(), "score".into()],
        rows,
        rows_affected: 0,
    })
}

/// SortedIndexCount: total count of entries in a sorted index.
pub async fn kv_sorted_index_count<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    index_name: &str,
) -> Result<QueryResult, LiteError> {
    purge_outside_window(engine, index_name, crate::runtime::now_millis()).await?;

    let score_pfx = score_prefix(index_name);
    let all = engine
        .storage
        .scan_range_bounded(Namespace::Meta, Some(score_pfx.as_bytes()), None, None)
        .await
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

// ─── Internal helpers ─────────────────────────────────────────────────────────

/// For windowed score keys, the pk is followed by `{SEP}{ts:8}`. Strip that
/// suffix so callers receive the original pk bytes.
///
/// For non-windowed keys, the pk runs to the end of the slice — no stripping.
fn strip_windowed_suffix(pk_and_maybe_ts: &[u8]) -> &[u8] {
    use super::keys::SCORE_TS_SEPARATOR;
    // If the last 9 bytes are {SEP}{ts:8}, strip them.
    let len = pk_and_maybe_ts.len();
    if len >= 9 && pk_and_maybe_ts[len - 9] == SCORE_TS_SEPARATOR {
        &pk_and_maybe_ts[..len - 9]
    } else {
        pk_and_maybe_ts
    }
}
