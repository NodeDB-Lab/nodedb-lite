// SPDX-License-Identifier: Apache-2.0
//! Sorted-index reads: rank, top-k, range, count, score.
//!
//! Every read commits the public KV API's buffered writes first, so a
//! buffered put is ranked. A read sees the entries of live rows only: an
//! entry whose row expired is hidden, and a windowed index hides entries
//! stamped before the current window's start. Entries rank in key order:
//! by score, then window timestamp, then primary key.

use std::sync::Arc;

use nodedb_types::Namespace;
use nodedb_types::result::QueryResult;
use nodedb_types::value::Value;

use crate::error::LiteError;
use crate::index::sorted_def::SortedIndexDef;
use crate::query::engine::LiteQueryEngine;
use crate::storage::engine::StorageEngine;

use super::keys::{FIELD_LEN, PkEntry, complement, decode_score, pk_entry_key, score_prefix};
use super::window::{decode_ts, window_start};

/// One visible entry: its score in key-order bytes and its row's key.
struct Visible {
    score: [u8; FIELD_LEN],
    pk: Vec<u8>,
}

fn corrupt(def: &SortedIndexDef, what: &str) -> LiteError {
    LiteError::Corrupted {
        detail: format!("sorted index '{}': {what}", def.name),
    }
}

fn not_found(index_name: &str) -> LiteError {
    LiteError::UndefinedObject {
        object: format!("sorted index '{index_name}'"),
    }
}

fn is_expired(deadline: u64, now_ms: u64) -> bool {
    deadline != 0 && now_ms >= deadline
}

fn result(columns: &[&str], rows: Vec<Vec<Value>>) -> QueryResult {
    QueryResult {
        columns: columns.iter().map(|c| (*c).to_string()).collect(),
        rows,
        rows_affected: 0,
        command: None,
    }
}

/// `[rank, key]` rows for `(position, entry)` pairs, where the position
/// is the entry's 0-based place among every visible entry.
fn ranked_rows<'a>(entries: impl Iterator<Item = (usize, &'a Visible)>) -> QueryResult {
    let rows = entries
        .map(|(position, entry)| {
            vec![
                Value::Integer(position as i64 + 1),
                Value::Bytes(entry.pk.clone()),
            ]
        })
        .collect();
    result(&["rank", "key"], rows)
}

/// Every entry of `def` a read at `now_ms` sees, in key order.
async fn visible_entries<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    def: &SortedIndexDef,
    now_ms: u64,
) -> Result<Vec<Visible>, LiteError> {
    engine.kv_local.flush_to(&*engine.storage).await?;
    let start = window_start(&def.window, now_ms);
    let prefix = score_prefix(&def.name);
    let pk_offset = if def.window.is_windowed() {
        2 * FIELD_LEN
    } else {
        FIELD_LEN
    };
    let mut visible = Vec::new();
    for (key, value) in engine.storage.scan_prefix(Namespace::Meta, &prefix).await? {
        let rest = key
            .get(prefix.len()..)
            .filter(|rest| rest.len() >= pk_offset)
            .ok_or_else(|| corrupt(def, "score entry key too short"))?;
        let deadline_bytes: [u8; FIELD_LEN] = value
            .as_slice()
            .try_into()
            .map_err(|_| corrupt(def, "score entry value is not a deadline"))?;
        if is_expired(u64::from_le_bytes(deadline_bytes), now_ms) {
            continue;
        }
        if let Some(start) = start {
            let ts: [u8; FIELD_LEN] = rest[FIELD_LEN..2 * FIELD_LEN]
                .try_into()
                .map_err(|_| corrupt(def, "score entry timestamp malformed"))?;
            if decode_ts(&ts, def.window.timestamp_desc) < start {
                continue;
            }
        }
        let score: [u8; FIELD_LEN] = rest[..FIELD_LEN]
            .try_into()
            .map_err(|_| corrupt(def, "score entry score malformed"))?;
        visible.push(Visible {
            score,
            pk: rest[pk_offset..].to_vec(),
        });
    }
    Ok(visible)
}

/// The sorted index named `index_name`, or `NotFound`.
fn require<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    index_name: &str,
) -> Result<Arc<SortedIndexDef>, LiteError> {
    engine
        .indexes
        .sorted_named(index_name)
        .ok_or_else(|| not_found(index_name))
}

/// SortedIndexScore: the score of the row `primary_key` (ZSCORE). NULL for
/// an unknown index, an absent row, or an expired row. The window does not
/// apply: a score reads the row whatever its timestamp.
pub async fn kv_sorted_index_score<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    index_name: &str,
    primary_key: &[u8],
) -> Result<QueryResult, LiteError> {
    let Some(def) = engine.indexes.sorted_named(index_name) else {
        return Ok(result(&["score"], vec![vec![Value::Null]]));
    };
    engine.kv_local.flush_to(&*engine.storage).await?;
    let stored = engine
        .storage
        .get(Namespace::Meta, &pk_entry_key(&def.name, primary_key))
        .await?;
    let score = match stored {
        None => Value::Null,
        Some(bytes) => {
            let entry =
                PkEntry::decode(&bytes).ok_or_else(|| corrupt(&def, "pk entry does not decode"))?;
            if is_expired(entry.deadline, crate::runtime::now_millis()) {
                Value::Null
            } else {
                Value::Float(decode_score(&entry.score, def.score_desc))
            }
        }
    };
    Ok(result(&["score"], vec![vec![score]]))
}

/// SortedIndexRank: the 1-based rank of the row `primary_key`. NULL for an
/// unknown index or a row the read does not see.
pub async fn kv_sorted_index_rank<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    index_name: &str,
    primary_key: &[u8],
) -> Result<QueryResult, LiteError> {
    let Some(def) = engine.indexes.sorted_named(index_name) else {
        return Ok(result(&["rank"], vec![vec![Value::Null]]));
    };
    let entries = visible_entries(engine, &def, crate::runtime::now_millis()).await?;
    let rank = entries
        .iter()
        .position(|entry| entry.pk == primary_key)
        .map_or(Value::Null, |position| Value::Integer(position as i64 + 1));
    Ok(result(&["rank"], vec![vec![rank]]))
}

/// SortedIndexTopK: the first `k` visible entries, as `[rank, key]` rows.
pub async fn kv_sorted_index_top_k<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    index_name: &str,
    k: u32,
) -> Result<QueryResult, LiteError> {
    let def = require(engine, index_name)?;
    let entries = visible_entries(engine, &def, crate::runtime::now_millis()).await?;
    Ok(ranked_rows(entries.iter().take(k as usize).enumerate()))
}

/// An 8-byte score bound, encoded as `f64_to_sort_bytes` encodes it.
fn score_bound(def: &SortedIndexDef, bound: Option<&[u8]>) -> Result<Option<[u8; 8]>, LiteError> {
    bound
        .map(|bytes| {
            <[u8; FIELD_LEN]>::try_from(bytes).map_err(|_| LiteError::BadRequest {
                detail: format!(
                    "SortedIndexRange '{}': a score bound must be 8 bytes, got {}",
                    def.name,
                    bytes.len()
                ),
            })
        })
        .transpose()
}

/// SortedIndexRange: the visible entries whose score is in
/// `[score_min, score_max]`, both inclusive, as `[rank, key]` rows. An
/// absent bound is open. The rank is the entry's place among every visible
/// entry.
pub async fn kv_sorted_index_range<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    index_name: &str,
    score_min: Option<&[u8]>,
    score_max: Option<&[u8]>,
) -> Result<QueryResult, LiteError> {
    let def = require(engine, index_name)?;
    let min = score_bound(&def, score_min)?;
    let max = score_bound(&def, score_max)?;
    // Complementing a descending score reverses order, so `min` becomes
    // the greater key bound and the bounds swap.
    let (lower, upper) = if def.score_desc {
        (max.map(complement), min.map(complement))
    } else {
        (min, max)
    };
    let entries = visible_entries(engine, &def, crate::runtime::now_millis()).await?;
    Ok(ranked_rows(entries.iter().enumerate().filter(
        |(_, entry)| {
            lower.is_none_or(|lower| entry.score >= lower)
                && upper.is_none_or(|upper| entry.score <= upper)
        },
    )))
}

/// SortedIndexCount: the number of visible entries.
pub async fn kv_sorted_index_count<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    index_name: &str,
) -> Result<QueryResult, LiteError> {
    let def = require(engine, index_name)?;
    let entries = visible_entries(engine, &def, crate::runtime::now_millis()).await?;
    Ok(result(
        &["count"],
        vec![vec![Value::Integer(entries.len() as i64)]],
    ))
}
