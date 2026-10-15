// SPDX-License-Identifier: Apache-2.0
//! Batch put under a statement's rule for a stored key.
//!
//! Every entry is judged before any entry is written, and the batch commits
//! as one storage write. A key the batch names twice is judged against the
//! batch's own earlier entry, as one statement's rows are judged in row
//! order.

use std::collections::{HashMap, HashSet};

use nodedb_physical::physical_plan::{KvBatchIntent, UpdateValue};
use nodedb_types::Namespace;
use nodedb_types::result::QueryResult;

use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::query::kv_ops::body::{encode_kv_body, kv_body_columns};
use crate::query::on_conflict::apply_patch;
use crate::storage::engine::{StorageEngine, WriteOp};

use super::super::reads::{decode_value, encode_value, is_expired, kv_key};
use super::bulk::kv_batch_put;

/// Write `entries` under `intent`.
///
/// - `Put`, `Upsert`: overwrite every key.
/// - `Insert`: a stored or repeated key refuses the whole batch with
///   `unique_violation`. Nothing is written then.
/// - `InsertIfAbsent`: a stored or repeated key is skipped.
/// - `Merge`: an absent key stores its entry. A stored or repeated key
///   stores the merge of `updates` into the stored body, with `EXCLUDED`
///   bound to the entry.
///
/// The caller decides `Merge`'s write policy before this runs.
pub async fn kv_batch_write<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    collection: &str,
    entries: &[(Vec<u8>, Vec<u8>)],
    ttl_ms: u64,
    intent: &KvBatchIntent,
) -> Result<QueryResult, LiteError> {
    match intent {
        KvBatchIntent::Put | KvBatchIntent::Upsert => {
            kv_batch_put(engine, collection, entries, ttl_ms).await
        }
        KvBatchIntent::Insert => insert_batch(engine, collection, entries, ttl_ms, false).await,
        KvBatchIntent::InsertIfAbsent => {
            insert_batch(engine, collection, entries, ttl_ms, true).await
        }
        KvBatchIntent::Merge {
            updates,
            rls_write_check: _,
        } => merge_batch(engine, collection, entries, ttl_ms, updates).await,
    }
}

/// The deadline a write with `ttl_ms` stamps. 0 never expires.
fn deadline_for(ttl_ms: u64) -> u64 {
    if ttl_ms > 0 {
        crate::runtime::now_millis().saturating_add(ttl_ms)
    } else {
        0
    }
}

/// The live stored entry under `rkey`: its deadline and body. An expired
/// entry reads as absent.
async fn live_entry<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    rkey: &[u8],
) -> Result<Option<(u64, Vec<u8>)>, LiteError> {
    let raw = engine
        .storage
        .get(Namespace::Kv, rkey)
        .await
        .map_err(|e| LiteError::Storage {
            detail: e.to_string(),
        })?;
    Ok(raw.and_then(|raw| {
        decode_value(&raw)
            .filter(|(deadline, _)| !is_expired(*deadline))
            .map(|(deadline, body)| (deadline, body.to_vec()))
    }))
}

async fn insert_batch<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    collection: &str,
    entries: &[(Vec<u8>, Vec<u8>)],
    ttl_ms: u64,
    if_absent: bool,
) -> Result<QueryResult, LiteError> {
    let deadline = deadline_for(ttl_ms);
    let mut seen: HashSet<&[u8]> = HashSet::with_capacity(entries.len());
    let mut ops: Vec<WriteOp> = Vec::with_capacity(entries.len());
    for (key, value) in entries {
        let rkey = kv_key(collection, key);
        let taken = !seen.insert(key.as_slice()) || live_entry(engine, &rkey).await?.is_some();
        if taken {
            if if_absent {
                continue;
            }
            return Err(LiteError::BadRequest {
                detail: format!(
                    "unique_violation: key already exists in collection '{collection}'"
                ),
            });
        }
        ops.push(WriteOp::Put {
            ns: Namespace::Kv,
            key: rkey,
            value: encode_value(deadline, value),
        });
    }
    let count = ops.len() as u64;
    if !ops.is_empty() {
        engine.kv_local.commit(&*engine.storage, ops).await?;
    }
    Ok(QueryResult {
        columns: vec![],
        rows: vec![],
        rows_affected: count,
        command: Some("INSERT".into()),
    })
}

async fn merge_batch<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    collection: &str,
    entries: &[(Vec<u8>, Vec<u8>)],
    ttl_ms: u64,
    updates: &[(String, UpdateValue)],
) -> Result<QueryResult, LiteError> {
    let deadline = deadline_for(ttl_ms);
    // `(storage key, deadline, body)` each key holds after the entries
    // judged so far, in first-seen order. `slot` finds a key's position.
    let mut planned: Vec<(Vec<u8>, u64, Vec<u8>)> = Vec::with_capacity(entries.len());
    let mut slot: HashMap<Vec<u8>, usize> = HashMap::with_capacity(entries.len());
    for (key, value) in entries {
        let rkey = kv_key(collection, key);
        let position = slot.get(&rkey).copied();
        let current = match position.and_then(|i| planned.get(i)) {
            Some((_, entry_deadline, body)) => Some((*entry_deadline, body.clone())),
            None => live_entry(engine, &rkey).await?,
        };
        let (next_deadline, next_body) = match current {
            None => (deadline, value.clone()),
            Some((old_deadline, stored)) => {
                // Both sides decode to rows (a raw body reads as
                // `{"value": <text>}`). The merged row re-encodes in the
                // stored body's shape, so a raw row stays raw.
                let (mut row, shape) = kv_body_columns(&stored)?;
                let (excluded, _) = kv_body_columns(value)?;
                apply_patch(&mut row, updates, &excluded)?;
                let keep = if ttl_ms > 0 { deadline } else { old_deadline };
                (keep, encode_kv_body(row, shape)?)
            }
        };
        match position.and_then(|i| planned.get_mut(i)) {
            Some(entry) => {
                entry.1 = next_deadline;
                entry.2 = next_body;
            }
            None => {
                slot.insert(rkey.clone(), planned.len());
                planned.push((rkey, next_deadline, next_body));
            }
        }
    }
    let ops: Vec<WriteOp> = planned
        .into_iter()
        .map(|(rkey, entry_deadline, body)| WriteOp::Put {
            ns: Namespace::Kv,
            key: rkey,
            value: encode_value(entry_deadline, &body),
        })
        .collect();
    let count = entries.len() as u64;
    if !ops.is_empty() {
        engine.kv_local.commit(&*engine.storage, ops).await?;
    }
    Ok(QueryResult {
        columns: vec![],
        rows: vec![],
        rows_affected: count,
        command: Some("INSERT".into()),
    })
}
