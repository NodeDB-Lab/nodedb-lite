// SPDX-License-Identifier: Apache-2.0
//! DDL operations for sorted indexes: register and drop.

use nodedb_types::Namespace;
use nodedb_types::result::QueryResult;

use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::storage::engine::{StorageEngine, StorageEngineSync, WriteOp};

use super::keys::score_prefix;
use super::window::{WindowDef, delete_window_def, store_window_def};

/// RegisterSortedIndex: register a sorted index on a KV collection.
///
/// For `window_type = "none"` the index is ready immediately — no persistent
/// DDL record is required. For windowed types the window definition is
/// persisted in the Meta namespace so that the lazy purge survives restarts.
///
/// Supported window types: "none", "tumbling", "sliding", "session", "custom".
///
/// Sliding and session windows use `window_start_ms` as the window size in ms
/// (duration between window_start_ms and window_end_ms when both are non-zero,
/// or window_start_ms alone if window_end_ms is 0).
pub fn kv_register_sorted_index<S: StorageEngine + StorageEngineSync>(
    engine: &LiteQueryEngine<S>,
    index_name: &str,
    window_type: &str,
    window_timestamp_column: &str,
    window_start_ms: u64,
    window_end_ms: u64,
) -> Result<QueryResult, LiteError> {
    match window_type {
        "none" => {
            // Non-windowed: nothing to persist.
        }
        "tumbling" | "custom" => {
            let def = WindowDef {
                window_type: window_type.to_string(),
                window_timestamp_column: window_timestamp_column.to_string(),
                window_start_ms,
                window_end_ms,
            };
            store_window_def(engine, index_name, &def)?;
        }
        "sliding" | "session" => {
            // For sliding/session, window_start_ms holds the window duration.
            // If window_end_ms > window_start_ms the caller supplied explicit
            // bounds; derive size from them.
            let size_ms = if window_start_ms > 0 && window_end_ms > window_start_ms {
                window_end_ms - window_start_ms
            } else if window_start_ms > 0 {
                window_start_ms
            } else {
                // Default to 1 hour if neither is specified.
                3_600_000
            };
            let def = WindowDef {
                window_type: window_type.to_string(),
                window_timestamp_column: window_timestamp_column.to_string(),
                // Store size in window_start_ms for uniform retrieval.
                window_start_ms: size_ms,
                window_end_ms: 0,
            };
            store_window_def(engine, index_name, &def)?;
        }
        other => {
            return Err(LiteError::Storage {
                detail: format!(
                    "RegisterSortedIndex: unknown window_type '{other}'; \
                     expected one of: none, tumbling, sliding, session, custom"
                ),
            });
        }
    }

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
    let score_pfx = score_prefix(index_name);
    let score_entries = engine
        .storage
        .scan_range_bounded_sync(Namespace::Meta, Some(score_pfx.as_bytes()), None, None)
        .map_err(|e| LiteError::Storage {
            detail: e.to_string(),
        })?;

    let pk_pfx = format!("kv_sorted:{index_name}:pk:");
    let pk_entries = engine
        .storage
        .scan_range_bounded_sync(Namespace::Meta, Some(pk_pfx.as_bytes()), None, None)
        .map_err(|e| LiteError::Storage {
            detail: e.to_string(),
        })?;

    let mut ops: Vec<WriteOp> = Vec::with_capacity(score_entries.len() + pk_entries.len() + 1);

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

    // Remove the window definition if present.
    delete_window_def(engine, index_name)?;

    Ok(QueryResult {
        columns: vec![],
        rows: vec![],
        rows_affected: count,
    })
}
