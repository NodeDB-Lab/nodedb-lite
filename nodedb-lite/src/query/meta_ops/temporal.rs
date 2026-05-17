// SPDX-License-Identifier: Apache-2.0
//! Temporal audit-retention purge meta-ops.

use nodedb_types::result::QueryResult;
use nodedb_types::value::Value;

use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::storage::engine::{StorageEngine, StorageEngineSync};

/// `TemporalPurgeEdgeStore` — purge superseded edge versions older than cutoff.
///
/// Lite's graph engine is a thin re-export of `nodedb_graph::CsrIndex`, which
/// stores only the current adjacency matrix. There is no versioned edge-history
/// table and no bitemporal edge store; temporal purge has no target data.
pub async fn handle_temporal_purge_edge_store<S: StorageEngine + StorageEngineSync>(
    _engine: &LiteQueryEngine<S>,
    _tenant_id: u64,
    _collection: &str,
    _cutoff_system_ms: i64,
) -> Result<QueryResult, LiteError> {
    Err(LiteError::Unsupported {
        detail: "temporal purge on edge store requires bitemporal=true graph collection; \
                 Lite graph engine stores current-state edges only (nodedb_graph::CsrIndex \
                 has no version history)"
            .into(),
    })
}

/// `TemporalPurgeDocumentStrict` — purge superseded strict-document versions
/// older than `cutoff_system_ms`.
///
/// Lite's strict engine writes each document as a single current-state row in
/// redb (`Namespace::Strict`). There is no system_time_from/to history table;
/// overwriting a document replaces the row in-place. There are no superseded
/// versions to purge.
pub async fn handle_temporal_purge_document_strict<S: StorageEngine + StorageEngineSync>(
    _engine: &LiteQueryEngine<S>,
    _tenant_id: u64,
    _collection: &str,
    _cutoff_system_ms: i64,
) -> Result<QueryResult, LiteError> {
    Err(LiteError::Unsupported {
        detail: "temporal purge on strict documents requires a versioned history table; \
                 Lite strict engine stores only the current row (no system_time_from/to columns)"
            .into(),
    })
}

/// `TemporalPurgeColumnar` — purge superseded columnar partitions older than cutoff.
///
/// Lite's columnar engine stores compressed segments in redb keyed by
/// `{collection}:seg:{segment_id}` without a system_time column or bitemporal
/// partition manifest. Segments are compaction-merged, not version-chained;
/// there are no superseded partitions to purge by system time.
pub async fn handle_temporal_purge_columnar<S: StorageEngine + StorageEngineSync>(
    _engine: &LiteQueryEngine<S>,
    _tenant_id: u64,
    _collection: &str,
    _cutoff_system_ms: i64,
) -> Result<QueryResult, LiteError> {
    Err(LiteError::Unsupported {
        detail: "temporal purge on columnar requires bitemporal=true partitions; \
                 Lite columnar engine has no system_time column in segment metadata"
            .into(),
    })
}

/// `TemporalPurgeCrdt` — compact Loro oplog history up to the given cutoff.
///
/// Calls `CrdtEngine::compact_history()` which replaces the internal LoroDoc
/// oplog with a shallow snapshot, discarding history entries and freeing memory.
/// The current state is fully preserved. The cutoff is advisory — Loro's
/// `compact_history` discards all history before the current frontier, which
/// subsumes the cutoff when all operations before it have been applied.
pub async fn handle_temporal_purge_crdt<S: StorageEngine + StorageEngineSync>(
    engine: &LiteQueryEngine<S>,
    _tenant_id: u64,
    _collection: &str,
    _cutoff_system_ms: i64,
) -> Result<QueryResult, LiteError> {
    let mut crdt = engine.crdt.lock().map_err(|_| LiteError::LockPoisoned)?;
    crdt.compact_history()?;
    Ok(QueryResult {
        columns: vec!["compacted".into()],
        rows: vec![vec![Value::Bool(true)]],
        rows_affected: 1,
    })
}

/// `TemporalPurgeArray` — purge superseded tile versions older than cutoff.
///
/// Derives `audit_retain_ms` from the cutoff (`retain = now - cutoff`, clamped
/// to 0) and calls the array compact op, which merges out-of-horizon tile
/// versions in every segment and rewrites the manifest. `rows_affected` reflects
/// the number of segments rewritten by the compact pass.
pub async fn handle_temporal_purge_array<S: StorageEngine + StorageEngineSync>(
    engine: &LiteQueryEngine<S>,
    _tenant_id: u64,
    array_id: &str,
    cutoff_system_ms: i64,
) -> Result<QueryResult, LiteError> {
    let retain_ms = (crate::engine::array::ops::util::time::now_ms() - cutoff_system_ms).max(0);
    let result = crate::engine::array::ops::compact::compact(
        &engine.array_state,
        &engine.storage,
        array_id,
        Some(retain_ms),
    )
    .await?;
    Ok(result)
}

/// `EnforceTimeseriesRetention` — drop timeseries partitions older than max_age_ms.
pub async fn handle_enforce_timeseries_retention<S: StorageEngine + StorageEngineSync>(
    engine: &LiteQueryEngine<S>,
    _collection: &str,
    max_age_ms: i64,
) -> Result<QueryResult, LiteError> {
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64;
    let mut ts = engine
        .timeseries
        .lock()
        .map_err(|_| LiteError::LockPoisoned)?;
    let cutoff_ms = now_ms - max_age_ms;
    let dropped = ts.purge_before_ms(cutoff_ms);
    Ok(QueryResult {
        columns: vec!["dropped_partitions".into()],
        rows: vec![vec![Value::Integer(dropped.len() as i64)]],
        rows_affected: dropped.len() as u64,
    })
}
