// SPDX-License-Identifier: Apache-2.0
//! Temporal audit-retention purge meta-ops.

use nodedb_types::result::QueryResult;
use nodedb_types::value::Value;

use crate::engine::graph::history as graph_history;
use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::storage::engine::{StorageEngine, StorageEngineSync};

/// `TemporalPurgeEdgeStore` — purge superseded edge versions older than cutoff.
///
/// For graph collections declared with `bitemporal=true`, edges are tracked in
/// `Namespace::GraphHistory`. This handler deletes history entries whose
/// `system_to_ms < cutoff_system_ms`. Collections that are not bitemporal have
/// no history table; they return `rows_affected: 0` (correct — nothing to purge).
pub async fn handle_temporal_purge_edge_store<S: StorageEngine + StorageEngineSync>(
    engine: &LiteQueryEngine<S>,
    _tenant_id: u64,
    collection: &str,
    cutoff_system_ms: i64,
) -> Result<QueryResult, LiteError> {
    let rows_affected = graph_history::purge_edge_history_before(
        engine.storage.as_ref(),
        collection,
        cutoff_system_ms,
    )
    .await?;

    Ok(QueryResult {
        columns: vec!["rows_affected".into()],
        rows: vec![vec![Value::Integer(rows_affected as i64)]],
        rows_affected,
    })
}

/// `TemporalPurgeDocumentStrict` — purge superseded strict-document versions
/// older than `cutoff_system_ms`.
///
/// For strict collections with `bitemporal=true`, each update and delete writes
/// the old row version to `Namespace::StrictHistory`. This handler deletes
/// history entries whose `system_to_ms < cutoff_system_ms`. Non-bitemporal
/// collections have no history table and return `rows_affected: 0`.
pub async fn handle_temporal_purge_document_strict<S: StorageEngine + StorageEngineSync>(
    engine: &LiteQueryEngine<S>,
    _tenant_id: u64,
    collection: &str,
    cutoff_system_ms: i64,
) -> Result<QueryResult, LiteError> {
    let rows_affected = engine
        .strict
        .purge_history_before(collection, cutoff_system_ms)
        .await?;

    Ok(QueryResult {
        columns: vec!["rows_affected".into()],
        rows: vec![vec![Value::Integer(rows_affected as i64)]],
        rows_affected,
    })
}

/// `TemporalPurgeColumnar` — purge superseded columnar segment tombstones older
/// than `cutoff_system_ms`.
///
/// For columnar collections with `bitemporal=true`, fully-compacted (all-rows-
/// deleted) segments are retained as tombstones with a `fully_deleted_at_ms`
/// timestamp rather than being immediately purged. This handler removes those
/// tombstones where `fully_deleted_at_ms < cutoff_system_ms`. Non-bitemporal
/// collections have no tombstones and return `rows_affected: 0`.
pub async fn handle_temporal_purge_columnar<S: StorageEngine + StorageEngineSync>(
    engine: &LiteQueryEngine<S>,
    _tenant_id: u64,
    collection: &str,
    cutoff_system_ms: i64,
) -> Result<QueryResult, LiteError> {
    let rows_affected = engine
        .columnar
        .purge_bitemporal_before(collection, cutoff_system_ms)
        .await?;

    Ok(QueryResult {
        columns: vec!["rows_affected".into()],
        rows: vec![vec![Value::Integer(rows_affected as i64)]],
        rows_affected,
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

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use nodedb_types::columnar::{ColumnDef, ColumnType, StrictSchema};
    use nodedb_types::value::Value;

    use crate::engine::array::engine::ArrayEngineState;
    use crate::engine::columnar::ColumnarEngine;
    use crate::engine::crdt::CrdtEngine;
    use crate::engine::fts::FtsState;
    use crate::engine::htap::HtapBridge;
    use crate::engine::strict::StrictEngine;
    use crate::engine::vector::VectorState;
    use crate::query::engine::LiteQueryEngine;
    use crate::storage::redb_storage::RedbStorage;

    use super::*;

    fn make_engine(storage: Arc<RedbStorage>) -> LiteQueryEngine<RedbStorage> {
        let crdt = Arc::new(Mutex::new(
            CrdtEngine::new(1).expect("CrdtEngine::new failed in test"),
        ));
        let strict = Arc::new(StrictEngine::new(Arc::clone(&storage)));
        let columnar = Arc::new(ColumnarEngine::new(Arc::clone(&storage)));
        let htap = Arc::new(HtapBridge::new());
        let timeseries = Arc::new(Mutex::new(
            crate::engine::timeseries::engine::TimeseriesEngine::new(),
        ));
        let vector_state = Arc::new(VectorState::new(Arc::clone(&storage), 50));
        let array_state = Arc::new(Mutex::new(ArrayEngineState::new()));
        let fts_state = Arc::new(FtsState::new());
        LiteQueryEngine::new(
            crdt,
            strict,
            columnar,
            htap,
            storage,
            timeseries,
            vector_state,
            array_state,
            fts_state,
        )
    }

    #[tokio::test]
    async fn strict_bitemporal_purge_removes_superseded_rows() {
        let dir = tempfile::tempdir().unwrap();
        let storage = Arc::new(RedbStorage::open(dir.path().join("test.db")).unwrap());
        let engine = make_engine(Arc::clone(&storage));

        // Create a bitemporal strict collection.
        let user_cols = vec![
            ColumnDef::required("id", ColumnType::Int64).with_primary_key(),
            ColumnDef::nullable("name", ColumnType::String),
        ];
        let schema = StrictSchema::new_bitemporal(user_cols).unwrap();
        engine
            .strict
            .create_collection("users", schema)
            .await
            .unwrap();

        // Insert a row. For bitemporal schemas, slot 0 = __system_from_ms.
        let now = crate::engine::array::ops::util::time::now_ms();
        let row = vec![
            Value::Integer(now),           // __system_from_ms
            Value::Integer(0),             // __valid_from_ms
            Value::Integer(i64::MAX),      // __valid_until_ms
            Value::Integer(42),            // id
            Value::String("alice".into()), // name
        ];
        engine.strict.insert("users", &row).await.unwrap();

        // Delete the row — records a history supersession entry.
        engine
            .strict
            .delete("users", &Value::Integer(42))
            .await
            .unwrap();

        // Purge with a cutoff far in the future — must remove the superseded entry.
        let far_future: i64 = 9_999_999_999_999;
        let result = handle_temporal_purge_document_strict(&engine, 0, "users", far_future)
            .await
            .unwrap();

        assert!(
            result.rows_affected >= 1,
            "expected rows_affected >= 1, got {}",
            result.rows_affected
        );
    }

    #[tokio::test]
    async fn strict_non_bitemporal_purge_returns_zero() {
        let dir = tempfile::tempdir().unwrap();
        let storage = Arc::new(RedbStorage::open(dir.path().join("test.db")).unwrap());
        let engine = make_engine(Arc::clone(&storage));

        let cols = vec![ColumnDef::required("id", ColumnType::Int64).with_primary_key()];
        let schema = StrictSchema::new(cols).unwrap();
        engine
            .strict
            .create_collection("plain", schema)
            .await
            .unwrap();

        let result = handle_temporal_purge_document_strict(&engine, 0, "plain", 9_999_999_999)
            .await
            .unwrap();
        assert_eq!(result.rows_affected, 0);
    }

    #[tokio::test]
    async fn columnar_non_bitemporal_purge_returns_zero() {
        let dir = tempfile::tempdir().unwrap();
        let storage = Arc::new(RedbStorage::open(dir.path().join("test.db")).unwrap());
        let engine = make_engine(Arc::clone(&storage));

        let schema = nodedb_types::columnar::ColumnarSchema::new(vec![
            ColumnDef::required("id", ColumnType::Int64).with_primary_key(),
        ])
        .unwrap();
        engine
            .columnar
            .create_collection(
                "metrics",
                schema,
                nodedb_types::columnar::ColumnarProfile::Plain,
                false,
            )
            .await
            .unwrap();

        let result = handle_temporal_purge_columnar(&engine, 0, "metrics", 9_999_999_999)
            .await
            .unwrap();
        assert_eq!(result.rows_affected, 0);
    }

    #[tokio::test]
    async fn columnar_bitemporal_purge_removes_tombstoned_segments() {
        let dir = tempfile::tempdir().unwrap();
        let storage = Arc::new(RedbStorage::open(dir.path().join("test.db")).unwrap());
        let engine = make_engine(Arc::clone(&storage));

        let schema = nodedb_types::columnar::ColumnarSchema::new(vec![
            ColumnDef::required("id", ColumnType::Int64).with_primary_key(),
            ColumnDef::nullable("val", ColumnType::Int64),
        ])
        .unwrap();
        engine
            .columnar
            .create_collection(
                "events",
                schema,
                nodedb_types::columnar::ColumnarProfile::Plain,
                true,
            )
            .await
            .unwrap();

        // Insert and flush a row so a segment exists.
        engine
            .columnar
            .insert("events", &[Value::Integer(1), Value::Integer(100)])
            .unwrap();
        engine.columnar.flush_collection("events").await.unwrap();

        // Delete the row so the segment becomes fully-deleted.
        engine
            .columnar
            .delete("events", &Value::Integer(1))
            .unwrap();

        // Compact — for bitemporal collections this sets fully_deleted_at_ms.
        engine
            .columnar
            .try_compact_collection("events")
            .await
            .unwrap();

        // Purge with far-future cutoff — should remove the tombstoned segment.
        let result = handle_temporal_purge_columnar(&engine, 0, "events", 9_999_999_999_999)
            .await
            .unwrap();

        assert!(
            result.rows_affected >= 1,
            "expected tombstone purge, got {}",
            result.rows_affected
        );
    }

    #[tokio::test]
    async fn graph_non_bitemporal_purge_returns_zero() {
        let dir = tempfile::tempdir().unwrap();
        let storage = Arc::new(RedbStorage::open(dir.path().join("test.db")).unwrap());
        let engine = make_engine(Arc::clone(&storage));

        // Collection "social" has no bitemporal flag set — returns 0.
        let result = handle_temporal_purge_edge_store(&engine, 0, "social", 9_999_999_999)
            .await
            .unwrap();
        assert_eq!(result.rows_affected, 0);
    }

    #[tokio::test]
    async fn strict_bitemporal_purge_cutoff_before_deletion_retains_history() {
        let dir = tempfile::tempdir().unwrap();
        let storage = Arc::new(RedbStorage::open(dir.path().join("test.db")).unwrap());
        let engine = make_engine(Arc::clone(&storage));

        let user_cols = vec![ColumnDef::required("id", ColumnType::Int64).with_primary_key()];
        let schema = StrictSchema::new_bitemporal(user_cols).unwrap();
        engine
            .strict
            .create_collection("users2", schema)
            .await
            .unwrap();

        let now = crate::engine::array::ops::util::time::now_ms();
        let row = vec![
            Value::Integer(now),
            Value::Integer(0),
            Value::Integer(i64::MAX),
            Value::Integer(99),
        ];
        engine.strict.insert("users2", &row).await.unwrap();
        engine
            .strict
            .delete("users2", &Value::Integer(99))
            .await
            .unwrap();

        // Purge with a cutoff of 1 ms (in the past) — should retain everything.
        let result = handle_temporal_purge_document_strict(&engine, 0, "users2", 1)
            .await
            .unwrap();

        assert_eq!(
            result.rows_affected, 0,
            "cutoff before deletion should retain history"
        );
    }
}
