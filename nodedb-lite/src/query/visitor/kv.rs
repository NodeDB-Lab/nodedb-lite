// SPDX-License-Identifier: Apache-2.0
//! SQL-visitor lowering for KV SqlPlan variants: KvInsert.

use nodedb_physical::PhysicalTaskVisitor;
use nodedb_physical::physical_plan::KvOp;
use nodedb_sql::types::KvInsertIntent;
use nodedb_sql::types_expr::SqlValue;
use nodedb_types::Surrogate;

use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::query::physical_visitor::LiteDataPlaneVisitor;
use crate::storage::engine::{StorageEngine, StorageEngineSync};

use super::adapter::LiteFut;

// ── Value encoding ────────────────────────────────────────────────────────────

/// Encode a `SqlValue` as raw bytes for use as a KV key.
/// Mirrors Origin's `sql_value_to_bytes`: strings and bytes are returned
/// as-is; integers are stringified; everything else uses the debug string.
fn sql_value_to_bytes(v: &SqlValue) -> Vec<u8> {
    match v {
        SqlValue::String(s) => s.as_bytes().to_vec(),
        SqlValue::Bytes(b) => b.clone(),
        SqlValue::Int(i) => i.to_string().into_bytes(),
        _ => format!("{v:?}").into_bytes(),
    }
}

/// Encode a KV value column set as a MessagePack map.
///
/// When a single `value` column is present its bytes are stored directly
/// (plain-value path). Otherwise a msgpack map `{col: val, ...}` is stored.
fn encode_kv_value(value_cols: &[(String, SqlValue)]) -> Result<Vec<u8>, LiteError> {
    if value_cols.len() == 1 && value_cols[0].0 == "value" {
        return Ok(sql_value_to_bytes(&value_cols[0].1));
    }
    // Build a msgpack map with one entry per column.
    use nodedb_types::value::Value;
    use std::collections::HashMap;
    let mut map: HashMap<String, Value> = HashMap::with_capacity(value_cols.len());
    for (col, sv) in value_cols {
        let v = crate::query::filter_convert::sql_value_to_value(sv)?;
        map.insert(col.clone(), v);
    }
    zerompk::to_msgpack_vec(&map).map_err(|e| LiteError::Serialization {
        detail: format!("encode KV value map: {e}"),
    })
}

// ── KvInsert ─────────────────────────────────────────────────────────────────

/// Lower `SqlPlan::KvInsert` → `KvOp::{Insert, InsertIfAbsent, Put, InsertOnConflictUpdate}`.
pub(super) fn lower_kv_insert<'a, S: StorageEngine + StorageEngineSync + 'a>(
    engine: &'a LiteQueryEngine<S>,
    collection: &str,
    entries: &[(SqlValue, Vec<(String, SqlValue)>)],
    ttl_secs: u64,
    intent: KvInsertIntent,
    on_conflict_updates: &[(String, nodedb_sql::types_expr::SqlExpr)],
) -> Result<LiteFut<'a>, LiteError> {
    if entries.is_empty() {
        return Ok(Box::pin(async move {
            Ok(nodedb_types::result::QueryResult {
                columns: vec![],
                rows: vec![],
                rows_affected: 0,
            })
        }));
    }

    let ttl_ms = ttl_secs * 1000;
    let collection = collection.to_string();

    // Pre-encode all entries so errors surface before the future is spawned.
    let mut ops: Vec<KvOp> = Vec::with_capacity(entries.len());

    for (key_val, value_cols) in entries {
        let key = sql_value_to_bytes(key_val);
        let value = encode_kv_value(value_cols)?;

        // Build per-entry update values for `ON CONFLICT DO UPDATE`.
        let updates: Vec<(
            String,
            nodedb_physical::physical_plan::document::UpdateValue,
        )> = if !on_conflict_updates.is_empty() {
            on_conflict_updates
                .iter()
                .map(|(col, _expr)| {
                    // For Lite, expressions on conflict updates are evaluated
                    // as the new value column for the same column name when
                    // available, or treated as a constant null otherwise.
                    // This mirrors the simple-update path in the physical visitor.
                    let new_val_bytes = value_cols
                        .iter()
                        .find(|(c, _)| c == col)
                        .map(|(_, sv)| sql_value_to_bytes(sv))
                        .unwrap_or_default();
                    (
                        col.clone(),
                        nodedb_physical::physical_plan::document::UpdateValue::Literal(
                            new_val_bytes,
                        ),
                    )
                })
                .collect()
        } else {
            Vec::new()
        };

        let op = match intent {
            KvInsertIntent::Insert => KvOp::Insert {
                collection: collection.clone(),
                key,
                value,
                ttl_ms,
                surrogate: Surrogate::ZERO,
            },
            KvInsertIntent::InsertIfAbsent => KvOp::InsertIfAbsent {
                collection: collection.clone(),
                key,
                value,
                ttl_ms,
                surrogate: Surrogate::ZERO,
            },
            KvInsertIntent::Put if !updates.is_empty() => KvOp::InsertOnConflictUpdate {
                collection: collection.clone(),
                key,
                value,
                ttl_ms,
                updates,
                surrogate: Surrogate::ZERO,
            },
            KvInsertIntent::Put => KvOp::Put {
                collection: collection.clone(),
                key,
                value,
                ttl_ms,
                surrogate: Surrogate::ZERO,
            },
        };
        ops.push(op);
    }

    // Execute all ops sequentially, accumulating rows_affected.
    Ok(Box::pin(async move {
        let mut total: u64 = 0;
        for op in ops {
            let mut phys = LiteDataPlaneVisitor { engine };
            let result = phys.kv(&op)?.await?;
            total += result.rows_affected;
        }
        Ok(nodedb_types::result::QueryResult {
            columns: vec![],
            rows: vec![],
            rows_affected: total,
        })
    }))
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::Mutex;

    use nodedb_sql::types::KvInsertIntent;
    use nodedb_sql::types_expr::SqlValue;

    use crate::engine::array::engine::ArrayEngineState;
    use crate::engine::fts::FtsState;
    use crate::engine::spatial::SpatialIndexManager;
    use crate::engine::vector::VectorState;
    use crate::query::engine::LiteQueryEngine;
    use crate::storage::redb_storage::RedbStorage;

    fn make_engine() -> LiteQueryEngine<RedbStorage> {
        let storage = Arc::new(RedbStorage::open_in_memory().expect("in-memory redb"));
        let crdt = Arc::new(Mutex::new(
            crate::engine::crdt::CrdtEngine::new(1).expect("crdt"),
        ));
        let strict = Arc::new(crate::engine::strict::StrictEngine::new(Arc::clone(
            &storage,
        )));
        let columnar = Arc::new(crate::engine::columnar::ColumnarEngine::new(Arc::clone(
            &storage,
        )));
        let htap = Arc::new(crate::engine::htap::HtapBridge::new());
        let timeseries = Arc::new(Mutex::new(
            crate::engine::timeseries::engine::TimeseriesEngine::new(),
        ));
        let vector_state = Arc::new(VectorState::new(Arc::clone(&storage), 100));
        let array_state = Arc::new(Mutex::new(ArrayEngineState::open(&storage).expect("array")));
        let fts_state = Arc::new(FtsState::new());
        let spatial = Arc::new(Mutex::new(SpatialIndexManager::new()));
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
            spatial,
            Arc::new(Mutex::new(std::collections::HashMap::new())),
        )
    }

    #[tokio::test]
    async fn test_kv_insert_plain() {
        let engine = make_engine();
        let entries = vec![(
            SqlValue::String("key1".to_string()),
            vec![("value".to_string(), SqlValue::String("hello".to_string()))],
        )];
        let fut = super::lower_kv_insert(&engine, "mykv", &entries, 0, KvInsertIntent::Put, &[])
            .expect("lower");
        let r = fut.await.expect("execute");
        assert_eq!(r.rows_affected, 1);
    }

    #[tokio::test]
    async fn test_kv_insert_duplicate_raises() {
        let engine = make_engine();
        let entries = vec![(
            SqlValue::String("dup_key".to_string()),
            vec![("value".to_string(), SqlValue::Int(42))],
        )];
        super::lower_kv_insert(&engine, "mykv2", &entries, 0, KvInsertIntent::Put, &[])
            .unwrap()
            .await
            .unwrap();
        // Second INSERT (not PUT) on same key should error.
        let err =
            super::lower_kv_insert(&engine, "mykv2", &entries, 0, KvInsertIntent::Insert, &[])
                .unwrap()
                .await;
        assert!(err.is_err());
    }

    #[tokio::test]
    async fn test_kv_insert_if_absent_no_op() {
        let engine = make_engine();
        let entries = vec![(
            SqlValue::String("absent_key".to_string()),
            vec![("value".to_string(), SqlValue::Int(1))],
        )];
        super::lower_kv_insert(&engine, "mykv3", &entries, 0, KvInsertIntent::Put, &[])
            .unwrap()
            .await
            .unwrap();
        // InsertIfAbsent should succeed silently (0 rows affected).
        let r = super::lower_kv_insert(
            &engine,
            "mykv3",
            &entries,
            0,
            KvInsertIntent::InsertIfAbsent,
            &[],
        )
        .unwrap()
        .await
        .unwrap();
        assert_eq!(r.rows_affected, 0);
    }

    #[tokio::test]
    async fn test_kv_insert_multi_column_value() {
        let engine = make_engine();
        let entries = vec![(
            SqlValue::String("mkey".to_string()),
            vec![
                ("field_a".to_string(), SqlValue::Int(10)),
                ("field_b".to_string(), SqlValue::String("foo".to_string())),
            ],
        )];
        let fut = super::lower_kv_insert(&engine, "mykv4", &entries, 0, KvInsertIntent::Put, &[])
            .expect("lower");
        let r = fut.await.expect("execute");
        assert_eq!(r.rows_affected, 1);
    }
}
