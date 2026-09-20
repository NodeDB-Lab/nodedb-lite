// SPDX-License-Identifier: Apache-2.0
//! SQL-visitor lowering for KV SqlPlan variants: KvInsert.

use nodedb_physical::PhysicalTaskVisitor;
use nodedb_physical::physical_plan::KvOp;
use nodedb_physical::physical_plan::document::UpdateValue;
use nodedb_sql::types::KvInsertIntent;
use nodedb_sql::types_expr::{SqlExpr, SqlValue};
use nodedb_types::Surrogate;

use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::query::expr_convert::convert_sql_expr;
use crate::query::filter_convert::sql_value_to_value;
use crate::query::physical_visitor::LiteDataPlaneVisitor;
use crate::storage::engine::StorageEngine;

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

/// Convert one `ON CONFLICT DO UPDATE SET col = <expr>` assignment.
///
/// Mirrors Origin's `assignments_to_update_values`: a literal RHS
/// pre-encodes to msgpack (`UpdateValue::Literal`); anything else (a
/// column reference, arithmetic, `EXCLUDED.col`, a function call, ...)
/// converts to a query-side expression (`UpdateValue::Expr`) that
/// `kv_ops::writes::basic::kv_insert_on_conflict_update` evaluates via
/// `query::on_conflict::apply_patch`.
fn assignment_to_update_value(expr: &SqlExpr) -> Result<UpdateValue, LiteError> {
    match expr {
        SqlExpr::Literal(v) => {
            let value = sql_value_to_value(v)?;
            let bytes = zerompk::to_msgpack_vec(&value).map_err(|e| LiteError::Serialization {
                detail: format!("encode ON CONFLICT literal: {e}"),
            })?;
            Ok(UpdateValue::Literal(bytes))
        }
        other => Ok(UpdateValue::Expr(convert_sql_expr(other)?)),
    }
}

// ── KvInsert ─────────────────────────────────────────────────────────────────

/// Lower `SqlPlan::KvInsert` → `KvOp::{Insert, InsertIfAbsent, Put, InsertOnConflictUpdate}`.
pub(super) fn lower_kv_insert<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    collection: &str,
    entries: &[(SqlValue, Vec<(String, SqlValue)>)],
    ttl_secs: u64,
    intent: KvInsertIntent,
    on_conflict_updates: &[(String, SqlExpr)],
) -> Result<LiteFut<'a>, LiteError> {
    if entries.is_empty() {
        let verb = match intent {
            KvInsertIntent::Insert | KvInsertIntent::InsertIfAbsent => "INSERT",
            KvInsertIntent::Put if !on_conflict_updates.is_empty() => "INSERT",
            KvInsertIntent::Put => "UPSERT",
        };
        return Ok(Box::pin(async move {
            Ok(nodedb_types::result::QueryResult {
                columns: vec![],
                rows: vec![],
                rows_affected: 0,
                command: Some(verb.into()),
            })
        }));
    }

    let ttl_ms = ttl_secs * 1000;
    // Lite holds a bare collection name; DatabaseId::DEFAULT keeps it unqualified.
    let collection =
        nodedb_types::QualifiedCollection::new(nodedb_types::DatabaseId::DEFAULT, collection);

    // Convert `ON CONFLICT DO UPDATE SET` assignments once for the whole
    // statement, mirroring Origin's `assignments_to_update_values`: a
    // literal RHS pre-encodes to msgpack, anything else carries the
    // expression for `kv_insert_on_conflict_update` to evaluate against
    // the existing row (`col`) and the incoming row (`EXCLUDED.col`).
    let updates: Vec<(String, UpdateValue)> = on_conflict_updates
        .iter()
        .map(|(col, expr)| Ok((col.clone(), assignment_to_update_value(expr)?)))
        .collect::<Result<Vec<_>, LiteError>>()?;

    // Pre-encode all entries so errors surface before the future is spawned.
    let mut ops: Vec<KvOp> = Vec::with_capacity(entries.len());

    for (key_val, value_cols) in entries {
        let key = sql_value_to_bytes(key_val);
        let value = encode_kv_value(value_cols)?;
        let updates = updates.clone();

        let op = match intent {
            KvInsertIntent::Insert => KvOp::Insert {
                collection: collection.clone(),
                key,
                value,
                ttl_ms,
                surrogate: Surrogate::ZERO,
                returning: None,
                rls_filters: Vec::new(),
            },
            KvInsertIntent::InsertIfAbsent => KvOp::InsertIfAbsent {
                collection: collection.clone(),
                key,
                value,
                ttl_ms,
                surrogate: Surrogate::ZERO,
                returning: None,
                rls_filters: Vec::new(),
            },
            KvInsertIntent::Put if !updates.is_empty() => KvOp::InsertOnConflictUpdate {
                collection: collection.clone(),
                key,
                value,
                ttl_ms,
                updates,
                surrogate: Surrogate::ZERO,
                // Lite has no RLS policy engine: no write policy applies.
                rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
                returning: None,
                rls_filters: Vec::new(),
            },
            KvInsertIntent::Put => KvOp::Put {
                collection: collection.clone(),
                key,
                value,
                ttl_ms,
                surrogate: Surrogate::ZERO,
                returning: None,
                rls_filters: Vec::new(),
            },
        };
        ops.push(op);
    }

    // Execute all ops sequentially, accumulating rows_affected. Fold each
    // op's reported verb the same way Postgres folds a multi-row
    // `INSERT ... ON CONFLICT DO UPDATE`: `INSERT`/`UPDATE` mix collapses to
    // `INSERT`, any other verb is uniform across every op in one statement.
    Ok(Box::pin(async move {
        let mut total: u64 = 0;
        let mut verb: Option<&'static str> = None;
        for op in ops {
            let mut phys = LiteDataPlaneVisitor { engine };
            let result = phys.kv(&op)?.await?;
            total += result.rows_affected;
            if let Some(op_verb) = result.command.as_deref() {
                verb = Some(match (verb, op_verb) {
                    (None, "UPDATE") => "UPDATE",
                    (None, "UPSERT") => "UPSERT",
                    (None, _) => "INSERT",
                    (Some("INSERT"), "UPDATE") | (Some("UPDATE"), "INSERT") => "INSERT",
                    (Some(prev), _) => prev,
                });
            }
        }
        Ok(nodedb_types::result::QueryResult {
            columns: vec![],
            rows: vec![],
            rows_affected: total,
            command: Some(verb.unwrap_or("INSERT").into()),
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

    use crate::PagedbStorageMem;
    use crate::engine::array::engine::ArrayEngineState;
    use crate::engine::fts::FtsState;
    use crate::engine::spatial::SpatialIndexManager;
    use crate::engine::vector::VectorState;
    use crate::query::engine::{LiteQueryEngine, LiteQueryEngineParams};

    async fn make_engine() -> LiteQueryEngine<PagedbStorageMem> {
        let storage = Arc::new(
            PagedbStorageMem::open_in_memory()
                .await
                .expect("in-memory pagedb"),
        );
        let crdt = Arc::new(Mutex::new(
            crate::engine::crdt::CrdtEngine::new(1).expect("crdt"),
        ));
        let governor = crate::query::engine::test_governor();
        let strict = Arc::new(crate::engine::strict::StrictEngine::new(Arc::clone(
            &storage,
        )));
        let columnar = Arc::new(crate::engine::columnar::ColumnarEngine::new(
            Arc::clone(&storage),
            crate::query::engine::test_scoped_memory(&governor, nodedb_mem::EngineId::Columnar),
        ));
        let htap = Arc::new(crate::engine::htap::HtapBridge::new());
        let timeseries = Arc::new(Mutex::new(
            crate::engine::timeseries::engine::TimeseriesEngine::new(),
        ));
        let vector_state = Arc::new(VectorState::new(
            Arc::clone(&storage),
            100,
            crate::query::engine::test_scoped_memory(&governor, nodedb_mem::EngineId::Vector),
        ));
        let array_state = Arc::new(tokio::sync::Mutex::new(
            ArrayEngineState::open(&storage).await.expect("array"),
        ));
        let fts_state = Arc::new(FtsState::new(Arc::clone(&governor)));
        let spatial = Arc::new(Mutex::new(SpatialIndexManager::new(
            crate::query::engine::test_scoped_memory(&governor, nodedb_mem::EngineId::Spatial),
        )));
        LiteQueryEngine::new(LiteQueryEngineParams {
            crdt,
            strict,
            columnar,
            htap,
            storage,
            timeseries,
            vector_state,
            array_state,
            fts_state,
            sparse_state: Arc::new(crate::engine::sparse_vector::SparseVectorState::new()),
            spatial,
            csr: Arc::new(Mutex::new(std::collections::HashMap::new())),
            governor,
            kv_local: crate::query::engine::test_kv_local(),
        })
    }

    #[tokio::test]
    async fn test_kv_insert_plain() {
        let engine = make_engine().await;
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
        let engine = make_engine().await;
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
        let engine = make_engine().await;
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
        let engine = make_engine().await;
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

    /// `ON CONFLICT DO UPDATE SET n = n + 1` evaluates against the existing
    /// row, not the incoming one. End-to-end through
    /// `lower_kv_insert` → `KvOp::InsertOnConflictUpdate` → the KV engine.
    #[tokio::test]
    async fn test_kv_insert_on_conflict_expr_evaluates_against_existing_row() {
        use nodedb_sql::types_expr::{BinaryOp, SqlExpr};

        let engine = make_engine().await;
        let seed = vec![(
            SqlValue::String("k".to_string()),
            vec![("n".to_string(), SqlValue::Int(1))],
        )];
        super::lower_kv_insert(&engine, "mykv5", &seed, 0, KvInsertIntent::Put, &[])
            .expect("lower seed")
            .await
            .expect("seed");

        let entries = vec![(
            SqlValue::String("k".to_string()),
            vec![("n".to_string(), SqlValue::Int(99))],
        )];
        let on_conflict = vec![(
            "n".to_string(),
            SqlExpr::BinaryOp {
                left: Box::new(SqlExpr::Column {
                    table: None,
                    name: "n".to_string(),
                }),
                op: BinaryOp::Add,
                right: Box::new(SqlExpr::Literal(SqlValue::Int(1))),
            },
        )];
        let r = super::lower_kv_insert(
            &engine,
            "mykv5",
            &entries,
            0,
            KvInsertIntent::Put,
            &on_conflict,
        )
        .expect("lower")
        .await
        .expect("execute");
        assert_eq!(r.rows_affected, 1);

        let stored = crate::query::kv_ops::reads::kv_get(&engine, "mykv5", b"k", None)
            .await
            .expect("get");
        let nodedb_types::value::Value::Bytes(bytes) = &stored.rows[0][1] else {
            panic!("value column is not bytes");
        };
        let map: std::collections::HashMap<String, nodedb_types::value::Value> =
            zerompk::from_msgpack(bytes).expect("decode row");
        // `n + 1` against the existing row (1), not the incoming row (99).
        assert_eq!(map.get("n"), Some(&nodedb_types::value::Value::Integer(2)));
    }
}
