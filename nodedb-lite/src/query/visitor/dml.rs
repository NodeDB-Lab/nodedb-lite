// SPDX-License-Identifier: Apache-2.0
//! SQL-visitor lowering for DML SqlPlan variants: InsertSelect, UpdateFrom, Merge.

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};

use nodedb_physical::physical_plan::document::merge_types::{
    MergeActionOp, MergeClauseKind, MergeClauseOp,
};
use nodedb_sql::types::SqlPlan;
use nodedb_sql::types::filter::Filter;
use nodedb_sql::types::plan::{MergeClauseKind as SqlMergeKind, MergePlanAction, MergePlanClause};
use nodedb_sql::types::query::EngineType;
use nodedb_sql::types_expr::SqlExpr;
use nodedb_types::result::QueryResult;
use nodedb_types::value::Value;

use crate::error::LiteError;
use crate::query::document_ops::is_strict;
use crate::query::document_ops::sets::{
    build_insert_map, collect_ids_pub, fetch_document_value_pub,
};
use crate::query::document_ops::writes::{point_delete, point_insert, point_update};
use crate::query::engine::LiteQueryEngine;
use crate::query::expr_convert::convert_sql_expr;
use crate::query::filter_convert::sql_value_to_value;
use crate::query::value_utils::value_to_string;
use crate::storage::engine::StorageEngine;

use super::adapter::LiteFut;
use super::scan_post::row_to_typed_value;

type UpdateValue = nodedb_physical::physical_plan::document::types::UpdateValue;

/// Convert a `SqlExpr` to `UpdateValue` for use in point_update.
fn expr_to_update_value(expr: &SqlExpr) -> Result<UpdateValue, LiteError> {
    match expr {
        SqlExpr::Literal(v) => {
            let ndb_val = sql_value_to_value(v)?;
            let bytes =
                zerompk::to_msgpack_vec(&ndb_val).map_err(|e| LiteError::Serialization {
                    detail: format!("encode update literal: {e}"),
                })?;
            Ok(UpdateValue::Literal(bytes))
        }
        other => {
            let q_expr = crate::query::expr_convert::convert_sql_expr(other)?;
            Ok(UpdateValue::Expr(q_expr))
        }
    }
}

/// Convert `Vec<(String, SqlExpr)>` assignments to `Vec<(String, UpdateValue)>`.
pub(super) fn convert_assignments(
    assignments: &[(String, SqlExpr)],
) -> Result<Vec<(String, UpdateValue)>, LiteError> {
    assignments
        .iter()
        .map(|(col, expr)| Ok((col.clone(), expr_to_update_value(expr)?)))
        .collect()
}

/// Serialize a row map to msgpack bytes for `point_insert`.
fn row_to_msgpack(row: &HashMap<String, Value>) -> Result<Vec<u8>, LiteError> {
    zerompk::to_msgpack_vec(row).map_err(|e| LiteError::Serialization {
        detail: format!("encode row msgpack: {e}"),
    })
}

/// Process-wide counter used to guarantee uniqueness within the same millisecond.
static GEN_ID_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Extract the "id" column value from a row map, or generate a synthetic key.
///
/// The fallback id combines the current millisecond timestamp with a
/// process-wide monotonic counter so two inserts in the same millisecond
/// produce distinct keys. `crate::runtime::now_millis()` is used instead of
/// `SystemTime::now()` because the latter panics on wasm32.
fn extract_id(row: &HashMap<String, Value>) -> String {
    row.get("id").map(value_to_string).unwrap_or_else(|| {
        let ms = crate::runtime::now_millis();
        let seq = GEN_ID_COUNTER.fetch_add(1, Ordering::Relaxed);
        format!("gen-{ms:x}-{seq:x}")
    })
}

/// Convert `QueryResult` rows to `Vec<HashMap<String, Value>>`.
fn result_to_maps(result: QueryResult) -> Vec<HashMap<String, Value>> {
    let cols = result.columns;
    result
        .rows
        .into_iter()
        .map(|row| cols.iter().cloned().zip(row).collect())
        .collect()
}

/// Resolve `UpdateValue::Expr` column references against a source row.
///
/// Column expressions of the form `table.col` or `col` are resolved
/// against the source row map; others are left as `UpdateValue::Expr`.
fn resolve_updates_with_source(
    updates: &[(String, UpdateValue)],
    source_row: &HashMap<String, Value>,
) -> Result<Vec<(String, UpdateValue)>, LiteError> {
    updates
        .iter()
        .map(|(col, uv)| {
            let resolved = match uv {
                UpdateValue::Literal(_) => uv.clone(),
                UpdateValue::Expr(expr) => {
                    if let nodedb_query::expr::types::SqlExpr::Column(name) = expr {
                        let field = name.rsplit('.').next().unwrap_or(name.as_str());
                        if let Some(val) = source_row.get(field) {
                            let bytes = zerompk::to_msgpack_vec(val).map_err(|e| {
                                LiteError::Serialization {
                                    detail: format!("resolve source col '{field}': {e}"),
                                }
                            })?;
                            return Ok((col.clone(), UpdateValue::Literal(bytes)));
                        }
                    }
                    uv.clone()
                }
            };
            Ok((col.clone(), resolved))
        })
        .collect()
}

// ── InsertSelect ─────────────────────────────────────────────────────────────

/// `INSERT INTO target [(cols)] SELECT ... FROM source`.
///
/// Executes the source plan and inserts one target row per source row.
/// `column_map` is the target list: one `(target column, source
/// expression)` per declared column, evaluated against the source row.
/// Empty means copy each source row unchanged. Routing (strict vs
/// schemaless CRDT) is detected via `is_strict`.
pub(super) fn lower_insert_select<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    target: &str,
    source: &SqlPlan,
    limit: usize,
    column_map: &[(String, SqlExpr)],
) -> Result<LiteFut<'a>, LiteError> {
    let target = target.to_string();
    let source = source.clone();
    let effective_limit = if limit == 0 { usize::MAX } else { limit };

    // A declared PRIMARY KEY implies NOT NULL. A literal `SELECT NULL` into
    // the pk column is knowable before any row is scanned.
    let declared_pk = declared_primary_key(engine, &target);
    if let Some(pk) = &declared_pk
        && column_map.iter().any(|(field, expr)| {
            field == pk && matches!(expr, SqlExpr::Literal(nodedb_sql::types::SqlValue::Null))
        })
    {
        return Err(LiteError::NotNullViolation {
            collection: target,
            column: pk.clone(),
        });
    }
    let column_map: Vec<(String, nodedb_query::expr::types::SqlExpr)> = column_map
        .iter()
        .map(|(field, expr)| Ok((field.clone(), convert_sql_expr(expr)?)))
        .collect::<Result<_, LiteError>>()?;

    Ok(Box::pin(async move {
        let source_result = engine.execute_plan(&source).await?;
        let cols = source_result.columns.clone();
        let mut maps: Vec<HashMap<String, Value>> = Vec::new();
        for row in source_result.rows.into_iter().take(effective_limit) {
            if column_map.is_empty() {
                maps.push(cols.iter().cloned().zip(row).collect());
                continue;
            }
            let doc = row_to_typed_value(&cols, &row);
            let mut shaped = HashMap::with_capacity(column_map.len());
            for (field, expr) in &column_map {
                let value = expr.eval(&doc)?;
                if matches!(value, Value::Null) && declared_pk.as_deref() == Some(field.as_str()) {
                    return Err(LiteError::NotNullViolation {
                        collection: target.clone(),
                        column: field.clone(),
                    });
                }
                shaped.insert(field.clone(), value);
            }
            maps.push(shaped);
        }

        let mut affected: u64 = 0;

        if is_strict(engine, &target) {
            // Strict path: convert Value rows to SqlValue rows and call strict_dml.
            use crate::query::strict_dml;
            use nodedb_sql::types::SqlValue;

            let sql_rows: Vec<Vec<(String, SqlValue)>> = maps
                .into_iter()
                .map(|row| {
                    row.into_iter()
                        .map(|(col, val)| (col, value_to_sql_value(val)))
                        .collect()
                })
                .collect();

            let res = strict_dml::insert_strict(&engine.strict, &target, &sql_rows, false).await?;
            affected = res.rows_affected;
        } else {
            // Schemaless CRDT path.
            for row_map in maps {
                let doc_id = extract_id(&row_map);
                let value_bytes = row_to_msgpack(&row_map)?;
                point_insert(engine, &target, &doc_id, &value_bytes, false).await?;
                affected += 1;
            }
        }

        Ok(QueryResult {
            columns: Vec::new(),
            rows: Vec::new(),
            rows_affected: affected,
        })
    }))
}

/// The declared primary-key column of a strict target, or `None` when the
/// target is schemaless (its `id` key is implicit, not declared).
fn declared_primary_key<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    collection: &str,
) -> Option<String> {
    engine
        .strict
        .schema(collection)?
        .columns
        .iter()
        .find(|c| c.primary_key)
        .map(|c| c.name.clone())
}

/// Convert a `nodedb_types::Value` to the nearest `SqlValue` equivalent.
fn value_to_sql_value(v: Value) -> nodedb_sql::types::SqlValue {
    use nodedb_sql::types::SqlValue;
    match v {
        Value::String(s) => SqlValue::String(s),
        Value::Integer(i) => SqlValue::Int(i),
        Value::Float(f) => SqlValue::Float(f),
        Value::Bool(b) => SqlValue::Bool(b),
        Value::Null => SqlValue::Null,
        _ => SqlValue::Null,
    }
}

// ── UpdateFrom ───────────────────────────────────────────────────────────────

/// `UPDATE target SET ... FROM source WHERE target.col = source.col`.
#[allow(clippy::too_many_arguments)]
pub(super) fn lower_update_from<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    collection: &str,
    _engine_type: EngineType,
    source: &SqlPlan,
    target_join_col: &str,
    source_join_col: &str,
    assignments: &[(String, SqlExpr)],
    _target_filters: &[Filter],
    _returning: bool,
) -> Result<LiteFut<'a>, LiteError> {
    let target = collection.to_string();
    let source = source.clone();
    let t_join = target_join_col.to_string();
    let s_join = source_join_col.to_string();
    let updates = convert_assignments(assignments)?;

    Ok(Box::pin(async move {
        let source_result = engine.execute_plan(&source).await?;
        let source_maps = result_to_maps(source_result);

        let mut source_index: HashMap<String, HashMap<String, Value>> = HashMap::new();
        for row in source_maps {
            if let Some(key_val) = row.get(&s_join) {
                source_index.insert(value_to_string(key_val), row);
            }
        }

        let target_ids = collect_ids_pub(engine, &target).await?;
        let mut affected: u64 = 0;

        for doc_id in &target_ids {
            let target_val = fetch_document_value_pub(engine, &target, doc_id).await?;
            let join_key = match target_val.get(&t_join).map(value_to_string) {
                Some(k) => k,
                None => continue,
            };

            let source_val = match source_index.get(&join_key) {
                Some(v) => v.clone(),
                None => continue,
            };

            let resolved = resolve_updates_with_source(&updates, &source_val)?;
            point_update(engine, &target, doc_id, &resolved).await?;
            affected += 1;
        }

        Ok(QueryResult {
            columns: Vec::new(),
            rows: Vec::new(),
            rows_affected: affected,
        })
    }))
}

// ── Merge ────────────────────────────────────────────────────────────────────

/// `MERGE INTO target USING source ON ... WHEN ...`
#[allow(clippy::too_many_arguments)]
pub(super) fn lower_merge<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    target: &str,
    _engine_type: EngineType,
    source: &SqlPlan,
    target_join_col: &str,
    source_join_col: &str,
    _source_alias: &str,
    clauses: &[MergePlanClause],
    _returning: bool,
) -> Result<LiteFut<'a>, LiteError> {
    let target = target.to_string();
    let source = source.clone();
    let t_join = target_join_col.to_string();
    let s_join = source_join_col.to_string();
    let phys_clauses = convert_merge_clauses(clauses)?;

    Ok(Box::pin(async move {
        let source_result = engine.execute_plan(&source).await?;
        let source_maps = result_to_maps(source_result);

        let mut source_index: HashMap<String, HashMap<String, Value>> = HashMap::new();
        for row in &source_maps {
            if let Some(key_val) = row.get(&s_join) {
                source_index.insert(value_to_string(key_val), row.clone());
            }
        }

        let target_ids = collect_ids_pub(engine, &target).await?;
        let mut matched_source_keys: HashSet<String> = HashSet::new();
        let mut affected: u64 = 0;

        for doc_id in &target_ids {
            let target_val = fetch_document_value_pub(engine, &target, doc_id).await?;
            let join_key = match target_val.get(&t_join).map(value_to_string) {
                Some(k) => k,
                None => continue,
            };

            if let Some(source_row) = source_index.get(&join_key) {
                matched_source_keys.insert(join_key.clone());
                let arm = phys_clauses
                    .iter()
                    .find(|c| c.kind == MergeClauseKind::Matched);
                if let Some(arm) = arm {
                    apply_merge_action(engine, &target, doc_id, &arm.action, source_row).await?;
                    affected += 1;
                }
            } else {
                let arm = phys_clauses
                    .iter()
                    .find(|c| c.kind == MergeClauseKind::NotMatchedBySource);
                if let Some(arm) = arm {
                    // No source row for this target — NOT MATCHED BY SOURCE arms
                    // are UPDATE/DELETE only, so an empty source suffices.
                    apply_merge_action(engine, &target, doc_id, &arm.action, &HashMap::new())
                        .await?;
                    affected += 1;
                }
            }
        }

        // Unmatched source rows → WHEN NOT MATCHED.
        let not_matched_arm = phys_clauses
            .iter()
            .find(|c| c.kind == MergeClauseKind::NotMatched);
        if let Some(arm) = not_matched_arm {
            for (source_key, source_row) in &source_index {
                if !matched_source_keys.contains(source_key) {
                    let doc_id = extract_id(source_row);
                    apply_merge_action(engine, &target, &doc_id, &arm.action, source_row).await?;
                    affected += 1;
                }
            }
        }

        Ok(QueryResult {
            columns: Vec::new(),
            rows: Vec::new(),
            rows_affected: affected,
        })
    }))
}

async fn apply_merge_action<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    target: &str,
    doc_id: &str,
    action: &MergeActionOp,
    source_row: &HashMap<String, Value>,
) -> Result<(), LiteError> {
    match action {
        MergeActionOp::Update { updates } => {
            point_update(engine, target, doc_id, updates).await?;
        }
        MergeActionOp::Delete => {
            point_delete(engine, target, doc_id).await?;
        }
        MergeActionOp::Insert { columns, values } => {
            // Evaluate each value against the source row: literals decode
            // directly, expressions (`s.new_embedding`, `s.qty * 2`) evaluate
            // against the bare-keyed source fields. Result keyed by target column.
            let row_map = build_insert_map(columns, values, source_row)?;
            let id = extract_id(&row_map);
            let bytes = row_to_msgpack(&row_map)?;
            point_insert(engine, target, &id, &bytes, true).await?;
        }
        MergeActionOp::DoNothing => {}
    }
    Ok(())
}

fn convert_merge_clauses(clauses: &[MergePlanClause]) -> Result<Vec<MergeClauseOp>, LiteError> {
    clauses.iter().map(convert_one_clause).collect()
}

fn convert_one_clause(clause: &MergePlanClause) -> Result<MergeClauseOp, LiteError> {
    let kind = match clause.kind {
        SqlMergeKind::Matched => MergeClauseKind::Matched,
        SqlMergeKind::NotMatched => MergeClauseKind::NotMatched,
        SqlMergeKind::NotMatchedBySource => MergeClauseKind::NotMatchedBySource,
    };
    let action = convert_merge_action(&clause.action)?;
    Ok(MergeClauseOp {
        kind,
        extra_predicate: Vec::new(),
        action,
    })
}

fn convert_merge_action(action: &MergePlanAction) -> Result<MergeActionOp, LiteError> {
    match action {
        MergePlanAction::Update { assignments } => {
            let updates = convert_assignments(assignments)?;
            Ok(MergeActionOp::Update { updates })
        }
        MergePlanAction::Delete => Ok(MergeActionOp::Delete),
        MergePlanAction::Insert { columns, values } => {
            // Literal values are pre-encoded; source-referencing expressions
            // (`s.new_embedding`, `s.qty * 2`) are carried as `UpdateValue::Expr`
            // and evaluated against the source row at apply time.
            let encoded = values
                .iter()
                .map(expr_to_update_value)
                .collect::<Result<Vec<_>, LiteError>>()?;
            Ok(MergeActionOp::Insert {
                columns: columns.clone(),
                values: encoded,
            })
        }
        MergePlanAction::DoNothing => Ok(MergeActionOp::DoNothing),
    }
}

#[cfg(test)]
mod tests {
    use nodedb_sql::types::SqlValue;
    use nodedb_sql::types_expr::BinaryOp;
    use nodedb_types::columnar::{ColumnDef, ColumnType, StrictSchema};

    use super::*;
    use crate::query::engine::test_engine;

    fn schema() -> StrictSchema {
        StrictSchema {
            columns: vec![
                ColumnDef::required("id", ColumnType::String).with_primary_key(),
                ColumnDef::nullable("n", ColumnType::Int64),
            ],
            version: 1,
            dropped_columns: Vec::new(),
            bitemporal: false,
        }
    }

    fn scan(collection: &str) -> SqlPlan {
        SqlPlan::Scan {
            collection: collection.to_string(),
            alias: None,
            engine: EngineType::DocumentStrict,
            filters: Vec::new(),
            projection: Vec::new(),
            sort_keys: Vec::new(),
            limit: None,
            offset: 0,
            distinct: false,
            window_functions: Vec::new(),
            temporal: nodedb_sql::temporal::TemporalScope::default(),
        }
    }

    fn col(name: &str) -> SqlExpr {
        SqlExpr::Column {
            table: None,
            name: name.to_string(),
        }
    }

    async fn seed(engine: &LiteQueryEngine<crate::PagedbStorageMem>) {
        engine
            .strict
            .create_collection("src", schema())
            .await
            .expect("src");
        engine
            .strict
            .create_collection("dst", schema())
            .await
            .expect("dst");
        engine
            .strict
            .insert("src", &[Value::String("a".into()), Value::Integer(5)])
            .await
            .expect("row");
    }

    #[tokio::test]
    async fn insert_select_column_map_shapes_each_target_row() {
        let engine = test_engine().await;
        seed(&engine).await;
        let column_map = vec![
            (
                "id".to_string(),
                SqlExpr::BinaryOp {
                    left: Box::new(col("id")),
                    op: BinaryOp::Concat,
                    right: Box::new(SqlExpr::Literal(SqlValue::String("-copy".into()))),
                },
            ),
            (
                "n".to_string(),
                SqlExpr::BinaryOp {
                    left: Box::new(col("n")),
                    op: BinaryOp::Mul,
                    right: Box::new(SqlExpr::Literal(SqlValue::Int(2))),
                },
            ),
        ];
        let r = lower_insert_select(&engine, "dst", &scan("src"), 0, &column_map)
            .expect("lower")
            .await
            .expect("run");
        assert_eq!(r.rows_affected, 1);
        let row = engine
            .strict
            .get("dst", &Value::String("a-copy".into()))
            .await
            .expect("get")
            .expect("row present");
        assert_eq!(row[1], Value::Integer(10));
    }

    #[tokio::test]
    async fn insert_select_literal_null_into_pk_is_rejected_before_scanning() {
        let engine = test_engine().await;
        seed(&engine).await;
        let column_map = vec![
            ("id".to_string(), SqlExpr::Literal(SqlValue::Null)),
            ("n".to_string(), col("n")),
        ];
        let err = match lower_insert_select(&engine, "dst", &scan("src"), 0, &column_map) {
            Ok(_) => panic!("a literal NULL into the pk must be rejected"),
            Err(e) => e,
        };
        assert!(matches!(err, LiteError::NotNullViolation { .. }), "{err}");
    }

    #[tokio::test]
    async fn insert_select_empty_map_copies_rows_unchanged() {
        let engine = test_engine().await;
        seed(&engine).await;
        let r = lower_insert_select(&engine, "dst", &scan("src"), 0, &[])
            .expect("lower")
            .await
            .expect("run");
        assert_eq!(r.rows_affected, 1);
        let row = engine
            .strict
            .get("dst", &Value::String("a".into()))
            .await
            .expect("get")
            .expect("row present");
        assert_eq!(row[1], Value::Integer(5));
    }
}
