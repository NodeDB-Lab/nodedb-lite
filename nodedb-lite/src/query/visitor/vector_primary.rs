// SPDX-License-Identifier: Apache-2.0
//! SQL-visitor lowering for the vector-primary SqlPlan variants:
//! `VectorPrimaryInsert`, `VectorPrimaryDelete`, `VectorPrimaryUpdate`.
//!
//! Lite binds no surrogate to a primary key, so a row's identity is the
//! text of its declared key: the insert carries it as `pk_bytes`, and a
//! point `DELETE` / `UPDATE` targets it through a predicate on the key
//! column evaluated against the stored payload row.

use std::collections::HashMap;

use nodedb_physical::PhysicalTaskVisitor;
use nodedb_physical::physical_plan::{VectorOp, VectorWriteTargets};
use nodedb_sql::types::filter::{Filter, FilterExpr};
use nodedb_sql::types::plan::{VectorPrimaryInsertIntent, VectorPrimaryRow};
use nodedb_sql::types_expr::SqlValue;
use nodedb_sql::{
    VectorPrimaryDeleteVisitArgs, VectorPrimaryInsertVisitArgs, VectorPrimaryUpdateVisitArgs,
};
use nodedb_types::RlsWriteCheck;
use nodedb_types::result::QueryResult;
use nodedb_types::value::Value;

use crate::error::LiteError;
use crate::query::engine::{LiteQueryEngine, sql_value_to_string};
use crate::query::filter_convert::sql_value_to_value;
use crate::query::physical_visitor::LiteDataPlaneVisitor;
use crate::query::scan_filter_convert::encode_scan_filters;
use crate::storage::engine::StorageEngine;

use super::adapter::LiteFut;
use super::dml::convert_assignments;

/// The key column a vector-primary collection is keyed by; `id` when the
/// statement declares none.
fn key_column(primary_key: Option<&str>) -> &str {
    primary_key.unwrap_or("id")
}

/// Encode payload fields (non-vector columns) as MessagePack bytes.
fn encode_payload(payload_fields: &HashMap<String, SqlValue>) -> Result<Vec<u8>, LiteError> {
    if payload_fields.is_empty() {
        return Ok(Vec::new());
    }
    let value_map: HashMap<String, Value> = payload_fields
        .iter()
        .map(|(k, sv)| Ok((k.clone(), sql_value_to_value(sv)?)))
        .collect::<Result<_, LiteError>>()?;
    zerompk::to_msgpack_vec(&value_map).map_err(|e| LiteError::Serialization {
        detail: format!("encode vector primary payload: {e}"),
    })
}

/// The `(pk_bytes, payload)` of one insert. A row with no key value mints
/// one and carries it under the key column so a later point read finds it.
fn row_identity(row: &VectorPrimaryRow, key_column: &str) -> Result<(Vec<u8>, Vec<u8>), LiteError> {
    let mut fields = row.payload_fields.clone();
    let doc_id = match fields.get(key_column) {
        Some(v) if !matches!(v, SqlValue::Null) => sql_value_to_string(v),
        _ => {
            let minted = nodedb_types::id_gen::uuid_v7();
            fields.insert(key_column.to_string(), SqlValue::String(minted.clone()));
            minted
        }
    };
    Ok((doc_id.into_bytes(), encode_payload(&fields)?))
}

fn rows_affected(n: u64) -> QueryResult {
    QueryResult {
        columns: vec!["rows_affected".to_string()],
        rows: vec![vec![Value::Integer(n as i64)]],
        rows_affected: n,
    }
}

// ── VectorPrimaryInsert ───────────────────────────────────────────────────────

/// Lower `SqlPlan::VectorPrimaryInsert` to one direct write per row, picked
/// by `intent`: `DirectInsert`, `DirectInsertIfAbsent`, or `DirectUpsert`.
pub(super) fn lower_vector_primary_insert<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    args: VectorPrimaryInsertVisitArgs<'_>,
) -> Result<LiteFut<'a>, LiteError> {
    let VectorPrimaryInsertVisitArgs {
        collection,
        field,
        quantization,
        storage_dtype,
        payload_indexes,
        rows,
        intent,
        on_conflict_updates,
        primary_key,
    } = args;
    let key_column = key_column(primary_key);
    let on_conflict_updates = convert_assignments(on_conflict_updates)?;
    // Lite holds a bare collection name; DatabaseId::DEFAULT keeps it unqualified.
    let qualified =
        nodedb_types::QualifiedCollection::new(nodedb_types::DatabaseId::DEFAULT, collection);
    let field = field.to_string();
    let payload_indexes = payload_indexes.to_vec();

    let mut ops = Vec::with_capacity(rows.len());
    for row in rows {
        let (pk_bytes, payload) = row_identity(row, key_column)?;
        // Lite's planner produces no RLS program and no RETURNING projection;
        // the adapter rejects either if one ever appears.
        let op = match intent {
            VectorPrimaryInsertIntent::Insert => VectorOp::DirectInsert {
                collection: qualified.clone(),
                field: field.clone(),
                surrogate: row.surrogate,
                pk_bytes,
                vector: row.vector.clone(),
                payload,
                quantization,
                storage_dtype,
                payload_indexes: payload_indexes.clone(),
                returning: None,
                rls_filters: Vec::new(),
            },
            VectorPrimaryInsertIntent::InsertIfAbsent => VectorOp::DirectInsertIfAbsent {
                collection: qualified.clone(),
                field: field.clone(),
                surrogate: row.surrogate,
                pk_bytes,
                vector: row.vector.clone(),
                payload,
                quantization,
                storage_dtype,
                payload_indexes: payload_indexes.clone(),
                returning: None,
                rls_filters: Vec::new(),
            },
            VectorPrimaryInsertIntent::Upsert => VectorOp::DirectUpsert {
                collection: qualified.clone(),
                field: field.clone(),
                surrogate: row.surrogate,
                pk_bytes,
                vector: row.vector.clone(),
                payload,
                quantization,
                storage_dtype,
                payload_indexes: payload_indexes.clone(),
                returning: None,
                rls_filters: Vec::new(),
                on_conflict_updates: on_conflict_updates.clone(),
                rls_write_check: RlsWriteCheck::NoPolicyApplies,
            },
        };
        ops.push(op);
    }

    Ok(Box::pin(async move {
        let mut affected = 0u64;
        for op in ops {
            let mut phys = LiteDataPlaneVisitor { engine };
            let result = phys.vector(&op)?.await?;
            affected += result.rows_affected;
        }
        Ok(rows_affected(affected))
    }))
}

// ── VectorPrimaryDelete / VectorPrimaryUpdate ─────────────────────────────────

/// The rows a `DELETE` / `UPDATE` targets. Point keys become a predicate on
/// the key column, the only key binding Lite keeps; a statement with no
/// keys carries its WHERE clause.
fn write_targets(
    filters: &[Filter],
    target_keys: &[SqlValue],
    key_column: &str,
) -> Result<VectorWriteTargets, LiteError> {
    if target_keys.is_empty() {
        return Ok(VectorWriteTargets::Predicate(encode_scan_filters(filters)?));
    }
    let keys = Filter {
        expr: FilterExpr::InList {
            field: key_column.to_string(),
            values: target_keys.to_vec(),
        },
    };
    Ok(VectorWriteTargets::Predicate(encode_scan_filters(&[keys])?))
}

/// Lower `SqlPlan::VectorPrimaryDelete` to `VectorOp::DirectDelete`.
pub(super) fn lower_vector_primary_delete<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    args: VectorPrimaryDeleteVisitArgs<'_>,
) -> Result<LiteFut<'a>, LiteError> {
    let VectorPrimaryDeleteVisitArgs {
        collection,
        field,
        filters,
        target_keys,
        primary_key,
    } = args;
    let targets = write_targets(filters, target_keys, key_column(primary_key))?;
    let op = VectorOp::DirectDelete {
        collection: nodedb_types::QualifiedCollection::new(
            nodedb_types::DatabaseId::DEFAULT,
            collection,
        ),
        field: field.to_string(),
        targets,
        returning: None,
        rls_filters: Vec::new(),
        rls_write_check: RlsWriteCheck::NoPolicyApplies,
    };
    Ok(Box::pin(async move {
        let mut phys = LiteDataPlaneVisitor { engine };
        let result = phys.vector(&op)?.await?;
        Ok(rows_affected(result.rows_affected))
    }))
}

/// Lower `SqlPlan::VectorPrimaryUpdate` to `VectorOp::DirectUpdate`.
pub(super) fn lower_vector_primary_update<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    args: VectorPrimaryUpdateVisitArgs<'_>,
) -> Result<LiteFut<'a>, LiteError> {
    let VectorPrimaryUpdateVisitArgs {
        collection,
        field,
        quantization,
        storage_dtype,
        payload_indexes,
        new_vector,
        assignments,
        filters,
        target_keys,
        returning,
        primary_key,
    } = args;
    if returning {
        return Err(LiteError::Unsupported {
            detail: "VectorPrimaryUpdate: RETURNING is unsupported on the Lite engine".into(),
        });
    }
    let targets = write_targets(filters, target_keys, key_column(primary_key))?;
    let op = VectorOp::DirectUpdate {
        collection: nodedb_types::QualifiedCollection::new(
            nodedb_types::DatabaseId::DEFAULT,
            collection,
        ),
        field: field.to_string(),
        targets,
        new_vector: new_vector.map(<[f32]>::to_vec),
        payload_patch: convert_assignments(assignments)?,
        quantization,
        storage_dtype,
        payload_indexes: payload_indexes.to_vec(),
        returning: None,
        rls_filters: Vec::new(),
        rls_write_check: RlsWriteCheck::NoPolicyApplies,
    };
    Ok(Box::pin(async move {
        let mut phys = LiteDataPlaneVisitor { engine };
        let result = phys.vector(&op)?.await?;
        Ok(rows_affected(result.rows_affected))
    }))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use nodedb_sql::types::filter::{CompareOp, Filter, FilterExpr};
    use nodedb_sql::types::plan::VectorPrimaryRow;
    use nodedb_sql::types_expr::{SqlExpr, SqlValue};
    use nodedb_types::{Surrogate, VectorQuantization, VectorStorageDtype};

    use super::*;
    use crate::PagedbStorageMem;
    use crate::nodedb::LockExt;
    use crate::query::engine::test_engine;

    const COLLECTION: &str = "embeddings";
    const FIELD: &str = "vec";

    fn row(id: &str, vector: Vec<f32>, extra: &[(&str, SqlValue)]) -> VectorPrimaryRow {
        let mut payload_fields = HashMap::new();
        payload_fields.insert("id".to_string(), SqlValue::String(id.to_string()));
        for (k, v) in extra {
            payload_fields.insert((*k).to_string(), v.clone());
        }
        VectorPrimaryRow {
            surrogate: Surrogate::ZERO,
            vector,
            payload_fields,
        }
    }

    async fn insert(
        engine: &LiteQueryEngine<PagedbStorageMem>,
        rows: &[VectorPrimaryRow],
        intent: VectorPrimaryInsertIntent,
        on_conflict_updates: &[(String, SqlExpr)],
    ) -> Result<QueryResult, LiteError> {
        lower_vector_primary_insert(
            engine,
            VectorPrimaryInsertVisitArgs {
                collection: COLLECTION,
                field: FIELD,
                quantization: VectorQuantization::None,
                storage_dtype: VectorStorageDtype::F32,
                payload_indexes: &[],
                rows,
                intent,
                on_conflict_updates,
                primary_key: Some("id"),
            },
        )?
        .await
    }

    async fn delete(
        engine: &LiteQueryEngine<PagedbStorageMem>,
        filters: &[Filter],
        target_keys: &[SqlValue],
    ) -> Result<QueryResult, LiteError> {
        lower_vector_primary_delete(
            engine,
            VectorPrimaryDeleteVisitArgs {
                collection: COLLECTION,
                field: FIELD,
                filters,
                target_keys,
                primary_key: Some("id"),
            },
        )?
        .await
    }

    async fn update(
        engine: &LiteQueryEngine<PagedbStorageMem>,
        new_vector: Option<&[f32]>,
        assignments: &[(String, SqlExpr)],
        target_keys: &[SqlValue],
    ) -> Result<QueryResult, LiteError> {
        lower_vector_primary_update(
            engine,
            VectorPrimaryUpdateVisitArgs {
                collection: COLLECTION,
                field: FIELD,
                quantization: VectorQuantization::None,
                storage_dtype: VectorStorageDtype::F32,
                payload_indexes: &[],
                new_vector,
                assignments,
                filters: &[],
                target_keys,
                returning: false,
                primary_key: Some("id"),
            },
        )?
        .await
    }

    fn live_nodes(engine: &LiteQueryEngine<PagedbStorageMem>) -> usize {
        let indices = engine.vector_state.hnsw_indices.lock_or_recover();
        indices
            .get(&format!("{COLLECTION}:{FIELD}"))
            .map(|idx| idx.live_count())
            .unwrap_or(0)
    }

    fn stored_field(
        engine: &LiteQueryEngine<PagedbStorageMem>,
        id: &str,
        field: &str,
    ) -> Option<Value> {
        let crdt = engine.crdt.lock_or_recover();
        let value = crdt.read(COLLECTION, id)?;
        crate::nodedb::convert::loro_value_to_document(id, &value)
            .fields
            .remove(field)
    }

    fn key(id: &str) -> SqlValue {
        SqlValue::String(id.to_string())
    }

    #[tokio::test]
    async fn insert_stores_payload_on_the_row() {
        let engine = test_engine().await;
        let rows = vec![row(
            "a",
            vec![0.1, 0.2],
            &[("tier", SqlValue::String("gold".into()))],
        )];
        let qr = insert(&engine, &rows, VectorPrimaryInsertIntent::Insert, &[])
            .await
            .expect("insert");
        assert_eq!(qr.rows_affected, 1);
        assert_eq!(
            stored_field(&engine, "a", "tier"),
            Some(Value::String("gold".into()))
        );
        assert_eq!(
            stored_field(&engine, "a", "id"),
            Some(Value::String("a".into()))
        );
        assert_eq!(live_nodes(&engine), 1);
    }

    #[tokio::test]
    async fn multiple_rows_get_one_node_each() {
        let engine = test_engine().await;
        let rows: Vec<VectorPrimaryRow> = (1..=3)
            .map(|i| row(&format!("r{i}"), vec![i as f32 * 0.1, i as f32 * 0.2], &[]))
            .collect();
        let qr = insert(&engine, &rows, VectorPrimaryInsertIntent::Insert, &[])
            .await
            .expect("insert");
        assert_eq!(qr.rows_affected, 3);
        assert_eq!(live_nodes(&engine), 3);
    }

    #[tokio::test]
    async fn upsert_on_existing_key_leaves_one_live_node() {
        let engine = test_engine().await;
        let first = vec![row("a", vec![0.1, 0.2], &[("n", SqlValue::Int(1))])];
        insert(&engine, &first, VectorPrimaryInsertIntent::Upsert, &[])
            .await
            .expect("first");
        let second = vec![row("a", vec![0.9, 0.8], &[("n", SqlValue::Int(2))])];
        let qr = insert(&engine, &second, VectorPrimaryInsertIntent::Upsert, &[])
            .await
            .expect("second");
        assert_eq!(qr.rows_affected, 1);
        assert_eq!(live_nodes(&engine), 1, "the old node is tombstoned");
        assert_eq!(stored_field(&engine, "a", "n"), Some(Value::Integer(2)));
    }

    #[tokio::test]
    async fn upsert_with_conflict_updates_patches_the_stored_row() {
        let engine = test_engine().await;
        let first = vec![row(
            "a",
            vec![0.1, 0.2],
            &[
                ("n", SqlValue::Int(1)),
                ("keep", SqlValue::String("x".into())),
            ],
        )];
        insert(&engine, &first, VectorPrimaryInsertIntent::Upsert, &[])
            .await
            .expect("first");
        let patch = vec![("n".to_string(), SqlExpr::Literal(SqlValue::Int(7)))];
        let second = vec![row("a", vec![0.3, 0.4], &[("n", SqlValue::Int(2))])];
        insert(&engine, &second, VectorPrimaryInsertIntent::Upsert, &patch)
            .await
            .expect("second");
        assert_eq!(stored_field(&engine, "a", "n"), Some(Value::Integer(7)));
        assert_eq!(
            stored_field(&engine, "a", "keep"),
            Some(Value::String("x".into())),
            "a patch keeps the columns it does not name"
        );
        assert_eq!(live_nodes(&engine), 1);
    }

    #[tokio::test]
    async fn direct_insert_on_existing_key_is_a_unique_violation() {
        let engine = test_engine().await;
        let rows = vec![row("a", vec![0.1, 0.2], &[])];
        insert(&engine, &rows, VectorPrimaryInsertIntent::Insert, &[])
            .await
            .expect("first");
        let err = insert(&engine, &rows, VectorPrimaryInsertIntent::Insert, &[])
            .await
            .expect_err("duplicate");
        assert!(matches!(err, LiteError::UniqueViolation { .. }), "{err}");
        assert_eq!(live_nodes(&engine), 1);
    }

    #[tokio::test]
    async fn insert_if_absent_on_existing_key_reports_zero() {
        let engine = test_engine().await;
        let rows = vec![row("a", vec![0.1, 0.2], &[("n", SqlValue::Int(1))])];
        insert(&engine, &rows, VectorPrimaryInsertIntent::Insert, &[])
            .await
            .expect("first");
        let again = vec![row("a", vec![0.5, 0.5], &[("n", SqlValue::Int(2))])];
        let qr = insert(
            &engine,
            &again,
            VectorPrimaryInsertIntent::InsertIfAbsent,
            &[],
        )
        .await
        .expect("if absent");
        assert_eq!(qr.rows_affected, 0);
        assert_eq!(stored_field(&engine, "a", "n"), Some(Value::Integer(1)));
        assert_eq!(live_nodes(&engine), 1);
    }

    #[tokio::test]
    async fn delete_by_key_counts_only_rows_that_existed() {
        let engine = test_engine().await;
        let rows = vec![row("a", vec![0.1, 0.2], &[])];
        insert(&engine, &rows, VectorPrimaryInsertIntent::Insert, &[])
            .await
            .expect("insert");
        let first = delete(&engine, &[], &[key("a")]).await.expect("delete");
        assert_eq!(first.rows_affected, 1);
        assert_eq!(live_nodes(&engine), 0);
        assert!(stored_field(&engine, "a", "id").is_none());
        let second = delete(&engine, &[], &[key("a")])
            .await
            .expect("delete again");
        assert_eq!(second.rows_affected, 0);
    }

    #[tokio::test]
    async fn delete_by_predicate_removes_matching_rows() {
        let engine = test_engine().await;
        let rows = vec![
            row(
                "a",
                vec![0.1, 0.2],
                &[("tier", SqlValue::String("gold".into()))],
            ),
            row(
                "b",
                vec![0.3, 0.4],
                &[("tier", SqlValue::String("free".into()))],
            ),
        ];
        insert(&engine, &rows, VectorPrimaryInsertIntent::Insert, &[])
            .await
            .expect("insert");
        let filters = vec![Filter {
            expr: FilterExpr::Comparison {
                field: "tier".into(),
                op: CompareOp::Eq,
                value: SqlValue::String("free".into()),
            },
        }];
        let qr = delete(&engine, &filters, &[]).await.expect("delete");
        assert_eq!(qr.rows_affected, 1);
        assert!(stored_field(&engine, "b", "id").is_none());
        assert!(stored_field(&engine, "a", "id").is_some());
        assert_eq!(live_nodes(&engine), 1);
    }

    #[tokio::test]
    async fn update_re_embed_keeps_one_node_and_patches_payload() {
        let engine = test_engine().await;
        let rows = vec![row("a", vec![0.1, 0.2], &[("n", SqlValue::Int(1))])];
        insert(&engine, &rows, VectorPrimaryInsertIntent::Insert, &[])
            .await
            .expect("insert");
        let patch = vec![("n".to_string(), SqlExpr::Literal(SqlValue::Int(5)))];
        let qr = update(&engine, Some(&[0.7, 0.7]), &patch, &[key("a")])
            .await
            .expect("update");
        assert_eq!(qr.rows_affected, 1);
        assert_eq!(live_nodes(&engine), 1);
        assert_eq!(stored_field(&engine, "a", "n"), Some(Value::Integer(5)));
        let missing = update(&engine, None, &patch, &[key("zzz")])
            .await
            .expect("update missing");
        assert_eq!(missing.rows_affected, 0);
    }
}
