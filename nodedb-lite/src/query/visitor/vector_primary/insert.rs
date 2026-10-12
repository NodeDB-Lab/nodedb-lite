// SPDX-License-Identifier: Apache-2.0
use nodedb_physical::PhysicalTaskVisitor;
use nodedb_physical::physical_plan::VectorOp;
use nodedb_sql::VectorPrimaryInsertVisitArgs;
use nodedb_sql::types::plan::VectorPrimaryInsertIntent;
use nodedb_types::{RlsWriteCheck, Surrogate};

use super::identity::{key_column, row_identity, rows_affected};
use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::query::physical_visitor::LiteDataPlaneVisitor;
use crate::query::visitor::adapter::LiteFut;
use crate::query::visitor::dml::convert_assignments;
use crate::storage::engine::StorageEngine;

/// Lower `SqlPlan::VectorPrimaryInsert` to one direct write per row, picked
/// by `intent`: `DirectInsert`, `DirectInsertIfAbsent`, or `DirectUpsert`.
pub(in crate::query::visitor) fn lower_vector_primary_insert<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    permit: &'a crate::engine::fts::coordinator::TextMutationPermit,
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
        // Lite binds no surrogate to a primary key. Identity is the pk bytes.
        let op = match intent {
            VectorPrimaryInsertIntent::Insert => VectorOp::DirectInsert {
                collection: qualified.clone(),
                field: field.clone(),
                surrogate: Surrogate::ZERO,
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
                surrogate: Surrogate::ZERO,
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
                surrogate: Surrogate::ZERO,
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
        let mut verb: Option<&'static str> = None;
        for op in ops {
            let mut phys = LiteDataPlaneVisitor {
                engine,
                permit: Some(permit),
            };
            let result = phys.vector(&op)?.await?;
            affected += result.rows_affected;
            // `UPSERT` (no ON CONFLICT UPDATE arm) is the same verb on every
            // row; `INSERT`/`UPDATE` (per-row ON CONFLICT outcome) fold to
            // `INSERT` on a mix, matching Postgres's `INSERT 0 n` tag.
            if let Some(op_verb) = result.command.as_deref() {
                verb = Some(match (verb, op_verb) {
                    (None, v) => static_verb(v),
                    (Some("INSERT"), "UPDATE") | (Some("UPDATE"), "INSERT") => "INSERT",
                    (Some(prev), _) => prev,
                });
            }
        }
        Ok(rows_affected(affected, verb.unwrap_or("INSERT")))
    }))
}

/// `result.command` is a heap `String`; the fold only ever compares it
/// against a handful of static verb literals, so this maps to a `'static`
/// copy instead of cloning the string each row.
fn static_verb(v: &str) -> &'static str {
    match v {
        "UPSERT" => "UPSERT",
        "UPDATE" => "UPDATE",
        _ => "INSERT",
    }
}

#[cfg(test)]
mod tests {
    use super::super::identity::support::*;
    use super::*;
    use crate::query::engine::test_engine;
    use nodedb_sql::types::plan::VectorPrimaryRow;
    use nodedb_sql::types_expr::SqlExpr;
    use nodedb_sql::types_expr::SqlValue;
    use nodedb_types::value::Value;
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
}
