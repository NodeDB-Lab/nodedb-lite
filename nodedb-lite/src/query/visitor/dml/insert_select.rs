// SPDX-License-Identifier: Apache-2.0

//! `INSERT ... SELECT` lowering.

use std::collections::HashMap;

use nodedb_sql::types::SqlPlan;
use nodedb_sql::types_expr::SqlExpr;
use nodedb_types::result::QueryResult;
use nodedb_types::value::Value;

use crate::error::LiteError;
use crate::query::document_ops::is_strict;
use crate::query::document_ops::writes::point_insert_admitted;
use crate::query::engine::LiteQueryEngine;
use crate::storage::engine::StorageEngine;

use crate::query::visitor::adapter::LiteFut;

use super::rows::{declared_primary_key, extract_id, row_to_msgpack, value_to_sql_value};

/// `INSERT INTO target [(cols)] SELECT ... FROM source`.
///
/// Executes the source plan and inserts one target row per output row. The
/// source plan carries its own LIMIT. `source_columns` pairs each target
/// column with the source output column it takes. Empty means copy each
/// output row unchanged. `column_map` binds the same target columns as
/// expressions over a stored source row. Lite reads every source as output
/// rows, so `column_map` serves only the plan-time NULL primary-key check.
/// Routing (strict vs schemaless CRDT) is detected via `is_strict`.
pub(in crate::query::visitor) fn lower_insert_select<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    permit: &'a crate::engine::fts::coordinator::TextMutationPermit,
    target: &str,
    source: &SqlPlan,
    column_map: &[(String, SqlExpr)],
    source_columns: &[(String, String)],
) -> Result<LiteFut<'a>, LiteError> {
    let target = target.to_string();
    let source = source.clone();

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
    let source_columns = source_columns.to_vec();

    Ok(Box::pin(async move {
        let source_result = engine.execute_plan_admitted(&source, Some(permit)).await?;
        let cols = source_result.columns;
        // The output position each target column reads.
        let positions: Vec<(String, usize)> = source_columns
            .iter()
            .map(|(field, output)| {
                cols.iter()
                    .position(|c| c == output)
                    .map(|i| (field.clone(), i))
                    .ok_or_else(|| {
                        LiteError::Query(format!(
                            "INSERT ... SELECT source has no output column '{output}' for \
                             target column '{field}'"
                        ))
                    })
            })
            .collect::<Result<_, LiteError>>()?;
        let mut maps: Vec<HashMap<String, Value>> = Vec::with_capacity(source_result.rows.len());
        for row in source_result.rows {
            if positions.is_empty() {
                maps.push(cols.iter().cloned().zip(row).collect());
                continue;
            }
            let mut shaped = HashMap::with_capacity(positions.len());
            for (field, index) in &positions {
                let value = row.get(*index).cloned().ok_or_else(|| {
                    LiteError::Query(format!(
                        "INSERT ... SELECT source row has {} values for {} output columns",
                        row.len(),
                        cols.len()
                    ))
                })?;
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

            let res = strict_dml::insert_strict_admitted(engine, permit, &target, &sql_rows, false)
                .await?;
            affected = res.rows_affected;
        } else {
            // Schemaless CRDT path.
            for row_map in maps {
                let doc_id = extract_id(&row_map);
                let value_bytes = row_to_msgpack(row_map)?;
                point_insert_admitted(engine, permit, &target, &doc_id, &value_bytes, false)
                    .await?;
                affected += 1;
            }
        }

        Ok(QueryResult {
            columns: Vec::new(),
            rows: Vec::new(),
            rows_affected: affected,
            command: Some("INSERT".into()),
        })
    }))
}

#[cfg(test)]
mod tests {
    use nodedb_sql::types::SqlValue;
    use nodedb_types::columnar::{ColumnDef, ColumnType, StrictSchema};

    use nodedb_sql::types::SqlPlan;
    use nodedb_sql::types::query::EngineType;
    use nodedb_sql::types_expr::SqlExpr;
    use nodedb_types::value::Value;

    use crate::error::LiteError;
    use crate::query::engine::LiteQueryEngine;

    use super::lower_insert_select;
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
    async fn insert_select_source_columns_pair_targets_with_outputs() {
        let engine = test_engine().await;
        seed(&engine).await;
        engine
            .strict
            .create_collection(
                "renamed",
                StrictSchema {
                    columns: vec![
                        ColumnDef::required("k", ColumnType::String).with_primary_key(),
                        ColumnDef::nullable("m", ColumnType::Int64),
                    ],
                    version: 1,
                    dropped_columns: Vec::new(),
                    bitemporal: false,
                },
            )
            .await
            .expect("renamed");
        let guard = engine.fts_state.admit_mutation().await;
        let column_map = vec![("k".to_string(), col("id")), ("m".to_string(), col("n"))];
        let source_columns = vec![
            ("k".to_string(), "id".to_string()),
            ("m".to_string(), "n".to_string()),
        ];
        let r = lower_insert_select(
            &engine,
            guard.permit(),
            "renamed",
            &scan("src"),
            &column_map,
            &source_columns,
        )
        .expect("lower")
        .await;
        let r = guard.finish(r).expect("run");
        assert_eq!(r.rows_affected, 1);
        let row = engine
            .strict
            .get("renamed", &Value::String("a".into()))
            .await
            .expect("get")
            .expect("row present");
        assert_eq!(row[1], Value::Integer(5));
    }

    #[tokio::test]
    async fn insert_select_missing_output_column_is_refused() {
        let engine = test_engine().await;
        seed(&engine).await;
        let guard = engine.fts_state.admit_mutation().await;
        let source_columns = vec![
            ("id".to_string(), "id".to_string()),
            ("n".to_string(), "absent".to_string()),
        ];
        let r = lower_insert_select(
            &engine,
            guard.permit(),
            "dst",
            &scan("src"),
            &[],
            &source_columns,
        )
        .expect("lower")
        .await;
        let err = guard
            .finish(r)
            .expect_err("an unknown output column is refused");
        assert!(matches!(err, LiteError::Query(_)), "{err}");
    }

    #[tokio::test]
    async fn insert_select_literal_null_into_pk_is_rejected_before_scanning() {
        let engine = test_engine().await;
        seed(&engine).await;
        let guard = engine.fts_state.admit_mutation().await;
        let column_map = vec![
            ("id".to_string(), SqlExpr::Literal(SqlValue::Null)),
            ("n".to_string(), col("n")),
        ];
        let source_columns = vec![
            ("id".to_string(), "__nodedb_insert_select_0".to_string()),
            ("n".to_string(), "n".to_string()),
        ];
        let err = match lower_insert_select(
            &engine,
            guard.permit(),
            "dst",
            &scan("src"),
            &column_map,
            &source_columns,
        ) {
            Ok(_) => panic!("a literal NULL into the pk must be rejected"),
            Err(e) => e,
        };
        let err = guard
            .finish::<(), _>(Err(err))
            .expect_err("refused mutation");
        assert!(matches!(err, LiteError::NotNullViolation { .. }), "{err}");
    }

    #[tokio::test]
    async fn insert_select_empty_map_copies_rows_unchanged() {
        let engine = test_engine().await;
        seed(&engine).await;
        let guard = engine.fts_state.admit_mutation().await;
        let r = lower_insert_select(&engine, guard.permit(), "dst", &scan("src"), &[], &[])
            .expect("lower")
            .await;
        let r = guard.finish(r).expect("run");
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
