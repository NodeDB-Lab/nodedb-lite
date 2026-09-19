// SPDX-License-Identifier: Apache-2.0
//! Columnar write operations: insert (by intent), update, delete.

use nodedb_physical::physical_plan::columnar::ColumnarInsertIntent;
use nodedb_query::scan_filter::ScanFilter;
use nodedb_types::columnar::ColumnarSchema;
use nodedb_types::result::QueryResult;
use nodedb_types::value::Value;

use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::storage::engine::StorageEngine;

use super::super::reads::row_to_object;
use super::payload::decode_payload;
use super::rows::{apply_conflict_updates, find_row};

/// Parameters for a columnar insert operation.
pub struct InsertParams<'a> {
    pub payload: &'a [u8],
    pub format: &'a str,
    pub intent: ColumnarInsertIntent,
    pub on_conflict_updates: &'a [(
        String,
        nodedb_physical::physical_plan::document::types::UpdateValue,
    )],
    pub surrogates: &'a [nodedb_types::Surrogate],
    pub schema_bytes: &'a [u8],
}

/// Insert rows into a columnar collection.
///
/// Decodes the payload per `format` ("json", "msgpack", "ilp"), respects
/// `intent` (Insert / InsertIfAbsent / InsertUnique / Put), and assigns surrogates from the
/// provided list falling back to 0 when the list is shorter than the row count.
///
/// Returns the `QueryResult` together with the column-ordered rows that were
/// actually written to the memtable. The caller is responsible for calling
/// `engine.columnar.enqueue_outbound` from an async context to durably queue
/// those rows for replication to Origin.
pub async fn insert<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    collection: &str,
    params: InsertParams<'_>,
) -> Result<(QueryResult, Vec<Vec<Value>>), LiteError> {
    let InsertParams {
        payload,
        format,
        intent,
        on_conflict_updates,
        surrogates,
        schema_bytes,
    } = params;
    let schema = engine
        .columnar
        .schema(collection)
        .ok_or(LiteError::BadRequest {
            detail: format!("columnar collection '{collection}' does not exist"),
        })?;

    // If caller supplied a schema override, decode it and use it for column ordering.
    let effective_schema: ColumnarSchema = if !schema_bytes.is_empty() {
        zerompk::from_msgpack(schema_bytes).unwrap_or(schema)
    } else {
        schema
    };

    let col_names: Vec<String> = effective_schema
        .columns
        .iter()
        .map(|c| c.name.clone())
        .collect();
    // The declared primary-key column, or column 0 when none is declared.
    let pk_idx = effective_schema
        .columns
        .iter()
        .position(|c| c.primary_key)
        .unwrap_or(0);

    let rows = decode_payload(payload, format, &col_names)?;

    let mut affected: u64 = 0;
    let mut inserted_rows: Vec<Vec<Value>> = Vec::new();

    for (row_idx, row_values) in rows.into_iter().enumerate() {
        let _surrogate = surrogates
            .get(row_idx)
            .copied()
            .unwrap_or(nodedb_types::Surrogate(0));

        match intent {
            ColumnarInsertIntent::Insert => {
                engine.columnar.insert(collection, &row_values)?;
                inserted_rows.push(row_values);
                affected += 1;
            }
            ColumnarInsertIntent::InsertIfAbsent => {
                if let Some(pk) = row_values.get(pk_idx)
                    && find_row(engine, collection, pk).await?.is_some()
                {
                    continue;
                }
                engine.columnar.insert(collection, &row_values)?;
                inserted_rows.push(row_values);
                affected += 1;
            }
            ColumnarInsertIntent::InsertUnique => {
                // A declared natural-key PRIMARY KEY: a duplicate refuses the
                // row instead of tombstoning the prior one.
                if let Some(pk) = row_values.get(pk_idx)
                    && find_row(engine, collection, pk).await?.is_some()
                {
                    return Err(LiteError::UniqueViolation {
                        collection: collection.to_string(),
                        detail: format!(
                            "{} = {pk:?}",
                            col_names.get(pk_idx).map(String::as_str).unwrap_or("pk")
                        ),
                    });
                }
                engine.columnar.insert(collection, &row_values)?;
                inserted_rows.push(row_values);
                affected += 1;
            }
            ColumnarInsertIntent::Put => {
                if on_conflict_updates.is_empty() {
                    // Plain upsert: delete-then-insert (whole-row overwrite).
                    if let Some(pk) = row_values.get(pk_idx) {
                        let _ = engine.columnar.delete(collection, pk);
                    }
                    engine.columnar.insert(collection, &row_values)?;
                    inserted_rows.push(row_values);
                    affected += 1;
                } else {
                    // Merge: read existing row, apply conflict updates, write merged.
                    let existing = match row_values.get(pk_idx) {
                        Some(pk) => find_row(engine, collection, pk).await?,
                        None => None,
                    };
                    let merged = match existing {
                        Some(existing) => {
                            let incoming_obj = row_to_object(&col_names, &row_values);
                            apply_conflict_updates(
                                existing,
                                &incoming_obj,
                                on_conflict_updates,
                                &col_names,
                            )?
                        }
                        None => row_values.clone(),
                    };
                    if let Some(pk) = merged.get(pk_idx) {
                        let _ = engine.columnar.delete(collection, pk);
                    }
                    engine.columnar.insert(collection, &merged)?;
                    inserted_rows.push(merged);
                    affected += 1;
                }
            }
        }
    }

    Ok((
        QueryResult {
            columns: Vec::new(),
            rows: Vec::new(),
            rows_affected: affected,
        },
        inserted_rows,
    ))
}

/// Update rows matching filter predicates.
pub async fn update<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    collection: &str,
    filters_bytes: &[u8],
    updates: &[(String, Vec<u8>)],
) -> Result<QueryResult, LiteError> {
    let schema = engine
        .columnar
        .schema(collection)
        .ok_or(LiteError::BadRequest {
            detail: format!("columnar collection '{collection}' does not exist"),
        })?;

    let col_names: Vec<String> = schema.columns.iter().map(|c| c.name.clone()).collect();
    let pk_idx = schema
        .columns
        .iter()
        .position(|c| c.primary_key)
        .unwrap_or(0);

    let filters: Vec<ScanFilter> = if filters_bytes.is_empty() {
        Vec::new()
    } else {
        zerompk::from_msgpack(filters_bytes).map_err(|e| LiteError::Serialization {
            detail: format!("decode update filters: {e}"),
        })?
    };

    // Parse update value bytes (each value is msgpack-encoded).
    let parsed_updates: Vec<(String, Value)> = updates
        .iter()
        .map(|(field, bytes)| {
            let v: Value = zerompk::from_msgpack(bytes).unwrap_or(Value::Null);
            (field.clone(), v)
        })
        .collect();

    // Read current rows, apply filters, build modified rows.
    // collect PKs and new rows first, then mutate (borrow separation).
    let all_rows = engine.columnar.list_rows(collection).await?;

    let mut affected: u64 = 0;

    for row in all_rows {
        let doc = row_to_object(&col_names, &row);
        let matches = ScanFilter::all_match_value(&filters, &doc)?;
        if !matches {
            continue;
        }

        let pk = row.get(pk_idx).cloned().unwrap_or(Value::Null);

        // Build new_values: copy current row then apply updates.
        let mut new_values = row.clone();
        for (field, new_val) in &parsed_updates {
            if let Some(col_idx) = col_names.iter().position(|n| n == field)
                && col_idx < new_values.len()
            {
                new_values[col_idx] = new_val.clone();
            }
        }

        engine.columnar.update(collection, &pk, &new_values)?;
        affected += 1;
    }

    Ok(QueryResult {
        columns: Vec::new(),
        rows: Vec::new(),
        rows_affected: affected,
    })
}

/// Delete rows matching filter predicates.
pub async fn delete<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    collection: &str,
    filters_bytes: &[u8],
) -> Result<QueryResult, LiteError> {
    let schema = engine
        .columnar
        .schema(collection)
        .ok_or(LiteError::BadRequest {
            detail: format!("columnar collection '{collection}' does not exist"),
        })?;

    let col_names: Vec<String> = schema.columns.iter().map(|c| c.name.clone()).collect();
    let pk_idx = schema
        .columns
        .iter()
        .position(|c| c.primary_key)
        .unwrap_or(0);

    let filters: Vec<ScanFilter> = if filters_bytes.is_empty() {
        Vec::new()
    } else {
        zerompk::from_msgpack(filters_bytes).map_err(|e| LiteError::Serialization {
            detail: format!("decode delete filters: {e}"),
        })?
    };

    let all_rows = engine.columnar.list_rows(collection).await?;

    let mut pks_to_delete: Vec<Value> = Vec::new();
    for row in all_rows {
        let doc = row_to_object(&col_names, &row);
        let matches = filters.is_empty() || ScanFilter::all_match_value(&filters, &doc)?;
        if matches {
            pks_to_delete.push(row.get(pk_idx).cloned().unwrap_or(Value::Null));
        }
    }

    let mut affected: u64 = 0;
    for pk in pks_to_delete {
        if engine.columnar.delete(collection, &pk)? {
            affected += 1;
        }
    }

    Ok(QueryResult {
        columns: Vec::new(),
        rows: Vec::new(),
        rows_affected: affected,
    })
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use nodedb_types::columnar::{ColumnDef, ColumnType, ColumnarProfile};

    use super::*;
    use crate::query::engine::test_engine;

    async fn seed(engine: &LiteQueryEngine<crate::PagedbStorageMem>) {
        let schema = ColumnarSchema::new(vec![
            ColumnDef::required("sku", ColumnType::String).with_primary_key(),
            ColumnDef::nullable("qty", ColumnType::Int64),
        ])
        .expect("schema");
        engine
            .columnar
            .create_collection("items", schema, ColumnarProfile::Plain, false)
            .await
            .expect("create");
    }

    fn payload(sku: &str, qty: i64) -> Vec<u8> {
        let mut row = HashMap::new();
        row.insert("sku".to_string(), Value::String(sku.into()));
        row.insert("qty".to_string(), Value::Integer(qty));
        zerompk::to_msgpack_vec(&Value::Object(row)).expect("encode")
    }

    async fn run(
        engine: &LiteQueryEngine<crate::PagedbStorageMem>,
        intent: ColumnarInsertIntent,
        bytes: &[u8],
    ) -> Result<u64, LiteError> {
        insert(
            engine,
            "items",
            InsertParams {
                payload: bytes,
                format: "msgpack",
                intent,
                on_conflict_updates: &[],
                surrogates: &[],
                schema_bytes: &[],
            },
        )
        .await
        .map(|(r, _)| r.rows_affected)
    }

    #[tokio::test]
    async fn insert_unique_duplicate_pk_is_a_unique_violation() {
        let engine = test_engine().await;
        seed(&engine).await;
        let first = run(
            &engine,
            ColumnarInsertIntent::InsertUnique,
            &payload("a", 1),
        )
        .await
        .expect("first insert");
        assert_eq!(first, 1);
        let err = run(
            &engine,
            ColumnarInsertIntent::InsertUnique,
            &payload("a", 2),
        )
        .await
        .expect_err("duplicate");
        assert!(matches!(err, LiteError::UniqueViolation { .. }), "{err}");
        let rows = engine.columnar.list_rows("items").await.expect("rows");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][1], Value::Integer(1), "the first row is kept");
    }

    #[tokio::test]
    async fn insert_if_absent_duplicate_pk_is_skipped() {
        let engine = test_engine().await;
        seed(&engine).await;
        run(&engine, ColumnarInsertIntent::Insert, &payload("a", 1))
            .await
            .expect("first insert");
        let n = run(
            &engine,
            ColumnarInsertIntent::InsertIfAbsent,
            &payload("a", 2),
        )
        .await
        .expect("if absent");
        assert_eq!(n, 0);
    }
}
