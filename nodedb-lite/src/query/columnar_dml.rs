//! Columnar-engine DML dispatch for the Lite query layer.
//!
//! INSERT for columnar collections converts SQL values to `nodedb_types::Value`
//! in schema column order, then delegates to `ColumnarEngine::insert`.
//! UPDATE and DELETE target the rows the WHERE names by key, or else every
//! row it matches, and keep the text index in step.

use std::collections::HashMap;
use std::sync::Arc;

use nodedb_sql::types::filter::Filter;
use nodedb_sql::types::{SqlExpr, SqlValue};
use nodedb_types::columnar::ColumnarSchema;
use nodedb_types::result::QueryResult;
use nodedb_types::value::Value;

use crate::engine::columnar::ColumnarEngine;
use crate::error::LiteError;
use crate::query::assignments::{
    coerce_to_columns, convert_typed_assignments, has_expr, resolve_updates, row_object,
};
use crate::storage::engine::StorageEngine;

use super::coerce::{build_row, coerce_sql_value};
use super::engine::LiteQueryEngine;
use super::text_index::{index_columnar_rows, remove_columnar_rows};
use super::visitor::scan_post::filter_mask;

/// Insert rows into a columnar collection.
///
/// Each `row` is a list of `(column_name, SqlValue)` pairs. Values are
/// coerced to match the schema column type and ordered by schema position.
/// Returns the query result plus the column-ordered rows actually written, so
/// the async caller can durably enqueue them for outbound sync (the durable
/// enqueue can't run inside this sync path).
pub fn insert_columnar<S: StorageEngine>(
    columnar: &Arc<ColumnarEngine<S>>,
    collection: &str,
    rows: &[Vec<(String, SqlValue)>],
) -> Result<(QueryResult, Vec<Vec<nodedb_types::Value>>), LiteError> {
    let schema = columnar
        .schema(collection)
        .ok_or_else(|| LiteError::collection_not_found("columnar", collection))?;

    let mut affected: u64 = 0;
    let mut written: Vec<Vec<nodedb_types::Value>> = Vec::with_capacity(rows.len());
    for row_pairs in rows {
        let values = build_row(row_pairs, &schema.columns)?;
        columnar
            .insert(collection, &values)
            .map_err(|e| LiteError::BadRequest {
                detail: format!("columnar insert: {e}"),
            })?;
        written.push(values);
        affected += 1;
    }

    Ok((
        QueryResult {
            columns: Vec::new(),
            rows: Vec::new(),
            rows_affected: affected,
            command: Some("INSERT".into()),
        },
        written,
    ))
}

fn columnar_schema<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    collection: &str,
) -> Result<(ColumnarSchema, usize), LiteError> {
    let schema = engine
        .columnar
        .schema(collection)
        .ok_or_else(|| LiteError::collection_not_found("columnar", collection))?;
    let pk_idx = schema
        .columns
        .iter()
        .position(|c| c.primary_key)
        .ok_or_else(|| LiteError::BadRequest {
            detail: format!("columnar collection '{collection}' has no primary key column"),
        })?;
    Ok((schema, pk_idx))
}

/// The rows of a columnar collection a statement targets: the rows whose
/// key the WHERE names, or else every row the WHERE matches.
async fn target_rows<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    collection: &str,
    schema: &ColumnarSchema,
    pk_idx: usize,
    filters: &[Filter],
    target_keys: &[SqlValue],
) -> Result<Vec<Vec<Value>>, LiteError> {
    let rows = engine.columnar.list_rows(collection).await?;
    if !target_keys.is_empty() {
        let pk_type = &schema.columns[pk_idx].column_type;
        let wanted = target_keys
            .iter()
            .map(|k| coerce_sql_value(k, pk_type))
            .collect::<Result<Vec<Value>, _>>()?;
        return Ok(rows
            .into_iter()
            .filter(|row| row.get(pk_idx).is_some_and(|pk| wanted.contains(pk)))
            .collect());
    }
    let keep = filter_mask(
        &QueryResult {
            columns: schema.columns.iter().map(|c| c.name.clone()).collect(),
            rows: rows.clone(),
            rows_affected: 0,
            command: None,
        },
        filters,
    )?;
    Ok(rows
        .into_iter()
        .zip(keep)
        .filter_map(|(row, keep)| keep.then_some(row))
        .collect())
}

/// Update the targeted rows of a columnar collection.
///
/// A literal coerces to its column's type once. Any other right-hand side
/// evaluates against each row's pre-image, so one assignment never sees
/// another. Every post-image is built before the first row changes, so an
/// assignment that fails leaves every row unchanged.
pub async fn update_columnar<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    collection: &str,
    assignments: &[(String, SqlExpr)],
    filters: &[Filter],
    target_keys: &[SqlValue],
) -> Result<QueryResult, LiteError> {
    let (schema, pk_idx) = columnar_schema(engine, collection)?;
    for (field, _) in assignments {
        if !schema.columns.iter().any(|c| c.name == *field) {
            return Err(LiteError::BadRequest {
                detail: format!("columnar collection '{collection}' has no column '{field}'"),
            });
        }
    }
    let updates = convert_typed_assignments(assignments, &schema.columns)?;
    let shared = if has_expr(&updates) {
        None
    } else {
        let resolved = resolve_updates(&updates, &Value::Object(HashMap::new()))?;
        Some(coerce_to_columns(resolved, &schema.columns)?)
    };

    let mut planned: Vec<(Value, Vec<Value>)> = Vec::new();
    for row in target_rows(engine, collection, &schema, pk_idx, filters, target_keys).await? {
        let Some(pk) = row.get(pk_idx).cloned() else {
            continue;
        };
        let resolved = match &shared {
            Some(resolved) => resolved.clone(),
            None => {
                let current = row_object(&schema.columns, &row);
                coerce_to_columns(resolve_updates(&updates, &current)?, &schema.columns)?
            }
        };
        let mut new_values = row;
        for (field, value) in resolved {
            if let Some(idx) = schema.columns.iter().position(|c| c.name == field)
                && let Some(slot) = new_values.get_mut(idx)
            {
                *slot = value;
            }
        }
        planned.push((pk, new_values));
    }

    let mut written: Vec<Vec<Value>> = Vec::with_capacity(planned.len());
    for (pk, new_values) in planned {
        if engine.columnar.update(collection, &pk, &new_values)? {
            written.push(new_values);
        }
    }
    index_columnar_rows(engine, collection, written.iter().map(Vec::as_slice))?;
    Ok(QueryResult {
        columns: Vec::new(),
        rows: Vec::new(),
        rows_affected: written.len() as u64,
        command: Some("UPDATE".into()),
    })
}

/// Delete the targeted rows of a columnar collection.
pub async fn delete_columnar<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    collection: &str,
    filters: &[Filter],
    target_keys: &[SqlValue],
) -> Result<QueryResult, LiteError> {
    let (schema, pk_idx) = columnar_schema(engine, collection)?;
    let mut removed: Vec<Value> = Vec::new();
    for row in target_rows(engine, collection, &schema, pk_idx, filters, target_keys).await? {
        let Some(pk) = row.get(pk_idx) else {
            continue;
        };
        if engine.columnar.delete(collection, pk)? {
            removed.push(pk.clone());
        }
    }
    remove_columnar_rows(engine, collection, &removed)?;
    Ok(QueryResult {
        columns: Vec::new(),
        rows: Vec::new(),
        rows_affected: removed.len() as u64,
        command: Some("DELETE".into()),
    })
}
