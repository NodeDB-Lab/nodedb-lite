//! Strict-engine DML dispatch for the Lite query layer.
//!
//! INSERT, UPDATE, and DELETE for strict collections convert SQL values to
//! `nodedb_types::Value` according to the collection schema, then delegate
//! to `StrictEngine` which validates types and encodes as Binary Tuples.
//! Every write then brings the rows' text-index entries up to date.

use std::collections::HashMap;

use nodedb_sql::types::filter::Filter;
use nodedb_sql::types::{SqlExpr, SqlValue};
use nodedb_types::result::QueryResult;
use nodedb_types::value::Value;

use crate::error::LiteError;
use crate::query::assignments::{
    coerce_to_columns, convert_typed_assignments, has_expr, resolve_updates, row_object,
};
use crate::query::dml_targets::strict_targets;
use crate::query::engine::LiteQueryEngine;
use crate::query::text_index::{index_strict_rows, reindex_strict_rows};
use crate::storage::engine::StorageEngine;

use super::coerce::{build_row, sql_value_to_string};
use super::engine_read::parse_pk_value;

/// Insert rows into a strict collection.
///
/// Each `row` is a list of `(column_name, SqlValue)` pairs. Values are
/// coerced to match the schema column type.
pub async fn insert_strict<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    collection: &str,
    rows: &[Vec<(String, SqlValue)>],
    if_absent: bool,
) -> Result<QueryResult, LiteError> {
    insert_strict_coordinated(engine, None, collection, rows, if_absent).await
}

pub(crate) async fn insert_strict_coordinated<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    permit: Option<&crate::engine::fts::coordinator::TextMutationPermit>,
    collection: &str,
    rows: &[Vec<(String, SqlValue)>],
    if_absent: bool,
) -> Result<QueryResult, LiteError> {
    if let Some(permit) = permit {
        return insert_strict_admitted(engine, permit, collection, rows, if_absent).await;
    }
    let guard = engine.fts_state.admit_mutation().await;
    let result = insert_strict_admitted(engine, guard.permit(), collection, rows, if_absent).await;
    guard.finish(result)
}

pub(crate) async fn insert_strict_admitted<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    permit: &crate::engine::fts::coordinator::TextMutationPermit,
    collection: &str,
    rows: &[Vec<(String, SqlValue)>],
    if_absent: bool,
) -> Result<QueryResult, LiteError> {
    let _permit = permit;
    let strict = &engine.strict;
    let schema = strict
        .schema(collection)
        .ok_or_else(|| LiteError::collection_not_found("strict", collection))?;

    // Cache the PK column position — `if_absent` requires a real PK; without one
    // we'd silently treat column 0 as the key and corrupt rows.
    let pk_idx = if if_absent {
        Some(
            schema
                .columns
                .iter()
                .position(|c| c.primary_key)
                .ok_or_else(|| LiteError::BadRequest {
                    detail: format!(
                        "strict collection '{collection}' has no primary key column; \
                         INSERT … ON CONFLICT DO NOTHING requires one"
                    ),
                })?,
        )
    } else {
        None
    };

    // Every row is checked before any is written, so a statement that fails
    // writes nothing.
    let mut planned: Vec<Vec<Value>> = Vec::with_capacity(rows.len());
    for row_pairs in rows {
        let values = build_row(row_pairs, &schema.columns)?;
        if let Some(idx) = pk_idx {
            let pk_val = &values[idx];
            // ON CONFLICT DO NOTHING skips a key stored or already inserted.
            if planned.iter().any(|p| p[idx] == *pk_val)
                || strict.get(collection, pk_val).await?.is_some()
            {
                continue;
            }
        }
        planned.push(values);
    }
    strict.insert_rows(collection, &planned).await?;
    index_strict_rows(engine, collection, planned.iter().map(Vec::as_slice))?;
    let affected = planned.len() as u64;
    Ok(QueryResult {
        columns: Vec::new(),
        rows: Vec::new(),
        rows_affected: affected,
        command: Some("INSERT".into()),
    })
}

/// Update the rows of a strict collection the WHERE targets: the named
/// primary keys, or else every row the WHERE matches.
pub async fn update_strict<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    collection: &str,
    assignments: &[(String, SqlExpr)],
    filters: &[Filter],
    target_keys: &[SqlValue],
) -> Result<QueryResult, LiteError> {
    update_strict_coordinated(engine, None, collection, assignments, filters, target_keys).await
}

pub(crate) async fn update_strict_coordinated<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    permit: Option<&crate::engine::fts::coordinator::TextMutationPermit>,
    collection: &str,
    assignments: &[(String, SqlExpr)],
    filters: &[Filter],
    target_keys: &[SqlValue],
) -> Result<QueryResult, LiteError> {
    if let Some(permit) = permit {
        return update_strict_admitted(
            engine,
            permit,
            collection,
            assignments,
            filters,
            target_keys,
        )
        .await;
    }
    let guard = engine.fts_state.admit_mutation().await;
    let result = update_strict_admitted(
        engine,
        guard.permit(),
        collection,
        assignments,
        filters,
        target_keys,
    )
    .await;
    guard.finish(result)
}

pub(crate) async fn update_strict_admitted<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    permit: &crate::engine::fts::coordinator::TextMutationPermit,
    collection: &str,
    assignments: &[(String, SqlExpr)],
    filters: &[Filter],
    target_keys: &[SqlValue],
) -> Result<QueryResult, LiteError> {
    let _permit = permit;
    let strict = &engine.strict;
    let schema = strict
        .schema(collection)
        .ok_or_else(|| LiteError::collection_not_found("strict", collection))?;
    let (pk_idx, pk_col) = schema
        .columns
        .iter()
        .enumerate()
        .find(|(_, c)| c.primary_key)
        .ok_or_else(|| LiteError::BadRequest {
            detail: format!("strict collection '{collection}' has no primary key column"),
        })?;

    let updates = convert_typed_assignments(assignments, &schema.columns)?;

    let named: Vec<Value> = target_keys
        .iter()
        .map(|key| parse_pk_value(&sql_value_to_string(key), &pk_col.column_type))
        .collect();
    let pks = strict_targets(engine, collection, filters, named, pk_idx).await?;
    // Each assignment resolves against its row's pre-image, and every
    // post-image is built before the first write: an assignment that fails
    // leaves every row unchanged. A literal-only update resolves once.
    let shared = if has_expr(&updates) {
        None
    } else {
        let resolved = resolve_updates(&updates, &Value::Object(HashMap::new()))?;
        Some(coerce_to_columns(resolved, &schema.columns)?)
    };
    let mut changes: Vec<(Value, HashMap<String, Value>)> = Vec::with_capacity(pks.len());
    for pk in &pks {
        let row_updates = match &shared {
            Some(resolved) => resolved.clone(),
            None => {
                let Some(row) = strict.get(collection, pk).await? else {
                    continue;
                };
                let current = row_object(&schema.columns, &row);
                coerce_to_columns(resolve_updates(&updates, &current)?, &schema.columns)?
            }
        };
        changes.push((pk.clone(), row_updates));
    }
    // One batch for the statement: a unique index it would break refuses
    // every row.
    let affected = strict.update_many(collection, &changes).await?;
    reindex_strict_rows(engine, collection, &pks).await?;
    Ok(QueryResult {
        columns: Vec::new(),
        rows: Vec::new(),
        rows_affected: affected,
        command: Some("UPDATE".into()),
    })
}

/// Delete the rows of a strict collection the WHERE targets: the named
/// primary keys, or else every row the WHERE matches.
pub async fn delete_strict<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    collection: &str,
    filters: &[Filter],
    target_keys: &[SqlValue],
) -> Result<QueryResult, LiteError> {
    delete_strict_coordinated(engine, None, collection, filters, target_keys).await
}

pub(crate) async fn delete_strict_coordinated<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    permit: Option<&crate::engine::fts::coordinator::TextMutationPermit>,
    collection: &str,
    filters: &[Filter],
    target_keys: &[SqlValue],
) -> Result<QueryResult, LiteError> {
    if let Some(permit) = permit {
        return delete_strict_admitted(engine, permit, collection, filters, target_keys).await;
    }
    let guard = engine.fts_state.admit_mutation().await;
    let result =
        delete_strict_admitted(engine, guard.permit(), collection, filters, target_keys).await;
    guard.finish(result)
}

pub(crate) async fn delete_strict_admitted<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    permit: &crate::engine::fts::coordinator::TextMutationPermit,
    collection: &str,
    filters: &[Filter],
    target_keys: &[SqlValue],
) -> Result<QueryResult, LiteError> {
    let _permit = permit;
    let strict = &engine.strict;
    let schema = strict
        .schema(collection)
        .ok_or_else(|| LiteError::collection_not_found("strict", collection))?;
    let (pk_idx, pk_col) = schema
        .columns
        .iter()
        .enumerate()
        .find(|(_, c)| c.primary_key)
        .ok_or_else(|| LiteError::BadRequest {
            detail: format!("strict collection '{collection}' has no primary key column"),
        })?;

    let named: Vec<Value> = target_keys
        .iter()
        .map(|key| parse_pk_value(&sql_value_to_string(key), &pk_col.column_type))
        .collect();
    let pks = strict_targets(engine, collection, filters, named, pk_idx).await?;
    let mut affected: u64 = 0;
    for pk_value in &pks {
        if strict.delete(collection, pk_value).await? {
            affected += 1;
        }
    }
    reindex_strict_rows(engine, collection, &pks).await?;
    Ok(QueryResult {
        columns: Vec::new(),
        rows: Vec::new(),
        rows_affected: affected,
        command: Some("DELETE".into()),
    })
}
