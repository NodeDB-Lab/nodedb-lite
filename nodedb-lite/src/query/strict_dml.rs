//! Strict-engine DML dispatch for the Lite query layer.
//!
//! INSERT, UPDATE, and DELETE for strict collections convert SQL values to
//! `nodedb_types::Value` according to the collection schema, then delegate
//! to `StrictEngine` which validates types and encodes as Binary Tuples.

use std::collections::HashMap;
use std::sync::Arc;

use nodedb_sql::types::{SqlExpr, SqlValue};
use nodedb_types::result::QueryResult;
use nodedb_types::value::Value;

use crate::engine::strict::StrictEngine;
use crate::error::LiteError;
use crate::storage::engine::StorageEngine;

use super::coerce::{build_row, coerce_sql_value, sql_value_to_string, sql_value_to_value};
use super::engine::parse_pk_value;

/// Insert rows into a strict collection.
///
/// Each `row` is a list of `(column_name, SqlValue)` pairs. Values are
/// coerced to match the schema column type.
pub async fn insert_strict<S: StorageEngine>(
    strict: &Arc<StrictEngine<S>>,
    collection: &str,
    rows: &[Vec<(String, SqlValue)>],
    if_absent: bool,
) -> Result<QueryResult, LiteError> {
    let schema = strict
        .schema(collection)
        .ok_or_else(|| LiteError::BadRequest {
            detail: format!("strict collection '{collection}' does not exist"),
        })?;

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

    let mut affected: u64 = 0;
    for row_pairs in rows {
        let values = build_row(row_pairs, &schema.columns)?;

        if let Some(idx) = pk_idx {
            let pk_val = &values[idx];
            // Check for duplicate by attempting a point read.
            if strict.get(collection, pk_val).await?.is_some() {
                continue;
            }
        }

        strict
            .insert(collection, &values)
            .await
            .map_err(|e| match e {
                LiteError::BadRequest { detail }
                    if detail.contains("duplicate primary key") && if_absent =>
                {
                    // Race: another insert won between our check and insert.
                    // if_absent semantics: skip.
                    LiteError::BadRequest { detail }
                }
                other => other,
            })?;
        affected += 1;
    }
    Ok(QueryResult {
        columns: Vec::new(),
        rows: Vec::new(),
        rows_affected: affected,
    })
}

/// Update rows in a strict collection by primary key.
pub async fn update_strict<S: StorageEngine>(
    strict: &Arc<StrictEngine<S>>,
    collection: &str,
    assignments: &[(String, SqlExpr)],
    target_keys: &[SqlValue],
) -> Result<QueryResult, LiteError> {
    let schema = strict
        .schema(collection)
        .ok_or_else(|| LiteError::BadRequest {
            detail: format!("strict collection '{collection}' does not exist"),
        })?;
    let pk_col = schema
        .columns
        .iter()
        .find(|c| c.primary_key)
        .ok_or_else(|| LiteError::BadRequest {
            detail: format!("strict collection '{collection}' has no primary key column"),
        })?;

    // Convert assignments to a HashMap<col_name, Value>.
    let updates: HashMap<String, Value> = assignments
        .iter()
        .filter_map(|(field, expr)| {
            if let SqlExpr::Literal(val) = expr {
                let col = schema.columns.iter().find(|c| c.name == *field);
                let typed = col
                    .map(|c| coerce_sql_value(val, &c.column_type))
                    .unwrap_or_else(|| sql_value_to_value(val));
                Some((field.clone(), typed))
            } else {
                None
            }
        })
        .collect();

    let mut affected: u64 = 0;
    for key in target_keys {
        let key_str = sql_value_to_string(key);
        let pk_value = parse_pk_value(&key_str, &pk_col.column_type);
        if strict.update(collection, &pk_value, &updates).await? {
            affected += 1;
        }
    }
    Ok(QueryResult {
        columns: Vec::new(),
        rows: Vec::new(),
        rows_affected: affected,
    })
}

/// Delete rows from a strict collection by primary key.
pub async fn delete_strict<S: StorageEngine>(
    strict: &Arc<StrictEngine<S>>,
    collection: &str,
    target_keys: &[SqlValue],
) -> Result<QueryResult, LiteError> {
    let schema = strict
        .schema(collection)
        .ok_or_else(|| LiteError::BadRequest {
            detail: format!("strict collection '{collection}' does not exist"),
        })?;
    let pk_col = schema
        .columns
        .iter()
        .find(|c| c.primary_key)
        .ok_or_else(|| LiteError::BadRequest {
            detail: format!("strict collection '{collection}' has no primary key column"),
        })?;

    let mut affected: u64 = 0;
    for key in target_keys {
        let key_str = sql_value_to_string(key);
        let pk_value = parse_pk_value(&key_str, &pk_col.column_type);
        if strict.delete(collection, &pk_value).await? {
            affected += 1;
        }
    }
    Ok(QueryResult {
        columns: Vec::new(),
        rows: Vec::new(),
        rows_affected: affected,
    })
}
