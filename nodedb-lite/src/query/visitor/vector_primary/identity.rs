// SPDX-License-Identifier: Apache-2.0
//! SQL-visitor lowering for the vector-primary SqlPlan variants:
//! `VectorPrimaryInsert`, `VectorPrimaryDelete`, `VectorPrimaryUpdate`,
//! `VectorPrimaryTruncate`.
//!
//! Lite binds no surrogate to a primary key, so a row's identity is the
//! text of its declared key: the insert carries it as `pk_bytes`, and a
//! point `DELETE` / `UPDATE` targets it through a predicate on the key
//! column evaluated against the stored payload row.

use crate::error::LiteError;
use crate::query::engine::sql_value_to_string;
use crate::query::filter_convert::sql_value_to_value;
use nodedb_sql::types::plan::VectorPrimaryRow;
use nodedb_sql::types_expr::SqlValue;
use nodedb_types::result::QueryResult;
use nodedb_types::value::Value;
use std::collections::HashMap;

/// The key column a vector-primary collection is keyed by; `id` when the
/// statement declares none.
pub(super) fn key_column(primary_key: Option<&str>) -> &str {
    primary_key.unwrap_or("id")
}

/// Encode payload fields (non-vector columns) as MessagePack bytes.
fn encode_payload(payload_fields: &HashMap<String, Value>) -> Result<Vec<u8>, LiteError> {
    if payload_fields.is_empty() {
        return Ok(Vec::new());
    }
    zerompk::to_msgpack_vec(payload_fields).map_err(|e| LiteError::Serialization {
        detail: format!("encode vector primary payload: {e}"),
    })
}

/// Encode the key and payload, minting an absent or null key.
pub(super) fn row_identity(
    row: &VectorPrimaryRow,
    key_column: &str,
) -> Result<(Vec<u8>, Vec<u8>), LiteError> {
    let mut fields: HashMap<String, Value> = row
        .payload_fields
        .iter()
        .map(|(key, value)| Ok((key.clone(), sql_value_to_value(value)?)))
        .collect::<Result<_, LiteError>>()?;
    let doc_id = match row.payload_fields.get(key_column) {
        Some(v) if !matches!(v, SqlValue::Null) => sql_value_to_string(v),
        _ => {
            let minted = nodedb_types::id_gen::uuid_v7();
            fields.insert(key_column.to_string(), Value::String(minted.clone()));
            minted
        }
    };
    Ok((doc_id.into_bytes(), encode_payload(&fields)?))
}

pub(super) fn rows_affected(n: u64, command: &'static str) -> QueryResult {
    QueryResult {
        columns: vec!["rows_affected".to_string()],
        rows: vec![vec![Value::Integer(n as i64)]],
        rows_affected: n,
        command: Some(command.into()),
    }
}

#[cfg(test)]
pub(super) mod support {
    use std::collections::HashMap;

    use nodedb_sql::types::filter::Filter;
    use nodedb_sql::types::plan::{VectorPrimaryInsertIntent, VectorPrimaryRow};
    use nodedb_sql::types_expr::{SqlExpr, SqlValue};
    use nodedb_types::{VectorQuantization, VectorStorageDtype};

    use super::super::insert::lower_vector_primary_insert;
    use super::super::mutation::{lower_vector_primary_delete, lower_vector_primary_update};
    use super::*;
    use crate::PagedbStorageMem;
    use crate::nodedb::LockExt;
    use crate::query::engine::LiteQueryEngine;
    use nodedb_sql::{
        VectorPrimaryDeleteVisitArgs, VectorPrimaryInsertVisitArgs, VectorPrimaryUpdateVisitArgs,
    };

    pub(crate) const COLLECTION: &str = "embeddings";
    pub(crate) const FIELD: &str = "vec";

    pub(crate) fn row(id: &str, vector: Vec<f32>, extra: &[(&str, SqlValue)]) -> VectorPrimaryRow {
        let mut payload_fields = HashMap::new();
        payload_fields.insert("id".to_string(), SqlValue::String(id.to_string()));
        for (k, v) in extra {
            payload_fields.insert((*k).to_string(), v.clone());
        }
        VectorPrimaryRow {
            vector,
            payload_fields,
        }
    }

    pub(crate) async fn insert(
        engine: &LiteQueryEngine<PagedbStorageMem>,
        rows: &[VectorPrimaryRow],
        intent: VectorPrimaryInsertIntent,
        on_conflict_updates: &[(String, SqlExpr)],
    ) -> Result<QueryResult, LiteError> {
        let guard = engine.fts_state.admit_mutation().await;
        let result = lower_vector_primary_insert(
            engine,
            guard.permit(),
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
        .await;
        guard.finish(result)
    }

    pub(crate) async fn delete(
        engine: &LiteQueryEngine<PagedbStorageMem>,
        filters: &[Filter],
        target_keys: &[SqlValue],
    ) -> Result<QueryResult, LiteError> {
        let guard = engine.fts_state.admit_mutation().await;
        let result = lower_vector_primary_delete(
            engine,
            guard.permit(),
            VectorPrimaryDeleteVisitArgs {
                collection: COLLECTION,
                field: FIELD,
                filters,
                target_keys,
                primary_key: Some("id"),
            },
        )?
        .await;
        guard.finish(result)
    }

    pub(crate) async fn update(
        engine: &LiteQueryEngine<PagedbStorageMem>,
        new_vector: Option<&[f32]>,
        assignments: &[(String, SqlExpr)],
        target_keys: &[SqlValue],
    ) -> Result<QueryResult, LiteError> {
        let guard = engine.fts_state.admit_mutation().await;
        let result = lower_vector_primary_update(
            engine,
            guard.permit(),
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
        .await;
        guard.finish(result)
    }

    pub(crate) fn live_nodes(engine: &LiteQueryEngine<PagedbStorageMem>) -> usize {
        let indices = engine.vector_state.hnsw_indices.lock_or_recover();
        indices
            .get(&format!("{COLLECTION}:{FIELD}"))
            .map(|idx| idx.live_count())
            .unwrap_or(0)
    }

    pub(crate) fn stored_field(
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

    pub(crate) fn key(id: &str) -> SqlValue {
        SqlValue::String(id.to_string())
    }
}
