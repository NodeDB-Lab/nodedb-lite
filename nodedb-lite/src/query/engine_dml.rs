//! DML execution methods for `LiteQueryEngine` (INSERT / UPDATE / DELETE /
//! TRUNCATE). Split out of `engine.rs` as a second inherent `impl` block to
//! keep that file under the size limit; behavior is unchanged.

use nodedb_sql::types::{EngineType, SqlValue, WriteRoute};
use nodedb_types::result::QueryResult;

use super::engine::{LiteQueryEngine, sql_value_to_loro, sql_value_to_string};
use crate::error::LiteError;
use crate::storage::engine::StorageEngine;

impl<S: StorageEngine> LiteQueryEngine<S> {
    /// `route` is the planner's `EngineRules` decision and picks the store
    /// family: `ColumnarFamily` writes one columnar batch, `Document` writes
    /// per row. Within `Document`, `engine` picks the strict store or the
    /// CRDT store because Lite keeps those as separate engines.
    pub(super) async fn execute_insert(
        &self,
        collection: &str,
        engine: &EngineType,
        route: WriteRoute,
        rows: &[Vec<(String, SqlValue)>],
        if_absent: bool,
        primary_key: Option<&str>,
    ) -> Result<QueryResult, LiteError> {
        match route {
            WriteRoute::ColumnarFamily => {
                // `written` feeds outbound sync, which is compiled out on wasm32.
                #[cfg_attr(target_arch = "wasm32", allow(unused_variables))]
                let (result, written) =
                    super::columnar_dml::insert_columnar(&self.columnar, collection, rows)?;
                // Durable outbound enqueue must run here (async) — the sync insert
                // path cannot await. Covers the SQL-INSERT route to Origin sync.
                #[cfg(not(target_arch = "wasm32"))]
                crate::sync::reconcile_outbound_enqueue(
                    self.columnar.enqueue_outbound(collection, &written).await,
                    "columnar insert (sql)",
                    collection,
                    "",
                )?;
                return Ok(result);
            }
            WriteRoute::Document => {}
        }
        if *engine == EngineType::DocumentStrict {
            return super::strict_dml::insert_strict(&self.strict, collection, rows, if_absent)
                .await;
        }
        // CRDT / schemaless path.
        let mut crdt = self.crdt.lock().map_err(|_| LiteError::LockPoisoned)?;
        let mut affected = 0;
        for row in rows {
            let id = row
                .iter()
                .find(|(k, _)| match primary_key {
                    Some(pk) => k == pk,
                    None => k == "id",
                })
                .map(|(_, v)| sql_value_to_string(v))
                .unwrap_or_default();
            if crdt.exists(collection, &id) {
                if if_absent {
                    continue;
                }
                return Err(LiteError::Query(format!(
                    "duplicate key value violates unique constraint on '{collection}' (id = '{id}')"
                )));
            }
            let fields: Vec<(&str, loro::LoroValue)> = row
                .iter()
                .map(|(k, v)| (k.as_str(), sql_value_to_loro(v)))
                .collect();
            crdt.upsert(collection, &id, &fields)
                .map_err(|e| LiteError::Query(format!("insert: {e}")))?;
            affected += 1;
        }
        Ok(QueryResult {
            columns: Vec::new(),
            rows: Vec::new(),
            rows_affected: affected,
            command: Some("INSERT".into()),
        })
    }

    pub(super) async fn execute_update(
        &self,
        collection: &str,
        engine: &EngineType,
        assignments: &[(String, nodedb_sql::types::SqlExpr)],
        target_keys: &[SqlValue],
    ) -> Result<QueryResult, LiteError> {
        if *engine == EngineType::DocumentStrict {
            return super::strict_dml::update_strict(
                &self.strict,
                collection,
                assignments,
                target_keys,
            )
            .await;
        }
        // CRDT / schemaless path.
        let mut crdt = self.crdt.lock().map_err(|_| LiteError::LockPoisoned)?;
        let mut affected = 0;
        for key in target_keys {
            let key_str = sql_value_to_string(key);
            let fields: Vec<(&str, loro::LoroValue)> = assignments
                .iter()
                .filter_map(|(field, expr)| {
                    if let nodedb_sql::types::SqlExpr::Literal(val) = expr {
                        Some((field.as_str(), sql_value_to_loro(val)))
                    } else {
                        None
                    }
                })
                .collect();
            crdt.upsert(collection, &key_str, &fields)
                .map_err(|e| LiteError::Query(format!("update: {e}")))?;
            affected += 1;
        }
        Ok(QueryResult {
            columns: Vec::new(),
            rows: Vec::new(),
            rows_affected: affected,
            command: Some("UPDATE".into()),
        })
    }

    pub(super) async fn execute_delete(
        &self,
        collection: &str,
        engine: &EngineType,
        target_keys: &[SqlValue],
    ) -> Result<QueryResult, LiteError> {
        if *engine == EngineType::DocumentStrict {
            return super::strict_dml::delete_strict(&self.strict, collection, target_keys).await;
        }
        // CRDT / schemaless path.
        let mut crdt = self.crdt.lock().map_err(|_| LiteError::LockPoisoned)?;
        let mut affected = 0;
        for key in target_keys {
            let key_str = sql_value_to_string(key);
            crdt.delete(collection, &key_str)
                .map_err(|e| LiteError::Query(format!("delete: {e}")))?;
            affected += 1;
        }
        Ok(QueryResult {
            columns: Vec::new(),
            rows: Vec::new(),
            rows_affected: affected,
            command: Some("DELETE".into()),
        })
    }
}
