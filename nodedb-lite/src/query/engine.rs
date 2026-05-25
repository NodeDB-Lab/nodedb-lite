//! Lite query engine: SQL via nodedb-sql over local engines.
//!
//! Parses SQL with nodedb-sql, then executes against CRDT, strict,
//! and columnar engines directly — no DataFusion dependency.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use nodedb_sql::types::*;
use nodedb_types::result::QueryResult;
use nodedb_types::value::Value;

use crate::engine::columnar::ColumnarEngine;
use crate::engine::crdt::CrdtEngine;
use crate::engine::fts::FtsState;
use crate::engine::graph::index::CsrIndex;
use crate::engine::htap::HtapBridge;
use crate::engine::spatial::SpatialIndexManager;
use crate::engine::strict::StrictEngine;
use crate::engine::vector::VectorState;
use crate::error::LiteError;
use crate::storage::engine::StorageEngine;

use super::catalog::LiteCatalog;
use super::meta_ops::CancellationRegistry;

/// Lite-side query engine.
pub struct LiteQueryEngine<S: StorageEngine> {
    pub(in crate::query) crdt: Arc<Mutex<CrdtEngine>>,
    pub(in crate::query) strict: Arc<StrictEngine<S>>,
    pub(in crate::query) columnar: Arc<ColumnarEngine<S>>,
    pub(in crate::query) htap: Arc<HtapBridge>,
    pub(in crate::query) storage: Arc<S>,
    pub(in crate::query) timeseries:
        Arc<Mutex<crate::engine::timeseries::engine::TimeseriesEngine>>,
    pub(crate) vector_state: Arc<VectorState<S>>,
    pub(crate) array_state: Arc<tokio::sync::Mutex<crate::engine::array::engine::ArrayEngineState>>,
    pub(crate) fts_state: Arc<FtsState>,
    pub(in crate::query) spatial: Arc<Mutex<SpatialIndexManager>>,
    pub(crate) cancellation: CancellationRegistry,
    /// Per-collection CSR graph indices shared with the owning NodeDbLite.
    pub(crate) csr: Arc<Mutex<HashMap<String, CsrIndex>>>,
}

impl<S: StorageEngine> LiteQueryEngine<S> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        crdt: Arc<Mutex<CrdtEngine>>,
        strict: Arc<StrictEngine<S>>,
        columnar: Arc<ColumnarEngine<S>>,
        htap: Arc<HtapBridge>,
        storage: Arc<S>,
        timeseries: Arc<Mutex<crate::engine::timeseries::engine::TimeseriesEngine>>,
        vector_state: Arc<VectorState<S>>,
        array_state: Arc<tokio::sync::Mutex<crate::engine::array::engine::ArrayEngineState>>,
        fts_state: Arc<FtsState>,
        spatial: Arc<Mutex<SpatialIndexManager>>,
        csr: Arc<Mutex<HashMap<String, CsrIndex>>>,
    ) -> Self {
        Self {
            crdt,
            strict,
            columnar,
            htap,
            storage,
            timeseries,
            vector_state,
            array_state,
            fts_state,
            spatial,
            cancellation: CancellationRegistry::new(),
            csr,
        }
    }

    /// No-op — collections are auto-discovered via catalog.
    pub fn register_collection(&self, _name: &str) {}
    /// No-op — collections are auto-discovered via catalog.
    pub fn register_strict_collection(&self, _name: &str) {}
    /// No-op — collections are auto-discovered via catalog.
    pub fn register_all_collections(&self) {}
    /// No-op — collections are auto-discovered via catalog.
    pub fn register_columnar_collection(&self, _name: &str) {}

    /// Execute a SQL query and return results.
    pub async fn execute_sql(&self, sql: &str) -> Result<QueryResult, LiteError> {
        self.execute_sql_with_params(sql, &[]).await
    }

    /// Execute a SQL query with bound `$N` parameters and return results.
    ///
    /// Each `Value` in `params` is bound to the corresponding `$1`, `$2`, …
    /// placeholder in `sql` at the AST level before planning. Supported
    /// `Value` variants: `Null`, `Bool`, `Integer`, `Float`, `String`, `Uuid`.
    /// Other variants are treated as `Null`.
    pub async fn execute_sql_with_params(
        &self,
        sql: &str,
        params: &[Value],
    ) -> Result<QueryResult, LiteError> {
        if let Some(result) = self.try_handle_ddl(sql).await {
            return result;
        }

        let catalog = LiteCatalog::new(
            Arc::clone(&self.crdt),
            Arc::clone(&self.strict),
            Arc::clone(&self.columnar),
        );

        let sql_params: Vec<nodedb_sql::ParamValue> = params.iter().map(value_to_param).collect();

        let plans = if sql_params.is_empty() {
            nodedb_sql::plan_sql(sql, &catalog)
        } else {
            nodedb_sql::plan_sql_with_params(sql, &sql_params, &catalog)
        }
        .map_err(|e| LiteError::Query(format!("SQL plan: {e}")))?;

        if plans.is_empty() {
            return Ok(QueryResult::empty());
        }

        self.execute_plan(&plans[0]).await
    }

    pub(in crate::query) async fn execute_plan(
        &self,
        plan: &SqlPlan,
    ) -> Result<QueryResult, LiteError> {
        let mut visitor = super::visitor::LiteVisitor { engine: self };
        nodedb_sql::dispatch(&mut visitor, plan)?.await
    }

    pub(super) async fn execute_constant_result(
        &self,
        columns: &[String],
        values: &[nodedb_sql::types::SqlValue],
    ) -> Result<QueryResult, LiteError> {
        let row = values.iter().map(sql_value_to_value).collect();
        Ok(QueryResult {
            columns: columns.to_vec(),
            rows: vec![row],
            rows_affected: 0,
        })
    }

    pub(super) async fn execute_scan(
        &self,
        collection: &str,
        engine: &EngineType,
    ) -> Result<QueryResult, LiteError> {
        match engine {
            EngineType::DocumentSchemaless => {
                let crdt = self.crdt.lock().map_err(|_| LiteError::LockPoisoned)?;
                let ids = crdt.list_ids(collection);
                let mut rows = Vec::with_capacity(ids.len());
                for id in &ids {
                    if let Some(val) = crdt.read(collection, id) {
                        let json = loro_value_to_json(&val);
                        let doc_str = sonic_rs::to_string(&json).unwrap_or_default();
                        rows.push(vec![Value::String(id.clone()), Value::String(doc_str)]);
                    }
                }
                Ok(QueryResult {
                    columns: vec!["id".into(), "document".into()],
                    rows,
                    rows_affected: 0,
                })
            }
            EngineType::DocumentStrict => {
                let schema =
                    self.strict
                        .schema(collection)
                        .ok_or_else(|| LiteError::BadRequest {
                            detail: format!("strict collection '{collection}' does not exist"),
                        })?;
                let columns: Vec<String> = schema.columns.iter().map(|c| c.name.clone()).collect();
                let rows = self.strict.list_rows(collection).await?;
                Ok(QueryResult {
                    columns,
                    rows,
                    rows_affected: 0,
                })
            }
            EngineType::Columnar => {
                let schema =
                    self.columnar
                        .schema(collection)
                        .ok_or_else(|| LiteError::BadRequest {
                            detail: format!("columnar collection '{collection}' does not exist"),
                        })?;
                let columns: Vec<String> = schema.columns.iter().map(|c| c.name.clone()).collect();
                let rows = self.columnar.list_rows(collection).await?;
                Ok(QueryResult {
                    columns,
                    rows,
                    rows_affected: 0,
                })
            }
            _ => Ok(QueryResult::empty()),
        }
    }

    pub(super) async fn execute_point_get(
        &self,
        collection: &str,
        engine: &EngineType,
        key: &SqlValue,
    ) -> Result<QueryResult, LiteError> {
        let key_str = sql_value_to_string(key);
        match engine {
            EngineType::DocumentSchemaless => {
                let crdt = self.crdt.lock().map_err(|_| LiteError::LockPoisoned)?;
                match crdt.read(collection, &key_str) {
                    Some(val) => {
                        let json = loro_value_to_json(&val);
                        let doc_str = sonic_rs::to_string(&json).unwrap_or_default();
                        Ok(QueryResult {
                            columns: vec!["id".into(), "document".into()],
                            rows: vec![vec![Value::String(key_str), Value::String(doc_str)]],
                            rows_affected: 0,
                        })
                    }
                    None => Ok(QueryResult::empty()),
                }
            }
            EngineType::DocumentStrict => {
                let schema =
                    self.strict
                        .schema(collection)
                        .ok_or_else(|| LiteError::BadRequest {
                            detail: format!("strict collection '{collection}' does not exist"),
                        })?;
                let columns: Vec<String> = schema.columns.iter().map(|c| c.name.clone()).collect();
                // The PK column type determines how to parse the key string.
                let pk_col = schema
                    .columns
                    .iter()
                    .find(|c| c.primary_key)
                    .ok_or_else(|| LiteError::BadRequest {
                        detail: format!(
                            "strict collection '{collection}' has no primary key column"
                        ),
                    })?;
                let pk_value = parse_pk_value(&key_str, &pk_col.column_type);
                match self.strict.get(collection, &pk_value).await? {
                    Some(values) => Ok(QueryResult {
                        columns,
                        rows: vec![values],
                        rows_affected: 0,
                    }),
                    None => Ok(QueryResult {
                        columns,
                        rows: Vec::new(),
                        rows_affected: 0,
                    }),
                }
            }
            _ => Ok(QueryResult::empty()),
        }
    }

    pub(super) async fn execute_insert(
        &self,
        collection: &str,
        engine: &EngineType,
        rows: &[Vec<(String, SqlValue)>],
        if_absent: bool,
    ) -> Result<QueryResult, LiteError> {
        if *engine == EngineType::DocumentStrict {
            return super::strict_dml::insert_strict(&self.strict, collection, rows, if_absent)
                .await;
        }
        if *engine == EngineType::Columnar {
            return super::columnar_dml::insert_columnar(&self.columnar, collection, rows);
        }
        // CRDT / schemaless path.
        let mut crdt = self.crdt.lock().map_err(|_| LiteError::LockPoisoned)?;
        let mut affected = 0;
        for row in rows {
            let id = row
                .iter()
                .find(|(k, _)| k == "id")
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
        })
    }

    pub(super) async fn execute_truncate(
        &self,
        collection: &str,
    ) -> Result<QueryResult, LiteError> {
        self.crdt
            .lock()
            .map_err(|_| LiteError::LockPoisoned)?
            .clear_collection(collection)
            .map_err(|e| LiteError::Query(format!("truncate: {e}")))?;
        Ok(QueryResult::empty())
    }
}

fn sql_value_to_string(v: &SqlValue) -> String {
    match v {
        SqlValue::String(s) => s.clone(),
        SqlValue::Int(i) => i.to_string(),
        SqlValue::Float(f) => f.to_string(),
        SqlValue::Bool(b) => b.to_string(),
        _ => String::new(),
    }
}

fn sql_value_to_loro(v: &SqlValue) -> loro::LoroValue {
    match v {
        SqlValue::Int(i) => loro::LoroValue::I64(*i),
        SqlValue::Float(f) => loro::LoroValue::Double(*f),
        SqlValue::String(s) => loro::LoroValue::String(s.clone().into()),
        SqlValue::Bool(b) => loro::LoroValue::Bool(*b),
        SqlValue::Null => loro::LoroValue::Null,
        _ => loro::LoroValue::Null,
    }
}

pub(super) fn sql_value_to_value(v: &nodedb_sql::types::SqlValue) -> Value {
    match v {
        nodedb_sql::types::SqlValue::Int(i) => Value::Integer(*i),
        nodedb_sql::types::SqlValue::Float(f) => Value::Float(*f),
        nodedb_sql::types::SqlValue::String(s) => Value::String(s.clone()),
        nodedb_sql::types::SqlValue::Bool(b) => Value::Bool(*b),
        nodedb_sql::types::SqlValue::Null => Value::Null,
        _ => Value::Null,
    }
}

/// Convert a primary-key string from a SQL literal into the appropriate `Value`
/// variant based on the column's declared type.
pub(super) fn parse_pk_value(
    key_str: &str,
    col_type: &nodedb_types::columnar::ColumnType,
) -> Value {
    use nodedb_types::columnar::ColumnType;
    match col_type {
        ColumnType::Int64 => key_str
            .parse::<i64>()
            .map(Value::Integer)
            .unwrap_or_else(|_| Value::String(key_str.to_string())),
        ColumnType::Uuid => Value::Uuid(key_str.to_string()),
        _ => Value::String(key_str.to_string()),
    }
}

/// Convert a `nodedb_types::Value` to the `nodedb_sql::ParamValue` type used
/// for AST-level parameter binding in `plan_sql_with_params`.
fn value_to_param(v: &Value) -> nodedb_sql::ParamValue {
    match v {
        Value::Null => nodedb_sql::ParamValue::Null,
        Value::Bool(b) => nodedb_sql::ParamValue::Bool(*b),
        Value::Integer(n) => nodedb_sql::ParamValue::Int64(*n),
        Value::Float(f) => nodedb_sql::ParamValue::Float64(*f),
        Value::String(s) => nodedb_sql::ParamValue::Text(s.clone()),
        Value::Uuid(s) => nodedb_sql::ParamValue::Text(s.clone()),
        _ => nodedb_sql::ParamValue::Null,
    }
}

fn loro_value_to_json(v: &loro::LoroValue) -> serde_json::Value {
    match v {
        loro::LoroValue::Null => serde_json::Value::Null,
        loro::LoroValue::Bool(b) => serde_json::Value::Bool(*b),
        loro::LoroValue::I64(n) => serde_json::json!(*n),
        loro::LoroValue::Double(f) => serde_json::json!(*f),
        loro::LoroValue::String(s) => serde_json::Value::String(s.to_string()),
        loro::LoroValue::Map(m) => {
            let mut obj = serde_json::Map::new();
            for (k, val) in m.iter() {
                obj.insert(k.to_string(), loro_value_to_json(val));
            }
            serde_json::Value::Object(obj)
        }
        loro::LoroValue::List(arr) => {
            serde_json::Value::Array(arr.iter().map(loro_value_to_json).collect())
        }
        _ => serde_json::Value::Null,
    }
}
