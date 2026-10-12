//! Lite query engine: SQL via nodedb-sql over local engines.
//!
//! Parses SQL with nodedb-sql, then executes against CRDT, strict,
//! and columnar engines directly — no DataFusion dependency.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use nodedb_mem::MemoryGovernor;
use nodedb_sql::types::*;
use nodedb_types::result::QueryResult;
use nodedb_types::value::Value;

use crate::engine::columnar::ColumnarEngine;
use crate::engine::crdt::CrdtEngine;
use crate::engine::fts::FtsState;
use crate::engine::graph::index::CsrIndex;
use crate::engine::htap::HtapBridge;
use crate::engine::sparse_vector::SparseVectorState;
use crate::engine::spatial::SpatialIndexManager;
use crate::engine::strict::StrictEngine;
use crate::engine::vector::VectorState;
use crate::error::LiteError;
use crate::nodedb::{KvLocalState, LockExt};
use crate::sequence::LiteSequenceRegistry;
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
    /// Sparse-vector inverted index state, shared with the owning `NodeDbLite`.
    pub(crate) sparse_state: Arc<SparseVectorState>,
    pub(in crate::query) spatial: Arc<Mutex<SpatialIndexManager>>,
    pub(crate) cancellation: CancellationRegistry,
    /// Per-collection CSR graph indices shared with the owning NodeDbLite.
    pub(crate) csr: Arc<Mutex<HashMap<String, CsrIndex>>>,
    /// Memory budget governor, shared with the owning NodeDbLite.
    pub(crate) governor: Arc<MemoryGovernor>,
    /// Sequence registry backing `nextval` / `currval` / `setval` in a
    /// SELECT list. Definitions are registered through
    /// `LiteQueryEngine::sequences`; counters live in memory.
    pub(crate) sequences: Arc<LiteSequenceRegistry>,
    /// The public KV API's write buffer and read cache. A SQL-path `TRUNCATE`
    /// forgets what they hold for the cleared collection.
    pub(in crate::query) kv_local: Arc<KvLocalState>,
    /// Secondary-index definitions and entries. The CRDT store maintains them
    /// on every row write; flush persists them with the CRDT state.
    pub(crate) indexes: Arc<crate::index::IndexCatalog>,
    /// Durable outbound queue for FTS sync — `None` when sync is disabled.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fts_outbound: Option<Arc<crate::sync::FtsOutbound<S>>>,
    /// Durable outbound queue for spatial sync — `None` when sync is disabled.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) spatial_outbound: Option<Arc<crate::sync::SpatialOutbound<S>>>,
    /// Durable outbound queue for KV write sync — `None` when sync is disabled.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) kv_outbound: Option<Arc<crate::sync::KvOutbound<S>>>,
}

/// Construction fields for [`LiteQueryEngine::new`].
pub struct LiteQueryEngineParams<S: StorageEngine> {
    pub crdt: Arc<Mutex<CrdtEngine>>,
    pub strict: Arc<StrictEngine<S>>,
    pub columnar: Arc<ColumnarEngine<S>>,
    pub htap: Arc<HtapBridge>,
    pub storage: Arc<S>,
    pub timeseries: Arc<Mutex<crate::engine::timeseries::engine::TimeseriesEngine>>,
    pub vector_state: Arc<VectorState<S>>,
    pub array_state: Arc<tokio::sync::Mutex<crate::engine::array::engine::ArrayEngineState>>,
    pub fts_state: Arc<FtsState>,
    pub sparse_state: Arc<SparseVectorState>,
    pub spatial: Arc<Mutex<SpatialIndexManager>>,
    pub csr: Arc<Mutex<HashMap<String, CsrIndex>>>,
    pub governor: Arc<MemoryGovernor>,
    pub kv_local: Arc<KvLocalState>,
}

impl<S: StorageEngine> LiteQueryEngine<S> {
    pub fn new(params: LiteQueryEngineParams<S>) -> Self {
        let indexes = Arc::new(crate::index::IndexCatalog::new());
        params
            .crdt
            .lock_or_recover()
            .set_index_catalog(Arc::clone(&indexes));
        params.strict.set_index_catalog(Arc::clone(&indexes));
        params.kv_local.set_index_catalog(Arc::clone(&indexes));
        Self {
            crdt: params.crdt,
            strict: params.strict,
            columnar: params.columnar,
            htap: params.htap,
            storage: params.storage,
            timeseries: params.timeseries,
            vector_state: params.vector_state,
            array_state: params.array_state,
            fts_state: params.fts_state,
            sparse_state: params.sparse_state,
            spatial: params.spatial,
            cancellation: CancellationRegistry::new(),
            csr: params.csr,
            governor: params.governor,
            sequences: Arc::new(LiteSequenceRegistry::new()),
            kv_local: params.kv_local,
            indexes,
            #[cfg(not(target_arch = "wasm32"))]
            fts_outbound: None,
            #[cfg(not(target_arch = "wasm32"))]
            spatial_outbound: None,
            #[cfg(not(target_arch = "wasm32"))]
            kv_outbound: None,
        }
    }

    /// Wire the durable FTS outbound queue so SQL-path spatial writes are sync-tracked.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn set_fts_outbound(&mut self, q: Arc<crate::sync::FtsOutbound<S>>) {
        self.fts_outbound = Some(q);
    }

    /// Wire the durable spatial outbound queue so SQL-path spatial writes are sync-tracked.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn set_spatial_outbound(&mut self, q: Arc<crate::sync::SpatialOutbound<S>>) {
        self.spatial_outbound = Some(q);
    }

    /// Wire the durable KV outbound queue so SQL-path KV writes are sync-tracked.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn set_kv_outbound(&mut self, q: Arc<crate::sync::KvOutbound<S>>) {
        self.kv_outbound = Some(q);
    }

    /// The sequence registry SELECT-list accessors read and advance.
    pub fn sequences(&self) -> &Arc<LiteSequenceRegistry> {
        &self.sequences
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

        let metas =
            crate::nodedb::collection::ddl::load_persisted_collection_metas(self.storage.as_ref())
                .await
                .unwrap_or_default();
        let array_names: Vec<String> = self
            .array_state
            .lock()
            .await
            .arrays
            .keys()
            .cloned()
            .collect();
        let catalog = LiteCatalog::new(
            Arc::clone(&self.crdt),
            Arc::clone(&self.strict),
            Arc::clone(&self.columnar),
            metas,
        )
        .with_arrays(array_names)
        .with_indexes(Arc::clone(&self.indexes));

        let sql_params: Vec<nodedb_sql::ParamValue> = params.iter().map(value_to_param).collect();

        let plans = if sql_params.is_empty() {
            nodedb_sql::plan_sql(sql, &catalog)
        } else {
            nodedb_sql::plan_sql_with_params(sql, &sql_params, &catalog)
        }
        .map_err(LiteError::from)?;

        // One statement can plan to several units: `TRUNCATE a, b` yields one
        // plan per collection. Every unit runs; the last result is the answer.
        let mut result = QueryResult::empty();
        for plan in &plans {
            result = self.execute_plan(plan).await?;
        }
        Ok(result)
    }

    pub(in crate::query) async fn execute_plan(
        &self,
        plan: &SqlPlan,
    ) -> Result<QueryResult, LiteError> {
        if plan_mutates_source(plan) {
            let guard = self.fts_state.admit_mutation().await;
            let result = self.execute_plan_admitted(plan, Some(guard.permit())).await;
            guard.finish(result)
        } else {
            self.execute_plan_admitted(plan, None).await
        }
    }

    pub(in crate::query) async fn execute_plan_admitted(
        &self,
        plan: &SqlPlan,
        permit: Option<&crate::engine::fts::coordinator::TextMutationPermit>,
    ) -> Result<QueryResult, LiteError> {
        let mut visitor = super::visitor::LiteVisitor {
            engine: self,
            permit,
        };
        let mut result = nodedb_sql::dispatch(&mut visitor, plan)?.await?;
        // The shared plan dispatcher hands the visitor a point get without its
        // SELECT list, so the list is applied here, as the scan path applies
        // it to a scan.
        if let SqlPlan::PointGet { projection, .. } = plan {
            super::visitor::project_scan_result(&mut result, projection, &[], &self.sequences)?;
        }
        Ok(result)
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
            command: None,
        })
    }
}

pub(super) fn sql_value_to_string(v: &SqlValue) -> String {
    match v {
        SqlValue::String(s) => s.clone(),
        SqlValue::Int(i) => i.to_string(),
        SqlValue::Float(f) => f.to_string(),
        SqlValue::Bool(b) => b.to_string(),
        _ => String::new(),
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

/// Build a real governor for tests, with a ceiling covering every engine's limit.
///
/// `nodedb_mem` requires `global_ceiling >= sum(engine_limits)`, so the
/// per-engine limit is `usize::MAX / EngineId::ALL.len()` to avoid overflow.
#[cfg(test)]
pub(crate) fn test_governor() -> Arc<nodedb_mem::MemoryGovernor> {
    let per_engine = usize::MAX / nodedb_mem::EngineId::ALL.len();
    Arc::new(
        nodedb_mem::MemoryGovernor::new(nodedb_mem::GovernorConfig {
            global_ceiling: per_engine * nodedb_mem::EngineId::ALL.len(),
            engine_limits: nodedb_mem::EngineLimits::uniform(per_engine),
        })
        .expect("test governor"),
    )
}

/// Build a `ScopedMemory` handle from a test governor for `engine`.
#[cfg(test)]
pub(crate) fn test_scoped_memory(
    governor: &Arc<nodedb_mem::MemoryGovernor>,
    engine: nodedb_mem::EngineId,
) -> nodedb_mem::ScopedMemory {
    nodedb_mem::ScopedMemory::new(
        Arc::clone(governor),
        nodedb_types::DatabaseId::DEFAULT,
        nodedb_types::TenantId::new(0),
        engine,
    )
}

/// Build an in-memory `LiteQueryEngine` with every engine state wired up.
#[cfg(test)]
pub(crate) async fn test_engine() -> LiteQueryEngine<crate::PagedbStorageMem> {
    use crate::engine::array::engine::ArrayEngineState;
    use crate::engine::spatial::SpatialIndexManager;

    let storage = Arc::new(
        crate::PagedbStorageMem::open_in_memory()
            .await
            .expect("in-memory pagedb"),
    );
    let crdt = Arc::new(Mutex::new(CrdtEngine::new(1).expect("crdt")));
    let governor = test_governor();
    let strict = Arc::new(StrictEngine::new(Arc::clone(&storage)));
    let columnar = Arc::new(ColumnarEngine::new(
        Arc::clone(&storage),
        test_scoped_memory(&governor, nodedb_mem::EngineId::Columnar),
    ));
    let htap = Arc::new(HtapBridge::new());
    let timeseries = Arc::new(Mutex::new(
        crate::engine::timeseries::engine::TimeseriesEngine::new(),
    ));
    let vector_state = Arc::new(VectorState::new(
        Arc::clone(&storage),
        100,
        test_scoped_memory(&governor, nodedb_mem::EngineId::Vector),
    ));
    let array_state = Arc::new(tokio::sync::Mutex::new(
        ArrayEngineState::open(&storage).await.expect("array"),
    ));
    let fts_state = Arc::new(FtsState::new(Arc::clone(&governor)));
    let spatial = Arc::new(Mutex::new(SpatialIndexManager::new(test_scoped_memory(
        &governor,
        nodedb_mem::EngineId::Spatial,
    ))));
    LiteQueryEngine::new(LiteQueryEngineParams {
        crdt,
        strict,
        columnar,
        htap,
        storage,
        timeseries,
        vector_state,
        array_state,
        fts_state,
        sparse_state: Arc::new(SparseVectorState::new()),
        spatial,
        csr: Arc::new(Mutex::new(HashMap::new())),
        governor,
        kv_local: test_kv_local(),
    })
}

/// A KV write buffer and cache for engines built outside `NodeDbLite`.
#[cfg(test)]
pub(crate) fn test_kv_local() -> Arc<KvLocalState> {
    Arc::new(KvLocalState::new(
        std::num::NonZeroUsize::new(64).expect("non-zero"),
    ))
}

fn plan_mutates_source(plan: &SqlPlan) -> bool {
    match plan {
        SqlPlan::Insert(_)
        | SqlPlan::KvInsert(_)
        | SqlPlan::Upsert(_)
        | SqlPlan::InsertSelect { .. }
        | SqlPlan::Update { .. }
        | SqlPlan::UpdateFrom { .. }
        | SqlPlan::Delete { .. }
        | SqlPlan::Truncate { .. }
        | SqlPlan::Merge(_)
        | SqlPlan::TimeseriesIngest(_)
        | SqlPlan::VectorPrimaryInsert(_)
        | SqlPlan::VectorPrimaryDelete(_)
        | SqlPlan::VectorPrimaryUpdate(_)
        | SqlPlan::VectorPrimaryTruncate(_)
        | SqlPlan::CreateArray(_)
        | SqlPlan::DropArray { .. }
        | SqlPlan::AlterArray(_)
        | SqlPlan::InsertArray(_)
        | SqlPlan::DeleteArray(_)
        | SqlPlan::ArrayFlush { .. }
        | SqlPlan::ArrayCompact { .. } => true,
        SqlPlan::Join { left, right, .. }
        | SqlPlan::Intersect { left, right, .. }
        | SqlPlan::Except { left, right, .. } => {
            plan_mutates_source(left) || plan_mutates_source(right)
        }
        SqlPlan::Aggregate { input, .. } | SqlPlan::Subquery { input, .. } => {
            plan_mutates_source(input)
        }
        SqlPlan::Union { inputs, .. } => inputs.iter().any(plan_mutates_source),
        SqlPlan::LateralTopK(plan) => plan_mutates_source(&plan.outer),
        SqlPlan::LateralLoop(plan) => {
            plan_mutates_source(&plan.outer) || plan_mutates_source(&plan.inner)
        }
        SqlPlan::Cte(plan) => {
            plan.definitions
                .iter()
                .any(|(_, plan)| plan_mutates_source(plan))
                || plan_mutates_source(&plan.outer)
        }
        _ => false,
    }
}
