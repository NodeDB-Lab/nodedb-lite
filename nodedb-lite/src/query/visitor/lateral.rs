// SPDX-License-Identifier: Apache-2.0
//! SQL-visitor lowering for lateral SqlPlan variants:
//! LateralTopK, LateralLoop.

use nodedb_physical::physical_plan::query::JoinProjection;
use nodedb_sql::types::SqlPlan;
use nodedb_sql::types::filter::Filter;
use nodedb_sql::types::query::{Projection, SortKey};
use nodedb_sql::types_expr::SqlExpr;

use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::query::filter_convert::sql_filters_to_metadata;
use crate::storage::engine::StorageEngine;

use super::adapter::LiteFut;

fn encode_filters(filters: &[Filter]) -> Result<Vec<u8>, LiteError> {
    if filters.is_empty() {
        return Ok(Vec::new());
    }
    // Complex QExpr predicates are evaluated post-scan; only primitive conditions
    // are pushed to the physical visitor via serialized MetadataFilter.
    match sql_filters_to_metadata(filters, &[])?.meta {
        None => Ok(Vec::new()),
        Some(mf) => zerompk::to_msgpack_vec(&mf).map_err(|e| LiteError::Serialization {
            detail: format!("encode lateral filters: {e}"),
        }),
    }
}

fn sort_key_to_pair(k: &SortKey) -> (String, bool) {
    let name = match &k.expr {
        SqlExpr::Column { name, .. } => name.clone(),
        other => format!("{other:?}"),
    };
    (name, k.ascending)
}

fn build_join_projections(projection: &[Projection]) -> Vec<JoinProjection> {
    projection
        .iter()
        .filter_map(|p| match p {
            Projection::Column(name) => Some(JoinProjection {
                source: name.clone(),
                output: name.clone(),
            }),
            Projection::Computed { alias, .. } => Some(JoinProjection {
                source: alias.clone(),
                output: alias.clone(),
            }),
            _ => None,
        })
        .collect()
}

// ── LateralTopK ───────────────────────────────────────────────────────────────

/// Lower `SqlPlan::LateralTopK` to `QueryOp::LateralTopK`.
///
/// The outer plan is itself a `SqlPlan`. On Lite, we lower it to a
/// `PhysicalPlan` by re-visiting it through the `LiteDataPlaneVisitor`
/// and embed the result in `QueryOp::LateralTopK::outer_plan`.
///
/// The `execute_lateral_top_k` helper in `query_ops/lateral_top_k.rs`
/// materialises the outer rows by calling `engine.execute_physical_plan(outer_plan)`,
/// which dispatches to `LiteDataPlaneVisitor`. To close the loop, we produce
/// the physical outer plan by visiting the SQL outer plan here.
#[allow(clippy::too_many_arguments)]
pub(super) fn lower_lateral_top_k<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    outer: &SqlPlan,
    outer_alias: Option<&str>,
    inner_collection: &str,
    inner_filters: &[Filter],
    inner_order_by: &[SortKey],
    inner_limit: usize,
    correlation_keys: &[(String, String)],
    lateral_alias: &str,
    projection: &[Projection],
    left_join: bool,
) -> Result<LiteFut<'a>, LiteError> {
    let outer_sql = outer.clone();
    let outer_alias_str = outer_alias.unwrap_or("outer").to_string();
    let inner_col = inner_collection.to_string();
    let inner_filt_bytes = encode_filters(inner_filters)?;
    let inner_ob: Vec<(String, bool)> = inner_order_by.iter().map(sort_key_to_pair).collect();
    let inner_lim = inner_limit;
    let corr_keys = correlation_keys.to_vec();
    let lat_alias = lateral_alias.to_string();
    let proj = build_join_projections(projection);
    let lj = left_join;

    Ok(Box::pin(async move {
        use crate::query::query_ops::lateral_top_k::execute_lateral_top_k_sql;
        execute_lateral_top_k_sql(
            engine,
            &outer_sql,
            &outer_alias_str,
            &inner_col,
            &inner_filt_bytes,
            &inner_ob,
            inner_lim,
            &corr_keys,
            &lat_alias,
            &proj,
            lj,
        )
        .await
    }))
}

// ── LateralLoop ───────────────────────────────────────────────────────────────

/// Lower `SqlPlan::LateralLoop` to `QueryOp::LateralLoop`.
#[allow(clippy::too_many_arguments)]
pub(super) fn lower_lateral_loop<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    outer: &SqlPlan,
    outer_alias: Option<&str>,
    inner: &SqlPlan,
    correlation_predicates: &[(String, String)],
    lateral_alias: &str,
    projection: &[Projection],
    outer_row_cap: usize,
    left_join: bool,
) -> Result<LiteFut<'a>, LiteError> {
    let outer_sql = outer.clone();
    let outer_alias_str = outer_alias.unwrap_or("outer").to_string();
    let inner_sql = inner.clone();
    let corr_preds = correlation_predicates.to_vec();
    let lat_alias = lateral_alias.to_string();
    let proj = build_join_projections(projection);
    let cap = outer_row_cap;
    let lj = left_join;

    Ok(Box::pin(async move {
        use crate::query::query_ops::lateral_loop::execute_lateral_loop_sql;
        execute_lateral_loop_sql(
            engine,
            &outer_sql,
            &outer_alias_str,
            &inner_sql,
            &corr_preds,
            &lat_alias,
            &proj,
            lj,
            cap,
        )
        .await
    }))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use nodedb_sql::types::SqlPlan;
    use nodedb_sql::types::query::EngineType;

    use crate::PagedbStorageMem;
    use crate::query::engine::LiteQueryEngine;

    async fn make_engine() -> LiteQueryEngine<PagedbStorageMem> {
        use std::sync::Mutex;
        let storage = Arc::new(
            PagedbStorageMem::open_in_memory()
                .await
                .expect("in-memory pagedb"),
        );
        let crdt = Arc::new(Mutex::new(
            crate::engine::crdt::CrdtEngine::new(1).expect("crdt"),
        ));
        let strict = Arc::new(crate::engine::strict::StrictEngine::new(Arc::clone(
            &storage,
        )));
        let columnar = Arc::new(crate::engine::columnar::ColumnarEngine::new(Arc::clone(
            &storage,
        )));
        let htap = Arc::new(crate::engine::htap::HtapBridge::new());
        let timeseries = Arc::new(Mutex::new(
            crate::engine::timeseries::engine::TimeseriesEngine::new(),
        ));
        let vector_state = Arc::new(crate::engine::vector::VectorState::new(
            Arc::clone(&storage),
            100,
        ));
        let array_state = Arc::new(tokio::sync::Mutex::new(
            crate::engine::array::engine::ArrayEngineState::open(&storage)
                .await
                .expect("array"),
        ));
        let fts_state = Arc::new(crate::engine::fts::FtsState::new());
        let spatial = Arc::new(Mutex::new(
            crate::engine::spatial::SpatialIndexManager::new(),
        ));
        LiteQueryEngine::new(
            crdt,
            strict,
            columnar,
            htap,
            storage,
            timeseries,
            vector_state,
            array_state,
            fts_state,
            Arc::new(crate::engine::sparse_vector::SparseVectorState::new()),
            spatial,
            Arc::new(Mutex::new(std::collections::HashMap::new())),
        )
    }

    fn scan_plan(collection: &str) -> SqlPlan {
        SqlPlan::Scan {
            collection: collection.to_string(),
            alias: None,
            engine: EngineType::DocumentSchemaless,
            filters: vec![],
            projection: vec![],
            sort_keys: vec![],
            limit: None,
            offset: 0,
            distinct: false,
            window_functions: vec![],
            temporal: nodedb_sql::temporal::TemporalScope::default(),
        }
    }

    #[tokio::test]
    async fn test_lateral_top_k_lower() {
        let engine = make_engine().await;
        let outer = scan_plan("users");
        let result = super::lower_lateral_top_k(
            &engine,
            &outer,
            Some("u"),
            "orders",
            &[],
            &[],
            3,
            &[("id".to_string(), "user_id".to_string())],
            "o",
            &[],
            false,
        );
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_lateral_loop_lower() {
        let engine = make_engine().await;
        let outer = scan_plan("departments");
        let inner = scan_plan("employees");
        let result = super::lower_lateral_loop(
            &engine,
            &outer,
            Some("d"),
            &inner,
            &[("id".to_string(), "dept_id".to_string())],
            "e",
            &[],
            1000,
            false,
        );
        assert!(result.is_ok());
    }
}
