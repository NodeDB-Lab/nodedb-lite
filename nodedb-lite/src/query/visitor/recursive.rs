// SPDX-License-Identifier: Apache-2.0
//! SQL-visitor lowering for recursive SqlPlan variants:
//! RecursiveScan, RecursiveValue.

use nodedb_physical::PhysicalTaskVisitor;
use nodedb_physical::physical_plan::QueryOp;
use nodedb_sql::types::filter::Filter;

use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::query::filter_convert::sql_filters_to_metadata;
use crate::query::physical_visitor::LiteDataPlaneVisitor;
use crate::storage::engine::StorageEngine;

use super::adapter::LiteFut;

fn encode_filters(filters: &[Filter]) -> Result<Vec<u8>, LiteError> {
    if filters.is_empty() {
        return Ok(Vec::new());
    }
    // Only the primitive MetadataFilter part is serialized for the physical visitor.
    // Complex QExpr predicates (from FilterExpr::Expr) are not serializable through
    // this path; they are evaluated at the post-scan layer in apply_scan_post_processing.
    match sql_filters_to_metadata(filters, &[])?.meta {
        None => Ok(Vec::new()),
        Some(mf) => zerompk::to_msgpack_vec(&mf).map_err(|e| LiteError::Serialization {
            detail: format!("encode recursive filters: {e}"),
        }),
    }
}

// ── RecursiveScan ─────────────────────────────────────────────────────────────

/// Lower `SqlPlan::RecursiveScan` to `QueryOp::RecursiveScan`.
#[allow(clippy::too_many_arguments)]
pub(super) fn lower_recursive_scan<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    collection: &str,
    base_filters: &[Filter],
    recursive_filters: &[Filter],
    join_link: Option<&(String, String)>,
    max_iterations: usize,
    distinct: bool,
    limit: usize,
) -> Result<LiteFut<'a>, LiteError> {
    let base_bytes = encode_filters(base_filters)?;
    let rec_bytes = encode_filters(recursive_filters)?;

    let op = QueryOp::RecursiveScan {
        collection: collection.to_string(),
        base_filters: base_bytes,
        recursive_filters: rec_bytes,
        join_link: join_link.cloned(),
        max_iterations,
        distinct,
        limit,
    };

    let mut phys = LiteDataPlaneVisitor { engine };
    let fut = phys.query(&op)?;
    Ok(Box::pin(fut))
}

// ── RecursiveValue ────────────────────────────────────────────────────────────

/// Lower `SqlPlan::RecursiveValue` to `QueryOp::RecursiveValue`.
#[allow(clippy::too_many_arguments)]
pub(super) fn lower_recursive_value<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    cte_name: &str,
    columns: &[String],
    init_exprs: &[String],
    step_exprs: &[String],
    condition: Option<&str>,
    max_depth: usize,
    distinct: bool,
) -> Result<LiteFut<'a>, LiteError> {
    let op = QueryOp::RecursiveValue {
        cte_name: cte_name.to_string(),
        columns: columns.to_vec(),
        init_exprs: init_exprs.to_vec(),
        step_exprs: step_exprs.to_vec(),
        condition: condition.map(|s| s.to_string()),
        max_depth,
        distinct,
    };

    let mut phys = LiteDataPlaneVisitor { engine };
    let fut = phys.query(&op)?;
    Ok(Box::pin(fut))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

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

    #[tokio::test]
    async fn test_recursive_value_counting() {
        let engine = make_engine().await;
        // WITH RECURSIVE c(n) AS (SELECT 1 UNION ALL SELECT n + 1 WHERE n < 5)
        let result = super::lower_recursive_value(
            &engine,
            "counter",
            &["n".to_string()],
            &["1".to_string()],
            &["n + 1".to_string()],
            Some("n < 5"),
            100,
            false,
        );
        assert!(result.is_ok());
        let fut = result.unwrap();
        let qr = fut.await.expect("recursive value should execute");
        assert_eq!(qr.columns, vec!["n".to_string()]);
        assert_eq!(qr.rows.len(), 5);
    }

    #[tokio::test]
    async fn test_recursive_scan_lower() {
        let engine = make_engine().await;
        let result = super::lower_recursive_scan(
            &engine,
            "nodes",
            &[],
            &[],
            Some(&("parent_id".to_string(), "id".to_string())),
            100,
            true,
            500,
        );
        assert!(result.is_ok());
    }
}
